// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/queryport"
	"github.com/couchbaselabs/goprotobuf/proto"
	"math/rand"
	"sync"
)

var (
	ErrUnsupportedRequest = errors.New("Unsupported query request")
	ErrIndexNotFound      = errors.New("Index not found")
	ErrNotMyIndex         = errors.New("Index is not held by this node")
)

// TODO: wednesday
// 1. fill in makeresponsemsg
// 2. add dummy responses
// 3. test without index
// 4. test with index
// 5. add snapshotreader and stats resp
// 6. test with actual data

type statsResponse struct {
	min, max Key
	distinct uint64
	count    uint64
}

type scanType string

const (
	STATS   scanType = "stats"
	SCAN    scanType = "scan"
	SCANALL scanType = "scanall"
)

type scanParams struct {
	scanType  scanType
	indexName string
	bucket    string
	low       Key
	high      Key
	keys      []Key
	partnKey  []byte
	incl      Inclusion
	limit     int64
	pageSize  int64
}

type scanDescriptor struct {
	scanId int64
	p      *scanParams
	stopch StopChannel

	respch chan interface{}
}

//TODO
//For any query request, check if the replica is available. And use replica in case
//its more recent or serving less queries.

//ScanCoordinator handles scanning for an incoming index query. It will figure out
//the partitions/slices required to be scanned as per query parameters.

type ScanCoordinator interface {
}

type scanCoordinator struct {
	supvCmdch MsgChannel //supervisor sends commands on this channel
	supvMsgch MsgChannel //channel to send any async message to supervisor
	serv      *queryport.Server
	logPrefix string

	mu            sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel) (
	ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		logPrefix: "ScanCoordinator",
	}

	s.serv, err = queryport.NewServer(QUERY_PORT_ADDR, s.requestHandler,
		common.SystemConfig)

	if err != nil {
		errMsg := &MsgError{err: Error{code: ERROR_SCAN_COORD_QUERYPORT_FAIL,
			severity: FATAL,
			category: SCAN_COORD,
			cause:    err,
		},
		}
		return nil, errMsg
	}

	// main loop
	go s.run()

	return s, &MsgSuccess{}

}

func (s *scanCoordinator) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					common.Infof("ScanCoordinator: Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		}
	}
}

func (s *scanCoordinator) handleSupvervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	default:
		common.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

// Parse scan params from queryport request
func (s *scanCoordinator) parseScanParams(
	req interface{}) (p *scanParams, err error) {

	p = new(scanParams)
	p.partnKey = []byte("default")

	fillRanges := func(low, high []byte, keys [][]byte) error {
		var err error
		var key Key

		// range
		if p.low, err = NewKey([][]byte{low}, []byte{}); err != nil {
			return err
		}

		if p.high, err = NewKey([][]byte{high}, []byte{}); err != nil {
			return err
		}

		// point query for keys
		for _, k := range keys {
			if key, err = NewKey([][]byte{k}, []byte{}); err != nil {
				return err
			}

			p.keys = append(p.keys, key)
		}

		return nil
	}

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		r := req.(*protobuf.StatisticsRequest)
		p.scanType = STATS
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEqual())
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
	case *protobuf.ScanRequest:
		r := req.(*protobuf.ScanRequest)
		p.scanType = SCAN
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEqual())
		p.limit = r.GetLimit()
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
		p.pageSize = r.GetPageSize()
	case *protobuf.ScanAllRequest:
		p.scanType = SCANALL
		r := req.(*protobuf.ScanAllRequest)
		p.limit = r.GetLimit()
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
		p.pageSize = r.GetPageSize()
	default:
		err = ErrUnsupportedRequest
	}

	return
}

// Handle query requests arriving through queryport
func (s *scanCoordinator) requestHandler(
	req interface{},
	respch chan<- interface{},
	quitch <-chan interface{}) {

	var indexInst *common.IndexInst
	var partnInstMap *PartitionInstMap

	p, err := s.parseScanParams(req)
	if err != nil {
		// TODO: Add error response for invalid queryport reqs
		panic(err)
	}

	sd := &scanDescriptor{
		scanId: rand.Int63(),
		p:      p,
		stopch: make(StopChannel),
		respch: make(chan interface{}),
	}

	indexInst, partnInstMap, err = s.getIndexDS(p.indexName, p.bucket)
	if err != nil {
		respch <- s.makeErrResponseMessage(sd, err)
		close(respch)
		return
	}

	partnDefs := s.findPartitionDefsForScan(sd, indexInst)
	go s.scanPartitions(sd, partnDefs, partnInstMap)

	var respMsg interface{}
	var done bool = false

loop:
	// Read scan entries and send it to the client
	// Closing respch indicates that we have no more messages to be sent
	for !done {
		respMsg, done = s.makeResponseMessage(sd)
		if done {
			close(respch)
			break loop
		}

		select {
		case _, ok := <-quitch:
			if !ok {
				close(sd.stopch)
				close(respch)
				break loop
			}
		case respch <- respMsg:
			continue
		}
	}

	// Drain any leftover responses when client requests for graceful
	// end for streaming responses
	s.drainResponses(sd)
}

func (s *scanCoordinator) makeErrResponseMessage(sd *scanDescriptor, err error) (r interface{}) {
	protoErr := &protobuf.Error{Error: proto.String(err.Error())}
	switch sd.p.scanType {
	case STATS:
		r = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case SCAN:
		fallthrough
	case SCANALL:
		r = &protobuf.ResponseStream{
			Err: protoErr,
		}
	}

	return
}

// Create a queryport stream response message
// Perform necessary batching of rows into one message based requested page size
func (s *scanCoordinator) makeResponseMessage(sd *scanDescriptor) (r interface{}, done bool) {
	//var size int64
	//var resp interface{}

	//for {
	//resp, done = <-sd.respch
	//switch resp.(type) {
	//case Key:
	//case error:
	//r = s.makeErrResponseMessage(sd, resp.(error))
	//case statsResponse:
	//}
	//}

	return
}

func (s *scanCoordinator) drainResponses(sd *scanDescriptor) {
	for {
		_, closed := <-sd.respch
		if closed {
			break
		}
	}

}

// Find and return data structures for the specified index instance
func (s *scanCoordinator) getIndexDS(indexName, bucket string) (indexInst *common.IndexInst,
	partnInstMap *PartitionInstMap, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, inst := range s.indexInstMap {
		if inst.Defn.Name == indexName && inst.Defn.Bucket == bucket {
			indexInst = &inst
			if pmap, ok := s.indexPartnMap[inst.InstId]; ok {
				partnInstMap = &pmap
				return
			}
			common.Errorf("%v: Unable to find index partition map for %s/%s",
				s.logPrefix, bucket, indexName)
			err = ErrNotMyIndex
			return
		}
	}

	err = ErrIndexNotFound
	return
}

// Get defs of necessary partitions required for serving the scan request
func (s *scanCoordinator) findPartitionDefsForScan(sd *scanDescriptor,
	indexInst *common.IndexInst) []common.PartitionDefn {

	var partnDefs []common.PartitionDefn

	if string(sd.p.partnKey) != "" {
		id := indexInst.Pc.GetPartitionIdByPartitionKey(sd.p.partnKey)
		partnDefs = []common.PartitionDefn{indexInst.Pc.GetPartitionById(id)}
	} else {
		partnDefs = indexInst.Pc.GetAllPartitions()
	}

	return partnDefs
}

func (s *scanCoordinator) isLocalEndpoint(endpoint common.Endpoint) bool {
	// TODO: Detect local endpoint correctly
	// Since current indexer supports only single partition, this assumption
	// holds true
	return true
}

// Scan entries from the target partitions for index query
// Scan will be distributed across all the endpoints of the target partitions
// Scan entries/errors are written back into sd.respch channel
func (s *scanCoordinator) scanPartitions(sd *scanDescriptor,
	partDefs []common.PartitionDefn, partnInstMap *PartitionInstMap) {
	common.Debugf("ScanCoordinator: ScanPartitions %v", sd)

	var wg sync.WaitGroup
	var workerStopChannels []StopChannel

	for _, partnDefn := range partDefs {
		for _, endpoint := range partnDefn.Endpoints() {
			wg.Add(1)
			stopch := make(StopChannel)
			workerStopChannels = append(workerStopChannels, stopch)
			id := partnDefn.GetPartitionId()
			if s.isLocalEndpoint(endpoint) {
				// run local scan for local partition
				go s.scanLocalPartitionEndpoint(sd, id, partnInstMap, stopch, &wg)
			} else {
				go s.scanRemotePartitionEndpoint(sd, endpoint, id, stopch, &wg)
			}
		}

		//TODO: do we need this check ?
		//if partnInst, ok = partnInstMap[partnDefn.GetPartitionId()]; !ok {
		////partition doesn't exist on this node, run remote scan
	}

	s.monitorWorkers(&wg, sd.stopch, workerStopChannels, "scanPartitions")
	// We have no more responses to be sent
	close(sd.respch)
}

// Waits for the provided workers to finish and return
// It also listens to the stop channel and if that gets closed, all workers
// are stopped using workerStopChannels. Once all workers stop, the
// method retuns.
func (s *scanCoordinator) monitorWorkers(wg *sync.WaitGroup,
	stopch StopChannel, workerStopChannels []StopChannel, debugStr string) {

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		common.Tracef("ScanCoordinator: %s: Waiting for workers to finish.", debugStr)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		common.Tracef("ScanCoordinator: %s: All workers finished", debugStr)
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	select {
	case <-stopch:
		common.Debugf("ScanCoordinator: %s: Stopping All Workers.", debugStr)
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		common.Debugf("ScanCoordinator: %s: Stopped All Workers.", debugStr)

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

	}

}

// Locate the slices for the local partition endpoint and scan them
func (s *scanCoordinator) scanLocalPartitionEndpoint(sd *scanDescriptor,
	partnId common.PartitionId, partnInstMap *PartitionInstMap, stopch StopChannel,
	wg *sync.WaitGroup) {

	var partnInst PartitionInst
	var ok bool

	defer wg.Done()

	if partnInst, ok = (*partnInstMap)[partnId]; !ok {
		panic("Partition cannot be found in partition instance map")
	}

	common.Debugf("ScanCoordinator: ScanLocalPartition %v", sd)

	var workerWg sync.WaitGroup
	var workerStopChannels []StopChannel

	sliceList := partnInst.Sc.GetAllSlices()

	for _, slice := range sliceList {
		workerWg.Add(1)
		workerStopCh := make(StopChannel)
		workerStopChannels = append(workerStopChannels, workerStopCh)
		go s.scanLocalSlice(sd, slice, workerStopCh, &workerWg)
	}

	s.monitorWorkers(&workerWg, stopch, workerStopChannels, "scanLocalPartition")
}

func (s *scanCoordinator) scanRemotePartitionEndpoint(sd *scanDescriptor,
	endpoint common.Endpoint,
	partnId common.PartitionId, stopch StopChannel,
	wg *sync.WaitGroup) {

	defer wg.Done()
	panic("not implemented")
}

// Scan a snapshot from a local slice
// Snapshot to be scanned is determined by query parameters
func (s *scanCoordinator) scanLocalSlice(sd *scanDescriptor,
	slice Slice, stopch StopChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	common.Debugf("ScanCoordinator: ScanLocalSlice %v. SliceId %v", sd, slice.Id())

	snapContainer := slice.GetSnapshotContainer()
	snap := snapContainer.GetLatestSnapshot()

	if snap != nil {
		s.executeLocalScan(sd, snap, stopch)
	} else {
		common.Infof("ScanCoordinator: No Snapshot Available for ScanId %v "+
			"Index %s/%s, SliceId %v", sd.p.bucket, sd.p.indexName, sd.scanId, slice.Id())
	}
}

// Executes the actual scan of the snapshot
// Scan can be stopped anytime by closing the stop channel
func (s *scanCoordinator) executeLocalScan(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	switch sd.p.scanType {
	case STATS:
		s.statsQuery(sd, snap, stopch)
	case SCAN:
		s.scanQuery(sd, snap, stopch)
	case SCANALL:
		s.scanAllQuery(sd, snap, stopch)
	}
}

func (s *scanCoordinator) statsQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	totalRows, err := snap.CountRange(sd.p.low, sd.p.high, sd.p.incl, stopch)
	if err != nil {
		sd.respch <- err
	} else {
		sd.respch <- statsResponse{count: totalRows}
	}
}

func (s *scanCoordinator) scanQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	if len(sd.p.keys) == 0 {
		//ch, cherr := snap.Lookup(p.keys, stopch)
	} else {
		ch, cherr, _ := snap.KeyRange(sd.p.low, sd.p.high, sd.p.incl, stopch)
		s.receiveKeys(sd, ch, cherr)
	}

}

func (s *scanCoordinator) scanAllQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	ch, cherr := snap.KeySet(stopch)
	s.receiveKeys(sd, ch, cherr)
}

// receiveKeys receives results/errors from snapshot reader and forwards it to
// the caller till the result channel is closed by the snapshot reader
func (s *scanCoordinator) receiveKeys(sd *scanDescriptor, chval chan Key, cherr chan error) {
	ok := true
	var key Key
	var err error

	for ok {
		select {
		case key, ok = <-chval:
			if ok {
				common.Tracef("ScanCoordinator: ScanId %v Received Value %s", sd.scanId, key.String())
				sd.respch <- key
			}
		case err, _ = <-cherr:
			if err != nil {
				sd.respch <- err
			}
		}
	}
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	common.Infof("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexPartnMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	common.Infof("ScanCoordinator::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}
