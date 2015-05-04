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
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Errors
var (
	// Error strings for ErrIndexNotFound and ErrIndexNotReady need to be
	// in sync with errors defined in queryport/n1ql/secondary_index.go
	ErrIndexNotFound      = errors.New("Index not found")
	ErrIndexNotReady      = errors.New("Index not ready for serving queries")
	ErrNotMyIndex         = errors.New("Not my index")
	ErrInternal           = errors.New("Internal server error occured")
	ErrSnapNotAvailable   = errors.New("No snapshot available for scan")
	ErrScanTimedOut       = errors.New("Index scan timed out")
	ErrUnsupportedRequest = errors.New("Unsupported query request")
	ErrClientCancel       = errors.New("Client requested cancel")
)

type ScanReqType string

const (
	StatsReq   ScanReqType = "stats"
	CountReq               = "count"
	ScanReq                = "scan"
	ScanAllReq             = "scanAll"
)

type ScanRequest struct {
	ScanType    ScanReqType
	DefnID      uint64
	IndexInstId common.IndexInstId
	IndexName   string
	Bucket      string
	Ts          *common.TsVbuuid
	Low         IndexKey
	High        IndexKey
	Keys        []IndexKey
	Consistency *common.Consistency
	Stats       *indexScanStats

	// user supplied
	LowBytes, HighBytes []byte
	KeysBytes           [][]byte

	Incl      Inclusion
	Limit     int64
	isPrimary bool

	ScanId    uint64
	TimeoutCh <-chan time.Time
	CancelCh  <-chan interface{}

	LogPrefix string
}

type CancelCb struct {
	done    chan struct{}
	timeout <-chan time.Time
	cancel  <-chan interface{}
	callb   func(error)
}

func (c *CancelCb) Run() {
	go func() {
		select {
		case <-c.done:
		case <-c.cancel:
			c.callb(ErrClientCancel)
		case <-c.timeout:
			c.callb(ErrScanTimedOut)
		}
	}()
}

func (c *CancelCb) Done() {
	close(c.done)
}

func NewCancelCallback(req *ScanRequest, callb func(error)) *CancelCb {
	return &CancelCb{
		done:    make(chan struct{}),
		timeout: req.TimeoutCh,
		cancel:  req.CancelCh,
		callb:   callb,
	}
}

func (r ScanRequest) String() string {
	var incl, span string

	switch r.Incl {
	case Low:
		incl = "incl:low"
	case High:
		incl = "incl:high"
	case Both:
		incl = "incl:both"
	default:
		incl = "incl:none"
	}

	if len(r.Keys) == 0 {
		if r.ScanType == StatsReq || r.ScanType == ScanReq {
			span = fmt.Sprintf("range (%s,%s %s)", r.Low, r.High, incl)
		} else {
			span = "all"
		}
	} else {
		span = "keys ( "
		for _, k := range r.Keys {
			span = span + k.String() + " "
		}
		span = span + ")"
	}

	str := fmt.Sprintf("defnId:%v, index:%v/%v, type:%v, span:%s",
		r.DefnID, r.Bucket, r.IndexName, r.ScanType, span)

	if r.Limit > 0 {
		str += fmt.Sprintf(", limit:%d", r.Limit)
	}

	if r.Consistency != nil {
		str += fmt.Sprintf(", consistency:%s", strings.ToLower(r.Consistency.String()))
	}

	return str
}

type ScanCoordinator interface {
}

type indexScanStats struct {
	Requests  *uint64
	Rows      *uint64
	BytesRead *uint64
	ScanTime  *uint64
	WaitTime  *uint64
}

type scanCoordinator struct {
	supvCmdch MsgChannel //supervisor sends commands on this channel
	supvMsgch MsgChannel //channel to send any async message to supervisor
	serv      *queryport.Server
	logPrefix string

	mu            sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	reqCounter   *uint64
	config       common.ConfigHolder
	scanStatsMap map[common.IndexInstId]*indexScanStats
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel,
	config common.Config) (ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch:    supvCmdch,
		supvMsgch:    supvMsgch,
		logPrefix:    "ScanCoordinator",
		reqCounter:   new(uint64),
		scanStatsMap: make(map[common.IndexInstId]*indexScanStats),
	}

	s.config.Store(config)

	addr := net.JoinHostPort("", config["scanPort"].String())
	queryportCfg := config.SectionConfig("queryport.", true)
	s.serv, err = queryport.NewServer(addr, s.serverCallback, queryportCfg)

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

func (s *scanCoordinator) handleStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	statsMap := make(map[string]interface{})
	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	var instList []common.IndexInst
	s.mu.RLock()
	defer s.mu.RUnlock()

	for instId, stat := range s.scanStatsMap {
		inst := s.indexInstMap[instId]
		//skip deleted indexes
		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		instList = append(instList, inst)

		k := fmt.Sprintf("%s:%s:num_requests", inst.Defn.Bucket, inst.Defn.Name)
		v := atomic.LoadUint64(stat.Requests)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:num_rows_returned", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.Rows)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:scan_bytes_read", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.BytesRead)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:total_scan_duration", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.ScanTime)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:scan_wait_duration", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.WaitTime)
		statsMap[k] = v

		st := s.serv.Statistics()
		statsMap["num_connections"] = st.Connections
	}

	// Compute counts asynchronously and reply to stats request
	go func() {
		for _, inst := range instList {
			c, err := s.getItemsCount(inst.InstId)
			if err == nil {
				k := fmt.Sprintf("%s:%s:items_count", inst.Defn.Bucket, inst.Defn.Name)
				statsMap[k] = c
			} else {
				logging.Errorf("%v: Unable compute index count for %v/%v (%v)", s.logPrefix,
					inst.Defn.Bucket, inst.Defn.Name, err)
			}
		}
		replych <- statsMap
	}()
}

func (s *scanCoordinator) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					logging.Infof("ScanCoordinator: Shutting Down")
					s.serv.Close()
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

	case SCAN_STATS:
		s.handleStats(cmd)

	case CONFIG_SETTINGS_UPDATE:
		s.handleConfigUpdate(cmd)

	default:
		logging.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

func (s *scanCoordinator) newRequest(protoReq interface{},
	cancelCh <-chan interface{}) (r *ScanRequest, err error) {

	var indexInst *common.IndexInst
	r = new(ScanRequest)
	r.ScanId = atomic.AddUint64(s.reqCounter, 1)
	r.LogPrefix = fmt.Sprintf("SCAN##%d", r.ScanId)

	cfg := s.config.Load()
	timeout := time.Millisecond * time.Duration(cfg["settings.scan_timeout"].Int())
	if timeout != 0 {
		r.TimeoutCh = time.After(timeout)
	}

	r.CancelCh = cancelCh

	isNil := func(k []byte) bool {
		if len(k) == 0 || (!r.isPrimary && string(k) == "[]") {
			return true
		}
		return false
	}

	newKey := func(k []byte) (IndexKey, error) {
		if len(k) == 0 {
			return nil, fmt.Errorf("Key is null")
		}

		if r.isPrimary {
			return NewPrimaryKey(k)
		} else {
			return NewSecondaryKey(k)
		}
	}

	newLowKey := func(k []byte) (IndexKey, error) {
		if isNil(k) {
			return MinIndexKey, nil
		}

		return newKey(k)
	}

	newHighKey := func(k []byte) (IndexKey, error) {
		if isNil(k) {
			return MaxIndexKey, nil
		}

		return newKey(k)
	}

	fillRanges := func(low, high []byte, keys [][]byte) {
		var key IndexKey
		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()

		// range
		r.LowBytes = low
		r.HighBytes = high

		if r.Low, localErr = newLowKey(low); localErr != nil {
			localErr = fmt.Errorf("Invalid low key %s (%s)", string(low), localErr)
			return
		}

		if r.High, localErr = newHighKey(high); localErr != nil {
			localErr = fmt.Errorf("Invalid high key %s (%s)", string(high), localErr)
			return
		}

		// point query for keys
		for _, k := range keys {
			r.KeysBytes = append(r.KeysBytes, k)
			if key, localErr = newKey(k); localErr != nil {
				localErr = fmt.Errorf("Invalid equal key %s (%s)", string(k), localErr)
				return
			}
			r.Keys = append(r.Keys, key)
		}
	}

	setConsistency := func(cons common.Consistency, vector *protobuf.TsConsistency) {
		r.Consistency = &cons
		checkVector :=
			cons == common.QueryConsistency || cons == common.SessionConsistency
		if checkVector && vector != nil {
			cfg := s.config.Load()
			r.Ts = common.NewTsVbuuid("", cfg["numVbuckets"].Int())
			for i, vbno := range vector.Vbnos {
				r.Ts.Seqnos[vbno] = vector.Seqnos[i]
				r.Ts.Vbuuids[vbno] = vector.Vbuuids[i]
			}
		}
	}

	setIndexParams := func() {
		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()
		s.mu.RLock()
		defer s.mu.RUnlock()
		indexInst, localErr = s.findIndexInstance(r.DefnID)
		if localErr == nil {
			r.isPrimary = indexInst.Defn.IsPrimary
			r.IndexName, r.Bucket = indexInst.Defn.Name, indexInst.Defn.Bucket
			r.IndexInstId = indexInst.InstId
			if r.Ts != nil {
				r.Ts.Bucket = r.Bucket
			}
			if indexInst.State != common.INDEX_STATE_ACTIVE {
				localErr = ErrIndexNotReady
			} else {
				r.Stats = s.scanStatsMap[r.IndexInstId]
			}
		}
	}

	switch req := protoReq.(type) {
	case *protobuf.StatisticsRequest:
		r.DefnID = req.GetDefnID()
		r.ScanType = StatsReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		setIndexParams()
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())

	case *protobuf.CountRequest:
		r.DefnID = req.GetDefnID()
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		setConsistency(cons, vector)
		r.ScanType = CountReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		setIndexParams()
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())

	case *protobuf.ScanRequest:
		r.DefnID = req.GetDefnID()
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		setConsistency(cons, vector)
		r.ScanType = ScanReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		setIndexParams()
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		r.Limit = req.GetLimit()
	case *protobuf.ScanAllRequest:
		r.DefnID = req.GetDefnID()
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		setConsistency(cons, vector)
		r.ScanType = ScanAllReq
		r.Limit = req.GetLimit()
		setIndexParams()
	default:
		err = ErrUnsupportedRequest
	}

	return
}

// Before starting the index scan, we have to find out the snapshot timestamp
// that can fullfil this query by considering atleast-timestamp provided in
// the query request. A timestamp request message is sent to the storage
// manager. The storage manager will respond immediately if a snapshot
// is available, otherwise it will wait until a matching snapshot is
// available and return the timestamp. Util then, the query processor
// will block wait.
// This mechanism can be used to implement RYOW.
func (s *scanCoordinator) getRequestedIndexSnapshot(r *ScanRequest) (snap IndexSnapshot, err error) {
	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		ts:        r.Ts,
		respch:    snapResch,
		idxInstId: r.IndexInstId,
	}

	// Block wait until a ts is available for fullfilling the request
	s.supvMsgch <- snapReqMsg
	var msg interface{}
	select {
	case msg = <-snapResch:
	case <-r.TimeoutCh:
		go readDeallocSnapshot(snapResch)
		msg = ErrScanTimedOut
	}

	switch msg.(type) {
	case IndexSnapshot:
		snap = msg.(IndexSnapshot)
	case error:
		err = msg.(error)
	}

	return
}

func (s *scanCoordinator) respondWithError(conn net.Conn, req *ScanRequest, err error) {
	var res interface{}

	buf := p.GetBlock()
	defer p.PutBlock(buf)

	protoErr := &protobuf.Error{Error: proto.String(err.Error())}

	switch req.ScanType {
	case StatsReq:
		res = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case CountReq:
		res = &protobuf.CountResponse{
			Count: proto.Int64(0), Err: protoErr,
		}
	case ScanAllReq, ScanReq:
		res = &protobuf.ResponseStream{
			Err: protoErr,
		}
	}

	err2 := protobuf.EncodeAndWrite(conn, *buf, res)
	if err2 != nil {
		err = fmt.Errorf("%s, %s", err, err2)
		goto finish
	}
	err2 = protobuf.EncodeAndWrite(conn, *buf, &protobuf.StreamEndResponse{})
	if err2 != nil {
		err = fmt.Errorf("%s, %s", err, err2)
	}

finish:
	logging.Errorf("%s RESPONSE Failed with error (%s)", req.LogPrefix, err)
}

func (s *scanCoordinator) handleError(prefix string, err error) {
	if err != nil {
		logging.Errorf("%s Error occured %s", prefix, err)
	}
}

func (s *scanCoordinator) tryRespondWithError(w ScanResponseWriter, req *ScanRequest, err error) bool {
	if err != nil {
		logging.Infof("%s RESPONSE status:(error = %s)", req.LogPrefix, err)
		s.handleError(req.LogPrefix, w.Error(err))
		return true
	}

	return false
}

func (s *scanCoordinator) serverCallback(protoReq interface{}, conn net.Conn,
	cancelCh <-chan interface{}) {

	req, err := s.newRequest(protoReq, cancelCh)
	w := NewProtoWriter(req.ScanType, conn)
	defer func() { s.handleError(req.LogPrefix, w.Done()) }()

	logging.Infof("%s REQUEST %s", req.LogPrefix, req)

	if req.Consistency != nil {
		logging.LazyDebug(func() string {
			return fmt.Sprintf("%s requested timestamp: %s => %s", req.LogPrefix,
				strings.ToLower(req.Consistency.String()), ScanTStoString(req.Ts))
		})
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	atomic.AddUint64(req.Stats.Requests, 1)

	t0 := time.Now()
	is, err := s.getRequestedIndexSnapshot(req)
	if s.tryRespondWithError(w, req, err) {
		return
	}

	defer DestroyIndexSnapshot(is)

	logging.LazyDebug(func() string {
		return fmt.Sprintf("%s snapshot timestamp: %s",
			req.LogPrefix, ScanTStoString(is.Timestamp()))
	})
	s.processRequest(req, w, is, t0)
}

func (s *scanCoordinator) processRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {

	switch req.ScanType {
	case ScanReq, ScanAllReq:
		s.handleScanRequest(req, w, is, t0)
	case CountReq:
		s.handleCountRequest(req, w, is, t0)
	case StatsReq:
		s.handleStatsRequest(req, w, is)
	}
}

func (s *scanCoordinator) handleScanRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {
	waitTime := time.Now().Sub(t0)

	scanPipeline := NewScanPipeline(req, w, is)
	cancelCb := NewCancelCallback(req, func(e error) {
		scanPipeline.Cancel(e)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	err := scanPipeline.Execute()
	scanTime := time.Now().Sub(t0)

	atomic.AddUint64(req.Stats.Rows, scanPipeline.RowsRead())
	atomic.AddUint64(req.Stats.BytesRead, scanPipeline.BytesRead())
	atomic.AddUint64(req.Stats.ScanTime, uint64(scanTime.Nanoseconds()))
	atomic.AddUint64(req.Stats.WaitTime, uint64(waitTime.Nanoseconds()))

	var status string
	if err != nil {
		status = fmt.Sprintf("(error = %s)", err)
	} else {
		status = "ok"
	}

	logging.Infof("%s RESPONSE rows:%d, waitTime:%v, totalTime:%v, status:%s",
		req.LogPrefix, scanPipeline.RowsRead(), waitTime, scanTime, status)
}

func (s *scanCoordinator) handleCountRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {
	var rows uint64
	var err error

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	for _, s := range GetSliceSnapshots(is) {
		var r uint64
		snap := s.Snapshot()
		if len(req.Keys) > 0 {
			r, err = snap.CountLookup(req.Keys, stopch)
		} else if req.Low.Bytes() == nil && req.Low.Bytes() == nil {
			r, err = snap.CountTotal(stopch)
		} else {
			r, err = snap.CountRange(req.Low, req.High, req.Incl, stopch)
		}

		if err != nil {
			break
		}

		rows += r
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Infof("%s RESPONSE count:%d status:ok", req.LogPrefix, rows)
	err = w.Count(rows)
	s.handleError(req.LogPrefix, err)
}

func (s *scanCoordinator) handleStatsRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot) {
	var rows uint64
	var err error

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	for _, s := range GetSliceSnapshots(is) {
		var r uint64
		snap := s.Snapshot()
		if req.Low.Bytes() == nil && req.Low.Bytes() == nil {
			r, err = snap.StatCountTotal()
		} else {
			r, err = snap.CountRange(req.Low, req.High, req.Incl, stopch)
		}

		if err != nil {
			break
		}

		rows += r
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Infof("%s RESPONSE status:ok", req.LogPrefix)
	err = w.Stats(rows, 0, nil, nil)
	s.handleError(req.LogPrefix, err)
}

// Find and return data structures for the specified index
func (s *scanCoordinator) findIndexInstance(
	defnID uint64) (*common.IndexInst, error) {

	for _, inst := range s.indexInstMap {
		if inst.Defn.DefnId == common.IndexDefnId(defnID) {
			if _, ok := s.indexPartnMap[inst.InstId]; ok {
				return &inst, nil
			}
			return nil, ErrNotMyIndex
		}
	}
	return nil, ErrIndexNotFound
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logging.Tracef("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	// Remove invalid indexes
	for instId, _ := range s.scanStatsMap {
		if _, ok := s.indexInstMap[instId]; !ok {
			delete(s.scanStatsMap, instId)
		}
	}

	// Add newly added indexes
	for instId, _ := range s.indexInstMap {
		if _, ok := s.scanStatsMap[instId]; !ok {
			s.scanStatsMap[instId] = &indexScanStats{
				Requests:  new(uint64),
				Rows:      new(uint64),
				BytesRead: new(uint64),
				ScanTime:  new(uint64),
				WaitTime:  new(uint64),
			}
		}
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexPartnMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logging.Tracef("ScanCoordinator::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	s.config.Store(cfgUpdate.GetConfig())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) getItemsCount(instId common.IndexInstId) (uint64, error) {
	var count uint64

	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		ts:        nil,
		respch:    snapResch,
		idxInstId: instId,
	}

	s.supvMsgch <- snapReqMsg
	msg := <-snapResch

	// Index snapshot is not available yet (non-active index or empty index)
	if msg == nil {
		return 0, nil
	}

	var is IndexSnapshot

	switch msg.(type) {
	case IndexSnapshot:
		is = msg.(IndexSnapshot)
		if is == nil {
			return 0, nil
		}
		defer DestroyIndexSnapshot(is)
	case error:
		return 0, msg.(error)
	}

	for _, ps := range is.Partitions() {
		for _, ss := range ps.Slices() {
			snap := ss.Snapshot()
			c, err := snap.StatCountTotal()
			if err != nil {
				return 0, err
			}
			count += c
		}
	}

	return count, nil
}

// Helper method to pretty print timestamp
func ScanTStoString(ts *common.TsVbuuid) string {
	var seqsStr string = "["

	if ts != nil {
		for i, s := range ts.Seqnos {
			if i > 0 {
				seqsStr += ","
			}
			seqsStr += fmt.Sprintf("%d=%d", i, s)
		}
	}

	seqsStr += "]"

	return seqsStr
}

func readDeallocSnapshot(ch chan interface{}) {
	msg := <-ch
	if msg == nil {
		return
	}

	var is IndexSnapshot
	switch msg.(type) {
	case IndexSnapshot:
		is = msg.(IndexSnapshot)
		if is == nil {
			return
		}

		DestroyIndexSnapshot(is)
	}
}
