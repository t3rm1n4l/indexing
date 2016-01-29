package projector

import "fmt"
import "time"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/dcp"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import projC "github.com/couchbase/indexing/secondary/projector/client"
import "github.com/golang/protobuf/proto"

// NOTE1:
// https://github.com/couchbase/indexing/commit/
//         9a2ea0f0ffaf9f103ace6ffe54e253001b28b9a8
// above commit mandates that back-channel should be sufficiently large to
// consume all DCP response for STREAM_REQ posted for 1 or more bucket. Here
// is the way it plays out,
//
//   * once a stream is opened DCP will start batch pushing data for that
//     stream.
//   * while data for each stream is pushed to vbucket-routines the control
//     messages, STREAMREQ & SNAPSHOT, are sent to back-channel.
//   * by the time the last vbucket is StreamRequested and corresponding
//     response and snapshot message is back-channeled, there would have
//     been a lot of data pushed to vbucket-routines.
//   * data will eventually drained to downstream endpoints and its
//     connection, but back-channel cannot be blocked until all STREAMREQ
//     is posted to DCP server. In simple terms,
//     THIS IS IMPORTANT: NEVER REDUCE THE BACK-CHANNEL SIZE TO < MAXVBUCKETS

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	cluster      string // immutable
	pooln        string // immutable
	topic        string // immutable
	opaque       uint16 // opaque that created this feed.
	endpointType string // immutable

	// upstream
	// reqTs, book-keeping on outstanding request posted to feeder.
	// vbucket entry from this timestamp is deleted only when a SUCCESS,
	// ROLLBACK or ERROR response is received from feeder.
	reqTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// actTs, once StreamBegin SUCCESS response is got back from DCP,
	// vbucket entry is moved here.
	actTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// rollTs, when StreamBegin ROLLBACK response is got back from DCP,
	// vbucket entry is moved here.
	rollTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid

	feeders map[string]BucketFeeder // bucket -> BucketFeeder{}
	// downstream
	kvdata    map[string]*KVData            // bucket -> kvdata
	engines   map[string]map[uint64]*Engine // bucket -> uuid -> engine
	endpoints map[string]c.RouterEndpoint
	// genServer channel
	reqch  chan []interface{}
	backch chan []interface{}
	finch  chan bool
	// book-keeping for stale-ness
	stale int

	// config params
	reqTimeout time.Duration
	endTimeout time.Duration
	epFactory  c.RouterEndpointFactory
	config     c.Config
	logPrefix  string
}

// NewFeed creates a new topic feed.
// `config` contains following keys.
//    clusterAddr: KV cluster address <host:port>.
//    feedWaitStreamReqTimeout: wait for a response to StreamRequest
//    feedWaitStreamEndTimeout: wait for a response to StreamEnd
//    feedChanSize: channel size for feed's control path and back path
//    mutationChanSize: channel size of projector's data path routine
//    syncTimeout: timeout, in ms, for sending periodic Sync messages
//    kvstatTick: timeout, in ms, for logging kvstats
//    routerEndpointFactory: endpoint factory
func NewFeed(
	pooln, topic string, config c.Config, opaque uint16) (*Feed, error) {

	epf := config["routerEndpointFactory"].Value.(c.RouterEndpointFactory)
	chsize := config["feedChanSize"].Int()
	backchsize := config["backChanSize"].Int()
	feed := &Feed{
		cluster: config["clusterAddr"].String(),
		pooln:   pooln,
		topic:   topic,
		opaque:  opaque,

		// upstream
		reqTss:  make(map[string]*protobuf.TsVbuuid),
		actTss:  make(map[string]*protobuf.TsVbuuid),
		rollTss: make(map[string]*protobuf.TsVbuuid),
		feeders: make(map[string]BucketFeeder),
		// downstream
		kvdata:    make(map[string]*KVData),
		engines:   make(map[string]map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		// genServer channel
		reqch:  make(chan []interface{}, chsize),
		backch: make(chan []interface{}, backchsize),
		finch:  make(chan bool),

		reqTimeout: time.Duration(config["feedWaitStreamReqTimeout"].Int()),
		endTimeout: time.Duration(config["feedWaitStreamEndTimeout"].Int()),
		epFactory:  epf,
		config:     config,
	}
	feed.logPrefix = fmt.Sprintf("FEED[<=>%v(%v)]", topic, feed.cluster)

	go feed.genServer()
	logging.Infof("%v ##%x feed started ...\n", feed.logPrefix, opaque)
	return feed, nil
}

// GetOpaque return the opaque id that created this feed.
func (feed *Feed) GetOpaque() uint16 {
	return feed.opaque
}

const (
	fCmdStart byte = iota + 1
	fCmdRestartVbuckets
	fCmdShutdownVbuckets
	fCmdAddBuckets
	fCmdDelBuckets
	fCmdAddInstances
	fCmdDelInstances
	fCmdRepairEndpoints
	fCmdStaleCheck
	fCmdShutdown
	fCmdGetTopicResponse
	fCmdGetStatistics
	fCmdResetConfig
	fCmdDeleteEndpoint
	fCmdPing
)

// ResetConfig for this feed.
func (feed *Feed) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// MutationTopic will start the feed.
// Synchronous call.
func (feed *Feed) MutationTopic(
	req *protobuf.MutationTopicRequest, opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStart, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
}

// RestartVbuckets will restart upstream vbuckets for specified buckets.
// Synchronous call.
func (feed *Feed) RestartVbuckets(
	req *protobuf.RestartVbucketsRequest, opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRestartVbuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
}

// ShutdownVbuckets will shutdown streams for
// specified buckets.
// Synchronous call.
func (feed *Feed) ShutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest, opaque uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdownVbuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) AddBuckets(
	req *protobuf.AddBucketsRequest,
	opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddBuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
}

// DelBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) DelBuckets(
	req *protobuf.DelBucketsRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelBuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) AddInstances(
	req *protobuf.AddInstancesRequest,
	opaque uint16) (*protobuf.TimestampResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddInstances, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TimestampResponse{}, err
	}
	return resp[0].(*protobuf.TimestampResponse), nil
}

// DelInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) DelInstances(
	req *protobuf.DelInstancesRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelInstances, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// RepairEndpoints will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) RepairEndpoints(
	req *protobuf.RepairEndpointsRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRepairEndpoints, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// StaleCheck will check for feed sanity and return "exit" if feed
// has was already stale and still stale.
// Synchronous call.
func (feed *Feed) StaleCheck(staleTimeout int) (string, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStaleCheck, respch}

	// modified `FailsafeOp()` to implement a nuke-switch
	// for indefinitely blocked feeds.
	tm := time.After(time.Duration(staleTimeout) * time.Millisecond)
	select {
	case feed.reqch <- cmd:
		select {
		case resp := <-respch:
			return resp[0].(string), nil
		case <-feed.finch:
			return "exit", c.ErrorClosed
			// NOTE: don't use the nuke-timeout if feed's gen-server has
			// accepted to service the request. Reason being,
			// a. staleCheck() will try to ping endpoint() for its health, and
			//    if endpoint is stuck with downstream, it might trigger the
			//    the following timeout.
			//case <-tm:
			//    logging.Fatalf("%v StaleCheck() timesout !!", feed.logPrefix)
			//    feed.shutdown(feed.opaque)
			//    return "exit", c.ErrorClosed
		}
	case <-feed.finch:
		return "exit", c.ErrorClosed
	case <-tm:
		logging.Fatalf("%v StaleCheck() timesout !!", feed.logPrefix)
		feed.shutdown(feed.opaque)
		return "exit", c.ErrorClosed
	}
	return "exit", c.ErrorClosed
}

// GetTopicResponse for this feed.
// Synchronous call.
func (feed *Feed) GetTopicResponse() *protobuf.TopicResponse {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetTopicResponse, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if resp != nil && err == nil {
		return resp[0].(*protobuf.TopicResponse)
	}
	return nil
}

// GetStatistics for this feed.
// Synchronous call.
func (feed *Feed) GetStatistics() c.Statistics {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStatistics, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if resp != nil && err == nil {
		return resp[0].(c.Statistics)
	}
	return nil
}

// Shutdown feed, its upstream connection with kv and downstream endpoints.
// Synchronous call.
func (feed *Feed) Shutdown(opaque uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdown, opaque, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// DeleteEndpoint will delete the specified endpoint address
// from feed.
func (feed *Feed) DeleteEndpoint(raddr string) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDeleteEndpoint, raddr, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// Ping whether the feed is active or not.
func (feed *Feed) Ping() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdPing, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

type controlStreamRequest struct {
	bucket string
	opaque uint16
	status mcd.Status
	vbno   uint16
	vbuuid uint64
	seqno  uint64 // also doubles as rollback-seqno
}

func (v *controlStreamRequest) Repr() string {
	return fmt.Sprintf("{controlStreamRequest, %v, %s, %d, %x, %d, ##%x}",
		v.status, v.bucket, v.vbno, v.vbuuid, v.seqno, v.opaque)
}

// PostStreamRequest feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamRequest(bucket string, m *mc.DcpEvent) {
	var respch chan []interface{}
	cmd := &controlStreamRequest{
		bucket: bucket,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
		vbuuid: m.VBuuid,
		seqno:  m.Seqno, // can also be roll-back seqno, based on status
	}
	fmsg := "%v ##%x backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, m.Opaque, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v ##%x backch blocked on PostStreamRequest\n"
		logging.Errorf(fmsg, feed.logPrefix, m.Opaque)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

type controlStreamEnd struct {
	bucket string
	opaque uint16
	status mcd.Status
	vbno   uint16
}

func (v *controlStreamEnd) Repr() string {
	return fmt.Sprintf("{controlStreamEnd, %v, %s, %d, %x}",
		v.status, v.bucket, v.vbno, v.opaque)
}

// PostStreamEnd feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamEnd(bucket string, m *mc.DcpEvent) {
	var respch chan []interface{}
	cmd := &controlStreamEnd{
		bucket: bucket,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
	}
	fmsg := "%v ##%x backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, m.Opaque, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v ##%x backch blocked on PostStreamEnd\n"
		logging.Errorf(fmsg, feed.logPrefix, m.Opaque)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

type controlFinKVData struct {
	bucket string
}

func (v *controlFinKVData) Repr() string {
	return fmt.Sprintf("{controlFinKVData, %s}", v.bucket)
}

// PostFinKVdata feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostFinKVdata(bucket string) {
	var respch chan []interface{}
	cmd := &controlFinKVData{bucket: bucket}
	fmsg := "%v backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v backch blocked on PostFinKVdata\n"
		logging.Errorf(fmsg, feed.logPrefix)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

func (feed *Feed) genServer() {
	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v feed gen-server crashed: %v\n", feed.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
			feed.shutdown(feed.opaque)
		}
	}()

	var msg []interface{}

	timeout := time.Tick(1000 * time.Millisecond)
	ctrlMsg := "%v ##%x control channel has %v messages"
	prefix := feed.logPrefix

loop:
	for {
		select {
		case msg = <-feed.reqch:
			switch feed.handleCommand(msg) {
			case "ok":
				feed.stale = 0
			case "exit":
				break loop
			}

		case msg = <-feed.backch:
			if cmd, ok := msg[0].(*controlStreamRequest); ok {
				reqTs, ok := feed.reqTss[cmd.bucket]
				seqno, _, sStart, sEnd, err := reqTs.Get(cmd.vbno)
				if err != nil {
					fmsg := "%v ##%x backch flush %v: %v\n"
					logging.Fatalf(fmsg, prefix, cmd.opaque, cmd, err)

				}
				if ok && reqTs != nil {
					reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
					feed.reqTss[cmd.bucket] = reqTs
				}
				if cmd.status == mcd.ROLLBACK {
					fmsg := "%v ##%x backch flush rollback %T: %v\n"
					logging.Infof(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
					rollTs, ok := feed.rollTss[cmd.bucket]
					if ok {
						rollTs = rollTs.Append(cmd.vbno, cmd.seqno, cmd.vbuuid, sStart, sEnd)
						feed.rollTss[cmd.bucket] = rollTs
					}

				} else if cmd.status == mcd.SUCCESS {
					fmsg := "%v ##%x backch flush success %T: %v\n"
					logging.Infof(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
					actTs, _ := feed.actTss[cmd.bucket]
					actTs = actTs.Append(cmd.vbno, seqno, cmd.vbuuid, sStart, sEnd)
					feed.actTss[cmd.bucket] = actTs

				} else {
					fmsg := "%v ##%x backch flush error %T: %v\n"
					logging.Errorf(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
				}

			} else if cmd, ok := msg[0].(*controlStreamEnd); ok {
				fmsg := "%v ##%x backch flush %T: %v\n"
				logging.Infof(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
				reqTs := feed.reqTss[cmd.bucket]
				reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.reqTss[cmd.bucket] = reqTs

				actTs := feed.actTss[cmd.bucket]
				actTs = actTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.actTss[cmd.bucket] = actTs

				rollTs := feed.rollTss[cmd.bucket]
				rollTs = rollTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.rollTss[cmd.bucket] = rollTs

			} else if cmd, ok := msg[0].(*controlFinKVData); ok {
				fmsg := "%v ##%x backch flush %T -- %v\n"
				logging.Infof(fmsg, prefix, feed.opaque, cmd, cmd.Repr())
				actTs, ok := feed.actTss[cmd.bucket]
				if ok && actTs != nil && actTs.Len() == 0 { // bucket is done
					fmsg = "%v ##%x self deleting bucket\n"
					logging.Infof(fmsg, prefix, feed.opaque)
					feed.cleanupBucket(cmd.bucket, false)

				} else if actTs != nil && actTs.Len() == 0 {
					fmsg = "%v ##%x FinKVData before StreamEnds %v\n"
					logging.Fatalf(fmsg, prefix, feed.opaque, actTs)

				} else {
					// Note: bucket could have gone because of a downstream
					// delBucket() request.
					fmsg := "%v ##%x FinKVData can't find bucket %q\n"
					logging.Warnf(fmsg, prefix, feed.opaque, cmd.bucket)
				}

			} else {
				fmsg := "%v ##%x backch flush %T: %v\n"
				logging.Fatalf(fmsg, prefix, feed.opaque, msg[0], msg[0])
			}

		case <-timeout:
			if len(feed.backch) > 0 { // can happend during rebalance.
				logging.Warnf(ctrlMsg, prefix, feed.opaque, len(feed.backch))
			}
		}
	}
	timeout = nil
}

// "ok"    - command handled.
// "stale" - feed has gone stale.
// "exit"  - feed was already stale, so exit feed.
func (feed *Feed) handleCommand(msg []interface{}) (status string) {
	status = "ok"

	switch cmd := msg[0].(byte); cmd {
	case fCmdStart:
		req := msg[1].(*protobuf.MutationTopicRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.start(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdRestartVbuckets:
		req := msg[1].(*protobuf.RestartVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.restartVbuckets(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdShutdownVbuckets:
		req := msg[1].(*protobuf.ShutdownVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.shutdownVbuckets(req, opaque)}

	case fCmdAddBuckets:
		req := msg[1].(*protobuf.AddBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.addBuckets(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdDelBuckets:
		req := msg[1].(*protobuf.DelBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.delBuckets(req, opaque)
		if len(feed.kvdata) == 0 {
			status = "exit"
			fmsg := "%v no more buckets left, closing the feed ..."
			logging.Warnf(fmsg, feed.logPrefix)
			feed.shutdown(feed.opaque)
		}
		respch <- []interface{}{err}

	case fCmdAddInstances:
		req := msg[1].(*protobuf.AddInstancesRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		resp, err := feed.addInstances(req, opaque)
		respch <- []interface{}{resp, err}

	case fCmdDelInstances:
		req := msg[1].(*protobuf.DelInstancesRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.delInstances(req, opaque)}

	case fCmdRepairEndpoints:
		req := msg[1].(*protobuf.RepairEndpointsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.repairEndpoints(req, opaque)}

	case fCmdStaleCheck:
		respch := msg[1].(chan []interface{})
		status = feed.staleCheck()
		if status == "stale" && feed.stale == 1 { // already gone stale.
			status = "exit"
			feed.shutdown(feed.opaque)
			logging.Warnf("%v feed collect stale...", feed.logPrefix)
		} else if status == "stale" {
			logging.Warnf("%v feed mark stale...", feed.logPrefix)
			feed.stale++
		}
		respch <- []interface{}{status}

	case fCmdGetTopicResponse:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.topicResponse()}

	case fCmdGetStatistics:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.getStatistics()}

	case fCmdResetConfig:
		config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
		feed.resetConfig(config)
		respch <- []interface{}{nil}

	case fCmdShutdown:
		opaque := msg[1].(uint16)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.shutdown(opaque)}
		status = "exit"

	case fCmdDeleteEndpoint:
		raddr := msg[1].(string)
		respch := msg[2].(chan []interface{})
		endpoint, ok := feed.endpoints[raddr]
		if ok && !endpoint.Ping() { // delete endpoint only if not alive.
			delete(feed.endpoints, raddr)
			logging.Infof("%v endpoint %v deleted\n", feed.logPrefix, raddr)
		}
		// If there are no more endpoints, shutdown the feed.
		// Note that this feed might still be referred by the applications
		// book-keeping entries. It is upto the application to detect
		// that this feed has closed and clean up itself.
		if len(feed.endpoints) == 0 {
			fmsg := "%v no endpoint left automatic shutdown\n"
			logging.Infof(fmsg, feed.logPrefix)
			respch <- []interface{}{feed.shutdown(feed.opaque /*opaque*/)}
			status = "exit"
		}

	case fCmdPing:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{true}
	}
	return status
}

// start a new feed.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) start(
	req *protobuf.MutationTopicRequest, opaque uint16) (err error) {

	feed.endpointType = req.GetEndpointType()

	// update engines and endpoints
	if err = feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return err
	}
	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos) // take only local vbuckets

		actTs, ok := feed.actTss[bucketn]
		if ok { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[bucketn]
		if ok { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[bucketn]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(reqTs)
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		engines, _ := feed.engines[bucketn]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// start upstream, after filtering out vbuckets.
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		// wait for stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect:
		// forget vbuckets for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
			logging.Errorf(
				"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque, err,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		} else {
			logging.Infof(
				"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		}
	}
	return err
}

// a subset of upstreams are restarted.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) restartVbuckets(
	req *protobuf.RestartVbucketsRequest, opaque uint16) (err error) {

	// FIXME: restart-vbuckets implies a repair Endpoint.
	raddrs := feed.endpointRaddrs()
	rpReq := protobuf.NewRepairEndpointsRequest(feed.topic, raddrs)
	if err := feed.repairEndpoints(rpReq, opaque); err != nil {
		return err
	}

	for _, ts := range req.GetRestartTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok1 := feed.actTss[bucketn]
		rollTs, ok2 := feed.rollTss[bucketn]
		reqTs, ok3 := feed.reqTss[bucketn]
		engines, ok4 := feed.engines[bucketn]

		if !ok1 || !ok2 || !ok3 || !ok4 || len(engines) == 0 {
			fmsg := "%v ##%x restartVbuckets() invalid-bucket %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		if ok1 { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		if ok2 { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok3 {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(ts)

		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// (re)start the upstream, after filtering out remote vbuckets.
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		// wait stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect:
		// forget vbuckets for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
			logging.Errorf(
				"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque, err,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		} else {
			logging.Infof(
				"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		}
	}
	return err
}

// a subset of upstreams are closed.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamEnd if StreamEnd failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) shutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest, opaque uint16) (err error) {

	// iterate request-timestamp for each bucket.
	for _, ts := range req.GetShutdownTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			//FIXME: in case of shutdown we are not cleaning the bucket !
			//wait for the code to settle-down and remove this.
			//feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok1 := feed.actTss[bucketn]
		rollTs, ok2 := feed.rollTss[bucketn]
		reqTs, ok3 := feed.reqTss[bucketn]
		if !ok1 || !ok2 || !ok3 {
			fmsg := "%v ##%x shutdownVbuckets() invalid-bucket %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		// only shutdown active-streams.
		if ok1 && actTs != nil {
			ts = ts.SelectByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		feeder, ok := feed.feeders[bucketn]
		if !ok {
			fmsg := "%v ##%x shutdownVbuckets() invalid-feeder %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		// shutdown upstream
		e = feed.bucketFeed(opaque, true, false, ts, feeder)
		if e != nil {
			err = e
			//FIXME: in case of shutdown we are not cleaning the bucket !
			//wait for the code to settle-down and remove this.
			//feed.cleanupBucket(bucketn, false)
			continue
		}
		endTs, _, e := feed.waitStreamEnds(opaque, bucketn, ts)
		vbnos = c.Vbno32to16(endTs.GetVbnos())
		// forget vbnos that are shutdown
		feed.actTss[bucketn] = actTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.reqTss[bucketn] = reqTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.rollTss[bucketn] = rollTs.FilterByVbuckets(vbnos) // :SideEffect:
		if e != nil {
			err = e
			logging.Errorf(
				"%v ##%x stream-end (err: %v) vbnos: %v\n",
				feed.logPrefix, opaque, err, vbnos)
		} else {
			logging.Infof(
				"%v ##%x stream-end (success) vbnos: %v\n",
				feed.logPrefix, opaque, vbnos)
		}
	}
	return err
}

// upstreams are added for buckets data-path opened and
// vbucket-routines started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) addBuckets(
	req *protobuf.AddBucketsRequest, opaque uint16) (err error) {

	// update engines and endpoints
	if err = feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return err
	}

	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok := feed.actTss[bucketn]
		if ok { // don't re-request for already active vbuckets
			ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[bucketn]
		if ok { // foget previous rollback for the current set of buckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[bucketn]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(ts)
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		engines, _ := feed.engines[bucketn]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// start upstream
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		// wait for stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect
		// forget vbucket for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
			logging.Errorf(
				"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque, err,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		} else {
			logging.Infof(
				"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
				feed.logPrefix, opaque,
				feed.rollTss[bucketn].GetVbnos(),
				feed.actTss[bucketn].GetVbnos())
		}
	}
	return err
}

// upstreams are closed for buckets, data-path is closed for downstream,
// vbucket-routines exits on StreamEnd
func (feed *Feed) delBuckets(
	req *protobuf.DelBucketsRequest, opaque uint16) error {

	for _, bucketn := range req.GetBuckets() {
		feed.cleanupBucket(bucketn, true)
	}
	return nil
}

// only data-path shall be updated.
// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) addInstances(
	req *protobuf.AddInstancesRequest,
	opaque uint16) (*protobuf.TimestampResponse, error) {

	tsResp := &protobuf.TimestampResponse{
		Topic:             proto.String(feed.topic),
		CurrentTimestamps: make([]*protobuf.TsVbuuid, 0, 4),
	}
	errResp := &protobuf.TimestampResponse{Topic: proto.String(feed.topic)}

	// update engines and endpoints
	if err := feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return errResp, err
	}
	var err error
	// post to kv data-path
	for bucketn, engines := range feed.engines {
		if kvdata, ok := feed.kvdata[bucketn]; ok {
			curSeqnos, err := kvdata.AddEngines(opaque, engines, feed.endpoints)
			if err != nil {
				return errResp, err
			}
			tsResp = tsResp.AddCurrentTimestamp(feed.pooln, bucketn, curSeqnos)

		} else {
			fmsg := "%v ##%x addInstances() invalid-bucket %q\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
		}
	}
	return tsResp, err
}

// only data-path shall be updated.
// * if it is the last instance defined on the bucket, then
//   use delBuckets() API to delete the bucket.
func (feed *Feed) delInstances(
	req *protobuf.DelInstancesRequest, opaque uint16) error {

	// reconstruct instance uuids bucket-wise.
	instanceIds := req.GetInstanceIds()
	bucknIds := make(map[string][]uint64)           // bucket -> []instance
	fengines := make(map[string]map[uint64]*Engine) // bucket-> uuid-> instance
	for bucketn, engines := range feed.engines {
		uuids := make([]uint64, 0)
		m := make(map[uint64]*Engine)
		for uuid, engine := range engines {
			if c.HasUint64(uuid, instanceIds) {
				uuids = append(uuids, uuid)
			} else {
				m[uuid] = engine
			}
		}
		bucknIds[bucketn] = uuids
		fengines[bucketn] = m
	}
	var err error
	// posted post to kv data-path.
	for bucketn, uuids := range bucknIds {
		if _, ok := feed.kvdata[bucketn]; ok {
			feed.kvdata[bucketn].DeleteEngines(opaque, uuids)
		} else {
			fmsg := "%v ##%x delInstances() invalid-bucket %q"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
		}
	}
	feed.engines = fengines // :SideEffect:
	return err
}

// endpoints are independent.
func (feed *Feed) repairEndpoints(
	req *protobuf.RepairEndpointsRequest, opaque uint16) (err error) {

	prefix := feed.logPrefix
	for _, raddr := range req.GetEndpoints() {
		logging.Infof("%v ##%x trying to repair %q\n", prefix, opaque, raddr)
		raddr1, endpoint, e := feed.getEndpoint(raddr, opaque)
		if e != nil {
			err = e
			continue

		} else if (endpoint == nil) || !endpoint.Ping() {
			topic, typ := feed.topic, feed.endpointType
			config := feed.config.SectionConfig("dataport.", true /*trim*/)
			endpoint, e = feed.epFactory(topic, typ, raddr, config)
			if e != nil {
				fmsg := "%v ##%x endpoint-factory %q: %v\n"
				logging.Errorf(fmsg, prefix, opaque, raddr1, e)
				err = e
				continue
			}
			go feed.watchEndpoint(raddr, endpoint)
			fmsg := "%v ##%x endpoint %q restarted\n"
			logging.Infof(fmsg, prefix, opaque, raddr)

		} else {
			fmsg := "%v ##%x endpoint %q active ...\n"
			logging.Infof(fmsg, prefix, opaque, raddr)
		}
		// FIXME: hack to make both node-name available from
		// endpoints table.
		feed.endpoints[raddr] = endpoint  // :SideEffect:
		feed.endpoints[raddr1] = endpoint // :SideEffect:
	}

	// posted to each kv data-path
	for bucketn, kvdata := range feed.kvdata {
		// though only endpoints have been updated
		kvdata.AddEngines(opaque, feed.engines[bucketn], feed.endpoints)
	}
	//return nil
	return err
}

// return,
// "ok", feed is active.
// "stale", feed is stale.
func (feed *Feed) staleCheck() string {
	raddrs := []string{}
	for raddr, endpoint := range feed.endpoints {
		if endpoint.Ping() {
			return "ok" // feed active
		}
		raddrs = append(raddrs, raddr)
	}
	return "stale"
}

func (feed *Feed) getStatistics() c.Statistics {
	stats, _ := c.NewStatistics(nil)
	stats.Set("topic", feed.topic)
	stats.Set("engines", feed.engineNames())
	for bucketn, kvdata := range feed.kvdata {
		stats.Set("bucket-"+bucketn, kvdata.GetStatistics())
	}
	endStats, _ := c.NewStatistics(nil)
	for raddr, endpoint := range feed.endpoints {
		endStats.Set(raddr, endpoint.GetStatistics())
	}
	stats.Set("endpoints", endStats)
	return stats
}

func (feed *Feed) resetConfig(config c.Config) {
	if cv, ok := config["feedWaitStreamReqTimeout"]; ok {
		feed.reqTimeout = time.Duration(cv.Int())
	}
	if cv, ok := config["feedWaitStreamEndTimeout"]; ok {
		feed.endTimeout = time.Duration(cv.Int())
	}
	// pass the configuration to active kvdata
	for _, kvdata := range feed.kvdata {
		kvdata.ResetConfig(config)
	}
	// pass the configuration to active endpoints
	econf := config.SectionConfig("dataport.", true /*trim*/)
	for _, endpoint := range feed.endpoints {
		endpoint.ResetConfig(econf)
	}
	feed.config = feed.config.Override(config)
}

func (feed *Feed) shutdown(opaque uint16) error {
	recovery := func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x shutdown() crashed: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}

	// close upstream
	for _, feeder := range feed.feeders {
		func() { defer recovery(); feeder.CloseFeed() }()
	}
	// close data-path
	for bucketn, kvdata := range feed.kvdata {
		func() { defer recovery(); kvdata.Close() }()
		delete(feed.kvdata, bucketn) // :SideEffect:
	}
	// close downstream
	for _, endpoint := range feed.endpoints {
		func() { defer recovery(); endpoint.Close() }()
	}
	// cleanup
	close(feed.finch)
	logging.Infof("%v ##%x feed ... stopped\n", feed.logPrefix, feed.opaque)
	return nil
}

// shutdown upstream, data-path and remove data-structure for this bucket.
func (feed *Feed) cleanupBucket(bucketn string, enginesOk bool) {
	if enginesOk {
		delete(feed.engines, bucketn) // :SideEffect:
	}
	delete(feed.reqTss, bucketn)  // :SideEffect:
	delete(feed.actTss, bucketn)  // :SideEffect:
	delete(feed.rollTss, bucketn) // :SideEffect:
	// close upstream
	feeder, ok := feed.feeders[bucketn]
	if ok {
		feeder.CloseFeed()
	}
	delete(feed.feeders, bucketn) // :SideEffect:
	// cleanup data structures.
	if kvdata, ok := feed.kvdata[bucketn]; ok {
		kvdata.Close()
	}
	delete(feed.kvdata, bucketn) // :SideEffect:
	fmsg := "%v ##%x bucket %v removed ..."
	logging.Infof(fmsg, feed.logPrefix, feed.opaque, bucketn)
}

func (feed *Feed) openFeeder(
	opaque uint16, pooln, bucketn string) (BucketFeeder, error) {

	feeder, ok := feed.feeders[bucketn]
	if ok {
		return feeder, nil
	}
	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn, opaque)
	if err != nil {
		return nil, projC.ErrorFeeder
	}

	uuid, err := c.NewUUID()
	if err != nil {
		fmsg := "%v ##%x c.NewUUID(): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		return nil, err
	}
	name := newDCPConnectionName(bucket.Name, feed.topic, uuid.Uint64())
	dcpConfig := map[string]interface{}{
		"genChanSize":    feed.config["dcp.genChanSize"].Int(),
		"dataChanSize":   feed.config["dcp.dataChanSize"].Int(),
		"numConnections": feed.config["dcp.numConnections"].Int(),
		"latencyTick":    feed.config["dcp.latencyTick"].Int(),
	}
	kvaddr, err := feed.getLocalKVAddrs(pooln, bucketn, opaque)
	if err != nil {
		return nil, err
	}
	kvaddrs := []string{kvaddr}
	feeder, err = OpenBucketFeed(name, bucket, opaque, kvaddrs, dcpConfig)
	if err != nil {
		fmsg := "%v ##%x OpenBucketFeed(%q): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorFeeder
	}
	return feeder, nil
}

// start a feed for a bucket with a set of kvfeeder,
// based on vbmap and failover-logs.
func (feed *Feed) bucketFeed(
	opaque uint16, stop, start bool,
	reqTs *protobuf.TsVbuuid, feeder BucketFeeder) error {

	pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()

	vbnos := c.Vbno32to16(reqTs.GetVbnos())
	_ /*vbuuids*/, err := feed.bucketDetails(pooln, bucketn, opaque, vbnos)
	if err != nil {
		return projC.ErrorFeeder
	}

	// stop and start are mutually exclusive
	if stop {
		fmsg := "%v ##%x stop-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err = feeder.EndVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x EndVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder
		}

	} else if start {
		fmsg := "%v ##%x start-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err = feeder.StartVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x StartVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder
		}
	}
	return nil
}

// - return dcp-client failures.
func (feed *Feed) bucketDetails(
	pooln, bucketn string, opaque uint16, vbnos []uint16) ([]uint64, error) {

	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn, opaque)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	// failover-logs
	dcpConfig := map[string]interface{}{
		"genChanSize":    feed.config["dcp.genChanSize"].Int(),
		"dataChanSize":   feed.config["dcp.dataChanSize"].Int(),
		"numConnections": feed.config["dcp.numConnections"].Int(),
	}
	flogs, err := bucket.GetFailoverLogs(opaque, vbnos, dcpConfig)
	if err != nil {
		fmsg := "%v ##%x GetFailoverLogs(%q): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, err
	}
	vbuuids := make([]uint64, len(vbnos))
	for i, vbno := range vbnos {
		flog := flogs[vbno]
		if len(flog) < 1 {
			fmsg := "%v ##%x %q.FailoverLog() empty"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			return nil, projC.ErrorInvalidVbucket
		}
		latestVbuuid, _, err := flog.Latest()
		if err != nil {
			fmsg := "%v ##%x %q.FailoverLog invalid log"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			return nil, err
		}
		vbuuids[i] = latestVbuuid
	}

	return vbuuids, nil
}

func (feed *Feed) getLocalKVAddrs(
	pooln, bucketn string, opaque uint16) (string, error) {

	prefix := feed.logPrefix
	url, err := c.ClusterAuthUrl(feed.config["clusterAddr"].String())
	if err != nil {
		fmsg := "%v ##%x ClusterAuthUrl(): %v\n"
		logging.Errorf(fmsg, prefix, opaque, err)
		return "", projC.ErrorClusterInfo
	}
	cinfo, err := c.NewClusterInfoCache(url, pooln)
	if err != nil {
		fmsg := "%v ##%x ClusterInfoCache(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return "", projC.ErrorClusterInfo
	}
	if err := cinfo.Fetch(); err != nil {
		fmsg := "%v ##%x cinfo.Fetch(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return "", projC.ErrorClusterInfo
	}
	kvaddr, err := cinfo.GetLocalServiceAddress("kv")
	if err != nil {
		fmsg := "%v ##%x cinfo.GetLocalServiceAddress(`kv`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, err)
		return "", projC.ErrorClusterInfo
	}
	return kvaddr, nil
}

func (feed *Feed) getLocalVbuckets(
	pooln, bucketn string, opaque uint16) ([]uint16, error) {

	prefix := feed.logPrefix
	// gather vbnos based on colocation policy.
	var cinfo *c.ClusterInfoCache
	url, err := c.ClusterAuthUrl(feed.config["clusterAddr"].String())
	if err == nil {
		cinfo, err = c.NewClusterInfoCache(url, pooln)
	}
	if err != nil {
		fmsg := "%v ##%x ClusterInfoCache(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	if err := cinfo.Fetch(); err != nil {
		fmsg := "%v ##%x cinfo.Fetch(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	nodeID := cinfo.GetCurrentNode()
	vbnos32, err := cinfo.GetVBuckets(nodeID, bucketn)
	if err != nil {
		fmsg := "%v ##%x cinfo.GetVBuckets(%d, `%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, nodeID, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	vbnos := c.Vbno32to16(vbnos32)
	fmsg := "%v ##%x vbmap {%v,%v} - %v\n"
	logging.Infof(fmsg, prefix, opaque, pooln, bucketn, vbnos)
	return vbnos, nil
}

// start data-path each kvaddr
func (feed *Feed) startDataPath(
	bucketn string, feeder BucketFeeder,
	opaque uint16,
	ts *protobuf.TsVbuuid) *KVData {

	mutch := feeder.GetChannel()
	kvdata, ok := feed.kvdata[bucketn]
	if ok {
		kvdata.UpdateTs(opaque, ts)

	} else { // pass engines & endpoints to kvdata.
		engs, ends := feed.engines[bucketn], feed.endpoints
		kvdata = NewKVData(
			feed, bucketn, opaque, ts, engs, ends, mutch, feed.config)
	}
	return kvdata
}

// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) processSubscribers(opaque uint16, req Subscriber) error {
	evaluators, routers, err := feed.subscribers(opaque, req)
	if err != nil {
		return err
	}

	// start fresh set of all endpoints from routers.
	if err = feed.startEndpoints(opaque, routers); err != nil {
		return err
	}
	// update feed engines.
	for uuid, evaluator := range evaluators {
		bucketn := evaluator.Bucket()
		m, ok := feed.engines[bucketn]
		if !ok {
			m = make(map[uint64]*Engine)
		}
		engine := NewEngine(uuid, evaluator, routers[uuid])
		m[uuid] = engine
		feed.engines[bucketn] = m // :SideEffect:
	}
	return nil
}

// feed.endpoints is updated with freshly started endpoint,
// if an endpoint is already present and active it is
// reused.
func (feed *Feed) startEndpoints(
	opaque uint16, routers map[uint64]c.Router) (err error) {

	prefix := feed.logPrefix
	for _, router := range routers {
		for _, raddr := range router.Endpoints() {
			raddr1, endpoint, e := feed.getEndpoint(raddr, opaque)
			if e != nil {
				err = e
				continue

			} else if endpoint == nil || !endpoint.Ping() {
				topic, typ := feed.topic, feed.endpointType
				config := feed.config.SectionConfig("dataport.", true /*trim*/)
				endpoint, e = feed.epFactory(topic, typ, raddr, config)
				if e != nil {
					fmsg := "%v ##%x endpoint-factory %q: %v\n"
					logging.Errorf(fmsg, prefix, opaque, raddr1, e)
					err = e
					continue
				}
				go feed.watchEndpoint(raddr, endpoint)
				fmsg := "%v ##%x endpoint %q started\n"
				logging.Infof(fmsg, prefix, opaque, raddr)

			} else {
				fmsg := "%v ##%x endpoint %q active ...\n"
				logging.Infof(fmsg, prefix, opaque, raddr)
			}
			// FIXME: hack to make both node-name available from
			// endpoints table.
			feed.endpoints[raddr] = endpoint  // :SideEffect:
			feed.endpoints[raddr1] = endpoint // :SideEffect:
		}
	}
	//return nil
	return err
}

func (feed *Feed) getEndpoint(
	raddr string, opaque uint16) (string, c.RouterEndpoint, error) {

	prefix := feed.logPrefix
	_, eqRaddr, err := c.EquivalentIP(raddr, feed.endpointRaddrs())
	if err != nil {
		fmsg := "%v ##%x EquivalentIP() for %q: %v"
		logging.Errorf(fmsg, prefix, opaque, raddr, err)
		return raddr, nil, err

	} else if raddr != eqRaddr {
		fmsg := "%v ##%x endpoint %q takenas %q ..."
		logging.Warnf(fmsg, prefix, opaque, raddr, eqRaddr)
		raddr = eqRaddr
	}
	endpoint, ok := feed.endpoints[raddr]
	if ok {
		return raddr, endpoint, nil
	}
	return raddr, nil, nil
}

// - return ErrorInconsistentFeed for malformed feeds.
func (feed *Feed) subscribers(
	opaque uint16,
	req Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {

	evaluators, err := req.GetEvaluators()
	if err != nil {
		fmsg := "%v ##%x malformed evaluators: %v\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque, err)
		return nil, nil, projC.ErrorInconsistentFeed
	}
	routers, err := req.GetRouters()
	if err != nil {
		fmsg := "%v ##%x malformed routers: %v\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque, err)
		return nil, nil, projC.ErrorInconsistentFeed
	}

	if len(evaluators) != len(routers) {
		fmsg := "%v ##%x mismatch in evaluators/routers\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque)
		return nil, nil, projC.ErrorInconsistentFeed
	}
	fmsg := "%v ##%x uuid mismatch: %v\n"
	for uuid := range evaluators {
		if _, ok := routers[uuid]; ok == false {
			logging.Fatalf(fmsg, feed.logPrefix, opaque, uuid)
			return nil, nil, projC.ErrorInconsistentFeed
		}
	}
	return evaluators, routers, nil
}

func (feed *Feed) engineNames() []string {
	names := make([]string, 0, len(feed.engines))
	for uuid := range feed.engines {
		names = append(names, fmt.Sprintf("%v", uuid))
	}
	return names
}

func (feed *Feed) endpointRaddrs() []string {
	raddrs := make([]string, 0, len(feed.endpoints))
	for raddr := range feed.endpoints {
		raddrs = append(raddrs, raddr)
	}
	return raddrs
}

// wait for kvdata to post StreamRequest.
// - return ErrorResponseTimeout if feedback is not completed within timeout
// - return ErrorNotMyVbucket if vbucket has migrated.
// - return ErrorStreamEnd for failed stream-end request.
func (feed *Feed) waitStreamRequests(
	opaque uint16,
	pooln, bucketn string,
	ts *protobuf.TsVbuuid) (rollTs, failTs, actTs *protobuf.TsVbuuid, err error) {

	vbnos := c.Vbno32to16(ts.GetVbnos())
	rollTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	failTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	actTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	if len(vbnos) == 0 {
		return rollTs, failTs, actTs, nil
	}

	timeout := time.After(feed.reqTimeout * time.Millisecond)
	err1 := feed.waitOnFeedback(timeout, opaque, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamRequest); ok && val.bucket == bucketn &&
			val.opaque == opaque && ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				actTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.ROLLBACK {
				rollTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = projC.ErrorStreamRequest
			}
			vbnos = c.RemoveUint16(val.vbno, vbnos)
			if len(vbnos) == 0 {
				return "done"
			}
			return "ok"
		}
		return "skip"
	})
	if err == nil {
		err = err1
	}
	return rollTs, failTs, actTs, err
}

// wait for kvdata to post StreamEnd.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
// - return ErrorNotMyVbucket if vbucket has migrated.
// - return ErrorStreamEnd for failed stream-end request.
func (feed *Feed) waitStreamEnds(
	opaque uint16,
	bucketn string,
	ts *protobuf.TsVbuuid) (endTs, failTs *protobuf.TsVbuuid, err error) {

	vbnos := c.Vbno32to16(ts.GetVbnos())
	endTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	failTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	if len(vbnos) == 0 {
		return endTs, failTs, nil
	}

	timeout := time.After(feed.endTimeout * time.Millisecond)
	err1 := feed.waitOnFeedback(timeout, opaque, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamEnd); ok && val.bucket == bucketn &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				endTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = projC.ErrorStreamEnd
			}
			vbnos = c.RemoveUint16(val.vbno, vbnos)
			if len(vbnos) == 0 {
				return "done"
			}
			return "ok"
		}
		return "skip"
	})
	if err == nil {
		err = err1
	}
	return endTs, failTs, err
}

// block feed until feedback posted back from kvdata.
// - return ErrorResponseTimeout if feedback is not completed within timeout
func (feed *Feed) waitOnFeedback(
	timeout <-chan time.Time,
	opaque uint16, callb func(msg interface{}) string) (err error) {

	msgs := make([][]interface{}, 0)
loop:
	for {
		select {
		case msg := <-feed.backch:
			switch callb(msg[0]) {
			case "skip":
				msgs = append(msgs, msg)
			case "done":
				break loop
			case "ok":
			}

		case <-timeout:
			logging.Errorf("%v ##%x dcp-timeout\n", feed.logPrefix, opaque)
			err = projC.ErrorResponseTimeout
			break loop
		}
	}
	// re-populate in the same order.
	if len(msgs) > 0 {
		fmsg := "%v ##%x re-populating back-channel with %d messages"
		logging.Infof(fmsg, feed.logPrefix, opaque, len(msgs))
	}
	for _, msg := range msgs {
		feed.backch <- msg
	}
	return
}

// compose topic-response for caller
func (feed *Feed) topicResponse() *protobuf.TopicResponse {
	uuids := make([]uint64, 0)
	for _, engines := range feed.engines {
		for uuid := range engines {
			uuids = append(uuids, uuid)
		}
	}
	xs := make([]*protobuf.TsVbuuid, 0, len(feed.actTss))
	for _, ts := range feed.actTss {
		if ts != nil {
			xs = append(xs, ts)
		}
	}
	ys := make([]*protobuf.TsVbuuid, 0, len(feed.rollTss))
	for _, ts := range feed.rollTss {
		if ts != nil && !ts.IsEmpty() {
			ys = append(ys, ts)
		}
	}
	return &protobuf.TopicResponse{
		Topic:              proto.String(feed.topic),
		InstanceIds:        uuids,
		ActiveTimestamps:   xs,
		RollbackTimestamps: ys,
	}
}

// generate a unique opaque identifier.
// NOTE: be careful while changing the DCP name, it might affect other
// parts of the system. ref: https://issues.couchbase.com/browse/MB-14300
func newDCPConnectionName(bucketn, topic string, uuid uint64) couchbase.DcpFeedName {
	return couchbase.NewDcpFeedName(fmt.Sprintf("proj-%s-%s-%v", bucketn, topic, uuid))
}

//---- endpoint watcher

func (feed *Feed) watchEndpoint(raddr string, endpoint c.RouterEndpoint) {
	err := endpoint.WaitForExit() // <-- will block until endpoint exits.
	logging.Infof("%v endpoint exited: %v", feed.logPrefix, err)
	if err := feed.DeleteEndpoint(raddr); err != nil && err != c.ErrorClosed {
		fmsg := "%v failed DeleteEndpoint(): %v"
		logging.Errorf(fmsg, feed.logPrefix, err)
	}
}

//---- local function

// connectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func (feed *Feed) connectBucket(
	cluster, pooln, bucketn string, opaque uint16) (*couchbase.Bucket, error) {

	ah := &c.CbAuthHandler{Hostport: cluster, Bucket: bucketn}
	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		fmsg := "%v ##%x connectBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorDCPConnection
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		fmsg := "%v ##%x GetPool(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, pooln, err)
		return nil, projC.ErrorDCPPool
	}
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		fmsg := "%v ##%x GetBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorDCPBucket
	}
	return bucket, nil
}

// FeedConfigParams return the list of configuration params
// supported by a feed.
func FeedConfigParams() []string {
	paramNames := []string{
		"clusterAddr",
		"feedChanSize",
		"backChanSize",
		"vbucketWorkers",
		"feedWaitStreamEndTimeout",
		"feedWaitStreamReqTimeout",
		"mutationChanSize",
		"encodeBufSize",
		"routerEndpointFactory",
		"syncTimeout",
		"kvstatTick",
		// dcp configuration
		"dcp.dataChanSize",
		"dcp.genChanSize",
		"dcp.numConnections",
		"dcp.latencyTick",
		// dataport
		"dataport.remoteBlock",
		"dataport.keyChanSize",
		"dataport.bufferSize",
		"dataport.bufferTimeout",
		"dataport.harakiriTimeout",
		"dataport.statTick",
		"dataport.maxPayload"}
	return paramNames
}
