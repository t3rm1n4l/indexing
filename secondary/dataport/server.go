// A gen server behavior for dataport consumer.
//
// Daemon listens for new connections and spawns a reader routine
// for each new connection.
//
// concurrency model:
//
//                                   Application back channels,
//                               mutation channel and sideband channel
//                          -----------------------------------------------
//                                                      ^
//     NewServer() ----------*                          |
//             |             |                          | []*VbKeyVersions
//          (spawn)          |                          |
//             |             |    *---------------------*
//             |          (spawn) |                     |
//           listener()      |    | ConnectionError     |
//                 |         |    |                     |
//   serverCmdNewConnection  |    |                     |
//                 |         |    |                     |
//                 V         |    |                     |
//  Close() -------*------->gen-server()-----*---- doReceive()----*
//          serverCmdClose       ^           |                    |
//                               |           *---- doReceive()----*
//                serverCmdVbmap |           |                    |
//        serverCmdVbKeyVersions |           *---- doReceive()----*
//                serverCmdError |                                |
//                               *--------------------------------*
//                                          (control & faults)
//
// server behavior:
//
// 1. can handle more than one connection from same router.
//
// 2. whenever a connection with router
//    a. gets closed
//    b. or timeout
//    all connections with that router will be closed and same will
//    be intimated to application for catchup connection, using
//    ConnectionError message.
//
// 3. StreamEnd, ConnectionError can be seen by serve due to,
//    a. rebalance
//    b. failover
//    c. projector crash
//    d. network partition
//    e. DCP dropping the connection
//    f. partial stream start
//    g. bucket delete
//    h. bucket flush
//    i. DCP feed error

package dataport

import "errors"
import "fmt"
import "io"
import "net"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
import "github.com/couchbase/indexing/secondary/transport"
import "github.com/couchbase/indexing/secondary/logging"

// Error codes

// ErrorPayload
var ErrorPayload = errors.New("dataport.daemonPayload")

// ErrorDuplicateStreamBegin
var ErrorDuplicateStreamBegin = errors.New("dataport.duplicateStreamBegin")

// ErrorMissingStreamBegin
var ErrorMissingStreamBegin = errors.New("dataport.missingStreamBegin")

// ErrorDaemonExit
var ErrorDaemonExit = errors.New("dataport.daemonExit")

// ErrorDuplicateClient
var ErrorDuplicateClient = errors.New("dataport.duplicateClient")

// ErrorWorkerKilled
var ErrorWorkerKilled = errors.New("dataport.workerKilled")

type activeVb struct {
	raddr  string // remote connection carrying this vbucket.
	bucket string
	vbno   uint16
	kvers  uint64
	seqno  uint64
}

type keeper map[string]*activeVb

func (avb *activeVb) id() string {
	return fmt.Sprintf("%v-%v-%v", avb.raddr, avb.bucket, avb.vbno)
}

func (hostUuids keeper) isActive(bucket string, vbno uint16) bool {
	for _, avb := range hostUuids {
		if avb.bucket == bucket && avb.vbno == vbno {
			return true
		}
	}
	return false
}

// messages to gen-server
type serverMessage struct {
	cmd   byte          // gen server command
	raddr string        // remote connection address, optional
	args  []interface{} // command arguments
	err   error         // in case the message is to notify error
}

// maintain information about each remote connection.
type netConn struct {
	conn   net.Conn
	worker chan interface{}
	active bool
	tpkt   *transport.TransportPacket
}

// Server handles an active dataport server of mutation for all vbuckets.
type Server struct {
	laddr string // address to listen
	lis   net.Listener
	appch chan<- interface{} // backchannel to application

	// gen-server management
	conns map[string]*netConn // resolve <host:port> to conn. obj
	reqch chan []interface{}
	finch chan bool

	// config parameters
	maxVbuckets  int
	genChSize    int           // channel size for genServer routine
	maxPayload   int           // maximum payload length from router
	readDeadline time.Duration // timeout, in millisecond, reading from socket
	logPrefix    string
}

// NewServer creates a new dataport daemon.
func NewServer(
	laddr string,
	maxvbs int,
	config c.Config,
	appch chan<- interface{}) (s *Server, err error) {

	genChSize := config["genServerChanSize"].Int()

	s = &Server{
		laddr: laddr,
		appch: appch,
		// Managing vbuckets and connections for all routers
		reqch: make(chan []interface{}, genChSize),
		finch: make(chan bool),
		conns: make(map[string]*netConn),
		// config parameters
		maxVbuckets:  maxvbs,
		genChSize:    genChSize,
		maxPayload:   config["maxPayload"].Int(),
		readDeadline: time.Duration(config["tcpReadDeadline"].Int()),
	}
	s.logPrefix = fmt.Sprintf("DATP[->dataport %q]", laddr)
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		logging.Errorf("%v failed starting ! %v\n", s.logPrefix, err)
		return nil, err
	}
	go listener(s.logPrefix, s.lis, s.reqch) // spawn daemon
	go s.genServer(s.reqch)                  // spawn gen-server
	logging.Infof("%v started ...", s.logPrefix)
	return s, nil
}

func (s *Server) addUuids(started, hostUuids keeper) keeper {
	for x, newvb := range started {
		if hostUuids.isActive(newvb.bucket, newvb.vbno) {
			logging.Errorf("%v duplicate vbucket %v\n", s.logPrefix, newvb.id())
		}
		hostUuids[x] = newvb
		logging.Debugf("%v added vbucket %v\n", s.logPrefix, newvb.id())
	}
	return hostUuids
}

func (s *Server) delUuids(finished, hostUuids keeper) keeper {
	for x := range finished {
		avb, ok := hostUuids[x]
		if ok {
			delete(hostUuids, x)
			logging.Debugf("%v deleted vbucket %v\n", s.logPrefix, avb.id())

		} else {
			logging.Errorf("%v not active vbucket %v\n", s.logPrefix, x)
		}
	}
	return hostUuids
}

// Close the daemon listening for new connections and shuts down all read
// routines for this dataport server. synchronous call.
func (s *Server) Close() (err error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{serverMessage{cmd: serverCmdClose}, respch}
	resp, err := c.FailsafeOp(s.reqch, respch, cmd, s.finch)
	return c.OpError(err, resp, 0)
}

// gen-server commands
const (
	serverCmdNewConnection byte = iota + 1
	serverCmdVbmap
	serverCmdVbKeyVersions
	serverCmdError
	serverCmdClose
)

// gen server routine for dataport server.
func (s *Server) genServer(reqch chan []interface{}) {
	var appmsg interface{}

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v gen-server crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
			s.handleClose()
		}
	}()

	hostUuids := make(keeper) // id() -> activeVb
	parseVbs := func(msg serverMessage) []*protobuf.VbKeyVersions {
		vbs := msg.args[0].([]*protobuf.VbKeyVersions)
		prune_off := 0
		for i := 0; i < len(vbs); i++ { //for each vbucket
			vb := vbs[i]
			bucket, vbno := vb.GetBucketname(), uint16(vb.GetVbucket())
			id := (&activeVb{raddr: msg.raddr, bucket: bucket, vbno: vbno}).id()
			kvs := vb.GetKvs() // mutations for each vbucket

			// filter mutations for vbucket that is not from the same
			// source as its StreamBegin.
			avb, avbok := hostUuids[id]
			if avbok && (msg.raddr != avb.raddr) {
				fmsg := "%v filter %d mutations for %v\n"
				logging.Warnf(fmsg, s.logPrefix, len(kvs), id)
				continue
			}
			vbok := false
			for _, kv := range kvs {
				if len(kv.GetCommands()) == 0 {
					continue
				}
				switch byte(kv.GetCommands()[0]) {
				case c.StreamBegin: // new vbucket stream(s) have started
					avb = &activeVb{raddr: msg.raddr, bucket: bucket, vbno: vbno}
					hostUuids = s.addUuids(keeper{id: avb}, hostUuids)
					vbok = true

				case c.StreamEnd: // vbucket stream(s) have finished
					avb = &activeVb{raddr: msg.raddr, bucket: bucket, vbno: vbno}
					if _, ok := hostUuids[id]; ok {
						hostUuids = s.delUuids(keeper{id: avb}, hostUuids)
						vbok = true
					} else {
						fmsg := "%v StreamEnd without StreamBegin for %v\n"
						logging.Warnf(fmsg, s.logPrefix, id)
					}
				case c.Upsert, c.Deletion, c.UpsertDeletion:
					if avbok && avb != nil {
						avb.seqno = kv.GetSeqno()
						avb.kvers++
					}
				}
			}
			// mutations received without a STREAM_BEGIN
			if _, ok := hostUuids[id]; ok || vbok {
				vbs[prune_off] = vb
				prune_off++
			} else {
				fmsg := "%v mutations filtered for %v\n"
				logging.Warnf(fmsg, s.logPrefix, id)
			}
			logging.Tracef("%v {%v, %v}\n", s.logPrefix, bucket, vbno)
		}
		vbs = vbs[:prune_off]
		return vbs
	}

loop:
	for {
		appmsg = nil
		select {
		case cmd := <-reqch:
			msg := cmd[0].(serverMessage)
			switch msg.cmd {
			case serverCmdNewConnection:
				conn, raddr := msg.args[0].(net.Conn), msg.raddr
				if _, ok := s.conns[raddr]; ok {
					logging.Errorf("%v %q already active\n", s.logPrefix, raddr)
					conn.Close()

				} else { // connection accepted
					worker := make(chan interface{}, s.maxVbuckets)
					s.conns[raddr] = &netConn{
						conn: conn, worker: worker,
						tpkt: newTransportPkt(s.maxPayload),
					}
					n := len(s.conns)
					fmsg := "%v new connection %q +%d\n"
					logging.Infof(fmsg, s.logPrefix, raddr, n)
					s.startWorker(raddr)
				}

			case serverCmdVbmap:
				vbmap := msg.args[0].(*protobuf.VbConnectionMap)
				b, raddr := vbmap.GetBucket(), msg.raddr
				for _, vbno := range vbmap.GetVbuckets() {
					avb := &activeVb{raddr: raddr, bucket: b, vbno: uint16(vbno)}
					hostUuids[avb.id()] = avb
				}
				s.startWorker(msg.raddr)

			case serverCmdVbKeyVersions:
				s.appch <- parseVbs(msg)

			case serverCmdClose: // This execution path never panics !!
				// before closing the dataport-server log a consolidated
				// stats on the active-vbuckets.
				s.logStats(hostUuids)
				respch := cmd[1].(chan []interface{})
				s.handleClose()
				respch <- []interface{}{nil}
				break loop

			case serverCmdError:
				hostUuids, appmsg =
					s.jumboErrorHandler(msg.raddr, hostUuids, msg.err)
			}

			if appmsg != nil {
				s.appch <- appmsg
				logging.Tracef("%v appmsg: %T:%+v\n", s.logPrefix, appmsg, appmsg)
			}
		}
	}
}

// shutdown this gen server and all its routines.
func (s *Server) handleClose() {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v handleClose() crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	s.lis.Close() // close listener daemon

	for raddr, nc := range s.conns {
		closeConnection(s.logPrefix, raddr, nc)
	}
	s.lis, s.conns = nil, nil
	close(s.finch)

	logging.Infof("%v ... stopped\n", s.logPrefix)
	return
}

// start a connection worker to read mutation message for a subset of vbuckets.
func (s *Server) startWorker(raddr string) {
	nc, ok := s.conns[raddr]
	if !ok {
		fmsg := "%v connection %q already gone stale !\n"
		logging.Infof(fmsg, s.logPrefix, raddr)
		return
	}
	logging.Tracef("%v starting worker for connection %q\n", s.logPrefix, raddr)
	go doReceive(s.logPrefix, nc, s.maxPayload, s.readDeadline, s.appch, s.reqch)
	nc.active = true
}

// jumbo size error handler, it either closes all connections and shutdown the
// server or it closes all open connections with faulting remote-host and
// returns back a message for application.
func (s *Server) jumboErrorHandler(
	raddr string, hostUuids keeper,
	err error) (actvUuids keeper, msg interface{}) {

	var whatJumbo string

	if _, ok := s.conns[raddr]; ok == false {
		logging.Errorf("%v fatal remote %q already gone\n", s.logPrefix, raddr)
		return hostUuids, nil
	}

	if err == io.EOF {
		logging.Errorf("%v remote %q closed\n", s.logPrefix, raddr)
		whatJumbo = "closeremote"

	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		logging.Errorf("%v remote %q timeout: %v\n", s.logPrefix, raddr, err)
		whatJumbo = "closeremote"

	} else if err != nil {
		fmsg := "%v remote %q unknown error: %v\n"
		logging.Errorf(fmsg, s.logPrefix, raddr, err)
		whatJumbo = "closeremote"

	} else {
		logging.Errorf("%v no error why did you call jumbo !!!\n", s.logPrefix)
		return hostUuids, nil
	}

	switch whatJumbo {
	case "closeremote":
		ce := NewConnectionError()
		finished := ce.Append(hostUuids, raddr)
		actvUuids = s.delUuids(finished, hostUuids)
		closeConnection(s.logPrefix, raddr, s.conns[raddr])
		delete(s.conns, raddr)
		msg = ce

	// NOTE: application does not expect dataport-server to be automatically
	// closed.
	case "closeall":
		ce := NewConnectionError()
		for raddr := range s.conns {
			finished := ce.Append(hostUuids, raddr)
			actvUuids = s.delUuids(finished, hostUuids)
		}
		msg = ce
		go s.Close()
	}
	return actvUuids, msg
}

func (s *Server) logStats(hostUuids keeper) {
	bucketkvs := make(map[string][]uint64)    // bucket -> []count
	bucketseqnos := make(map[string][]uint64) // bucket -> []seqno
	for _, avb := range hostUuids {
		counts, ok := bucketkvs[avb.bucket]
		seqnos, ok := bucketseqnos[avb.bucket]
		if !ok {
			counts = make([]uint64, s.maxVbuckets)
			seqnos = make([]uint64, s.maxVbuckets)
		}
		counts[avb.vbno] = avb.kvers
		seqnos[avb.vbno] = avb.seqno
		bucketkvs[avb.bucket] = counts
		bucketseqnos[avb.bucket] = seqnos
	}
	for bucket, counts := range bucketkvs {
		seqnos := bucketseqnos[bucket]
		fmsg := "%v bucket total received key-versions: %v\n"
		logging.Infof(fmsg, s.logPrefix, counts)
		fmsg = "%v bucket latest sequence numbers: %v\n"
		logging.Infof(fmsg, s.logPrefix, seqnos)
	}
}

func closeConnection(prefix, raddr string, nc *netConn) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v closeConnection(%q) crashed: %v\n", prefix, raddr, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()
	close(nc.worker)
	nc.conn.Close()
	logging.Infof("%v connection %q closed !\n", prefix, raddr)
}

// get all remote connections for `host`
func remoteConnections(raddr string, conns map[string]*netConn) []string {
	host, _, _ := net.SplitHostPort(raddr)
	raddrs := make([]string, 0)
	for s := range conns {
		if h, _, _ := net.SplitHostPort(s); h == host {
			raddrs = append(raddrs, s)
		}
	}
	return raddrs
}

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func listener(prefix string, lis net.Listener, reqch chan []interface{}) {
loop:
	for {
		// TODO: handle `err` for lis.Close() and avoid panic(err)
		if conn, err := lis.Accept(); err != nil {
			e, ok := err.(*net.OpError)
			// NOTE: AcceptEx for windows op.
			if ok && (e.Op == "accept" || e.Op == "AcceptEx") {
				logging.Infof("%v ... stopped\n", prefix)
				break loop
			} else {
				panic(err)
			}

		} else {
			msg := serverMessage{
				cmd:   serverCmdNewConnection,
				raddr: conn.RemoteAddr().String(),
				args:  []interface{}{conn},
			}
			reqch <- []interface{}{msg}
		}
	}
}

// per connection go-routine to read []*VbKeyVersions.
func doReceive(
	prefix string,
	nc *netConn,
	maxPayload int, readDeadline time.Duration,
	appch chan<- interface{},
	reqch chan<- []interface{}) {

	conn, worker := nc.conn, nc.worker

	pkt := nc.tpkt
	msg := serverMessage{raddr: conn.RemoteAddr().String()}

	var duration time.Duration
	var start time.Time
	var blocked bool

	epoc := time.Now()
	tick := time.Tick(time.Second * 5) // log every 5 second, if blocked
loop:
	for {
		//timeoutMs := readDeadline * time.Millisecond
		//conn.SetReadDeadline(time.Now().Add(timeoutMs))
		msg.cmd, msg.err, msg.args = 0, nil, nil
		if payload, err := pkt.Receive(conn); err != nil {
			msg.cmd, msg.err = serverCmdError, err
			reqch <- []interface{}{msg}
			logging.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, err)
			break loop

		} else if vbmap, ok := payload.(*protobuf.VbConnectionMap); ok {
			msg.cmd, msg.args = serverCmdVbmap, []interface{}{vbmap}
			reqch <- []interface{}{msg}
			fmsg := "%v worker %q exit: `serverCmdVbmap`\n"
			logging.Tracef(fmsg, prefix, msg.raddr)
			break loop

		} else if vbs, ok := payload.([]*protobuf.VbKeyVersions); ok {
			msg.cmd, msg.args = serverCmdVbKeyVersions, []interface{}{vbs}
			if len(reqch) == cap(reqch) {
				start, blocked = time.Now(), true
			}
			select {
			case reqch <- []interface{}{msg}:
			case <-worker:
				msg.cmd, msg.err = serverCmdError, ErrorWorkerKilled
				reqch <- []interface{}{msg}
				fmsg := "%v worker %q exit: %v\n"
				logging.Errorf(fmsg, prefix, msg.raddr, msg.err)
				break loop
			}
			if blocked {
				duration += time.Since(start)
				blocked = false
				select {
				case <-tick:
					percent := float64(duration) / float64(time.Since(epoc))
					fmsg := "%v DATP -> Indexer %f%% blocked"
					logging.Infof(fmsg, prefix, percent)
				default:
				}
			}

		} else {
			msg.cmd, msg.err = serverCmdError, ErrorPayload
			reqch <- []interface{}{msg}
			logging.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, msg.err)
			break loop
		}
	}
	nc.active = false
}

func vbucketSchedule(vb *protobuf.VbKeyVersions) (s, e *protobuf.KeyVersions) {
	for _, kv := range vb.GetKvs() {
		commands := kv.GetCommands()
		if len(commands) == 1 {
			switch byte(commands[0]) {
			case c.StreamBegin:
				s, e = kv, nil
			case c.StreamEnd:
				s, e = nil, kv
			}
		}
	}
	return s, e
}

// ConnectionError to application
type ConnectionError map[string][]uint16 // bucket -> []vbuckets

// NewConnectionError makes a new connection-error map.
func NewConnectionError() ConnectionError {
	return make(ConnectionError)
}

// Append {buckets,vbuckets} for connection error.
func (ce ConnectionError) Append(hostUuids keeper, raddr string) keeper {
	finished := make(keeper)
	for uuid, avb := range hostUuids {
		if avb.raddr != raddr {
			continue
		}
		finished[uuid] = avb
		vbs, ok := ce[avb.bucket]
		if !ok {
			vbs = make([]uint16, 0, 4)
		}
		vbs = append(vbs, avb.vbno)
		ce[avb.bucket] = vbs
	}
	return finished
}

func newTransportPkt(maxPayload int) *transport.TransportPacket {
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(maxPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)
	return pkt
}
