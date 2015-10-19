package queryport

import "fmt"
import "net"
import "sync"
import "time"
import "io"
import "github.com/couchbase/indexing/secondary/platform"
import "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import "github.com/couchbase/indexing/secondary/transport"

// RequestHandler shall interpret the request message
// from client and post response message(s) on `respch`
// channel, until `quitch` is closed. When there are
// no more response to post handler shall close `respch`.
type RequestHandler func(
	req interface{}, conn net.Conn, quitch <-chan bool)

// Server handles queryport connections.
type Server struct {
	laddr string         // address to listen
	callb RequestHandler // callback to application on incoming request.
	// local fields
	mu     sync.Mutex
	lis    net.Listener
	killch chan bool
	// config params
	maxPayload     int
	readDeadline   time.Duration
	writeDeadline  time.Duration
	streamChanSize int
	logPrefix      string
	nConnections   platform.AlignedInt64
}

type ServerStats struct {
	Connections int64
}

// NewServer creates a new queryport daemon.
func NewServer(
	laddr string, callb RequestHandler,
	config c.Config) (s *Server, err error) {

	s = &Server{
		laddr:          laddr,
		callb:          callb,
		killch:         make(chan bool),
		maxPayload:     config["maxPayload"].Int(),
		readDeadline:   time.Duration(config["readDeadline"].Int()),
		writeDeadline:  time.Duration(config["writeDeadline"].Int()),
		streamChanSize: config["streamChanSize"].Int(),
		logPrefix:      fmt.Sprintf("[Queryport %q]", laddr),
		nConnections:   platform.NewAlignedInt64(0),
	}
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		logging.Errorf("%v failed starting %v !!\n", s.logPrefix, err)
		return nil, err
	}

	go s.listener()
	logging.Infof("%v started ...\n", s.logPrefix)
	return s, nil
}

func (s *Server) Statistics() ServerStats {
	return ServerStats{
		Connections: platform.LoadInt64(&s.nConnections),
	}
}

// Close queryport daemon.
func (s *Server) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v Close() crashed: %v\n", s.logPrefix, r)
			err = fmt.Errorf("%v", r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lis != nil {
		s.lis.Close() // close listener daemon
		s.lis = nil
		close(s.killch)
		logging.Infof("%v ... stopped\n", s.logPrefix)
	}
	return
}

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func (s *Server) listener() {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v listener() crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		go s.Close()
	}()

	for {
		if conn, err := s.lis.Accept(); err == nil {
			go s.handleConnection(conn)
		} else {
			if e, ok := err.(*net.OpError); ok && e.Op != "accept" {
				panic(err)
			}
			break
		}
	}
}

// handle connection request. connection might be kept open in client's
// connection pool.
func (s *Server) handleConnection(conn net.Conn) {
	platform.AddInt64(&s.nConnections, 1)
	defer func() {
		platform.AddInt64(&s.nConnections, -1)
	}()

	raddr := conn.RemoteAddr()
	defer func() {
		conn.Close()
		logging.Infof("%v connection %v closed\n", s.logPrefix, raddr)
	}()

	// start a receive routine.
	quitch := make(chan bool)
	rcvch := make(chan interface{}, s.streamChanSize)
	go s.doReceive(conn, rcvch, quitch)

loop:
	for req := range rcvch {
		if _, yes := req.(*protobuf.EndStreamRequest); yes { // skip
			format := "%v connection %q skip protobuf.EndStreamRequest\n"
			logging.Infof(format, s.logPrefix, raddr)
			break loop
		}
		s.callb(req, conn, quitch) // blocking call
		// End response should be only sent after monitor is shutdown
		// otherwise it could lead to loss of next request coming through
		// same connection.

		transport.SendResponseEnd(conn)
	}
}

// receive requests from remote, when this function returns
// the connection is expected to be closed.
func (s *Server) doReceive(conn net.Conn, rcvch chan<- interface{}, quitch chan bool) {
	raddr := conn.RemoteAddr()

	// transport buffer for receiving
	flags := transport.TransportFlag(0).SetProtobuf()
	rpkt := transport.NewTransportPacket(s.maxPayload, flags)
	rpkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)

	logging.Infof("%v connection %q doReceive() ...\n", s.logPrefix, raddr)

loop:
	for {
		// TODO: Fix read timeout correctly
		// timeoutMs := s.readDeadline * time.Millisecond
		// conn.SetReadDeadline(time.Now().Add(timeoutMs))

		req, err := rpkt.Receive(conn)
		// TODO: handle close-connection and don't print error message.
		if err != nil {
			if err == io.EOF {
				logging.Tracef("%v connection %q exited %v\n", s.logPrefix, raddr, err)
			} else {
				logging.Errorf("%v connection %q exited %v\n", s.logPrefix, raddr, err)
			}
			break loop
		}
		if _, yes := req.(*protobuf.EndStreamRequest); yes {
			format := "%v connection %s client requested quit"
			logging.Debugf(format, s.logPrefix, raddr)
			quitch <- true
		} else {
			rcvch <- req
		}
	}
	close(rcvch)
}
