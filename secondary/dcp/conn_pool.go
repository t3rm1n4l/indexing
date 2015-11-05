package couchbase

import (
	"errors"
	"fmt"
	"time"

	"encoding/binary"
	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
)

var errClosedPool = errors.New("the pool is closed")
var errNoPool = errors.New("no pool")

// GenericMcdAuthHandler is a kind of AuthHandler that performs
// special auth exchange (like non-standard auth, possibly followed by
// select-bucket).
type GenericMcdAuthHandler interface {
	AuthHandler
	AuthenticateMemcachedConn(string, *memcached.Client) error
}

// Default timeout for retrieving a connection from the pool.
var ConnPoolTimeout = time.Hour * 24 * 30

// ConnPoolAvailWaitTime is the amount of time to wait for an existing
// connection from the pool before considering the creation of a new
// one.
var ConnPoolAvailWaitTime = time.Millisecond

type connectionPool struct {
	host        string
	mkConn      func(host string, ah AuthHandler) (*memcached.Client, error)
	auth        AuthHandler
	connections chan *memcached.Client
	createsem   chan bool
}

func newConnectionPool(host string, ah AuthHandler, poolSize, poolOverflow int) *connectionPool {
	return &connectionPool{
		host:        host,
		connections: make(chan *memcached.Client, poolSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		mkConn:      defaultMkConn,
		auth:        ah,
	}
}

// ConnPoolTimeout is notified whenever connections are acquired from a pool.
var ConnPoolCallback func(host string, source string, start time.Time, err error)

func defaultMkConn(host string, ah AuthHandler) (*memcached.Client, error) {
	conn, err := memcached.Connect("tcp", host)
	if err != nil {
		return nil, err
	}

	if gah, ok := ah.(GenericMcdAuthHandler); ok {
		err = gah.AuthenticateMemcachedConn(host, conn)
		if err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}
	name, pass := ah.GetCredentials()
	if name != "default" {
		_, err = conn.Auth(name, pass)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (cp *connectionPool) Close() (err error) {
	defer func() { err, _ = recover().(error) }()
	close(cp.connections)
	for c := range cp.connections {
		c.Close()
	}
	return
}

func (cp *connectionPool) GetWithTimeout(d time.Duration) (rv *memcached.Client, err error) {
	if cp == nil {
		return nil, errNoPool
	}

	path := ""

	if ConnPoolCallback != nil {
		defer func(path *string, start time.Time) {
			ConnPoolCallback(cp.host, *path, start, err)
		}(&path, time.Now())
	}

	path = "short-circuit"

	// short-circuit available connetions.
	select {
	case rv, isopen := <-cp.connections:
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	default:
	}

	t := time.NewTimer(ConnPoolAvailWaitTime)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case rv, isopen := <-cp.connections:
		path = "avail1"
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	case <-t.C:
		// No connection came around in time, let's see
		// whether we can get one or build a new one first.
		t.Reset(d) // Reuse the timer for the full timeout.
		select {
		case rv, isopen := <-cp.connections:
			path = "avail2"
			if !isopen {
				return nil, errClosedPool
			}
			return rv, nil
		case cp.createsem <- true:
			path = "create"
			// Build a connection if we can't get a real one.
			// This can potentially be an overflow connection, or
			// a pooled connection.
			rv, err := cp.mkConn(cp.host, cp.auth)
			if err != nil {
				// On error, release our create hold
				<-cp.createsem
			}
			return rv, err
		case <-t.C:
			return nil, ErrTimeout
		}
	}
}

func (cp *connectionPool) Get() (*memcached.Client, error) {
	return cp.GetWithTimeout(ConnPoolTimeout)
}

func (cp *connectionPool) Return(c *memcached.Client) {
	if c == nil {
		return
	}

	if cp == nil {
		c.Close()
	}

	if c.IsHealthy() {
		defer func() {
			if recover() != nil {
				// This happens when the pool has already been
				// closed and we're trying to return a
				// connection to it anyway.  Just close the
				// connection.
				c.Close()
			}
		}()

		select {
		case cp.connections <- c:
		default:
			// Overflow connection.
			<-cp.createsem
			c.Close()
		}
	} else {
		<-cp.createsem
		c.Close()
	}
}

func (cp *connectionPool) StartTapFeed(args *memcached.TapArguments) (*memcached.TapFeed, error) {
	if cp == nil {
		return nil, errNoPool
	}
	mc, err := cp.Get()
	if err != nil {
		return nil, err
	}

	// A connection can't be used after TAP; Dont' count it against the
	// connection pool capacity
	<-cp.createsem

	return mc.StartTapFeed(*args)
}

const DEFAULT_WINDOW_SIZE = uint32(20 * 1024 * 1024) // 20 Mb

func (cp *connectionPool) StartDcpFeed(
	name DcpFeedName, sequence uint32,
	outch chan *memcached.DcpEvent,
	opaque uint16,
	config map[string]interface{}) (*memcached.DcpFeed, error) {

	if cp == nil {
		return nil, errNoPool
	}

	mc, err := cp.Get() // Don't call Return() on this
	if err != nil {
		return nil, err
	}
	// A connection can't be used after it has been allocated to DCP;
	// Dont' count it against the connection pool capacity
	<-cp.createsem

	dcpf, err := memcached.NewDcpFeed(mc, string(name), outch, opaque, config)
	if err == nil {
		err = dcpf.DcpOpen(string(name), sequence, DEFAULT_WINDOW_SIZE, opaque)
		if err == nil {
			return dcpf, err
		}
	}
	mc.Close()
	return nil, err
}

func (cp *connectionPool) GetDcpConn(name string) (*memcached.Client, error) {
	mc, err := cp.Get() // Don't call Return() on this
	if err != nil {
		return nil, err
	}

	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(name),
		Opaque: 0,
	}
	rq.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(rq.Extras[:4], 0)
	binary.BigEndian.PutUint32(rq.Extras[4:], 1) // we are consumer
	if err := mc.Transmit(rq); err != nil {
		return nil, err
	}

	_, err = mc.Receive()
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func GetSeqs(mc *memcached.Client, seqnos []uint64) error {
	var res *transport.MCResponse
	var err error

	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: 0,
	}

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if res, err = mc.Receive(); err != nil {
		return err
	}

	if len(res.Body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(res.Body))
		return err
	}

	for i := 0; i < len(res.Body); i += 10 {
		vbno := int(binary.BigEndian.Uint16(res.Body[i : i+2]))
		seqno := binary.BigEndian.Uint64(res.Body[i+2 : i+10])
		seqnos[vbno] = seqno
	}

	return nil
}
