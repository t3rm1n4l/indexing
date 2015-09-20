package squash

import (
	"bufio"
	"encoding/binary"
	//	"github.com/couchbase/indexing/secondary/logging"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rBufSize      = 128 * 1024
	wBufSize      = 128 * 1024
	flushInterval = time.Millisecond
)

type ConnMux struct {
	counter         uint32
	werr            error
	rerr            error
	wquitch         chan struct{}
	rquitch         chan struct{}
	conn            net.Conn
	w               io.Writer
	r               io.Reader
	raccess         map[ConnId]chan bool
	waccess         map[ConnId]chan bool
	reqWrite        chan Request
	newConnCallback func(*Conn)
	sync.RWMutex
}

func (mux *ConnMux) nextConnId() ConnId {
	return ConnId(atomic.AddUint32(&mux.counter, 1))
}

func (mux *ConnMux) newConn(id ConnId) *Conn {
	mux.Lock()
	defer mux.Unlock()

	p := &Conn{
		id:  id,
		mux: mux,
		rch: make(chan bool),
		wch: make(chan bool),
	}

	mux.raccess[id] = p.rch
	mux.waccess[id] = p.wch

	return p
}

func (mux *ConnMux) delConn(id ConnId) error {
	mux.Lock()
	defer mux.Unlock()

	delete(mux.raccess, id)
	delete(mux.waccess, id)
	return nil
}

func (mux *ConnMux) write(bs []byte) (int, error) {
	a, b := mux.w.Write(bs)
	//logging.Infof("write %v,%v\n", a, b)
	//mux.w.(*bufio.Writer).Flush()
	return a, b
}

func (mux *ConnMux) read(bs []byte) (int, error) {
	a, b := io.ReadFull(mux.r, bs)
	//logging.Infof("read %v,%v\n", a, b)
	return a, b

}

func (mux *ConnMux) handleOutgoing() {
	defer close(mux.rquitch)

	for {
	loop:
		select {
		case req := <-mux.reqWrite:
			if req.typ == FlushReq {
				mux.w.(*bufio.Writer).Flush()
				goto loop
			}
			mux.RLock()
			ch := mux.waccess[req.id]
			mux.RUnlock()

			mux.werr = binary.Write(mux.w, binary.LittleEndian, req.id)
			//logging.Infof("write === id %v\n", req.id)
			if mux.werr != nil {
				return
			}
			ch <- true
			<-ch
			if mux.werr != nil {
				return
			}
		}
	}
}

func (mux *ConnMux) handleIncoming() {
	defer close(mux.wquitch)

	var id ConnId
	for {
		mux.rerr = binary.Read(mux.r, binary.LittleEndian, &id)
		//logging.Infof("read === id %v\n", id)
		if mux.rerr != nil {
			return
		}

		mux.RLock()
		ch, ok := mux.raccess[id]
		mux.RUnlock()
		if ok {
			ch <- true
			<-ch
		} else {
			p := mux.newConn(id)
			if mux.newConnCallback != nil {
				go mux.newConnCallback(p)
			}
			p.rch <- true
			<-p.rch
		}

		if mux.rerr != nil {
			return
		}
	}
}

func (mux *ConnMux) Close() error {
	return mux.conn.Close()
}

func NewConnMux(conn net.Conn, callb func(*Conn)) *ConnMux {
	//	w := bufio.NewWriterSize(conn, wBufSize)
	w := conn
	r := bufio.NewReaderSize(conn, rBufSize)

	mux := &ConnMux{
		wquitch:         make(chan struct{}),
		rquitch:         make(chan struct{}),
		conn:            conn,
		w:               w,
		r:               r,
		counter:         uint32(rand.Int()),
		raccess:         make(map[ConnId]chan bool),
		waccess:         make(map[ConnId]chan bool),
		reqWrite:        make(chan Request),
		newConnCallback: callb,
	}

	/*
		go func() {
			for {
				time.Sleep(flushInterval)
				mux.reqWrite <- Request{id: 0, typ: FlushReq}

			}
		}()
	*/
	go mux.handleOutgoing()
	go mux.handleIncoming()

	return mux
}
