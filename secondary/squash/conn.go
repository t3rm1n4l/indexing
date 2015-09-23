package squash

import (
	//	"fmt"
	"net"
	"time"
)

type ConnId uint32
type Reqtype int

const (
	WriteReq Reqtype = iota
	FlushReq
)

type Request struct {
	id   ConnId
	typ  Reqtype
	size int
}

type Conn struct {
	id    ConnId
	mux   *ConnMux
	rpipe *pipe
	wch   chan bool
}

func (p *Conn) Read(bs []byte) (int, error) {
	if len(bs) == 0 {
		return 0, nil
	}

	//	fmt.Println("READ", len(bs))
	return p.rpipe.Read(bs)
}

func (p *Conn) Write(bs []byte) (int, error) {
	if len(bs) == 0 {
		return 0, nil
	}

	//	fmt.Println("WRITE", len(bs))
	p.mux.reqWrite <- Request{id: p.id, typ: WriteReq, size: len(bs)}
	<-p.wch
	n, err := p.mux.write(bs)
	p.wch <- true
	return n, err
}

func (p *Conn) Close() error {
	return p.mux.delConn(p.id)
}

func (p *Conn) LocalAddr() net.Addr {
	return p.mux.conn.LocalAddr()
}

func (p *Conn) RemoteAddr() net.Addr {
	return p.mux.conn.RemoteAddr()
}

func (p *Conn) SetDeadline(time.Time) error {
	return nil
}

func (p *Conn) SetReadDeadline(time.Time) error {
	return nil
}

func (p *Conn) SetWriteDeadline(time.Time) error {
	return nil
}
