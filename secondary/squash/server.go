package squash

import (
	"net"
)

type Server struct {
	laddr string
	callb func(net.Conn)
}

func NewServer(laddr string, callb func(net.Conn)) *Server {
	return &Server{
		laddr: laddr,
		callb: callb,
	}
}

func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.laddr)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		cb := func(conn *Conn) {
			s.callb(conn)
		}

		NewConnMux(conn, cb)
	}

	return nil
}
