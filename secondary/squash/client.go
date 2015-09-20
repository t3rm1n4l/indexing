package squash

import (
	"net"
)

type Client struct {
	addr string
	mux  *ConnMux
}

func NewClient(addr string) (*Client, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	mux := NewConnMux(conn, nil)

	return &Client{addr: addr, mux: mux}, nil
}

func (c *Client) NewConn() *Conn {
	id := c.mux.nextConnId()
	return c.mux.newConn(id)
}

func (c *Client) Close() error {
	return c.mux.Close()
}
