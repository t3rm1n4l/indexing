package main

import (
	//"fmt"
	"fmt"
	"github.com/couchbase/indexing/secondary/squash"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"
	//	"time"
)

const (
	reqSize  = 24
	respSize = 200
)

var (
	pool sync.Pool
	resp []byte
)

func init() {
	resp, _ = ioutil.ReadFile("/Users/sarath/development/sherlock/goproj/src/github.com/couchbase/indexing/secondary/squash/test/client/flow/server")
	fmt.Println("size=", len(resp))
	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, reqSize+respSize)
		},
	}

	//resp = make([]byte, respSize)
}

func callback(p net.Conn) {
	buf := pool.Get()
	p.Read(buf.([]byte)[:6])
	p.Read(buf.([]byte)[:18])
	pool.Put(buf)

	time.Sleep(time.Nanosecond * 80003)
	p.Write(resp[4:10])
	p.Write(resp[14:62])
	p.Write(resp[66:72])
	p.Close()
}

func main() {
	addr := os.Args[1]
	s := squash.NewServer(addr, callback)
	s.Run()
}
