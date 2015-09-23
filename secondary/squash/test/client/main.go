package main

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/squash"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	reqSize  = 200
	respSize = 79
)

var (
	pool sync.Pool
	req  []byte
)

func init() {
	req, _ = ioutil.ReadFile("/Users/sarath/development/sherlock/goproj/src/github.com/couchbase/indexing/secondary/squash/test/client/flow/client")
	fmt.Println("size=", len(req))
	pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, reqSize+respSize)
		},
	}

	//req = make([]byte, respSize)
}

func main() {
	f, _ := os.Create("prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	var wg sync.WaitGroup
	var count uint64

	addr := os.Args[1]
	n, _ := strconv.Atoi(os.Args[2])
	thr, _ := strconv.Atoi(os.Args[3])
	nperthr := n / thr
	c, _ := squash.NewClient(addr)

	t0 := time.Now()
	tm := t0
	for i := 0; i < thr; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < nperthr; i++ {
				p := c.NewConn()
				p.Write(req[8:14])
				p.Write(req[22:])
				buf := pool.Get()
				p.Read(buf.([]byte)[:6])
				p.Read(buf.([]byte)[:48])
				p.Read(buf.([]byte)[:6])
				pool.Put(buf)
				p.Close()
				x := atomic.AddUint64(&count, 1)
				if x == 1000000 {
					tx := time.Now()
					fmt.Println(float64(x)/tx.Sub(tm).Seconds(), "req/sec")
					tm = tx
					atomic.StoreUint64(&count, 0)
				}

			}
		}()
	}

	wg.Wait()

	tx := time.Now()
	fmt.Println(float64(count)/tx.Sub(tm).Seconds(), "req/sec")
	fmt.Println(float64(n)/tx.Sub(t0).Seconds(), "req/sec")
}
