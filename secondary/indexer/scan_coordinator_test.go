package indexer

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/queryport"
	"testing"
)

var (
	count int
)

func keyFeederFn(keych chan Key, valch chan Value, errch chan error) {
	_ = valch
	_ = errch

	for i := 0; i < 100; i++ {
		k := Key{encoded: []byte(fmt.Sprint("key-%d", i))}
		keych <- k
	}

	close(keych)
}

func clientReader(val interface{}) bool {
	switch v := val.(type) {
	case *protobuf.ResponseStream:
		fmt.Println(v)
		count++
		if count == 10000 {
			return false
		}

	case error:
		fmt.Println("error occured is ", v.(error))
	}
	return true
}

func TestScan(t *testing.T) {
	c.LogEnable()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}

	h.createIndex("idx", "default2", keyFeederFn)

	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx1", "default", 100, 100, clientReader)
}
