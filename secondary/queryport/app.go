package queryport

import "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import "net"

// Application is example application logic that uses query-port server
func Application(config c.Config) {
	killch := make(chan bool)
	s, err := NewServer(
		"localhost:9990",
		func(req interface{},
			conn net.Conn, quitch <-chan interface{}) {
			requestHandler(req, conn, quitch, killch)
		},
		config)

	if err != nil {
		logging.Fatalf("Listen failed - %v", err)
	}
	<-killch
	s.Close()
}

// will be spawned as a go-routine by server's connection handler.
func requestHandler(
	req interface{},
	conn net.Conn, // Write handle to the tcp socket
	quitch <-chan interface{}, // client / connection might have quit (done)
	killch chan bool, // application is shutting down the server.
) {

	hdr := &protobuf.ResponseStreamHeader{}

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		// responses = getStatistics()
	case *protobuf.ScanRequest:
		// responses = scanIndex()
	case *protobuf.ScanAllRequest:
		// responses = fullTableScan()
	}

	buf := make([]byte, 1024, 1024)
loop:

	for i := 0; i < 10; i++ {
		// query storage backend for request
		protobuf.EncodeAndWrite(conn, buf, hdr)
		// if hdr.Len > 0, we can send rows with format [2_byte_len][sec_key][2_byte_len][docid]...

		select {
		case <-quitch:
			close(killch)
			break loop
		}
	}
	protobuf.EncodeAndWrite(conn, buf, &protobuf.StreamEndResponse{})
	// Free resources.
}
