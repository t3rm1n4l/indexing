package protobuf

import "github.com/couchbase/indexing/secondary/transport"
import "net"

func ReadAndDecode(conn net.Conn, buf []byte, pl *QueryPayload) (pl2 *QueryPayload, err error) {
	var data []byte
	_, data, err = transport.Receive(conn, buf)
	if err != nil {
		return
	}
	pl2, err = ProtobufDecodeFromBuf(data, pl)
	return
}
