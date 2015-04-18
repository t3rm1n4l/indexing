package protobuf

import "github.com/couchbase/indexing/secondary/transport"
import "net"

const (
	LenOffset  int = 0
	LenSize    int = 4
	FlagOffset int = LenOffset + LenSize
	FlagSize   int = 2
	DataOffset int = FlagOffset + FlagSize
)

func EncodeAndWrite(conn net.Conn, buf []byte, r interface{}) (err error) {
	var data []byte
	data, err = ProtobufEncode(r)
	if err != nil {
		return
	}
	flags := transport.TransportFlag(0).SetProtobuf()
	err = transport.Send(conn, buf, flags, data)
	return
}
