package client

import "github.com/couchbase/indexing/secondary/transport"
import "github.com/couchbase/indexing/secondary/squash"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import p "github.com/couchbase/indexing/secondary/pipeline"

type connMuxPool struct {
	client               *squash.Client
	maxPayload, poolSize int
}

func newConnectionMuxPool(host string, poolSize, maxPayload int) (*connMuxPool, error) {
	client, err := squash.NewClient(host)
	if err != nil {
		return nil, err
	}

	return &connMuxPool{
		client:     client,
		maxPayload: maxPayload,
		poolSize:   poolSize,
	}, nil
}

func (cp *connMuxPool) Get() (*connection, error) {
	conn := cp.client.NewConn()
	flags := transport.TransportFlag(0).SetProtobuf()
	b := p.GetBlock()
	pkt := transport.NewTransportPacket2(*b, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobuf.ProtobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)
	return &connection{conn, pkt, b}, nil
}

func (cp *connMuxPool) Return(connectn *connection, healthy bool) {
	if connectn.conn == nil {
		return
	}

	connectn.conn.Close()
	p.PutBlock(connectn.b)
}

func (cp *connMuxPool) Close() error {
	return cp.client.Close()
}
