// Copyright (c) 2015 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"code.google.com/p/goprotobuf/proto"
	p "github.com/couchbase/indexing/secondary/pipeline"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"net"
)

type ScanResponseWriter interface {
	Error(err error) error
	Stats(rows, unique uint64, min, max []byte) error
	Count(count uint64) error
	RowBytes([]byte) error
	Done() error
}

type protoResponseWriter struct {
	scanType ScanReqType
	conn     net.Conn
	encBuf   *[]byte
}

func NewProtoWriter(t ScanReqType, conn net.Conn) *protoResponseWriter {
	return &protoResponseWriter{
		scanType: t,
		conn:     conn,
		encBuf:   p.GetBlock(),
	}
}

func (w *protoResponseWriter) Error(err error) error {
	var res interface{}
	protoErr := &protobuf.Error{Error: proto.String(err.Error())}

	switch w.scanType {
	case StatsReq:
		res = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case CountReq:
		res = &protobuf.CountResponse{
			Count: proto.Int64(0), Err: protoErr,
		}
	case ScanAllReq, ScanReq:
		res = &protobuf.ResponseStreamHeader{
			Err: protoErr,
		}
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) Stats(rows, unique uint64, min, max []byte) error {
	res := &protobuf.StatisticsResponse{
		Stats: &protobuf.IndexStatistics{
			KeysCount:       proto.Uint64(rows),
			UniqueKeysCount: proto.Uint64(unique),
			KeyMin:          min,
			KeyMax:          max,
		},
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) Count(c uint64) error {
	res := &protobuf.CountResponse{
		Count: proto.Int64(int64(c)),
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) RowBytes(b []byte) error {
	res := &protobuf.ResponseStreamHeader{
		Len: proto.Uint64(uint64(len(b))),
	}

	err := protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
	if err != nil {
		return err
	}

	_, err = w.conn.Write(b)
	return err
}

func (w *protoResponseWriter) Done() error {
	defer p.PutBlock(w.encBuf)

	return nil
}
