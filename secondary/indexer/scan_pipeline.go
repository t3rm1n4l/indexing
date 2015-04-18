// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	p "github.com/couchbase/indexing/secondary/pipeline"
)

var (
	ErrLimitReached = errors.New("Row limit reached")
)

type ScanPipeline struct {
	src    p.Source
	object p.Pipeline
	req    *ScanRequest

	rowsRead  uint64
	bytesRead uint64
}

func (p *ScanPipeline) Cancel(err error) {
	p.src.Shutdown(err)
}

func (p *ScanPipeline) Execute() error {
	return p.object.Execute()
}

func (p ScanPipeline) RowsRead() uint64 {
	return p.rowsRead
}

func (p ScanPipeline) BytesRead() uint64 {
	return p.bytesRead
}

func NewScanPipeline(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot) *ScanPipeline {
	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req

	src := &IndexScanSource{is: is, p: scanPipeline}
	src.InitWriter()
	dec := &IndexScanDecoder{p: scanPipeline}
	dec.InitReadWriter()
	wr := &IndexScanWriter{w: w, p: scanPipeline}
	wr.InitReader()

	dec.SetSource(src)
	wr.SetSource(dec)

	scanPipeline.src = src
	scanPipeline.object.AddSource("source", src)
	scanPipeline.object.AddFilter("decoder", dec)
	scanPipeline.object.AddSink("writer", wr)

	return scanPipeline

}

type IndexScanSource struct {
	p.ItemWriter
	is IndexSnapshot
	p  *ScanPipeline
}

type IndexScanDecoder struct {
	p.ItemReadWriter
	p *ScanPipeline
}

type IndexScanWriter struct {
	p.ItemReader
	w ScanResponseWriter
	p *ScanPipeline
}

func (s *IndexScanSource) Routine() error {
	var err error
	defer s.CloseWrite()

	fn := func(entry []byte) error {
		s.p.rowsRead++
		wrErr := s.WriteItem(entry)
		if wrErr != nil {
			return wrErr
		}

		if s.p.rowsRead == uint64(s.p.req.limit) {
			return ErrLimitReached
		}

		return nil
	}

	r := s.p.req
loop:
	for _, snap := range GetSliceSnapshots(s.is) {
		if r.scanType == ScanAllReq {
			err = snap.Snapshot().All(fn)
		} else {
			if len(r.keys) > 0 {
				for _, k := range r.keys {
					if err = snap.Snapshot().Lookup(k, fn); err != nil {
						break
					}
				}
			} else {
				err = snap.Snapshot().Range(r.low, r.high, r.incl, fn)
			}
		}

		switch err {
		case nil:
		case p.ErrSupervisorKill, ErrLimitReached:
			break loop
		default:
			s.CloseWithError(err)
			break loop
		}
	}

	return nil
}

func (d *IndexScanDecoder) Routine() error {
	defer d.CloseWrite()
	defer d.CloseRead()

	var entry IndexEntry
	tmpBuf := p.GetBlock()
	defer p.PutBlock(tmpBuf)

loop:
	for {
		row, err := d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem, p.ErrSupervisorKill:
			break loop
		default:
			d.CloseWithError(err)
			break loop
		}

		if d.p.req.isPrimary {
			e := primaryIndexEntry(row)
			entry = &e
		} else {
			e := secondaryIndexEntry(row)
			entry = &e
		}
		c.CrashOnError(err)

		t := (*tmpBuf)[:0]
		sk, err := entry.ReadSecKey(t)
		c.CrashOnError(err)
		docid, err := entry.ReadDocId(sk)
		c.CrashOnError(err)
		d.p.bytesRead += uint64(len(docid))
		err = d.WriteItem(sk, docid[len(sk):])
		if err != nil {
			break
		}
	}

	return nil
}

func (d *IndexScanWriter) Routine() error {
	defer d.CloseRead()

loop:
	for {
		sk, err := d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem:
			break loop
		default:
			return err
		}

		pk, err := d.ReadItem()
		if err != nil {
			return err
		}

		if err = d.w.Row(pk, sk); err != nil {
			d.w.Error(err)
			return err
		}

		/*
			TODO: Use block chunk send protocol
				b, err := d.PeekBlock()
				if err == p.ErrNoMoreItem {
					d.CloseRead()
					return nil
				}

				d.W.RawBytes(b)
				d.FlushBlock()
		*/
	}

	return nil
}
