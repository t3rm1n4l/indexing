package indexer

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/pipeline"
	"io"
)

type IndexScanSource struct {
	pipeline.ItemWriter
	Snapshot Snapshot
}

type IndexScanDecoder struct {
	pipeline.ItemReadWriter
}

type IndexScanWriter struct {
	pipeline.ItemReader
	W io.Writer
}

func (s *IndexScanSource) Routine() error {
	fn := func(entry []byte) bool {
		s.WriteItem(entry)
		return true
	}
	cbr := s.Snapshot.(CallbackKeyReader)
	cbr.KeySetCb(fn)

	s.CloseWrite()

	return nil
}

func (d *IndexScanDecoder) Routine() error {
	tmpBuf := pipeline.GetBlock()
	for {
		entry, err := d.ReadItem()
		if err == pipeline.ErrNoMoreItem {
			d.CloseWrite()
			d.CloseRead()
			pipeline.PutBlock(tmpBuf)
			return nil
		}
		e := secondaryIndexEntry(entry)
		c.CrashOnError(err)

		t := (*tmpBuf)[:0]
		sk, err := e.ReadSecKey(t)
		c.CrashOnError(err)
		docid, err := e.ReadDocId(sk)
		c.CrashOnError(err)
		d.WriteItem(sk, docid[len(sk):])
	}

	return nil
}

func (d *IndexScanWriter) Routine() error {
	for {
		b, err := d.PeekBlock()
		if err == pipeline.ErrNoMoreItem {
			d.CloseRead()
			return nil
		}
		d.W.Write(b)
		d.FlushBlock()
	}
}
