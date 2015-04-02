package indexer

import (
	"encoding/binary"
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	"io"
)

var (
	ErrNoBlockSpace = errors.New("Not enough space in buffer")
	ErrNoMoreItem   = errors.New("No more item to be read from buffer")
)

var (
	BlockPool *c.BytesBufPool
)

func init() {
	BlockPool = c.NewByteBufferPool(16 * 1024)
}

type ByteBlockWriter struct {
	buf *[]byte
	cap int
	len int
}

func (b *ByteBlockWriter) Init(buf *[]byte) {
	b.buf = buf
	b.len = 2
	b.cap = cap(*buf)
}

func (b *ByteBlockWriter) Put(itms ...[]byte) error {
	l := 0
	for _, itm := range itms {
		l += len(itm)
	}

	if 2+l+2*len(itms) > b.cap-b.len {
		return ErrNoBlockSpace
	}

	for _, itm := range itms {
		//	fmt.Println("insert itm", len(itm))
		binary.LittleEndian.PutUint16((*b.buf)[b.len:b.len+2], uint16(len(itm)))
		b.len += 2
		copy((*b.buf)[b.len:], itm)
		b.len += len(itm)
	}

	return nil
}

func (b *ByteBlockWriter) IsEmpty() bool {
	return b.len == 2
}

func (b *ByteBlockWriter) Close() {
	binary.LittleEndian.PutUint16((*b.buf)[0:2], uint16(b.len))
}

type ByteBlockReader struct {
	buf    *[]byte
	len    int
	offset int
}

func (b *ByteBlockReader) Init(buf *[]byte) {
	b.buf = buf
	b.len = int(binary.LittleEndian.Uint16((*buf)[0:2]))
	b.offset = 2
}

func (b *ByteBlockReader) Len() int {
	return b.len
}

func (b *ByteBlockReader) Get() ([]byte, error) {
	if b.offset == b.len {
		return nil, ErrNoMoreItem
	}

	dlen := int(binary.LittleEndian.Uint16((*b.buf)[b.offset : b.offset+2]))
	//fmt.Println("got itm", dlen)
	b.offset += 2
	b.offset += dlen

	return (*b.buf)[b.offset-dlen : b.offset], nil
}

type IndexEntrySrc struct {
	stopch  chan struct{}
	timeout chan struct{}
	// slice   IndexSnapshot
	Snapshot Snapshot
	params   scanParams
	Outch    chan *[]byte
	err      error
	status   string
	// TODO update stats
}

func (src *IndexEntrySrc) Routine() {
	var w ByteBlockWriter
	cbr := src.Snapshot.(CallbackKeyReader)
	b := BlockPool.Get()
	w.Init(b)

	fn := func(entry []byte) bool {
		err := w.Put(entry)
		if err == ErrNoBlockSpace {
			w.Close()
			src.Outch <- b
			b = BlockPool.Get()
			w.Init(b)
			err = w.Put(entry)
			c.CrashOnError(err)
		}

		return true
	}

	cbr.KeySetCb(fn)

	if w.IsEmpty() {
		BlockPool.Put(b)
	} else {
		w.Close()
		src.Outch <- b
		b = nil
	}

	close(src.Outch)
}

type IndexEntryDecoder struct {
	Inch  chan *[]byte
	Outch chan *[]byte
	err   error
}

func (dec *IndexEntryDecoder) Routine() {
	tmpBuf := BlockPool.Get()
	block := BlockPool.Get()

	var r ByteBlockReader
	var w ByteBlockWriter

	w.Init(block)

	for b := range dec.Inch {
		r.Init(b)

		for {
			entry, err := r.Get()
			if err == ErrNoMoreItem {
				BlockPool.Put(b)
				break
			}

			//	e, err := BytesToSecondaryIndexEntry(entry)
			e := secondaryIndexEntry(entry)
			c.CrashOnError(err)
			//fmt.Println("cap=", cap(*tmpBuf), len(*tmpBuf), len(entry))
			t := (*tmpBuf)[:0]
			sk, err := e.ReadSecKey(t)
			c.CrashOnError(err)
			docid, err := e.ReadDocId(sk)
			c.CrashOnError(err)
			err = w.Put(sk, docid[len(sk):])
			if err == ErrNoBlockSpace {
				w.Close()
				dec.Outch <- block
				block = BlockPool.Get()
				w.Init(block)
				err = w.Put(sk, docid[len(sk):])
				c.CrashOnError(err)
			}
		}
	}

	if w.IsEmpty() {
		BlockPool.Put(block)
	} else {
		w.Close()
		dec.Outch <- block
		block = nil
	}

	close(dec.Outch)
}

type IndexEntrySerializer struct {
	Inch  chan *[]byte
	Outch chan *[]byte
	err   error
}

type IndexEntrySender struct {
	Inch chan *[]byte
	W    io.Writer
	err  error
}

func (sender *IndexEntrySender) Routine() {
	var r ByteBlockReader

	for b := range sender.Inch {
		r.Init(b)
		l := r.Len()
		sender.W.Write((*b)[2 : l+2])
		BlockPool.Put(b)
	}
}

type IndexStreamPipeline struct {
}

type IndexRequestPipeline struct {
}
