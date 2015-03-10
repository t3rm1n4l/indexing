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
	"github.com/couchbase/indexing/secondary/fdb"
	"sync/atomic"
)

//ForestDBIterator taken from
//https://github.com/couchbaselabs/bleve/blob/master/index/store/goforestdb/iterator.go
type ForestDBIterator struct {
	slice *fdbSlice
	db    *forestdb.KVStore
	valid bool
	curr  *forestdb.Doc
	iter  *forestdb.Iterator
}

func newFDBSnapshotIterator(s Snapshot) (*ForestDBIterator, error) {
	var seq forestdb.SeqNum
	fdbSnap := s.(*fdbSnapshot)
	fdbSnap.lock.Lock()
	defer fdbSnap.lock.Unlock()

	if !fdbSnap.committed {
		seq = FORESTDB_INMEMSEQ
	} else {
		seq = fdbSnap.mainSeqNum
	}

	itr, err := newForestDBIterator(fdbSnap.slice.(*fdbSlice), fdbSnap.main, seq)
	return itr, err
}

func newForestDBIterator(slice *fdbSlice, db *forestdb.KVStore,
	seq forestdb.SeqNum) (*ForestDBIterator, error) {
	dbInst, err := db.SnapshotOpen(seq)
	rv := ForestDBIterator{
		db:    dbInst,
		slice: slice,
	}

	if err != nil {
		err = errors.New("ForestDB iterator: alloc failed " + err.Error())
	}
	return &rv, err
}

func (f *ForestDBIterator) SeekFirst() {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.db.IteratorInit([]byte{}, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	if err != nil {
		f.valid = false
		return
	}

	//pre-allocate doc
	keybuf := make([]byte, MAX_SEC_KEY_BUFFER_LEN)
	f.curr, err = forestdb.NewDoc(keybuf, nil, nil)
	if err != nil {
		f.valid = false
		return
	}

	f.valid = true
	f.Get()
}

func (f *ForestDBIterator) Seek(key []byte) {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.db.IteratorInit(key, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	if err != nil {
		f.valid = false
		return
	}

	//pre-allocate doc
	keybuf := make([]byte, MAX_SEC_KEY_BUFFER_LEN)
	f.curr, err = forestdb.NewDoc(keybuf, nil, nil)
	if err != nil {
		f.valid = false
		return
	}

	f.valid = true
	f.Get()
}

func (f *ForestDBIterator) Next() {
	var err error
	err = f.iter.Next()
	if err != nil {
		f.valid = false
		return
	}

	f.Get()
}

func (f *ForestDBIterator) Get() {
	var err error
	err = f.iter.GetPreAlloc(f.curr)
	if err != nil {
		f.valid = false
	}
}

func (f *ForestDBIterator) Current() ([]byte, []byte, bool) {
	if f.valid {
		return f.Key(), f.Value(), true
	}
	return nil, nil, false
}

func (f *ForestDBIterator) Key() []byte {
	if f.valid && f.curr != nil {
		if f.slice != nil {
			atomic.AddInt64(&f.slice.get_bytes, int64(len(f.curr.Key())))
		}
		return f.curr.Key()
	}
	return nil
}

func (f *ForestDBIterator) Value() []byte {
	if f.valid && f.curr != nil {
		if f.slice != nil {
			atomic.AddInt64(&f.slice.get_bytes, int64(len(f.curr.Body())))
		}
		return f.curr.Body()
	}
	return nil
}

func (f *ForestDBIterator) Valid() bool {
	return f.valid
}

func (f *ForestDBIterator) Close() error {

	//free the doc allocated by forestdb
	if f.curr != nil {
		f.curr.Close()
		f.curr = nil
	}

	var err error
	f.valid = false
	err = f.iter.Close()
	if err != nil {
		return err
	}
	f.iter = nil
	err = f.db.Close()
	if err != nil {
		return err
	}
	f.db = nil
	return nil
}
