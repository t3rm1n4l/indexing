package indexer

import (
	"bytes"
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/memdb"
	"sync"
)

type memDBSlice struct {
	sync.Mutex
	id        SliceId
	instId    c.IndexInstId
	indDefId  c.IndexDefnId
	status    SliceStatus
	isActive  bool
	isPrimary bool
	conf      c.Config

	main memdb.MemDB
	back memdb.MemDB

	ts *c.TsVbuuid
}

type CallbackKeyReader interface {
	KeySetCb(func(Key) bool)
}

type memDBSnapInfo struct {
	db        memdb.MemDB
	ts        *c.TsVbuuid
	committed bool
}

func (i *memDBSnapInfo) Timestamp() *c.TsVbuuid {
	return i.ts
}

func (i *memDBSnapInfo) IsCommitted() bool {
	return i.committed
}

type KV struct {
	k []byte
	v []byte
}

func (kv *KV) Less(that memdb.Item) bool {
	thatKV := that.(*KV)
	return bytes.Compare(kv.k, thatKV.k) == -1
}

func NewMemDBSlice(id SliceId, defId c.IndexDefnId, instId c.IndexInstId, isPrimary bool, conf c.Config) (*memDBSlice, error) {

	slice := &memDBSlice{
		id:        id,
		instId:    instId,
		indDefId:  defId,
		isPrimary: isPrimary,
		conf:      conf,
		main:      memdb.New(),
		back:      memdb.New(),
	}

	return slice, nil
}

func (s *memDBSlice) Id() SliceId {
	return s.id
}

func (s *memDBSlice) Path() string {
	return "not-implemented"
}

func (s *memDBSlice) Status() SliceStatus {
	return SLICE_STATUS_ACTIVE
}

func (s *memDBSlice) IndexInstId() c.IndexInstId {
	return s.instId
}

func (s *memDBSlice) IndexDefnId() c.IndexDefnId {
	return s.indDefId
}

func (s *memDBSlice) IsActive() bool {
	return s.status == SLICE_STATUS_ACTIVE
}

func (s *memDBSlice) SetActive(b bool) {
	s.isActive = b

}

func (s *memDBSlice) SetStatus(ss SliceStatus) {
	s.status = ss
}

func (s *memDBSlice) Insert(k []byte, docid []byte) error {
	s.Lock()
	defer s.Unlock()

	entry, _ := NewSecondaryIndexEntry(k, docid)

	mainItm := &KV{
		k: entry.Bytes(),
	}
	backItm := &KV{
		k: docid,
		v: k,
	}

	oldkey := s.back.Get(backItm)
	if oldkey != nil {
		s.main.Delete(oldkey)
		s.back.Delete(backItm)
	}

	if mainItm.k == nil {
		return nil
	}

	s.back.InsertNoReplace(backItm)
	s.main.InsertNoReplace(mainItm)
	return nil
}

func (s *memDBSlice) Delete(d []byte) error {
	s.Lock()
	defer s.Unlock()
	backItm := &KV{
		k: d,
	}

	oldkey := s.back.Get(backItm)
	if oldkey != nil {
		s.main.Delete(oldkey)
		s.back.Delete(backItm)
	}

	return nil
}

func (s *memDBSlice) NewSnapshot(ts *c.TsVbuuid, commit bool) (SnapshotInfo, error) {
	s.Lock()
	defer s.Unlock()
	si := &memDBSnapInfo{
		ts:        ts,
		committed: commit,
		db:        s.main.Clone(),
	}

	return si, nil
}

func (s *memDBSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	s.Lock()
	defer s.Unlock()
	msi := info.(*memDBSnapInfo)
	snap := &memDBSnapshot{
		db:   msi.db,
		ts:   msi.ts,
		info: info.(*memDBSnapInfo),
	}

	return snap, nil
}

func (s *memDBSlice) GetSnapshots() ([]SnapshotInfo, error) {
	s.Lock()
	defer s.Unlock()
	return []SnapshotInfo{}, nil
}

func (s *memDBSlice) Rollback(info SnapshotInfo) error {
	return errors.New("not-implemented")
}

func (s *memDBSlice) RollbackToZero() error {
	s.main = memdb.New()
	s.back = memdb.New()
	return nil
}

func (s *memDBSlice) Close() {
	s.Lock()
	defer s.Unlock()
	s.main = nil
	s.back = nil
}

func (s *memDBSlice) Destroy() {
	s.Lock()
	defer s.Unlock()
	s.main = nil
	s.back = nil
}

func (s *memDBSlice) SetTimestamp(ts *c.TsVbuuid) error {
	s.ts = ts
	return nil
}

func (s *memDBSlice) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *memDBSlice) IncrRef() {
}

func (s *memDBSlice) DecrRef() {
}

func (s *memDBSlice) Compact() error {
	return nil
}

func (s *memDBSlice) Statistics() (StorageStatistics, error) {
	return StorageStatistics{}, nil
}

type memDBSnapshot struct {
	db   memdb.MemDB
	ts   *c.TsVbuuid
	info *memDBSnapInfo
}

func (s *memDBSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return uint64(s.db.Len()), nil
}

func (s *memDBSnapshot) StatCountTotal() (uint64, error) {
	return uint64(s.db.Len()), nil
}

func (s *memDBSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {
	itm := &KV{
		k: key.Bytes(),
	}
	return s.db.Get(itm) != nil, nil
}

func (s *memDBSnapshot) Lookup(key IndexKey, stopch StopChannel) (chan IndexEntry, chan error) {
	return nil, nil
}

func (s *memDBSnapshot) KeySetCb(cb func(Key) bool) {
	cb2 := func(i memdb.Item) bool {
		kv := i.(*KV)
		key, _ := NewKeyFromEncodedBytes(kv.k)
		return cb(key)
	}

	nilK := &KV{
		k: []byte(nil),
	}

	s.db.AscendGreaterOrEqual(nilK, cb2)
}

func (s *memDBSnapshot) KeySet(stopch StopChannel) (chan IndexEntry, chan error) {
	kch := make(chan IndexEntry)
	ech := make(chan error)

	cb := func(i memdb.Item) bool {
		select {
		case <-stopch:
			return false
		default:
		}
		kv := i.(*KV)
		key, _ := BytesToSecondaryIndexEntry(kv.k)
		kch <- key
		return true
	}

	nilK := &KV{
		k: []byte(nil),
	}

	go func() {
		s.db.AscendGreaterOrEqual(nilK, cb)
		close(kch)
	}()

	return kch, ech
}

func (s *memDBSnapshot) KeyRange(low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (chan IndexEntry, chan error, SortOrder) {
	kch := make(chan IndexEntry)
	ech := make(chan error)

	stK := &KV{
		k: low.Bytes(),
	}

	endK := &KV{
		k: high.Bytes(),
	}
	cb := func(i memdb.Item) bool {
		select {
		case <-stopch:
			return false
		default:
		}
		kv := i.(*KV)
		key, _ := BytesToSecondaryIndexEntry(kv.k)
		if len(high.Bytes()) > 0 && !kv.Less(endK) {
			return false
		}
		kch <- key

		return true
	}

	go func() {
		s.db.AscendGreaterOrEqual(stK, cb)
		close(kch)
	}()

	return kch, ech, Asc
}

func (s *memDBSnapshot) GetKeySetForKeyRange(low IndexKey, high IndexKey,
	inclusion Inclusion, chkey chan IndexKey, cherr chan error, stopch StopChannel) {
	panic("not implemented")
}

func (s *memDBSnapshot) CountRange(low IndexKey, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {
	count := uint64(0)
	kch, _, _ := s.KeyRange(low, high, inclusion, stopch)
	for _ = range kch {
		count++
	}
	return count, nil
}

func (s *memDBSnapshot) Open() error {
	return nil
}

func (s *memDBSnapshot) Close() error {
	return nil
}

func (s *memDBSnapshot) IsOpen() bool {
	return true
}

func (s *memDBSnapshot) Id() SliceId {
	return SliceId(0)
}

func (s *memDBSnapshot) IndexInstId() c.IndexInstId {
	return c.IndexInstId(0)
}

func (s *memDBSnapshot) IndexDefnId() c.IndexDefnId {
	return c.IndexDefnId(0)
}

func (s *memDBSnapshot) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *memDBSnapshot) SetTimestamp(ts *c.TsVbuuid) {
	s.ts = ts
}

func (s *memDBSnapshot) Info() SnapshotInfo {
	return s.info
}
