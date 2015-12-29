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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/indexing/secondary/memdb/nodetable"
	"github.com/couchbase/indexing/secondary/memdb/skiplist"
	"github.com/couchbase/indexing/secondary/platform"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unsafe"
)

func docIdFromEntryBytes(e []byte) []byte {
	offset := len(e) - 2
	l := int(binary.LittleEndian.Uint16(e[offset : offset+2]))
	offset -= l
	return e[offset : offset+l]
}

func entryBytesFromDocId(docid []byte) []byte {
	l := len(docid)
	entry := make([]byte, l+2)
	copy(entry, docid)
	binary.LittleEndian.PutUint16(entry[l:l+2], uint16(l))
	return entry
}

func vbucketFromEntryBytes(e []byte, numVbuckets int) int {
	docid := docIdFromEntryBytes(e)
	hash := crc32.ChecksumIEEE(docid)
	return int((hash >> 16) & uint32(numVbuckets-1))
}

func hashDocId(entry []byte) uint32 {
	return crc32.ChecksumIEEE(docIdFromEntryBytes(entry))
}

func nodeEquality(p unsafe.Pointer, entry []byte) bool {
	node := (*skiplist.Node)(p)
	docid1 := docIdFromEntryBytes(entry)
	itm := (*memdb.Item)(node.Item())
	docid2 := docIdFromEntryBytes(itm.Bytes())
	return bytes.Equal(docid1, docid2)
}

func byteItemCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type memdbSlice struct {
	get_bytes, insert_bytes, delete_bytes platform.AlignedInt64
	flushedCount                          platform.AlignedUint64
	committedCount                        platform.AlignedUint64

	path string
	id   SliceId

	refCount int
	lock     sync.RWMutex

	mainstore *memdb.MemDB

	// MemDB writers
	main []*memdb.Writer

	// One local table per writer
	// Each table is only operated by the writer owner
	back []*nodetable.NodeTable

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isDirty       bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  []chan interface{}
	stopCh []DoneChannel

	workerDone []chan bool

	fatalDbErr error

	numWriters   int
	maxRollbacks int

	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.RWMutex

	isPersistorActive int32
}

func NewMemDBSlice(path string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId, isPrimary bool,
	sysconf common.Config, idxStats *IndexStats) (*memdbSlice, error) {

	info, err := os.Stat(path)
	if err != nil || err == nil && info.IsDir() {
		os.Mkdir(path, 0777)
	}

	slice := &memdbSlice{}
	slice.idxStats = idxStats

	slice.get_bytes = platform.NewAlignedInt64(0)
	slice.insert_bytes = platform.NewAlignedInt64(0)
	slice.delete_bytes = platform.NewAlignedInt64(0)
	slice.flushedCount = platform.NewAlignedUint64(0)
	slice.committedCount = platform.NewAlignedUint64(0)
	slice.sysconf = sysconf
	slice.path = path
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId
	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.maxRollbacks = sysconf["settings.recovery.max_rollbacks"].Int()

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	slice.cmdCh = make([]chan interface{}, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.cmdCh[i] = make(chan interface{}, sliceBufSize)
	}
	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	slice.isPrimary = isPrimary
	slice.initStores()

	logging.Infof("MemDBSlice:NewMemDBSlice Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, slice.numWriters)

	for i := 0; i < slice.numWriters; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	slice.setCommittedCount()
	return slice, nil
}

func (slice *memdbSlice) initStores() {
	mdbCfg := memdb.DefaultConfig()
	mdbCfg.SetKeyComparator(byteItemCompare)
	mdbCfg.UseMemoryMgmt()
	slice.mainstore = memdb.NewWithConfig(mdbCfg)
	slice.main = make([]*memdb.Writer, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.main[i] = slice.mainstore.NewWriter()
	}

	if !slice.isPrimary {
		slice.back = make([]*nodetable.NodeTable, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			slice.back[i] = nodetable.New(hashDocId, nodeEquality)
		}
	}
}

func (mdb *memdbSlice) IncrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount++
}

func (mdb *memdbSlice) DecrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount--
	if mdb.refCount == 0 {
		if mdb.isSoftClosed {
			tryClosememdbSlice(mdb)
		}
		if mdb.isSoftDeleted {
			tryDeletememdbSlice(mdb)
		}
	}
}

func (mdb *memdbSlice) Insert(key []byte, docid []byte, meta *MutationMeta) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- &indexItem{key: key, docid: docid}
	return mdb.fatalDbErr
}

func (mdb *memdbSlice) Delete(docid []byte, meta *MutationMeta) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- docid
	return mdb.fatalDbErr
}

func (mdb *memdbSlice) handleCommandsWorker(workerId int) {
	var start time.Time
	var elapsed time.Duration
	var c interface{}
	var icmd *indexItem
	var dcmd []byte

loop:
	for {
		select {
		case c = <-mdb.cmdCh[workerId]:
			switch c.(type) {

			case *indexItem:
				icmd = c.(*indexItem)
				start = time.Now()
				mdb.insert((*icmd).key, (*icmd).docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			case []byte:
				dcmd = c.([]byte)
				start = time.Now()
				mdb.delete(dcmd, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			default:
				logging.Errorf("MemDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", mdb.id, mdb.idxInstId, c)
			}

			mdb.idxStats.flushQueueSize.Add(-1)

		case <-mdb.stopCh[workerId]:
			mdb.stopCh[workerId] <- true
			break loop

		case <-mdb.workerDone[workerId]:
			mdb.workerDone[workerId] <- true

		}
	}
}

func (mdb *memdbSlice) insert(entry []byte, docid []byte, workerId int) {
	if mdb.isPrimary {
		mdb.insertPrimaryIndex(entry, docid, workerId)
	} else {
		mdb.insertSecIndex(entry, docid, workerId)
	}

	mdb.logWriterStat()
}

func (mdb *memdbSlice) insertPrimaryIndex(entry []byte, docid []byte, workerId int) {
	logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", mdb.id, mdb.idxInstId, docid)

	t0 := time.Now()
	itm := memdb.NewItem(entry)
	mdb.main[workerId].Put(itm)
	mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.insert_bytes, int64(len(entry)))
	mdb.isDirty = true
}

func (mdb *memdbSlice) insertSecIndex(entry []byte, docid []byte, workerId int) {
	if entry == nil {
		// Delete existing entries from main and back
		lookupentry := entryBytesFromDocId(docid)
		t0 := time.Now()
		found, node := mdb.back[workerId].Remove(lookupentry)
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(lookupentry)))

		if found {
			t0 := time.Now()
			mdb.main[workerId].DeleteNode((*skiplist.Node)(node))
			mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
			platform.AddInt64(&mdb.delete_bytes, int64(len(lookupentry)))
		}

	} else {
		// 1. Insert entry into main index
		// 2. Upsert into backindex with docid, mainnode pointer
		// 3. Delete old entry from main index if back index had
		// a previous mainnode pointer entry
		t0 := time.Now()
		itm := memdb.NewItem(entry)
		newNode := mdb.main[workerId].Put2(itm)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(entry)))

		// Insert succeeded. Failure means same entry already exist.
		if newNode != nil {
			if updated, oldNode := mdb.back[workerId].Update(entry, unsafe.Pointer(newNode)); updated {
				t0 := time.Now()
				mdb.main[workerId].DeleteNode((*skiplist.Node)(oldNode))
				mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
				platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
			}
		}
	}

	mdb.isDirty = true
}

func (mdb *memdbSlice) delete(docid []byte, workerId int) {
	if mdb.isPrimary {
		mdb.deletePrimaryIndex(docid, workerId)
	} else {
		mdb.deleteSecIndex(docid, workerId)
	}

	mdb.logWriterStat()
}

func (mdb *memdbSlice) deletePrimaryIndex(docid []byte, workerId int) {
	if docid == nil {
		common.CrashOnError(errors.New("Nil Primary Key"))
		return
	}

	// docid -> key format
	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	// Delete from main index
	t0 := time.Now()
	itm := memdb.NewItem(entry.Bytes())
	mdb.main[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
	mdb.isDirty = true

}

func (mdb *memdbSlice) deleteSecIndex(docid []byte, workerId int) {
	lookupentry := entryBytesFromDocId(docid)

	// Delete entry from back and main index if present
	t0 := time.Now()
	success, node := mdb.back[workerId].Remove(lookupentry)
	if success {
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		t0 = time.Now()
		mdb.main[workerId].DeleteNode((*skiplist.Node)(node))
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
	}
	mdb.isDirty = true
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (mdb *memdbSlice) checkFatalDbError(err error) {

	//panic on all DB errors and recover rather than risk
	//inconsistent db state
	common.CrashOnError(err)

	errStr := err.Error()
	switch errStr {

	case "checksum error", "file corruption", "no db instance",
		"alloc fail", "seek fail", "fsync fail":
		mdb.fatalDbErr = err

	}

}

type memdbSnapshotInfo struct {
	Ts       *common.TsVbuuid
	MainSnap *memdb.Snapshot `json:"-"`

	Committed bool `json:"-"`
	dataPath  string
}

type memdbSnapshot struct {
	slice     *memdbSlice
	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId
	ts        *common.TsVbuuid
	info      *memdbSnapshotInfo
	committed bool

	refCount int32
}

// Creates an open snapshot handle from snapshot info
// Snapshot info is obtained from NewSnapshot() or GetSnapshots() API
// Returns error if snapshot handle cannot be created.
func (mdb *memdbSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	snapInfo := info.(*memdbSnapshotInfo)

	s := &memdbSnapshot{slice: mdb,
		idxDefnId: mdb.idxDefnId,
		idxInstId: mdb.idxInstId,
		info:      info.(*memdbSnapshotInfo),
		ts:        snapInfo.Timestamp(),
		committed: info.IsCommitted(),
	}

	s.Open()
	s.slice.IncrRef()
	if s.committed {
		s.Open()
		go mdb.doPersistSnapshot(s)
	}

	if s.info.MainSnap == nil {
		mdb.loadSnapshot(s.info)
	}

	logging.Infof("MemDBSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
		"Snapshot %v", mdb.id, mdb.idxInstId, snapInfo)

	return s, nil
}

func (mdb *memdbSlice) doPersistSnapshot(s *memdbSnapshot) {
	defer s.Close()
	if platform.CompareAndSwapInt32(&mdb.isPersistorActive, 0, 1) {
		defer platform.StoreInt32(&mdb.isPersistorActive, 0)

		t0 := time.Now()
		dir := newSnapshotPath(mdb.path)
		tmpdir := filepath.Join(mdb.path, ".tmp")
		manifest := filepath.Join(tmpdir, "manifest.json")
		os.RemoveAll(tmpdir)
		mdb.confLock.RLock()
		concurrency := mdb.sysconf["settings.memdb.persistence_threads"].Int()
		mdb.confLock.RUnlock()
		err := mdb.mainstore.StoreToDisk(tmpdir, s.info.MainSnap, concurrency, nil)
		if err == nil {
			var fd *os.File
			bs, err := json.Marshal(s.info)
			if err == nil {
				fd, err = os.OpenFile(manifest, os.O_WRONLY|os.O_CREATE, 0755)
				_, err = fd.Write(bs)
				if err == nil {
					err = fd.Close()
				}
			}

			if err == nil {
				err = os.Rename(tmpdir, dir)
				if err == nil {
					mdb.cleanupOldSnapshotFiles(mdb.maxRollbacks)
				}
			}
		}

		if err == nil {
			logging.Infof("MemDBSlice Slice Id %v, IndexInstId %v created ondisk snapshot %v. Took %v", mdb.id, mdb.idxInstId, dir, time.Since(t0))
		} else {
			logging.Errorf("MemDBSlice Slice Id %v, IndexInstId %v failed to create ondisk snapshot %v (error=%v)", mdb.id, mdb.idxInstId, dir, err)
			os.RemoveAll(tmpdir)
			os.RemoveAll(dir)
		}
	} else {
		logging.Infof("MemDBSlice Slice Id %v, IndexInstId %v Skipping ondisk snapshot. A snapshot writer is in progress.", mdb.id, mdb.idxInstId)
	}
}

func (mdb *memdbSlice) cleanupOldSnapshotFiles(keepn int) {
	manifests := mdb.getSnapshotManifests()
	if len(manifests) > keepn {
		toRemove := len(manifests) - keepn
		manifests = manifests[:toRemove]
		for _, m := range manifests {
			dir := path.Dir(m)
			logging.Infof("MemDBSlice Removing disk snapshot %v", dir)
			os.RemoveAll(dir)
		}
	}
}

func (mdb *memdbSlice) getSnapshotManifests() []string {
	pattern := "*/manifest.json"
	files, _ := filepath.Glob(filepath.Join(mdb.path, pattern))
	sort.Strings(files)
	return files
}

// Returns snapshot info list in reverse sorted order
func (mdb *memdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	var infos []SnapshotInfo

	files := mdb.getSnapshotManifests()
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		info := &memdbSnapshotInfo{dataPath: path.Dir(f)}
		fd, err := os.Open(f)
		if err == nil {
			defer fd.Close()
			bs, err := ioutil.ReadAll(fd)
			if err == nil {
				err = json.Unmarshal(bs, info)
				if err == nil {
					infos = append(infos, info)
				}
			}
		}

	}
	return infos, nil
}

func (mdb *memdbSlice) setCommittedCount() {
	platform.StoreUint64(&mdb.committedCount, uint64(mdb.mainstore.ItemsCount()))
}

func (mdb *memdbSlice) GetCommittedCount() uint64 {
	return platform.LoadUint64(&mdb.committedCount)
}

func (mdb *memdbSlice) resetStores() {
	mdb.mainstore.Close()
	if !mdb.isPrimary {
		for i := 0; i < mdb.numWriters; i++ {
			mdb.back[i].Close()
		}
	}

	mdb.initStores()
}

//Rollback slice to given snapshot. Return error if
//not possible
func (mdb *memdbSlice) Rollback(info SnapshotInfo) error {
	snapInfo := info.(*memdbSnapshotInfo)
	mdb.resetStores()
	return mdb.loadSnapshot(snapInfo)
}

func (mdb *memdbSlice) loadSnapshot(snapInfo *memdbSnapshotInfo) error {
	var wg sync.WaitGroup

	var backIndexCallback memdb.ItemCallback
	partShardCh := make([]chan *memdb.ItemEntry, mdb.numWriters)

	logging.Infof("MemDBSlice::loadSnapshot Slice Id %v, IndexInstId %v reading %v",
		mdb.id, mdb.idxInstId, snapInfo.dataPath)

	t0 := time.Now()
	if !mdb.isPrimary {
		for wId := 0; wId < mdb.numWriters; wId++ {
			wg.Add(1)
			partShardCh[wId] = make(chan *memdb.ItemEntry, 1000)
			go func(i int, wg *sync.WaitGroup) {
				defer wg.Done()
				for entry := range partShardCh[i] {
					if !mdb.isPrimary {
						entryBytes := entry.Item().Bytes()
						mdb.back[i].Update(entryBytes, unsafe.Pointer(entry.Node()))
					}
				}
			}(wId, &wg)
		}

		backIndexCallback = func(e *memdb.ItemEntry) {
			mdb.confLock.RLock()
			numVbuckets := mdb.sysconf["numVbuckets"].Int()
			mdb.confLock.RUnlock()

			wId := vbucketFromEntryBytes(e.Item().Bytes(), numVbuckets) % mdb.numWriters
			partShardCh[wId] <- e

			mdb.idxStats.numItemsRestored.Add(1)
		}
	}

	mdb.confLock.RLock()
	concurrency := mdb.sysconf["settings.memdb.recovery_threads"].Int()
	mdb.confLock.RUnlock()

	snap, err := mdb.mainstore.LoadFromDisk(snapInfo.dataPath, concurrency, backIndexCallback)

	if !mdb.isPrimary {
		for wId := 0; wId < mdb.numWriters; wId++ {
			close(partShardCh[wId])
		}
		wg.Wait()
	}

	if err == nil {
		snapInfo.MainSnap = snap
	}

	dur := time.Since(t0)
	logging.Infof("MemDBSlice::loadSnapshot Slice Id %v, IndexInstId %v finished reading %v. Took %v",
		mdb.id, mdb.idxInstId, snapInfo.dataPath, dur)

	return err
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *memdbSlice) RollbackToZero() error {
	mdb.resetStores()
	mdb.cleanupOldSnapshotFiles(0)
	return nil
}

//slice insert/delete methods are async. There
//can be outstanding mutations in internal queue to flush even
//after insert/delete have return success to caller.
//This method provides a mechanism to wait till internal
//queue is empty.
func (mdb *memdbSlice) waitPersist() {

	if !mdb.checkAllWorkersDone() {
		//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
		//check for outstanding mutations. If there are
		//none, proceed with the commit.
		mdb.confLock.RLock()
		commitPollInterval := mdb.sysconf["storage.commitPollInterval"].Uint64()
		mdb.confLock.RUnlock()

		for {
			if mdb.checkAllWorkersDone() {
				break
			}
			time.Sleep(time.Millisecond * time.Duration(commitPollInterval))
		}
	}

}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (mdb *memdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	mdb.waitPersist()
	mdb.isDirty = false

	newSnapshotInfo := &memdbSnapshotInfo{
		Ts:        ts,
		MainSnap:  mdb.mainstore.NewSnapshot(),
		Committed: commit,
	}
	mdb.setCommittedCount()

	return newSnapshotInfo, nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (mdb *memdbSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if mdb.getCmdsCount() > 0 {
		return false
	}

	//worker queue is empty, make sure both workers are done
	//processing the last mutation
	for i := 0; i < mdb.numWriters; i++ {
		mdb.workerDone[i] <- true
		<-mdb.workerDone[i]
	}
	return true
}

func (mdb *memdbSlice) Close() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	logging.Infof("MemDBSlice::Close Closing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", mdb.idxInstId, mdb.idxDefnId, mdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < mdb.numWriters; i++ {
		mdb.stopCh[i] <- true
		<-mdb.stopCh[i]
	}

	if mdb.refCount > 0 {
		mdb.isSoftClosed = true
	} else {
		tryClosememdbSlice(mdb)
	}
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (mdb *memdbSlice) Destroy() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.refCount > 0 {
		logging.Infof("MemDBSlice::Destroy Softdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxDefnId)
		mdb.isSoftDeleted = true
	} else {
		tryDeletememdbSlice(mdb)
	}
}

//Id returns the Id for this Slice
func (mdb *memdbSlice) Id() SliceId {
	return mdb.id
}

// FilePath returns the filepath for this Slice
func (mdb *memdbSlice) Path() string {
	return mdb.path
}

//IsActive returns if the slice is active
func (mdb *memdbSlice) IsActive() bool {
	return mdb.isActive
}

//SetActive sets the active state of this slice
func (mdb *memdbSlice) SetActive(isActive bool) {
	mdb.isActive = isActive
}

//Status returns the status for this slice
func (mdb *memdbSlice) Status() SliceStatus {
	return mdb.status
}

//SetStatus set new status for this slice
func (mdb *memdbSlice) SetStatus(status SliceStatus) {
	mdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (mdb *memdbSlice) IndexInstId() common.IndexInstId {
	return mdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (mdb *memdbSlice) IndexDefnId() common.IndexDefnId {
	return mdb.idxDefnId
}

// IsDirty returns true if there has been any change in
// in the slice storage after last in-mem/persistent snapshot
func (mdb *memdbSlice) IsDirty() bool {
	mdb.waitPersist()
	return mdb.isDirty
}

func (mdb *memdbSlice) Compact() error {
	return nil
}

func (mdb *memdbSlice) Statistics() (StorageStatistics, error) {
	var sts StorageStatistics

	var internalData []string

	internalData = append(internalData, fmt.Sprintf("----MainStore----\n%s", mdb.mainstore.DumpStats()))
	if !mdb.isPrimary {
		for i := 0; i < mdb.numWriters; i++ {
			internalData = append(internalData, fmt.Sprintf("\n----BackStore[%d]----\n%s", i, mdb.back[i].Stats()))
		}
	}

	sts.InternalData = internalData
	return sts, nil
}

func (mdb *memdbSlice) UpdateConfig(cfg common.Config) {
	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	mdb.sysconf = cfg
}

func (mdb *memdbSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", mdb.id)
	str += fmt.Sprintf("File: %v ", mdb.path)
	str += fmt.Sprintf("Index: %v ", mdb.idxInstId)

	return str

}

func tryDeletememdbSlice(mdb *memdbSlice) {

	//cleanup the disk directory
	if err := os.RemoveAll(mdb.path); err != nil {
		logging.Errorf("MemDBSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", mdb.id, mdb.idxInstId, mdb.idxDefnId, err)
	}
}

func tryClosememdbSlice(mdb *memdbSlice) {
	mdb.mainstore.Close()
	if !mdb.isPrimary {
		for i := 0; i < mdb.numWriters; i++ {
			mdb.back[i].Close()
		}
	}
}

func (mdb *memdbSlice) getCmdsCount() int {
	c := 0
	for i := 0; i < mdb.numWriters; i++ {
		c += len(mdb.cmdCh[i])
	}

	return c
}

func (mdb *memdbSlice) logWriterStat() {
	count := platform.AddUint64(&mdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Infof("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId,
			count, mdb.getCmdsCount())
	}

}

func (info *memdbSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *memdbSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *memdbSnapshotInfo) String() string {
	return fmt.Sprintf("SnapshotInfo: committed:%v", info.Committed)
}

func (s *memdbSnapshot) Create() error {
	return nil
}

func (s *memdbSnapshot) Open() error {
	platform.AddInt32(&s.refCount, int32(1))

	return nil
}

func (s *memdbSnapshot) IsOpen() bool {

	count := platform.LoadInt32(&s.refCount)
	return count > 0
}

func (s *memdbSnapshot) Id() SliceId {
	return s.slice.Id()
}

func (s *memdbSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *memdbSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *memdbSnapshot) Timestamp() *common.TsVbuuid {
	return s.ts
}

//Close the snapshot
func (s *memdbSnapshot) Close() error {

	count := platform.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("MemDBSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")

	} else if count == 0 {
		go s.Destroy()
	}

	return nil
}

func (s *memdbSnapshot) Destroy() {
	s.info.MainSnap.Close()

	defer s.slice.DecrRef()
}

func (s *memdbSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("SliceId: %v ", s.slice.Id())
	str += fmt.Sprintf("TS: %v ", s.ts)
	return str
}

func (s *memdbSnapshot) Info() SnapshotInfo {
	return s.info
}

// ==============================
// Snapshot reader implementation
// ==============================

// Approximate items count
func (s *memdbSnapshot) StatCountTotal() (uint64, error) {
	c := s.slice.GetCommittedCount()
	return c, nil
}

func (s *memdbSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return s.CountRange(MinIndexKey, MaxIndexKey, Both, stopch)
}

func (s *memdbSnapshot) CountRange(low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {

	var count uint64
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Range(low, high, inclusion, callb)
	return count, err
}

func (s *memdbSnapshot) CountLookup(keys []IndexKey, stopch StopChannel) (uint64, error) {
	var err error
	var count uint64

	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	for _, k := range keys {
		if err = s.Lookup(k, callb); err != nil {
			break
		}
	}

	return count, err
}

func (s *memdbSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {
	var count uint64
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Lookup(key, callb)
	return count != 0, err
}

func (s *memdbSnapshot) Lookup(key IndexKey, callb EntryCallback) error {
	return s.Iterate(key, key, Both, compareExact, callb)
}

func (s *memdbSnapshot) Range(low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(low, high, inclusion, cmpFn, callb)
}

func (s *memdbSnapshot) All(callb EntryCallback) error {
	return s.Range(MinIndexKey, MaxIndexKey, Both, callb)
}

func (s *memdbSnapshot) Iterate(low, high IndexKey, inclusion Inclusion,
	cmpFn CmpEntry, callback EntryCallback) error {

	var entry IndexEntry
	var err error
	it := s.info.MainSnap.NewIterator()
	defer it.Close()

	if low.Bytes() == nil {
		it.SeekFirst()
	} else {
		itm := memdb.NewItem(low.Bytes())
		it.Seek(itm)

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			err = s.iterEqualKeys(low, it, cmpFn, nil)
			if err != nil {
				return err
			}
		}
	}

loop:
	for ; it.Valid(); it.Next() {
		itm := it.Get().Bytes()
		entry = s.newIndexEntry(itm)

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(itm)
		if err != nil {
			return err
		}
	}

	// Include equal keys if high inclusion is requested
	if inclusion == Both || inclusion == High {
		err = s.iterEqualKeys(high, it, cmpFn, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *memdbSnapshot) isPrimary() bool {
	return s.slice.isPrimary
}

func (s *memdbSnapshot) newIndexEntry(b []byte) IndexEntry {
	var entry IndexEntry
	var err error

	if s.slice.isPrimary {
		entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
	return entry
}

func (s *memdbSnapshot) iterEqualKeys(k IndexKey, it *memdb.Iterator,
	cmpFn CmpEntry, callback func([]byte) error) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		itm := it.Get().Bytes()
		entry = s.newIndexEntry(itm)
		if cmpFn(k, entry) == 0 {
			if callback != nil {
				err = callback(itm)
				if err != nil {
					return err
				}
			}
		} else {
			break
		}
	}

	return err
}

func newSnapshotPath(dirpath string) string {
	file := time.Now().Format("snapshot.2006-01-02.15:04:05.000")
	return filepath.Join(dirpath, file)
}
