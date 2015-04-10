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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	snapshotMetaListKey = []byte("snapshots-list")
)

//NewForestDBSlice initiailizes a new slice with forestdb backend.
//Both main and back index gets initialized with default config.
//Slice methods are not thread-safe and application needs to
//handle the synchronization. The only exception being Insert and
//Delete can be called concurrently.
//Returns error in case slice cannot be initialized.
func NewForestDBSlice(path string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId, isPrimary bool, sysconf common.Config) (*fdbSlice, error) {

	info, err := os.Stat(path)
	if err != nil || err == nil && info.IsDir() {
		os.Mkdir(path, 0777)
	}

	filepath := newFdbFile(path, false)
	slice := &fdbSlice{}

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	memQuota := sysconf["settings.memory_quota"].Uint64()
	logging.Debugf("NewForestDBSlice(): buffer cache size %d", memQuota)
	config.SetBufferCacheSize(memQuota)
	logging.Debugf("NewForestDBSlice(): buffer cache size %d", memQuota)

	kvconfig := forestdb.DefaultKVStoreConfig()

	if slice.dbfile, err = forestdb.Open(filepath, config); err != nil {
		return nil, err
	}

	slice.config = config

	config.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)
	if slice.statFd, err = forestdb.Open(filepath, config); err != nil {
		return nil, err
	}

	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.main = make([]*forestdb.KVStore, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		if slice.main[i], err = slice.dbfile.OpenKVStore("main", kvconfig); err != nil {
			return nil, err
		}
	}

	//create a separate back-index for non-primary indexes
	if !isPrimary {
		slice.back = make([]*forestdb.KVStore, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			if slice.back[i], err = slice.dbfile.OpenKVStore("back", kvconfig); err != nil {
				return nil, err
			}
		}
	}

	// Make use of default kvstore provided by forestdb
	if slice.meta, err = slice.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, err
	}

	slice.path = path
	slice.currfile = filepath
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId

	slice.cmdCh = make(chan interface{}, SLICE_COMMAND_BUFFER_SIZE)
	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	slice.isPrimary = isPrimary

	for i := 0; i < slice.numWriters; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	logging.Debugf("ForestDBSlice:NewForestDBSlice \n\t Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, slice.numWriters)

	return slice, nil
}

//kv represents a key/value pair in storage format
type indexItem struct {
	key   []byte
	docid []byte
}

//fdbSlice represents a forestdb slice
type fdbSlice struct {
	path     string
	currfile string
	id       SliceId //slice id

	refCount int
	lock     sync.RWMutex
	dbfile   *forestdb.File
	statFd   *forestdb.File
	metaLock sync.Mutex
	meta     *forestdb.KVStore   // handle for index meta
	main     []*forestdb.KVStore // handle for forward index
	back     []*forestdb.KVStore // handle for reverse index

	config *forestdb.Config

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  chan interface{} //internal channel to buffer commands
	stopCh []DoneChannel    //internal channel to signal shutdown

	workerDone []chan bool //worker status check channel

	fatalDbErr error //store any fatal DB error

	numWriters int //number of writer threads

	//TODO: Remove this once these stats are
	//captured by the stats library
	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	// Statistics
	get_bytes, insert_bytes, delete_bytes int64
}

func (fdb *fdbSlice) IncrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount++
}

func (fdb *fdbSlice) DecrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount--
	if fdb.refCount == 0 {
		if fdb.isSoftClosed {
			tryCloseFdbSlice(fdb)
		}
		if fdb.isSoftDeleted {
			tryDeleteFdbSlice(fdb)
		}
	}
}

//Insert will insert the given key/value pair from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Insert(secKey []byte, docid []byte) error {

	fdb.cmdCh <- &indexItem{key: secKey, docid: docid}
	return fdb.fatalDbErr

}

//Delete will delete the given document from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Delete(docid []byte) error {

	fdb.cmdCh <- docid
	return fdb.fatalDbErr

}

//handleCommands keep listening to any buffered
//write requests for the slice and processes
//those. This will shut itself down internal
//shutdown channel is closed.
func (fdb *fdbSlice) handleCommandsWorker(workerId int) {

loop:
	for {
		select {
		case c := <-fdb.cmdCh:
			switch c.(type) {
			case *indexItem:
				cmd := c.(*indexItem)
				start := time.Now()
				fdb.insert((*cmd).key, (*cmd).docid, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			case []byte:
				cmd := c.([]byte)
				start := time.Now()
				fdb.delete(cmd, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			default:
				logging.Errorf("ForestDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", fdb.id, fdb.idxInstId, c)
			}

		case <-fdb.stopCh[workerId]:
			fdb.stopCh[workerId] <- true
			break loop

			//worker gets a status check message on this channel, it responds
			//when its not processing any mutation
		case <-fdb.workerDone[workerId]:
			fdb.workerDone[workerId] <- true

		}
	}

}

//insert does the actual insert in forestdb
func (fdb *fdbSlice) insert(k []byte, docid []byte, workerId int) {

	if fdb.isPrimary {
		fdb.insertPrimaryIndex(docid, workerId)
	} else {
		fdb.insertSecIndex(k, docid, workerId)
	}

}

func (fdb *fdbSlice) insertPrimaryIndex(docid []byte, workerId int) {
	var err error

	logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", fdb.id, fdb.idxInstId, docid)

	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	//check if the docid exists in the main index
	if _, err = fdb.main[workerId].GetKV(entry.Bytes()); err == nil {
		//skip
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Key %v Already Exists. "+
			"Primary Index Update Skipped.", fdb.id, fdb.idxInstId, string(docid))
	} else if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"mainindex entry %v", fdb.id, fdb.idxInstId, err)
	} else if err == forestdb.RESULT_KEY_NOT_FOUND {
		//set in main index
		if err = fdb.main[workerId].SetKV(entry.Bytes(), nil); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
				"Skipped Key %s. Error %v", fdb.id, fdb.idxInstId, string(docid), err)
		}
		atomic.AddInt64(&fdb.insert_bytes, int64(len(entry.Bytes())))
	}
}

func (fdb *fdbSlice) insertSecIndex(k []byte, docid []byte, workerId int) {
	var err error
	var olditm, itm []byte
	var isDeletedFieldEntry bool

	entry, err := NewSecondaryIndexEntry(k, docid)
	switch err {
	case nil:
		itm = entry.Bytes()
	case ErrSecKeyNil:
		err = nil
		isDeletedFieldEntry = true
	}
	common.CrashOnError(err)

	logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s "+
		"DocId - %s", fdb.id, fdb.idxInstId, k, docid)

	//check if the docid exists in the back index
	if olditm, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"backindex entry %v", fdb.id, fdb.idxInstId, err)
		return
	} else if olditm != nil {
		//If old-key from backindex matches with the new-key
		//in mutation, skip it.
		if bytes.Equal(olditm, itm) {
			logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %v. Key %v. Skipped.", fdb.id, fdb.idxInstId, string(docid), itm)
			return
		}

		//there is already an entry in main index for this docid
		//delete from main index
		if err = fdb.main[workerId].DeleteKV(olditm); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		atomic.AddInt64(&fdb.delete_bytes, int64(len(olditm)))

		//delete from back index
		if err = fdb.back[workerId].DeleteKV(docid); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from back index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))
	}

	if isDeletedFieldEntry {
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %s. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//set the back index entry <docid, encodedkey>
	if err = fdb.back[workerId].SetKV(docid, itm); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
			"Skipped Key %s. Value %v. Error %v", fdb.id, fdb.idxInstId, string(docid), itm, err)
		return
	}
	atomic.AddInt64(&fdb.insert_bytes, int64(len(docid)+len(itm)))

	//set in main index
	if err = fdb.main[workerId].SetKV(itm, nil); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
			"Skipped Key %v. Error %v", fdb.id, fdb.idxInstId, itm, err)
		return
	}
	atomic.AddInt64(&fdb.insert_bytes, int64(len(itm)))
}

//delete does the actual delete in forestdb
func (fdb *fdbSlice) delete(docid []byte, workerId int) {

	if fdb.isPrimary {
		fdb.deletePrimaryIndex(docid, workerId)
	} else {
		fdb.deleteSecIndex(docid, workerId)
	}

}

func (fdb *fdbSlice) deletePrimaryIndex(docid []byte, workerId int) {

	logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
		fdb.id, fdb.idxInstId, docid)

	if docid == nil {
		common.CrashOnError(errors.New("Nil Primary Key"))
		return
	}

	//docid -> key format
	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	//delete from main index
	if err := fdb.main[workerId].DeleteKV(entry.Bytes()); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Error %v", fdb.id, fdb.idxInstId,
			docid, err)
		return
	}
	atomic.AddInt64(&fdb.delete_bytes, int64(len(entry.Bytes())))

}

func (fdb *fdbSlice) deleteSecIndex(docid []byte, workerId int) {

	logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var olditm []byte
	var err error

	if olditm, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if olditm == nil {
		logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//delete from main index
	if err = fdb.main[workerId].DeleteKV(olditm); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Key %v. Error %v", fdb.id, fdb.idxInstId,
			docid, olditm, err)
		return
	}
	atomic.AddInt64(&fdb.delete_bytes, int64(len(olditm)))

	//delete from the back index
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}
	atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))

}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (fdb *fdbSlice) getBackIndexEntry(docid []byte, workerId int) ([]byte, error) {

	logging.Tracef("ForestDBSlice::getBackIndexEntry \n\tSliceId %v IndexInstId %v Get BackIndex Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var kbytes []byte
	var err error

	kbytes, err = fdb.back[workerId].GetKV([]byte(docid))
	atomic.AddInt64(&fdb.get_bytes, int64(len(kbytes)))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return nil, err
	}

	return kbytes, nil
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (fdb *fdbSlice) checkFatalDbError(err error) {

	//panic on all DB errors and recover rather than risk
	//inconsistent db state
	common.CrashOnError(err)

	errStr := err.Error()
	switch errStr {

	case "checksum error", "file corruption", "no db instance",
		"alloc fail", "seek fail", "fsync fail":
		fdb.fatalDbErr = err

	}

}

// Creates an open snapshot handle from snapshot info
// Snapshot info is obtained from NewSnapshot() or GetSnapshots() API
// Returns error if snapshot handle cannot be created.
func (fdb *fdbSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	snapInfo := info.(*fdbSnapshotInfo)

	var s *fdbSnapshot
	if fdb.isPrimary {
		s = &fdbSnapshot{slice: fdb,
			idxDefnId:  fdb.idxDefnId,
			idxInstId:  fdb.idxInstId,
			main:       fdb.main[0],
			ts:         snapInfo.Timestamp(),
			mainSeqNum: snapInfo.MainSeq,
			committed:  info.IsCommitted(),
		}
	} else {
		s = &fdbSnapshot{slice: fdb,
			idxDefnId:  fdb.idxDefnId,
			idxInstId:  fdb.idxInstId,
			main:       fdb.main[0],
			back:       fdb.back[0],
			ts:         snapInfo.Timestamp(),
			mainSeqNum: snapInfo.MainSeq,
			backSeqNum: snapInfo.BackSeq,
			committed:  info.IsCommitted(),
		}
	}

	if info.IsCommitted() {
		s.meta = fdb.meta
		s.metaSeqNum = snapInfo.MetaSeq
	}

	logging.Debugf("ForestDBSlice::OpenSnapshot \n\tSliceId %v IndexInstId %v Creating New "+
		"Snapshot %v committed:%v", fdb.id, fdb.idxInstId, snapInfo, s.committed)
	err := s.Open()

	return s, err
}

//Rollback slice to given snapshot. Return error if
//not possible
func (fdb *fdbSlice) Rollback(info SnapshotInfo) error {

	//get the seqnum from snapshot
	snapInfo := info.(*fdbSnapshotInfo)

	infos, err := fdb.getSnapshotsMeta()
	if err != nil {
		return err
	}

	sic := NewSnapshotInfoContainer(infos)
	sic.RemoveRecentThanTS(info.Timestamp())

	//rollback meta-store first, if main/back index rollback fails, recovery
	//will pick up the rolled-back meta information.
	err = fdb.meta.Rollback(snapInfo.MetaSeq)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Meta Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
		return err
	}

	//call forestdb to rollback for each kv store
	err = fdb.main[0].Rollback(snapInfo.MainSeq)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
		return err
	}

	//rollback back-index only for non-primary indexes
	if !fdb.isPrimary {
		err = fdb.back[0].Rollback(snapInfo.BackSeq)
		if err != nil {
			logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
				"Back Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
			return err
		}
	}

	// Update valid snapshot list and commit
	err = fdb.updateSnapshotsMeta(sic.List())
	if err != nil {
		return err
	}

	return fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (fdb *fdbSlice) RollbackToZero() error {

	zeroSeqNum := forestdb.SeqNum(0)
	var err error

	//rollback meta-store first, if main/back index rollback fails, recovery
	//will pick up the rolled-back meta information.
	err = fdb.meta.Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Meta Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	//call forestdb to rollback
	err = fdb.main[0].Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	//rollback back-index only for non-primary indexes
	if !fdb.isPrimary {
		err = fdb.back[0].Rollback(zeroSeqNum)
		if err != nil {
			logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
				"Back Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
			return err
		}
	}

	return nil
}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (fdb *fdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {
	//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
	//check for outstanding mutations. If there are
	//none, proceed with the commit.
	ticker := time.NewTicker(time.Millisecond * SLICE_COMMIT_POLL_INTERVAL)
	for _ = range ticker.C {
		if fdb.checkAllWorkersDone() {
			break
		}
	}

	mainDbInfo, err := fdb.main[0].Info()
	if err != nil {
		return nil, err
	}

	newSnapshotInfo := &fdbSnapshotInfo{
		Ts:        ts,
		MainSeq:   mainDbInfo.LastSeqNum(),
		Committed: commit,
	}

	//for non-primary index add info for back-index
	if !fdb.isPrimary {
		backDbInfo, err := fdb.back[0].Info()
		if err != nil {
			return nil, err
		}
		newSnapshotInfo.BackSeq = backDbInfo.LastSeqNum()
	}

	if commit {

		metaDbInfo, err := fdb.meta.Info()
		if err != nil {
			return nil, err
		}
		//the next meta seqno after this update
		newSnapshotInfo.MetaSeq = metaDbInfo.LastSeqNum() + 1

		infos, err := fdb.getSnapshotsMeta()
		if err != nil {
			return nil, err
		}
		sic := NewSnapshotInfoContainer(infos)
		sic.Add(newSnapshotInfo)

		if sic.Len() > MAX_SNAPSHOTS_PER_INDEX {
			sic.RemoveOldest()
		}

		err = fdb.updateSnapshotsMeta(sic.List())
		if err != nil {
			return nil, err
		}

		// Commit database file
		start := time.Now()
		err = fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
		elapsed := time.Since(start)

		fdb.totalCommitTime += elapsed
		logging.Debugf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v TotalFlushTime %v "+
			"TotalCommitTime %v", fdb.id, fdb.idxInstId, fdb.totalFlushTime, fdb.totalCommitTime)

		if err != nil {
			logging.Errorf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v Error in "+
				"Index Commit %v", fdb.id, fdb.idxInstId, err)
			return nil, err
		}
	}

	return newSnapshotInfo, nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (fdb *fdbSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if len(fdb.cmdCh) > 0 {
		return false
	}

	//worker queue is empty, make sure both workers are done
	//processing the last mutation
	for i := 0; i < fdb.numWriters; i++ {
		fdb.workerDone[i] <- true
		<-fdb.workerDone[i]
	}
	return true
}

func (fdb *fdbSlice) Close() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	logging.Infof("ForestDBSlice::Close \n\tClosing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.idxInstId, fdb.idxDefnId, fdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < fdb.numWriters; i++ {
		fdb.stopCh[i] <- true
		<-fdb.stopCh[i]
	}

	if fdb.refCount > 0 {
		fdb.isSoftClosed = true
	} else {
		tryCloseFdbSlice(fdb)
	}
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (fdb *fdbSlice) Destroy() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	if fdb.refCount > 0 {
		logging.Infof("ForestDBSlice::Destroy \n\tSoftdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		fdb.isSoftDeleted = true
	} else {
		tryDeleteFdbSlice(fdb)
	}
}

//Id returns the Id for this Slice
func (fdb *fdbSlice) Id() SliceId {
	return fdb.id
}

// FilePath returns the filepath for this Slice
func (fdb *fdbSlice) Path() string {
	return fdb.path
}

//IsActive returns if the slice is active
func (fdb *fdbSlice) IsActive() bool {
	return fdb.isActive
}

//SetActive sets the active state of this slice
func (fdb *fdbSlice) SetActive(isActive bool) {
	fdb.isActive = isActive
}

//Status returns the status for this slice
func (fdb *fdbSlice) Status() SliceStatus {
	return fdb.status
}

//SetStatus set new status for this slice
func (fdb *fdbSlice) SetStatus(status SliceStatus) {
	fdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (fdb *fdbSlice) IndexInstId() common.IndexInstId {
	return fdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (fdb *fdbSlice) IndexDefnId() common.IndexDefnId {
	return fdb.idxDefnId
}

// Returns snapshot info list
func (fdb *fdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	infos, err := fdb.getSnapshotsMeta()
	return infos, err
}

func (fdb *fdbSlice) Compact() error {
	fdb.IncrRef()
	defer fdb.DecrRef()

	//get oldest snapshot upto which compaction can be done
	infos, err := fdb.getSnapshotsMeta()
	if err != nil {
		return err
	}

	sic := NewSnapshotInfoContainer(infos)

	osnap := sic.GetOldest()
	if osnap == nil {
		logging.Debugf("ForestDBSlice::Compact \n\tNo Snapshot Found. Skipped Compaction."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		return nil
	}

	mainSeq := osnap.(*fdbSnapshotInfo).MainSeq

	//find the db snapshot lower than oldest snapshot
	snap, err := fdb.dbfile.GetAllSnapMarkers()
	if err != nil {
		return err
	}
	defer snap.FreeSnapMarkers()

	var snapMarker *forestdb.SnapMarker
	var compactSeqNum forestdb.SeqNum
snaploop:
	for _, s := range snap.SnapInfoList() {

		cm := s.GetKvsCommitMarkers()
		for _, c := range cm {
			//if seqNum of "main" kvs is less than or equal to oldest snapshot seqnum
			//it is safe to compact upto that snapshot
			if c.GetKvStoreName() == "main" && c.GetSeqNum() <= mainSeq {
				snapMarker = s.GetSnapMarker()
				compactSeqNum = c.GetSeqNum()
				break snaploop
			}
		}
	}

	if snapMarker == nil {
		logging.Debugf("ForestDBSlice::Compact \n\tNo Valid SnapMarker Found. Skipped Compaction."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		return nil
	} else {
		logging.Debugf("ForestDBSlice::Compact \n\tCompacting upto SeqNum %v. "+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", compactSeqNum, fdb.id,
			fdb.idxInstId, fdb.idxDefnId)
	}

	newpath := newFdbFile(fdb.path, true)
	err = fdb.dbfile.CompactUpto(newpath, snapMarker)
	if err != nil {
		return err
	}

	if _, e := os.Stat(fdb.currfile); e == nil {
		err = os.Remove(fdb.currfile)
	}

	fdb.currfile = newpath
	return err
}

func (fdb *fdbSlice) Statistics() (StorageStatistics, error) {
	var sts StorageStatistics
	f, err := os.Open(fdb.currfile)
	if err != nil {
		return sts, err
	}
	fi, err := f.Stat()
	if err != nil {
		return sts, err
	}

	sts.DataSize = int64(fdb.statFd.EstimateSpaceUsed())
	sts.DiskSize = fi.Size()
	sts.GetBytes = atomic.LoadInt64(&fdb.get_bytes)
	sts.InsertBytes = atomic.LoadInt64(&fdb.insert_bytes)
	sts.DeleteBytes = atomic.LoadInt64(&fdb.delete_bytes)

	return sts, nil
}

func (fdb *fdbSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", fdb.id)
	str += fmt.Sprintf("File: %v ", fdb.path)
	str += fmt.Sprintf("Index: %v ", fdb.idxInstId)

	return str

}

func (fdb *fdbSlice) updateSnapshotsMeta(infos []SnapshotInfo) error {
	fdb.metaLock.Lock()
	defer fdb.metaLock.Unlock()

	val, err := json.Marshal(infos)
	if err != nil {
		goto handle_err
	}
	err = fdb.meta.SetKV(snapshotMetaListKey, val)
	if err != nil {
		goto handle_err
	}
	return nil

handle_err:
	return errors.New("Failed to update snapshots list -" + err.Error())
}

func (fdb *fdbSlice) getSnapshotsMeta() ([]SnapshotInfo, error) {
	var tmp []*fdbSnapshotInfo
	var snapList []SnapshotInfo

	fdb.metaLock.Lock()
	defer fdb.metaLock.Unlock()

	data, err := fdb.meta.GetKV(snapshotMetaListKey)

	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return []SnapshotInfo(nil), nil
		}
		return nil, err
	}

	err = json.Unmarshal(data, &tmp)
	if err != nil {
		goto handle_err
	}

	for i := range tmp {
		snapList = append(snapList, tmp[i])
	}

	return snapList, nil

handle_err:
	return snapList, errors.New("Failed to retrieve snapshots list -" + err.Error())
}

func tryDeleteFdbSlice(fdb *fdbSlice) {
	logging.Infof("ForestDBSlice::Destroy \n\tDestroying Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)

	if err := forestdb.Destroy(fdb.currfile, fdb.config); err != nil {
		logging.Errorf("ForestDBSlice::Destroy \n\t Error Destroying  Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}

	//cleanup the disk directory
	if err := os.RemoveAll(fdb.path); err != nil {
		logging.Errorf("ForestDBSlice::Destroy \n\t Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}
}

func tryCloseFdbSlice(fdb *fdbSlice) {
	//close the main index
	if fdb.main[0] != nil {
		fdb.main[0].Close()
	}

	if !fdb.isPrimary {
		//close the back index
		if fdb.back[0] != nil {
			fdb.back[0].Close()
		}
	}

	if fdb.meta != nil {
		fdb.meta.Close()
	}

	fdb.statFd.Close()
	fdb.dbfile.Close()
}

func newFdbFile(dirpath string, newVersion bool) string {
	var version int = 0

	pattern := fmt.Sprintf("data.fdb.*")
	files, _ := filepath.Glob(filepath.Join(dirpath, pattern))
	sort.Strings(files)
	// Pick the first file with least version
	if len(files) > 0 {
		filename := filepath.Base(files[0])
		_, err := fmt.Sscanf(filename, "data.fdb.%d", &version)
		if err != nil {
			panic(fmt.Sprintf("Invalid data file %s (%v)", files[0], err))
		}
	}

	if newVersion {
		version++
	}

	newFilename := fmt.Sprintf("data.fdb.%d", version)
	return filepath.Join(dirpath, newFilename)
}
