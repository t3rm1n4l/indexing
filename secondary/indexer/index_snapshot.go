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
	"github.com/couchbase/indexing/secondary/common"
)

// IndexSnapshot is an immutable data structure that provides point-in-time
// snapshot of an index instance held by an indexer.
// A consumer receiving a snapshot object can use it for scanning index entries
// Once the consumer has finished using this object, DestroyIndexSnapshot() method
// should be called to deallocate resources held by this object. Otherwise, it is
// consumer's responsibility to deallocate resources.
// A copy of the snapshot object can be made using CloneIndexSnapshot() method.
// A snapshot object should not be shared across multiple go routines unless they
// are serialized. CloneIndexSnapshot() should be used to create a copy of the object
// if the snapshot needs to be concurrently shared to multiple go routines.
type IndexSnapshot interface {
	IndexInstId() common.IndexInstId
	Timestamp() *common.TsVbuuid
	Partitions() map[common.PartitionId]PartitionSnapshot
}

type PartitionSnapshot interface {
	PartitionId() common.PartitionId
	Slices() map[SliceId]SliceSnapshot
}

type SliceSnapshot interface {
	SliceId() SliceId
	Snapshot() Snapshot
}

type indexSnapshot struct {
	instId common.IndexInstId
	ts     *common.TsVbuuid
	partns map[common.PartitionId]PartitionSnapshot
}

func (is *indexSnapshot) IndexInstId() common.IndexInstId {
	return is.instId
}

func (is *indexSnapshot) Timestamp() *common.TsVbuuid {
	return is.ts
}

func (is *indexSnapshot) Partitions() map[common.PartitionId]PartitionSnapshot {
	return is.partns
}

type partitionSnapshot struct {
	id     common.PartitionId
	slices map[SliceId]SliceSnapshot
}

func (ps *partitionSnapshot) PartitionId() common.PartitionId {
	return ps.id
}

func (ps *partitionSnapshot) Slices() map[SliceId]SliceSnapshot {
	return ps.slices
}

type sliceSnapshot struct {
	id   SliceId
	snap Snapshot
}

func (ss *sliceSnapshot) SliceId() SliceId {
	return ss.id
}

func (ss *sliceSnapshot) Snapshot() Snapshot {
	return ss.snap
}

func DestroyIndexSnapshot(is IndexSnapshot) error {
	if is == nil {
		return nil
	}
	for _, ps := range is.Partitions() {
		for _, ss := range ps.Slices() {
			ss.Snapshot().Close()
		}
	}
	return nil
}

func CloneIndexSnapshot(is IndexSnapshot) IndexSnapshot {
	if is == nil {
		return nil
	}
	for _, ps := range is.Partitions() {
		for _, ss := range ps.Slices() {
			ss.Snapshot().Open()
		}
	}
	return is
}

func GetSliceSnapshots(is IndexSnapshot) (s []SliceSnapshot) {
	if is == nil {
		return
	}

	for _, p := range is.Partitions() {
		for _, sl := range p.Slices() {
			s = append(s, sl)
		}
	}

	return
}
