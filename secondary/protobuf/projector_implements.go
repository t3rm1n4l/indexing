package protobuf

import (
	"github.com/couchbaselabs/goprotobuf/proto"
	c "github.com/couchbase/indexing/secondary/common"
)

// Mask values for stream flag
const (
	// start vbucket-streams if it is not already active.
	maskMutationStreamStart = 0x00000001

	// start vbucket-streams whether it is already active or not.
	maskMutationStreamRestart = 0x00000002

	// shutdown vbucket-streams
	maskMutationStreamShutdown = 0x00000004

	// add new buckets and its engines.
	maskMutationStreamAddBuckets = 0x00000008

	// remove an existing bucket and its engines.
	maskMutationStreamDelBuckets = 0x00000010

	// add / update / delete engines
	maskAddEngines    = 0x00000020
	maskUpdateEngines = 0x00000040
	maskDeleteEngines = 0x00000080
)

// Interface API for RequestReader and Subscriber

func (req *MutationStreamRequest) SetStartFlag() {
	req.Flag = proto.Uint32(uint32(0x0) | maskMutationStreamStart)
}

func (req *MutationStreamRequest) IsStart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamStart) > 0
}

func (req *MutationStreamRequest) IsRestart() bool {
	return false
}

func (req *MutationStreamRequest) IsShutdown() bool {
	return false
}

func (req *MutationStreamRequest) IsAddBuckets() bool {
	return false
}

func (req *MutationStreamRequest) IsDelBuckets() bool {
	return false
}

func (req *MutationStreamRequest) RestartTimestamp(bucket string) *TsVbuuid {
	restartTimestamps := req.GetRestartTimestamps()
	for i, b := range req.GetBuckets() {
		if bucket == b {
			return restartTimestamps[i]
		}
	}
	return nil
}

func (req *MutationStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

func (req *MutationStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// interface API for RequestReader and Subscriber

func (req *UpdateMutationStreamRequest) SetRestartFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskMutationStreamRestart)
}

func (req *UpdateMutationStreamRequest) SetShutdownFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskMutationStreamShutdown)
}

func (req *UpdateMutationStreamRequest) SetAddBucketFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskMutationStreamAddBuckets)
}

func (req *UpdateMutationStreamRequest) SetDelBucketFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskMutationStreamDelBuckets)
}

func (req *UpdateMutationStreamRequest) IsStart() bool {
	return false
}

func (req *UpdateMutationStreamRequest) IsRestart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamRestart) > 0
}

func (req *UpdateMutationStreamRequest) IsShutdown() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamShutdown) > 0
}

func (req *UpdateMutationStreamRequest) IsAddBuckets() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamAddBuckets) > 0
}

func (req *UpdateMutationStreamRequest) IsDelBuckets() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamDelBuckets) > 0
}

func (req *UpdateMutationStreamRequest) RestartTimestamp(bucket string) *TsVbuuid {
	restartTimestamps := req.GetRestartTimestamps()
	for i, b := range req.GetBuckets() {
		if bucket == b {
			return restartTimestamps[i]
		}
	}
	return nil
}

func (req *UpdateMutationStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

func (req *UpdateMutationStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// interface API for flags and Subscriber

func (req *SubscribeStreamRequest) SetAddEnginesFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskAddEngines)
}

func (req *SubscribeStreamRequest) SetUpdateEnginesFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskUpdateEngines)
}

func (req *SubscribeStreamRequest) SetDeleteEnginesFlag() {
	req.Flag = proto.Uint32(req.GetFlag() | maskDeleteEngines)
}

func (req *SubscribeStreamRequest) IsAddEngines() bool {
	flag := req.GetFlag()
	return (flag & maskAddEngines) > 0
}

func (req *SubscribeStreamRequest) IsUpdateEngines() bool {
	flag := req.GetFlag()
	return (flag & maskUpdateEngines) > 0
}

func (req *SubscribeStreamRequest) IsDeleteEngines() bool {
	flag := req.GetFlag()
	return (flag & maskDeleteEngines) > 0
}

func (req *SubscribeStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

func (req *SubscribeStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// TODO: add other types of entities
func getEvaluators(instances []*IndexInst) (map[uint64]c.Evaluator, error) {
	var err error

	entities := make(map[uint64]c.Evaluator, 0)
	for _, instance := range instances {
		defn := instance.GetDefinition()
		ie := NewIndexEvaluator(instance)
		if err = ie.Compile(); err != nil {
			return nil, err
		}
		entities[defn.GetDefnID()] = ie
	}
	return entities, nil
}

// TODO: add other types of entities
func getRouters(instances []*IndexInst) (map[uint64]c.Router, error) {
	entities := make(map[uint64]c.Router, 0)
	for _, instance := range instances {
		defn := instance.GetDefinition()
		entities[defn.GetDefnID()] = instance
	}
	return entities, nil
}
