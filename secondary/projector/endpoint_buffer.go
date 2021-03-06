package projector

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
)

type endpointBuffers struct {
	raddr string
	vbs   map[string]*c.VbKeyVersions
}

func newEndpointBuffers(raddr string) *endpointBuffers {
	vbs := make(map[string]*c.VbKeyVersions)
	b := &endpointBuffers{raddr, vbs}
	return b
}

// addKeyVersions, add a mutation to VbKeyVersions.
func (b *endpointBuffers) addKeyVersions(bucket string, vbno uint16, vbuuid uint64, kv *c.KeyVersions) {
	uuid := c.ID(bucket, vbno)
	if _, ok := b.vbs[uuid]; !ok {
		nMuts := 10 // TODO: avoid magic numbers
		b.vbs[uuid] = c.NewVbKeyVersions(bucket, vbno, vbuuid, nMuts)
	}
	b.vbs[uuid].AddKeyVersions(kv)
}

// flush the buffers to the other end.
func (b *endpointBuffers) flushBuffers(client *dataport.Client) error {
	vbs := make([]*c.VbKeyVersions, 0, len(b.vbs))
	for _, vb := range b.vbs {
		vbs = append(vbs, vb)
	}
	b.vbs = make(map[string]*c.VbKeyVersions) // re-initialize
	if err := client.SendKeyVersions(vbs); err != nil {
		return err
	}
	return nil
}
