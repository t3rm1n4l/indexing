package projector

import "errors"
import "fmt"
import "sync"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbaselabs/go-couchbase"
import "github.com/couchbaselabs/goprotobuf/proto"

// ErrorTopicExist
var ErrorTopicExist = errors.New("projector.topicExist")

// ErrorTopicMissing
var ErrorTopicMissing = errors.New("projector.topicMissing")

// Projector data structure, a projector is connected to
// one or more upstream kv-nodes. Works in tandem with
// projector's adminport.
type Projector struct {
	mu      sync.RWMutex
	admind  ap.Server                    // admin-port server
	topics  map[string]*Feed             // active topics
	buckets map[string]*couchbase.Bucket // bucket instances

	// config params
	name        string   // human readable name of the projector
	clusterAddr string   // kv cluster's address to connect
	adminport   string   // projector listens on this adminport
	kvset       []string // set of kv-nodes to connect with
	logPrefix   string
	config      c.Config // full configuration information.
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(config c.Config) *Projector {
	pconf := config.SectionConfig("projector.", true)
	p := &Projector{
		name:        pconf["name"].String(),
		clusterAddr: pconf["clusterAddr"].String(),
		kvset:       pconf["kvAddrs"].Strings(),
		adminport:   pconf["adminport.listenAddr"].String(),
		topics:      make(map[string]*Feed),
		buckets:     make(map[string]*couchbase.Bucket),
	}
	p.logPrefix = fmt.Sprintf("[%s(%s)]", p.name, p.kvset)
	p.config = config

	apConfig := pconf.SectionConfig("adminport.", true)
	apConfig = apConfig.SetValue("name", p.name+"-adminport")
	reqch := make(chan ap.Request)
	p.admind = ap.NewHTTPServer(apConfig, reqch)

	go p.mainAdminPort(reqch)
	c.Infof("%v started ...\n", p.logPrefix)
	return p
}

// GetFeed object for `topic`
func (p *Projector) GetFeed(topic string) (*Feed, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if feed, ok := p.topics[topic]; ok {
		return feed, nil
	}
	return nil, ErrorTopicMissing
}

// AddFeed object for `topic`
func (p *Projector) AddFeed(topic string, feed *Feed) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.topics[topic]; ok {
		return ErrorTopicExist
	}
	p.topics[topic] = feed
	c.Infof("%v %q feed added ...", p.logPrefix, topic)
	return
}

// DelFeed object for `topic`
func (p *Projector) DelFeed(topic string) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.topics[topic]; ok == false {
		return ErrorTopicMissing
	}
	delete(p.topics, topic)
	c.Infof("%v ... %q feed deleted", p.logPrefix, topic)
	return
}

//---- handler for admin-port request

func (p *Projector) doVbmapRequest(
	request *protobuf.VbmapRequest) ap.MessageMarshaller {

	c.Debugf("%v doVbmapRequest\n", p.logPrefix)
	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	// get vbmap from bucket connection.
	bucket, err := p.getBucket(pooln, bucketn)
	if err != nil {
		c.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	bucket.Refresh()
	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		c.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	// compose response
	response.Kvaddrs = make([]string, 0, len(kvaddrs))
	response.Kvvbnos = make([]*protobuf.Vbuckets, 0, len(kvaddrs))
	for kvaddr, vbnos := range m {
		response.Kvaddrs = append(response.Kvaddrs, kvaddr)
		response.Kvvbnos = append(
			response.Kvvbnos, &protobuf.Vbuckets{Vbnos: c.Vbno16to32(vbnos)})
	}
	return response
}

func (p *Projector) doFailoverLog(
	request *protobuf.FailoverLogRequest) ap.MessageMarshaller {

	c.Debugf("%v doFailoverLog\n", p.logPrefix)
	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	bucket, err := p.getBucket(pooln, bucketn)
	if err != nil {
		c.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	vbnos := c.Vbno32to16(vbuckets)
	if flogs, err := bucket.GetFailoverLogs(vbnos); err == nil {
		for vbno, flog := range flogs {
			vbuuids := make([]uint64, 0, len(flog))
			seqnos := make([]uint64, 0, len(flog))
			for _, x := range flog {
				vbuuids = append(vbuuids, x[0])
				seqnos = append(seqnos, x[1])
			}
			protoFlog := &protobuf.FailoverLog{
				Vbno:    proto.Uint32(uint32(vbno)),
				Vbuuids: vbuuids,
				Seqnos:  seqnos,
			}
			protoFlogs = append(protoFlogs, protoFlog)
		}
	} else {
		c.Errorf("%v %s.GetFailoverLogs() %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	response.Logs = protoFlogs
	return response
}

func (p *Projector) doMutationTopic(
	request *protobuf.MutationTopicRequest) ap.MessageMarshaller {

	c.Debugf("%v doMutationTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic)
	if err == nil { // only fresh feed to be started
		c.Errorf("%v %v\n", p.logPrefix, ErrorTopicExist)
		return (&protobuf.TopicResponse{}).SetErr(ErrorTopicExist)
	}

	pconf := p.config.SectionConfig("projector.", true)
	config, _ := c.NewConfig(map[string]interface{}{})
	config.SetValue("name", p.adminport)
	config.Set("maxVbuckets", p.config["maxVbuckets"])
	config.Set("clusterAddr", pconf["clusterAddr"])
	config.Set("kvAddrs", pconf["kvAddrs"])
	config.Set("feedWaitStreamReqTimeout", pconf["feedWaitStreamReqTimeout"])
	config.Set("feedWaitStreamEndTimeout", pconf["feedWaitStreamEndTimeout"])
	config.Set("feedChanSize", pconf["feedChanSize"])
	config.Set("routerEndpointFactory", pconf["routerEndpointFactory"])

	feed = NewFeed(topic, config)
	response, err := feed.MutationTopic(request)
	if err == nil {
		p.AddFeed(topic, feed)
		return response
	}
	if feed != nil {
		feed.Shutdown() // on error close the feed
	}
	response.SetErr(err)
	return (&protobuf.TopicResponse{}).SetErr(err)
}

func (p *Projector) doRestartVbuckets(
	request *protobuf.RestartVbucketsRequest) ap.MessageMarshaller {

	c.Debugf("%v doRestartVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return (&protobuf.TopicResponse{}).SetErr(err)
	}

	response, err := feed.RestartVbuckets(request)
	if err == nil {
		return response
	}
	return (&protobuf.TopicResponse{}).SetErr(err)
}

func (p *Projector) doShutdownVbuckets(
	request *protobuf.ShutdownVbucketsRequest) ap.MessageMarshaller {

	c.Debugf("%v doShutdownVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.ShutdownVbuckets(request)
	return protobuf.NewError(err)
}

func (p *Projector) doAddBuckets(
	request *protobuf.AddBucketsRequest) ap.MessageMarshaller {

	c.Debugf("%v doAddBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return (&protobuf.TopicResponse{}).SetErr(err)
	}

	response, err := feed.AddBuckets(request)
	if err == nil {
		return response
	}
	return (&protobuf.TopicResponse{}).SetErr(err)
}

func (p *Projector) doDelBuckets(
	request *protobuf.DelBucketsRequest) ap.MessageMarshaller {

	c.Debugf("%v doDelBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.DelBuckets(request)
	return protobuf.NewError(err)
}

func (p *Projector) doAddInstances(
	request *protobuf.AddInstancesRequest) ap.MessageMarshaller {

	c.Debugf("%v doAddInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.AddInstances(request)
	return protobuf.NewError(err)
}

func (p *Projector) doDelInstances(
	request *protobuf.DelInstancesRequest) ap.MessageMarshaller {

	c.Debugf("%v doDelInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.DelInstances(request)
	return protobuf.NewError(err)
}

func (p *Projector) doRepairEndpoints(
	request *protobuf.RepairEndpointsRequest) ap.MessageMarshaller {

	c.Debugf("%v doRepairEndpoints()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.RepairEndpoints(request)
	return protobuf.NewError(err)
}

func (p *Projector) doShutdownTopic(
	request *protobuf.ShutdownTopicRequest) ap.MessageMarshaller {

	c.Debugf("%v doShutdownTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	p.DelFeed(topic)
	feed.Shutdown()
	return protobuf.NewError(err)
}

func (p *Projector) doStatistics(request c.Statistics) ap.MessageMarshaller {

	c.Debugf("%v doStatistics()\n", p.logPrefix)

	m := map[string]interface{}{
		"clusterAddr": p.clusterAddr,
		"adminport":   p.adminport,
		"kvset":       p.kvset,
		"topics":      p.listTopics(),
	}
	stats, _ := c.NewStatistics(m)

	feeds, _ := c.NewStatistics(nil)
	for topic, feed := range p.topics {
		feeds.Set(topic, feed.GetStatistics())
	}
	stats.Set("feeds", feeds)
	stats.Set("adminport", p.admind.GetStatistics())
	return stats
}

// return list of active topics
func (p *Projector) listTopics() []string {
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}

// get couchbase bucket from SDK.
func (p *Projector) getBucket(pooln, bucketn string) (*couchbase.Bucket, error) {
	bucket, ok := p.buckets[bucketn]
	if !ok {
		return c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	}
	return bucket, nil
}
