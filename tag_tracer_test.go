package pubsub

import (
	"github.com/benbjohnson/clock"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	connmgri "github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"testing"
	"time"
)

func TestTagTracerMeshTags(t *testing.T) {
	// test that tags are applied when the tagTracer sees graft and prune events

	cmgr := connmgr.NewConnManager(5, 10, time.Minute)
	tt := newTagTracer(cmgr)

	p := peer.ID("a-peer")
	topic := "a-topic"

	tt.Join(topic)
	tt.Graft(p, topic)

	tag := "pubsub:" + topic
	val := getTagValue(cmgr, p, tag)
	if val != GossipSubConnTagValueMeshPeer {
		t.Errorf("expected mesh peer to have tag %s with value %d, got %d",
			tag, GossipSubConnTagValueMeshPeer, val)
	}

	tt.Prune(p, topic)
	val = getTagValue(cmgr, p, tag)
	if val != 0 {
		t.Errorf("expected peer to be untagged when pruned from mesh, but tag %s was %d", tag, val)
	}
}

func TestTagTracerDirectPeerTags(t *testing.T) {
	// test that we add a tag to direct peers
	cmgr := connmgr.NewConnManager(5, 10, time.Minute)
	tt := newTagTracer(cmgr)

	p1 := peer.ID("1")
	p2 := peer.ID("2")
	p3 := peer.ID("3")

	// in the real world, tagTracer.direct is set in the WithDirectPeers option function
	tt.direct = make(map[peer.ID]struct{})
	tt.direct[p1] = struct{}{}

	tt.AddPeer(p1, GossipSubID_v10)
	tt.AddPeer(p2, GossipSubID_v10)
	tt.AddPeer(p3, GossipSubID_v10)

	tag := "pubsub:direct"
	val := getTagValue(cmgr, p1, tag)
	if val != GossipSubConnTagValueDirectPeer {
		t.Errorf("expected direct peer to have tag %s value %d, was %d", tag, GossipSubConnTagValueDirectPeer, val)
	}

	for _, p := range []peer.ID{p2, p3} {
		val := getTagValue(cmgr, p, tag)
		if val != 0 {
			t.Errorf("expected non-direct peer to have tag %s value %d, was %d", tag, 0, val)
		}
	}
}

func TestTagTracerDeliveryTags(t *testing.T) {
	// test decaying delivery tags

	// use fake time to test the tag decay
	clk := clock.NewMock()
	decayCfg := &connmgr.DecayerCfg{
		Clock:      clk,
		Resolution: time.Minute,
	}
	cmgr := connmgr.NewConnManager(5, 10, time.Minute, connmgr.DecayerConfig(decayCfg))

	tt := newTagTracer(cmgr)

	topic1 := "topic-1"
	topic2 := "topic-2"

	p := peer.ID("a-peer")

	tt.Join(topic1)
	tt.Join(topic2)

	for i := 0; i < 20; i++ {
		// deliver only 5 messages to topic 2 (less than the cap)
		topics := []string{topic1}
		if i < 5 {
			topics = append(topics, topic2)
		}
		msg := &Message{
			ReceivedFrom: p,
			Message: &pb.Message{
				From:     []byte(p),
				Data:     []byte("hello"),
				TopicIDs: topics,
			},
		}
		tt.DeliverMessage(msg)
	}

	// we have to tick the fake clock once to apply the bump
	clk.Add(time.Minute)

	// the tag value for topic-1 should be capped at GossipSubConnTagMessageDeliveryCap (default 15)
	val := getTagValue(cmgr, p, "pubsub-deliveries:topic-1")
	expected := GossipSubConnTagMessageDeliveryCap
	if val != expected {
		t.Errorf("expected delivery tag to be capped at %d, was %d", expected, val)
	}

	// the value for topic-2 should equal the number of messages delivered (5), since it was less than the cap
	val = getTagValue(cmgr, p, "pubsub-deliveries:topic-2")
	expected = 5
	if val != expected {
		t.Errorf("expected delivery tag value = %d, got %d", expected, val)
	}

	// if we jump forward a few minutes, we should see the tags decrease by 1 / minute
	clk.Add(5 * time.Minute)

	val = getTagValue(cmgr, p, "pubsub-deliveries:topic-1")
	expected = GossipSubConnTagMessageDeliveryCap - 5
	if val != expected {
		t.Errorf("expected delivery tag value = %d, got %d", expected, val)
	}

	// the tag for topic-2 should have reset to zero by now
	val = getTagValue(cmgr, p, "pubsub-deliveries:topic-2")
	expected = 0
	if val != expected {
		t.Errorf("expected delivery tag value = %d, got %d", expected, val)
	}
}

func getTagValue(mgr connmgri.ConnManager, p peer.ID, tag string) int {
	info := mgr.GetTagInfo(p)
	if info == nil {
		return 0
	}
	val, ok := info.Tags[tag]
	if !ok {
		return 0
	}
	return val
}
