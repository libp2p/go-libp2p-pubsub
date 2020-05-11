package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var (
	// GossipSubConnTagValueDirectPeer is the connection manager tag value to
	// apply to direct peers. This should be high, as we want to prioritize these
	// connections above all others.
	GossipSubConnTagValueDirectPeer = 1000

	// GossipSubConnTagValueMeshPeer is the connection manager tag value to apply to
	// peers in a topic mesh. If a peer is in the mesh for multiple topics, their
	// connection will be tagged separately for each.
	GossipSubConnTagValueMeshPeer = 20

	// GossipSubConnTagBumpMessageDelivery is the amount to add to the connection manager
	// tag that tracks message deliveries. Each time a peer is the first to deliver a
	// message within a topic, we "bump" a tag by this amount, up to a maximum
	// of GossipSubConnTagMessageDeliveryCap.
	// Note that the delivery tags decay over time, decreasing by GossipSubConnTagDecayAmount
	// at every GossipSubConnTagDecayInterval.
	GossipSubConnTagBumpMessageDelivery = 1

	// GossipSubConnTagDecayInterval is the decay interval for decaying connection manager tags.
	GossipSubConnTagDecayInterval = 10 * time.Minute

	// GossipSubConnTagDecayAmount is subtracted from decaying tag values at each decay interval.
	GossipSubConnTagDecayAmount = 1

	// GossipSubConnTagMessageDeliveryCap is the maximum value for the connection manager tags that
	// track message deliveries.
	GossipSubConnTagMessageDeliveryCap = 15
)

// tagTracer is an internal tracer that applies connection manager tags to peer
// connections based on their behavior.
//
// We tag a peer's connections for the following reasons:
// - Directly connected peers are tagged with GossipSubConnTagValueDirectPeer (default 1000).
// - Mesh peers are tagged with a value of GossipSubConnTagValueMeshPeer (default 20).
//   If a peer is in multiple topic meshes, they'll be tagged for each.
// - For each message that we receive, we bump a delivery tag for peer that delivered the message
//   first.
//   The delivery tags have a maximum value, GossipSubConnTagMessageDeliveryCap, and they decay at
//   a rate of GossipSubConnTagDecayAmount / GossipSubConnTagDecayInterval.
type tagTracer struct {
	sync.RWMutex

	cmgr     connmgr.ConnManager
	msgID    MsgIdFunction
	decayer  connmgr.Decayer
	decaying map[string]connmgr.DecayingTag
	direct   map[peer.ID]struct{}

	// track message deliveries to reward "near first" deliveries
	// (a delivery that occurs while we're still validating the message)
	deliveries *messageDeliveries
}

func newTagTracer(cmgr connmgr.ConnManager) *tagTracer {
	decayer, ok := connmgr.SupportsDecay(cmgr)
	if !ok {
		log.Warnf("connection manager does not support decaying tags, delivery tags will not be applied")
	}
	return &tagTracer{
		cmgr:       cmgr,
		msgID:      DefaultMsgIdFn,
		decayer:    decayer,
		decaying:   make(map[string]connmgr.DecayingTag),
		deliveries: &messageDeliveries{records: make(map[string]*deliveryRecord)},
	}
}

func (t *tagTracer) Start(gs *GossipSubRouter) {
	if t == nil {
		return
	}

	t.msgID = gs.p.msgID
	t.direct = gs.direct
	go t.background(gs.p.ctx)
}

func (t *tagTracer) background(ctx context.Context) {
	gcDeliveryRecords := time.NewTicker(time.Minute)
	defer gcDeliveryRecords.Stop()

	for {
		select {
		case <-gcDeliveryRecords.C:
			t.gcDeliveryRecords()
		case <-ctx.Done():
			return
		}
	}
}

func (t *tagTracer) gcDeliveryRecords() {
	t.Lock()
	defer t.Unlock()
	t.deliveries.gc()
}

func (t *tagTracer) tagPeerIfDirect(p peer.ID) {
	if t.direct == nil {
		return
	}

	// tag peer if it is a direct peer
	_, direct := t.direct[p]
	if direct {
		t.cmgr.TagPeer(p, "pubsub:direct", GossipSubConnTagValueDirectPeer)
	}
}

func (t *tagTracer) tagMeshPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	t.cmgr.TagPeer(p, tag, GossipSubConnTagValueMeshPeer)
}

func (t *tagTracer) untagMeshPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	t.cmgr.UntagPeer(p, tag)
}

func topicTag(topic string) string {
	return fmt.Sprintf("pubsub:%s", topic)
}

func (t *tagTracer) addDeliveryTag(topic string) {
	if t.decayer == nil {
		return
	}

	t.Lock()
	defer t.Unlock()
	tag, err := t.decayingDeliveryTag(topic)
	if err != nil {
		log.Warnf("unable to create decaying delivery tag: %s", err)
		return
	}
	t.decaying[topic] = tag
}

func (t *tagTracer) removeDeliveryTag(topic string) {
	t.Lock()
	defer t.Unlock()
	delete(t.decaying, topic)
}

func (t *tagTracer) decayingDeliveryTag(topic string) (connmgr.DecayingTag, error) {
	if t.decayer == nil {
		return nil, fmt.Errorf("connection manager does not support decaying tags")
	}
	name := fmt.Sprintf("pubsub-deliveries:%s", topic)

	// decrement tag value by GossipSubConnTagDecayAmount at each decay interval
	decayFn := func(value connmgr.DecayingValue) (after int, rm bool) {
		v := value.Value - GossipSubConnTagDecayAmount
		return v, v <= 0
	}

	// bump up to max of GossipSubConnTagMessageDeliveryCap
	bumpFn := func(value connmgr.DecayingValue, delta int) (after int) {
		val := value.Value + delta
		if val > GossipSubConnTagMessageDeliveryCap {
			return GossipSubConnTagMessageDeliveryCap
		}
		return val
	}

	return t.decayer.RegisterDecayingTag(name, GossipSubConnTagDecayInterval, decayFn, bumpFn)
}

func (t *tagTracer) bumpDeliveryTag(p peer.ID, topic string) error {
	t.RLock()
	defer t.RUnlock()

	tag, ok := t.decaying[topic]
	if !ok {
		return fmt.Errorf("no decaying tag registered for topic %s", topic)
	}
	return tag.Bump(p, GossipSubConnTagBumpMessageDelivery)
}

func (t *tagTracer) bumpTagsForMessage(p peer.ID, msg *Message) {
	for _, topic := range msg.TopicIDs {
		err := t.bumpDeliveryTag(p, topic)
		if err != nil {
			log.Warnf("error bumping delivery tag: %s", err)
		}
	}
}

// nearFirstPeers returns the peers who delivered the message while it was still validating
func (t *tagTracer) nearFirstPeers(msg *Message) []peer.ID {
	t.Lock()
	defer t.Unlock()
	drec := t.deliveries.getRecord(t.msgID(msg.Message))
	nearFirstPeers := make([]peer.ID, 0, len(drec.peers))
	// defensive check that this is the first delivery trace -- delivery status should be unknown
	if drec.status != deliveryUnknown {
		log.Warnf("unexpected delivery trace: message from %s was first seen %s ago and has delivery status %d", msg.ReceivedFrom, time.Now().Sub(drec.firstSeen), drec.status)
		return nearFirstPeers
	}

	drec.status = deliveryValid
	drec.validated = time.Now()

	for p := range drec.peers {
		// this check is to make sure a peer can't send us a message twice and get a double count
		// if it is a first delivery.
		if p != msg.ReceivedFrom {
			nearFirstPeers = append(nearFirstPeers, p)
		}
	}
	// we're done with the peers map and can reclaim the memory
	drec.peers = nil
	return nearFirstPeers
}

// -- internalTracer interface methods
var _ internalTracer = (*tagTracer)(nil)

func (t *tagTracer) AddPeer(p peer.ID, proto protocol.ID) {
	t.tagPeerIfDirect(p)
}

func (t *tagTracer) Join(topic string) {
	t.addDeliveryTag(topic)
}

func (t *tagTracer) DeliverMessage(msg *Message) {
	nearFirst := t.nearFirstPeers(msg)

	t.bumpTagsForMessage(msg.ReceivedFrom, msg)
	for _, p := range nearFirst {
		t.bumpTagsForMessage(p, msg)
	}
}

func (t *tagTracer) Leave(topic string) {
	t.removeDeliveryTag(topic)
}

func (t *tagTracer) Graft(p peer.ID, topic string) {
	t.tagMeshPeer(p, topic)
}

func (t *tagTracer) Prune(p peer.ID, topic string) {
	t.untagMeshPeer(p, topic)
}

func (t *tagTracer) ValidateMessage(msg *Message) {
	t.Lock()
	defer t.Unlock()

	// create a delivery record for the message
	_ = t.deliveries.getRecord(t.msgID(msg.Message))
}

func (t *tagTracer) DuplicateMessage(msg *Message) {
	t.Lock()
	defer t.Unlock()

	drec := t.deliveries.getRecord(t.msgID(msg.Message))
	if drec.status == deliveryUnknown {
		// the message is being validated; track the peer delivery and wait for
		// the Deliver/Reject notification.
		drec.peers[msg.ReceivedFrom] = struct{}{}
	}
}

func (t *tagTracer) RejectMessage(msg *Message, reason string) {
	t.Lock()
	defer t.Unlock()

	// mark message as invalid and release tracking info
	drec := t.deliveries.getRecord(t.msgID(msg.Message))

	// defensive check that this is the first rejection trace -- delivery status should be unknown
	if drec.status != deliveryUnknown {
		log.Warnf("unexpected rejection trace: message from %s was first seen %s ago and has delivery status %d", msg.ReceivedFrom, time.Now().Sub(drec.firstSeen), drec.status)
		return
	}

	drec.status = deliveryInvalid
	drec.peers = nil
}

func (t *tagTracer) RemovePeer(peer.ID) {}
