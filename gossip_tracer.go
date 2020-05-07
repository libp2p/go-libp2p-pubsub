package pubsub

import (
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// gossipTracer is an internal tracer that tracks IWANT requests in order to penalize
// peers who don't follow up on IWANT requests after an IHAVE advertisement.
// The tracking of promises is probabilistic to avoid using too much memory.
type gossipTracer struct {
	sync.Mutex
	msgID    MsgIdFunction
	promises map[string]map[peer.ID]time.Time
}

func newGossipTracer() *gossipTracer {
	return &gossipTracer{
		msgID:    DefaultMsgIdFn,
		promises: make(map[string]map[peer.ID]time.Time),
	}
}

func (gt *gossipTracer) Start(gs *GossipSubRouter) {
	if gt == nil {
		return
	}

	gt.msgID = gs.p.msgID
}

// track a promise to deliver a message from a list of msgIDs we are requesting
func (gt *gossipTracer) AddPromise(p peer.ID, msgIDs []string) {
	if gt == nil {
		return
	}

	idx := rand.Intn(len(msgIDs))
	mid := msgIDs[idx]

	gt.Lock()
	defer gt.Unlock()

	peers, ok := gt.promises[mid]
	if !ok {
		peers = make(map[peer.ID]time.Time)
		gt.promises[mid] = peers
	}

	_, ok = peers[p]
	if !ok {
		peers[p] = time.Now().Add(GossipSubIWantFollowupTime)
	}
}

// returns the number of broken promises for each peer who didn't follow up
// on an IWANT request.
func (gt *gossipTracer) GetBrokenPromises() map[peer.ID]int {
	if gt == nil {
		return nil
	}

	gt.Lock()
	defer gt.Unlock()

	var res map[peer.ID]int
	now := time.Now()

	for mid, peers := range gt.promises {
		for p, expire := range peers {
			if expire.Before(now) {
				if res == nil {
					res = make(map[peer.ID]int)
				}
				res[p]++

				delete(peers, p)
			}
		}
		if len(peers) == 0 {
			delete(gt.promises, mid)
		}
	}

	return res
}

var _ internalTracer = (*gossipTracer)(nil)

func (gt *gossipTracer) DeliverMessage(msg *Message) {
	// someone delivered a message, stop tracking promises for it
	mid := gt.msgID(msg.Message)

	gt.Lock()
	defer gt.Unlock()

	delete(gt.promises, mid)
}

func (gt *gossipTracer) RejectMessage(msg *Message, reason string) {
	// A message got rejected, so we can stop tracking promises and let the score penalty apply
	// from invalid message delivery.
	// We do take exception and apply promise penalty regardless in the following cases, where
	// the peer delivered an obviously invalid message.
	switch reason {
	case rejectMissingSignature:
		return
	case rejectInvalidSignature:
		return
	case rejectSelfOrigin:
		return
	}

	mid := gt.msgID(msg.Message)

	gt.Lock()
	defer gt.Unlock()

	delete(gt.promises, mid)
}

func (gt *gossipTracer) AddPeer(p peer.ID, proto protocol.ID) {}
func (gt *gossipTracer) RemovePeer(p peer.ID)                 {}
func (gt *gossipTracer) Join(topic string)                    {}
func (gt *gossipTracer) Leave(topic string)                   {}
func (gt *gossipTracer) Graft(p peer.ID, topic string)        {}
func (gt *gossipTracer) Prune(p peer.ID, topic string)        {}
func (gt *gossipTracer) ValidateMessage(msg *Message)         {}
func (gt *gossipTracer) DuplicateMessage(msg *Message)        {}
