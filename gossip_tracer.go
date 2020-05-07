package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// gossipTracer is an internal tracer that tracks IWANT requests in order to penalize
// peers who don't follow up on IWANT requests after an IHAVE advertisement.
// The tracking of promises is probabilistic to avoid using too much memory.
type gossipTracer struct {
	msgID MsgIdFunction
}

func newGossipTracer() *gossipTracer {
	return &gossipTracer{
		msgID: DefaultMsgIdFn,
	}
}

func (gt *gossipTracer) Start(gs *GossipSubRouter) {
	if gt == nil {
		return
	}

	gt.msgID = gs.p.msgID
}

func (gt *gossipTracer) AddPromise(p peer.ID, msgIDs []string) {
	if gt == nil {
		return
	}

	// TODO
}

func (gt *gossipTracer) GetBrokenPromises() map[peer.ID]int {
	// TODO
	return nil
}

var _ internalTracer = (*gossipTracer)(nil)

func (gt *gossipTracer) DeliverMessage(msg *Message) {
	// TODO
}

func (gt *gossipTracer) RejectMessage(msg *Message, reason string) {
	// TODO
}

func (gt *gossipTracer) AddPeer(p peer.ID, proto protocol.ID) {}
func (gt *gossipTracer) RemovePeer(p peer.ID)                 {}
func (gt *gossipTracer) Join(topic string)                    {}
func (gt *gossipTracer) Leave(topic string)                   {}
func (gt *gossipTracer) Graft(p peer.ID, topic string)        {}
func (gt *gossipTracer) Prune(p peer.ID, topic string)        {}
func (gt *gossipTracer) ValidateMessage(msg *Message)         {}
func (gt *gossipTracer) DuplicateMessage(msg *Message)        {}
