package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// Generic event tracer interface
type EventTracer interface {
	Trace(evt interface{})
}

type pubsubTracer struct {
	tracer EventTracer
}

func (t *pubsubTracer) PublishMessage(msg *Message) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) RejectMessage(msg *Message, reason string) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) DuplicateMessage(msg *Message) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) DeliverMessage(msg *Message) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) AddPeer(p peer.ID, proto protocol.ID) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) RemovePeer(p peer.ID) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) RecvRPC(rpc *RPC) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) SendRPC(rpc *RPC, p peer.ID) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) DropRPC(rpc *RPC, p peer.ID) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) Join(topic string) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) Leave(topic string) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) Graft(p peer.ID, topic string) {
	if t != nil {
		// TODO
	}
}

func (t *pubsubTracer) Prune(p peer.ID, topic string) {
	if t != nil {
		// TODO
	}
}
