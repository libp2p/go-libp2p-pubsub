package pubsub

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type peerGater struct {
}

// WithPeerGater is a gossipsub router option that enables reactive validation queue
// management.
func WithPeerGater() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.gate = newPeerGater()

		// hook the tracer
		if ps.tracer != nil {
			ps.tracer.internal = append(ps.tracer.internal, gs.gate)
		} else {
			ps.tracer = &pubsubTracer{
				internal: []internalTracer{gs.gate},
				pid:      ps.host.ID(),
				msgID:    ps.msgID,
			}
		}

		return nil
	}
}

func newPeerGater() *peerGater {
	return &peerGater{}
}

func (pg *peerGater) AcceptFrom(p peer.ID) AcceptStatus {
	if pg == nil {
		return AcceptAll
	}

	return AcceptAll
}

func (pg *peerGater) AddPeer(p peer.ID, proto protocol.ID)      {}
func (pg *peerGater) RemovePeer(p peer.ID)                      {}
func (pg *peerGater) Join(topic string)                         {}
func (pg *peerGater) Leave(topic string)                        {}
func (pg *peerGater) Graft(p peer.ID, topic string)             {}
func (pg *peerGater) Prune(p peer.ID, topic string)             {}
func (pg *peerGater) ValidateMessage(msg *Message)              {}
func (pg *peerGater) DeliverMessage(msg *Message)               {}
func (pg *peerGater) RejectMessage(msg *Message, reason string) {}
func (pg *peerGater) DuplicateMessage(msg *Message)             {}
