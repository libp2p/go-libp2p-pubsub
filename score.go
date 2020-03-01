package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type peerScore struct {
}

type PeerScoreParams struct {
}

type TopicScoreParams struct {
}

func newPeerScore(gs *GossipSubRouter, params *PeerScoreParams) *peerScore {
	return nil
}

// router interface
func (ps *peerScore) Score(p peer.ID, topic string) float64 {
	return 0
}

// tracer interface
func (ps *peerScore) AddPeer(p peer.ID, proto protocol.ID) {

}

func (ps *peerScore) RemovePeer(p peer.ID) {

}

func (ps *peerScore) Join(topic string) {

}

func (ps *peerScore) Leave(topic string) {

}

func (ps *peerScore) Graft(p peer.ID, topic string) {

}

func (ps *peerScore) Prune(p peer.ID, topic string) {

}

func (ps *peerScore) DeliverMessage(msg *Message) {

}

func (ps *peerScore) RejectMessage(msg *Message, reason string) {

}

func (ps *peerScore) DuplicateMessage(msg *Message) {

}
