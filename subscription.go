package pubsub

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
)

type EventType uint8

const (
	UNKNOWN EventType = iota
	PEER_JOIN
	PEER_LEAVE
)

type Subscription struct {
	topic    string
	ch       chan *Message
	cancelCh chan<- *Subscription
	joinCh   chan peer.ID
	leaveCh  chan peer.ID
	err      error
}

type PeerEvent struct {
	Type EventType
	Peer peer.ID
}

func (sub *Subscription) Topic() string {
	return sub.topic
}

// Next returns the next message in our subscription
func (sub *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case msg, ok := <-sub.ch:
		if !ok {
			return msg, sub.err
		}

		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sub *Subscription) Cancel() {
	sub.cancelCh <- sub
}

// NextPeerEvent returns the next event regarding subscribed peers
// Note: There is no guarantee that the Peer Join event will fire before
// the related Peer Leave event for a given peer
func (sub *Subscription) NextPeerEvent(ctx context.Context) (PeerEvent, error) {
	select {
	case newPeer, ok := <-sub.joinCh:
		event := PeerEvent{Type: PEER_JOIN, Peer: newPeer}
		if !ok {
			return event, sub.err
		}

		return event, nil
	case leavingPeer, ok := <-sub.leaveCh:
		event := PeerEvent{Type: PEER_LEAVE, Peer: leavingPeer}
		if !ok {
			return event, sub.err
		}

		return event, nil
	case <-ctx.Done():
		return PeerEvent{}, ctx.Err()
	}
}
