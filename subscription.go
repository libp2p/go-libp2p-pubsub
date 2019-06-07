package pubsub

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Subscription struct {
	topic       string
	ch          chan *Message
	cancelCh    chan<- *Subscription
	inboundSubs chan peer.ID
	err         error
}

func (sub *Subscription) Topic() string {
	return sub.topic
}

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

func (sub *Subscription) NextSubscribedPeer(ctx context.Context) (peer.ID, error) {
	select {
	case newPeer, ok := <-sub.inboundSubs:
		if !ok {
			return newPeer, sub.err
		}

		return newPeer, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
