package pubsub

import (
	"context"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

type Subscription struct {
	topic         string
	topicProtocol protocol.ID
	ch            chan *Message
	cancelCh      chan<- *Subscription
	err           error
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
