package pubsub

import (
	"context"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Publish publishes data to topic.
func (t *Topic) PublishWithSk(ctx context.Context, data []byte, signKey crypto.PrivKey, pid peer.ID, opts ...PubOpt) error {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return ErrTopicClosed
	}

	seqno := t.p.nextSeqno()
	id := pid
	m := &pb.Message{
		Data:     data,
		TopicIDs: []string{t.topic},
		From:     []byte(id),
		Seqno:    seqno,
	}
	if signKey != nil {
		m.From = []byte(pid)
		err := signMessage(pid, signKey, m)
		if err != nil {
			return err
		}
	}

	pub := &PublishOptions{}
	for _, opt := range opts {
		err := opt(pub)
		if err != nil {
			return err
		}
	}

	if pub.ready != nil {
		t.p.disc.Bootstrap(ctx, t.topic, pub.ready)
	}

	select {
	case t.p.publish <- &Message{m, id, nil}:
	case <-t.p.ctx.Done():
		return t.p.ctx.Err()
	}

	return nil
}
