package pubsub

import (
	"context"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// PublishWithSk publishes data to topic.
func (t *Topic) PublishWithSk(ctx context.Context, data []byte, signKey crypto.PrivKey, pid peer.ID, opts ...PubOpt) error {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return ErrTopicClosed
	}

	m := &pb.Message{
		Data:  data,
		Topic: &t.topic,
		From:  nil,
		Seqno: nil,
	}
	if t.p.signID != "" {
		m.From = []byte(t.p.signID)
		m.Seqno = t.p.nextSeqno()
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

	return t.p.val.PushLocal(&Message{m, t.p.host.ID(), nil})
}
