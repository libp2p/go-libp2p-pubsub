package pubsub

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

var errNilSignKey = errors.New("nil sign key")
var errEmptyPeerID = errors.New("empty peer ID")

// PublishWithSk publishes data to topic.
func (t *Topic) PublishWithSk(ctx context.Context, data []byte, signKey crypto.PrivKey, pid peer.ID, opts ...PubOpt) error {
	if signKey == nil {
		return errNilSignKey
	}
	if len(pid) == 0 {
		return errEmptyPeerID
	}

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
		m.Seqno = t.p.nextSeqno()
	}
	m.From = []byte(pid)
	err := signMessage(pid, signKey, m)
	if err != nil {
		return err
	}

	pub := &PublishOptions{}
	for _, opt := range opts {
		errOpts := opt(pub)
		if errOpts != nil {
			return errOpts
		}
	}

	if pub.ready != nil {
		t.p.disc.Bootstrap(ctx, t.topic, pub.ready)
	}

	return t.p.val.PushLocal(&Message{m, t.p.host.ID(), nil})
}
