package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-varint"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

var rng *rand.Rand

func init() {
	rng = rand.New(rand.NewSource(314159))
}

func TestBasicSeqnoValidator1(t *testing.T) {
	testBasicSeqnoValidator(t, time.Minute)
}

func TestBasicSeqnoValidator2(t *testing.T) {
	testBasicSeqnoValidator(t, time.Nanosecond)
}

func testBasicSeqnoValidator(t *testing.T, ttl time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 20)
	psubs := getPubsubsWithOptionC(ctx, hosts,
		func(i int) Option {
			return WithDefaultValidator(NewBasicSeqnoValidator(newMockPeerMetadataStore()))
		},
		func(i int) Option {
			return WithSeenMessagesTTL(ttl)
		},
	)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	// connectAll(t, hosts)
	sparseConnect(t, hosts)

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rng.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}
}

func TestBasicSeqnoValidatorReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 20)
	psubs := getPubsubsWithOptionC(ctx, hosts[:19],
		func(i int) Option {
			return WithDefaultValidator(NewBasicSeqnoValidator(newMockPeerMetadataStore()))
		},
		func(i int) Option {
			return WithSeenMessagesTTL(time.Nanosecond)
		},
	)
	_ = newReplayActor(t, ctx, hosts[19])

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	sparseConnect(t, hosts)

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rng.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}

	for _, sub := range msgs {
		assertNeverReceives(t, sub, time.Second)
	}
}

type mockPeerMetadataStore struct {
	meta map[peer.ID][]byte
}

func newMockPeerMetadataStore() *mockPeerMetadataStore {
	return &mockPeerMetadataStore{
		meta: make(map[peer.ID][]byte),
	}
}

func (m *mockPeerMetadataStore) Get(ctx context.Context, p peer.ID) ([]byte, error) {
	v, ok := m.meta[p]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *mockPeerMetadataStore) Put(ctx context.Context, p peer.ID, v []byte) error {
	m.meta[p] = v
	return nil
}

type replayActor struct {
	t *testing.T

	ctx context.Context
	h   host.Host

	mx  sync.Mutex
	out map[peer.ID]network.Stream
}

func newReplayActor(t *testing.T, ctx context.Context, h host.Host) *replayActor {
	replay := &replayActor{t: t, ctx: ctx, h: h, out: make(map[peer.ID]network.Stream)}
	h.SetStreamHandler(FloodSubID, replay.handleStream)
	h.Network().Notify(&network.NotifyBundle{ConnectedF: replay.connected})
	return replay
}

func (r *replayActor) handleStream(s network.Stream) {
	defer s.Close()

	p := s.Conn().RemotePeer()

	rd := msgio.NewVarintReaderSize(s, 65536)
	for {
		msgbytes, err := rd.ReadMsg()
		if err != nil {
			s.Reset()
			rd.ReleaseMsg(msgbytes)
			return
		}

		rpc := new(pb.RPC)
		err = rpc.Unmarshal(msgbytes)
		rd.ReleaseMsg(msgbytes)
		if err != nil {
			s.Reset()
			return
		}

		// subscribe to the same topics as our peer
		subs := rpc.GetSubscriptions()
		if len(subs) != 0 {
			go r.send(p, &pb.RPC{Subscriptions: subs})
		}

		// replay all received messages
		for _, pmsg := range rpc.GetPublish() {
			go r.replay(pmsg)
		}
	}
}

func (r *replayActor) send(p peer.ID, rpc *pb.RPC) {
	r.mx.Lock()
	defer r.mx.Unlock()

	s, ok := r.out[p]
	if !ok {
		r.t.Logf("cannot send message to %s: no stream", p)
		return
	}

	size := uint64(rpc.Size())

	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, size)

	_, err := rpc.MarshalTo(buf[n:])
	if err != nil {
		r.t.Logf("replay: error marshalling message: %s", err)
		return
	}

	_, err = s.Write(buf)
	if err != nil {
		r.t.Logf("replay: error sending message: %s", err)
	}
}

func (r *replayActor) replay(msg *pb.Message) {
	// replay the message 10 times to a random subset of peers
	for i := 0; i < 10; i++ {
		delay := time.Duration(1+rng.Intn(20)) * time.Millisecond
		time.Sleep(delay)

		var peers []peer.ID
		r.mx.Lock()
		for p := range r.out {
			if rng.Intn(2) > 0 {
				peers = append(peers, p)
			}
		}
		r.mx.Unlock()

		rpc := &pb.RPC{Publish: []*pb.Message{msg}}
		r.t.Logf("replaying msg to %d peers", len(peers))
		for _, p := range peers {
			r.send(p, rpc)
		}
	}
}

func (r *replayActor) handleConnected(p peer.ID) {
	s, err := r.h.NewStream(r.ctx, p, FloodSubID)
	if err != nil {
		r.t.Logf("replay: error opening stream: %s", err)
		return
	}

	r.mx.Lock()
	defer r.mx.Unlock()
	r.out[p] = s
}

func (r *replayActor) connected(_ network.Network, conn network.Conn) {
	go r.handleConnected(conn.RemotePeer())
}
