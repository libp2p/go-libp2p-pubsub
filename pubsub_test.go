package pubsub

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	"google.golang.org/protobuf/encoding/protowire"
)

// synctestTest wraps synctest.Test with GOMAXPROCS(1) to work around a Go
// runtime bug where concurrent bubble timer firing corrupts TSan state.
// https://github.com/golang/go/issues/78156
func synctestTest(t *testing.T, f func(t *testing.T)) {
	if raceEnabled {
		prev := runtime.GOMAXPROCS(1)
		t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	}
	synctest.Test(t, f)
}

func TestWrapInboundRPCPreservesUnknownFieldsAndDeepCopies(t *testing.T) {
	topic := "topic"
	source := &pb.RPC{
		Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic}},
		Publish:       []*pb.Message{{Data: []byte("payload"), Topic: &topic}},
	}
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("extension"))
	source.ProtoReflect().SetUnknown(unknown)

	from := peer.ID("peer-a")
	transport := peercomm.TransportTopic
	wrapped := wrapInboundRPC(source, from, transport)

	if len(wrapped.Subscriptions) != 1 || wrapped.Subscriptions[0].GetTopicid() != topic {
		t.Fatalf("known subscription field was not copied: %v", wrapped.Subscriptions)
	}
	if len(wrapped.Publish) != 1 || string(wrapped.Publish[0].GetData()) != "payload" || wrapped.Publish[0].GetTopic() != topic {
		t.Fatalf("known publish field was not copied: %v", wrapped.Publish)
	}
	if got := wrapped.ProtoReflect().GetUnknown(); !bytes.Equal(got, unknown) {
		t.Fatalf("unknown fields differ: got %x, want %x", got, unknown)
	}
	if wrapped.from != from {
		t.Fatalf("from metadata differs: got %q, want %q", wrapped.from, from)
	}
	if wrapped.transport != transport {
		t.Fatalf("transport metadata differs: got %v, want %v", wrapped.transport, transport)
	}

	source.Subscriptions[0].Topicid = nil
	source.Publish[0].Data[0] = 'P'
	source.Publish[0].Topic = nil
	source.ProtoReflect().SetUnknown(nil)
	if wrapped.Subscriptions[0].GetTopicid() != topic {
		t.Fatal("wrapped subscription changed after mutating source")
	}
	if string(wrapped.Publish[0].GetData()) != "payload" || wrapped.Publish[0].GetTopic() != topic {
		t.Fatal("wrapped publish changed after mutating source")
	}
	if got := wrapped.ProtoReflect().GetUnknown(); !bytes.Equal(got, unknown) {
		t.Fatalf("wrapped unknown fields changed after mutating source: got %x, want %x", got, unknown)
	}
}

func TestClearPeerFromTopicsStateRemovesEmptyTopicMap(t *testing.T) {
	pid := peer.ID("peer-a")
	other := peer.ID("peer-b")

	ps := &PubSub{
		topics: map[string]map[peer.ID]peerTopicState{
			"only-peer": {
				pid: {},
			},
			"shared": {
				pid:   {},
				other: {},
			},
			"other-only": {
				other: {},
			},
		},
	}

	ps.clearPeerFromTopicsState(pid)

	if _, ok := ps.topics["only-peer"]; ok {
		t.Fatal("expected topic map to be removed after clearing its last peer")
	}
	if _, ok := ps.topics["shared"][pid]; ok {
		t.Fatal("expected cleared peer to be removed from non-empty topic map")
	}
	if _, ok := ps.topics["shared"][other]; !ok {
		t.Fatal("expected other peer to remain in non-empty topic map")
	}
	if _, ok := ps.topics["other-only"][other]; !ok {
		t.Fatal("expected unrelated topic map to remain unchanged")
	}
}

func TestIsRecentlyUnsubscribedCleansUpExpiredEntry(t *testing.T) {
	now := time.Now()
	ps := &PubSub{
		recentUnsubscribed: map[string]time.Time{
			"recent":  now.Add(-GossipSubUnsubscribeBackoff),
			"expired": now.Add(-GossipSubUnsubscribeBackoff - time.Nanosecond),
		},
	}

	if !ps.isRecentlyUnsubscribed("recent", now) {
		t.Fatal("expected topic within unsubscribe backoff to be recent")
	}
	if ps.isRecentlyUnsubscribed("expired", now) {
		t.Fatal("expected topic past unsubscribe backoff not to be recent")
	}
	if _, ok := ps.recentUnsubscribed["recent"]; !ok {
		t.Fatal("expected recent topic to remain tracked")
	}
	if _, ok := ps.recentUnsubscribed["expired"]; ok {
		t.Fatal("expected expired topic to be removed during lookup")
	}
}

func TestHandleIncomingRPCUnsubscribeRemovesEmptyTopicMap(t *testing.T) {
	pid := peer.ID("peer-a")
	other := peer.ID("peer-b")
	onlyPeerTopic := "only-peer"
	sharedTopic := "shared"
	unsubscribe := false

	ps := &PubSub{
		rt: &FloodSubRouter{},
		topics: map[string]map[peer.ID]peerTopicState{
			onlyPeerTopic: {
				pid: {},
			},
			sharedTopic: {
				pid:   {},
				other: {},
			},
		},
	}

	ps.handleIncomingRPC(&RPC{
		RPC: pb.RPC{
			Subscriptions: []*pb.RPC_SubOpts{
				{
					Topicid:   &onlyPeerTopic,
					Subscribe: &unsubscribe,
				},
				{
					Topicid:   &sharedTopic,
					Subscribe: &unsubscribe,
				},
			},
		},
		from: pid,
	})

	if _, ok := ps.topics[onlyPeerTopic]; ok {
		t.Fatal("expected topic map to be removed after unsubscribe clears its last peer")
	}
	if _, ok := ps.topics[sharedTopic][pid]; ok {
		t.Fatal("expected unsubscribed peer to be removed from non-empty topic map")
	}
	if _, ok := ps.topics[sharedTopic][other]; !ok {
		t.Fatal("expected other peer to remain in non-empty topic map")
	}
}

func getDefaultHosts(t *testing.T, n int) []host.Host {
	net, meta, err := simlibp2p.SimpleLibp2pNetwork(
		[]simlibp2p.NodeLinkSettingsAndCount{{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
				Uplink:   simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
			},
			Count: n,
		}},
		simnet.StaticLatency(time.Millisecond),
		simlibp2p.NetworkSettings{},
	)
	if err != nil {
		t.Fatal(err)
	}
	net.Start()
	t.Cleanup(func() {
		for _, h := range meta.Nodes {
			h.Close()
		}
		net.Close()
	})
	return meta.Nodes
}

// See https://github.com/libp2p/go-libp2p-pubsub/issues/426
func TestPubSubRemovesBlacklistedPeer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		hosts := getDefaultHosts(t, 2)

		bl := NewMapBlacklist()

		psubs0 := getPubsub(ctx, hosts[0])
		psubs1 := getPubsub(ctx, hosts[1], WithBlacklist(bl))
		connect(t, hosts[0], hosts[1])

		// Bad peer is blacklisted after it has connected.
		// Calling p.BlacklistPeer directly does the right thing but we should also clean
		// up the peer if it has been added the the blacklist by another means.
		withRouter(psubs1, func(r PubSubRouter) {
			bl.Add(hosts[0].ID())
		})

		_, err := psubs0.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}

		sub1, err := psubs1.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Millisecond * 100)

		psubs0.Publish("test", []byte("message"))

		wctx, cancel2 := context.WithTimeout(ctx, 1*time.Second)
		defer cancel2()

		_, _ = sub1.Next(wctx)

		// Explicitly cancel context so PubSub cleans up peer channels.
		// Issue 426 reports a panic due to a peer channel being closed twice.
		cancel()
		time.Sleep(time.Millisecond * 100)
	})
}
