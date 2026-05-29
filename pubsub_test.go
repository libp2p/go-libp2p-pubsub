package pubsub

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
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
