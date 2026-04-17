package pubsub

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
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
