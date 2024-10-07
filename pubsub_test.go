package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

func getDefaultHosts(t *testing.T, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Close() })
		out = append(out, h)
	}

	return out
}

// See https://github.com/libp2p/go-libp2p-pubsub/issues/426
func TestPubSubRemovesBlacklistedPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hosts := getDefaultHosts(t, 2)

	bl := NewMapBlacklist()

	psubs0 := getPubsub(ctx, hosts[0])
	psubs1 := getPubsub(ctx, hosts[1], WithBlacklist(bl))
	connect(t, hosts[0], hosts[1])

	// Bad peer is blacklisted after it has connected.
	// Calling p.BlacklistPeer directly does the right thing but we should also clean
	// up the peer if it has been added the the blacklist by another means.
	bl.Add(hosts[0].ID())

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
}
