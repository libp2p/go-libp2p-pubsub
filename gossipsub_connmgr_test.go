package pubsub

import (
	"context"
	"testing"
	"time"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/peer"
)

// This file has tests for gossipsub's interaction with the libp2p connection manager.
// We tag connections for three reasons:
//
// - direct peers get a `pubsub:direct` tag with a value of GossipSubConnTagValueDirectPeer
// - mesh members get a `pubsub:$topic` tag with a value of GossipSubConnTagValueMeshPeer (default 20)
//   - applies for each topic they're a mesh member of
// - anyone who delivers a message first gets a bump to a decaying `pubsub:deliveries:$topic` tag

func TestGossipsubConnTagDirectPeers(t *testing.T) {
	// test that direct peers get tagged with GossipSubConnTagValueDirectPeer
	t.Skip("coming soon")
}

func TestGossipsubConnTagMeshPeers(t *testing.T) {
	// test that mesh peers get tagged with GossipSubConnTagValueMeshPeer
	t.Skip("coming soon")
}

func TestGossipsubConnTagMessageDeliveries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set the gossipsub D parameters low, so that not all
	// test peers will be in a mesh together
	oldGossipSubD := GossipSubD
	oldGossipSubDHi := GossipSubDhi
	GossipSubD = 4
	GossipSubDhi = 4
	defer func() {
		GossipSubD = oldGossipSubD
		GossipSubDhi = oldGossipSubDHi
	}()

	decayCfg := connmgr.DecayerCfg{
		Resolution: time.Second,
	}

	hosts := getNetHosts(t, ctx, 20, func() bhost.Option {
		return bhost.WithConnectionManager(
			connmgr.NewConnManager(1, 30, 10*time.Millisecond,
				connmgr.DecayerConfig(&decayCfg)))
	})

	// use flood publishing, so non-mesh peers will still be delivering messages
	// to everyone
	psubs := getGossipsubs(ctx, hosts,
		WithFloodPublish(true))
	denseConnect(t, hosts)

	// subscribe everyone to the topic
	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	// wait a few heartbeats for meshes to form
	time.Sleep(2 * time.Second)

	// have all the hosts publish messages

}

func getMeshPeers(ps PubSub, topic string) []peer.ID {
	gs := ps.rt.(*GossipSubRouter)

	peerCh := make(chan peer.ID)
	ps.eval <- func() {
		peers := gs.mesh[topic]
		for pid, _ := range peers {
			peerCh <- pid
		}
		close(peerCh)
	}

	var out []peer.ID
	for pid := range peerCh {
		out = append(out, pid)
	}
	return out
}
