package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/host"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

func TestGossipsubConnTagMessageDeliveries(t *testing.T) {
	t.Skip("flaky test disabled")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldGossipSubD := GossipSubD
	oldGossipSubDlo := GossipSubDlo
	oldGossipSubDHi := GossipSubDhi
	oldGossipSubConnTagDecayInterval := GossipSubConnTagDecayInterval
	oldGossipSubConnTagMessageDeliveryCap := GossipSubConnTagMessageDeliveryCap

	// set the gossipsub D parameters low, so that we have some peers outside the mesh
	GossipSubDlo = 3
	GossipSubD = 3
	GossipSubDhi = 3
	// also set the tag decay interval so we don't have to wait forever for tests
	GossipSubConnTagDecayInterval = time.Second

	// set the cap for deliveries above GossipSubConnTagValueMeshPeer, so the sybils
	// will be forced out even if they end up in someone's mesh
	GossipSubConnTagMessageDeliveryCap = 50

	// reset globals after test
	defer func() {
		GossipSubD = oldGossipSubD
		GossipSubDlo = oldGossipSubDlo
		GossipSubDhi = oldGossipSubDHi
		GossipSubConnTagDecayInterval = oldGossipSubConnTagDecayInterval
		GossipSubConnTagMessageDeliveryCap = oldGossipSubConnTagMessageDeliveryCap
	}()

	decayClock := clock.NewMock()
	decayCfg := connmgr.DecayerCfg{
		Resolution: time.Second,
		Clock:      decayClock,
	}

	nHonest := 5
	nSquatter := 10
	connLimit := 10

	connmgrs := make([]*connmgr.BasicConnMgr, nHonest)
	honestHosts := make([]host.Host, nHonest)
	honestPeers := make(map[peer.ID]struct{})

	for i := 0; i < nHonest; i++ {
		var err error
		connmgrs[i], err = connmgr.NewConnManager(nHonest, connLimit,
			connmgr.WithGracePeriod(0),
			connmgr.WithSilencePeriod(time.Millisecond),
			connmgr.DecayerConfig(&decayCfg),
		)
		if err != nil {
			t.Fatal(err)
		}

		h, err := libp2p.New(
			libp2p.ResourceManager(&network.NullResourceManager{}),
			libp2p.ConnectionManager(connmgrs[i]),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Close() })
		honestHosts[i] = h
		honestPeers[h.ID()] = struct{}{}
	}

	// use flood publishing, so non-mesh peers will still be delivering messages
	// to everyone
	psubs := getGossipsubs(ctx, honestHosts,
		WithFloodPublish(true))

	// sybil squatters to be connected later
	sybilHosts := getDefaultHosts(t, nSquatter)
	for _, h := range sybilHosts {
		squatter := &sybilSquatter{h: h, ignoreErrors: true}
		h.SetStreamHandler(GossipSubID_v10, squatter.handleStream)
	}

	// connect the honest hosts
	connectAll(t, honestHosts)

	for _, h := range honestHosts {
		if len(h.Network().Peers()) != nHonest-1 {
			t.Errorf("expected to have conns to all honest peers, have %d", len(h.Network().Conns()))
		}
	}

	// subscribe everyone to the topic
	topic := "test"
	for _, ps := range psubs {
		_, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
	}

	// sleep to allow meshes to form
	time.Sleep(2 * time.Second)

	// have all the hosts publish enough messages to ensure that they get some delivery credit
	nMessages := GossipSubConnTagMessageDeliveryCap * 2
	for _, ps := range psubs {
		for i := 0; i < nMessages; i++ {
			ps.Publish(topic, []byte("hello"))
		}
	}

	// advance the fake time for the tag decay
	decayClock.Add(time.Second)

	// verify that they've given each other delivery connection tags
	tag := "pubsub-deliveries:test"
	for _, h := range honestHosts {
		for _, h2 := range honestHosts {
			if h.ID() == h2.ID() {
				continue
			}
			val := getTagValue(h.ConnManager(), h2.ID(), tag)
			if val == 0 {
				t.Errorf("Expected non-zero delivery tag value for peer %s", h2.ID())
			}
		}
	}

	// now connect the sybils to put pressure on the real hosts' connection managers
	allHosts := append(honestHosts, sybilHosts...)
	connectAll(t, allHosts)

	// we should still have conns to all the honest peers, but not the sybils
	for _, h := range honestHosts {
		nHonestConns := 0
		nDishonestConns := 0
		for _, p := range h.Network().Peers() {
			if _, ok := honestPeers[p]; !ok {
				nDishonestConns++
			} else {
				nHonestConns++
			}
		}
		if nDishonestConns > connLimit-nHonest {
			t.Errorf("expected most dishonest conns to be pruned, have %d", nDishonestConns)
		}
		if nHonestConns != nHonest-1 {
			t.Errorf("expected all honest conns to be preserved, have %d", nHonestConns)
		}
	}
}
