package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

func TestGossipsubConnTagMessageDeliveries(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		oldGossipSubD := GossipSubD
		oldGossipSubDlo := GossipSubDlo
		oldGossipSubDHi := GossipSubDhi
		oldGossipSubDscore := GossipSubDscore
		oldGossipSubConnTagDecayInterval := GossipSubConnTagDecayInterval
		oldGossipSubConnTagMessageDeliveryCap := GossipSubConnTagMessageDeliveryCap

		// set the gossipsub D parameters low, so that we have some peers outside the mesh
		GossipSubDlo = 3
		GossipSubD = 3
		GossipSubDhi = 3
		GossipSubDscore = 3
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
			GossipSubDscore = oldGossipSubDscore
			GossipSubConnTagDecayInterval = oldGossipSubConnTagDecayInterval
			GossipSubConnTagMessageDeliveryCap = oldGossipSubConnTagMessageDeliveryCap
		}()

		nHonest := 5
		nSquatter := 10
		connLimit := 10

		connmgrs := make([]*connmgr.BasicConnMgr, nHonest)
		for i := 0; i < nHonest; i++ {
			var err error
			connmgrs[i], err = connmgr.NewConnManager(nHonest, connLimit,
				connmgr.WithGracePeriod(0),
				connmgr.WithSilencePeriod(time.Millisecond),
			)
			if err != nil {
				t.Fatal(err)
			}
		}

		totalNodes := nHonest + nSquatter
		net, meta, err := simlibp2p.SimpleLibp2pNetwork(
			[]simlibp2p.NodeLinkSettingsAndCount{{
				LinkSettings: simnet.NodeBiDiLinkSettings{
					Downlink: simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
					Uplink:   simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
				},
				Count: totalNodes,
			}},
			simnet.StaticLatency(time.Millisecond),
			simlibp2p.NetworkSettings{
				UseBlankHost: true,
				BlankHostOptsForHostIdx: func(idx int) simlibp2p.BlankHostOpts {
					if idx < nHonest {
						return simlibp2p.BlankHostOpts{ConnMgr: connmgrs[idx]}
					}
					return simlibp2p.BlankHostOpts{}
				},
			},
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

		honestHosts := meta.Nodes[:nHonest]
		sybilHosts := meta.Nodes[nHonest:]

		honestPeers := make(map[peer.ID]struct{})
		for _, h := range honestHosts {
			honestPeers[h.ID()] = struct{}{}
		}

		// use flood publishing, so non-mesh peers will still be delivering messages
		// to everyone
		psubs := getGossipsubs(ctx, honestHosts,
			WithFloodPublish(true))

		// sybil squatters to be connected later
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

		// advance time for the tag decay (under synctest, time.Sleep advances the fake clock)
		time.Sleep(time.Second)

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
	})
}
