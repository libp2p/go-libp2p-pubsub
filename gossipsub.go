package floodsub

import (
	"context"
	"time"

	pb "github.com/libp2p/go-floodsub/pb"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

const (
	GossipSubID = protocol.ID("/meshsub/1.0.0")

	// overlay parameters
	GossipSubD   = 6
	GossipSubDlo = 4
	GossipSubDhi = 12
)

func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := &GossipSubRouter{
		peers:  make(map[peer.ID]protocol.ID),
		mesh:   make(map[string]map[peer.ID]struct{}),
		fanout: make(map[string]map[peer.ID]struct{}),
		mcache: NewMessageCache(5),
	}
	return NewPubSub(ctx, h, rt, opts...)
}

type GossipSubRouter struct {
	p      *PubSub
	peers  map[peer.ID]protocol.ID         // peer protocols
	mesh   map[string]map[peer.ID]struct{} // topic meshes
	fanout map[string]map[peer.ID]struct{} // topic fanout
	mcache *MessageCache
}

func (gs *GossipSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{GossipSubID, FloodSubID}
}

func (gs *GossipSubRouter) Attach(p *PubSub) {
	gs.p = p
	go gs.heartbeatTimer()
}

func (gs *GossipSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	gs.peers[p] = proto
}

func (gs *GossipSubRouter) RemovePeer(p peer.ID) {
	delete(gs.peers, p)
	for _, peers := range gs.mesh {
		delete(peers, p)
	}
	for _, peers := range gs.fanout {
		delete(peers, p)
	}
}

func (gs *GossipSubRouter) HandleRPC(rpc *RPC) {
	// TODO
}

func (gs *GossipSubRouter) Publish(from peer.ID, msg *pb.Message) {
	gs.mcache.Add(msg)

	tosend := make(map[peer.ID]struct{})
	for _, topic := range msg.GetTopicIDs() {
		// any peers in the topic?
		tmap, ok := gs.p.topics[topic]
		if !ok {
			continue
		}

		// floodsub peers
		for p := range tmap {
			if gs.peers[p] == FloodSubID {
				tosend[p] = struct{}{}
			}
		}

		// gossipsub peers
		gmap, ok := gs.mesh[topic]
		if ok {
			// direct peers in the mesh for topic
			for p := range gmap {
				tosend[p] = struct{}{}
			}
		} else {
			// fanout peers, we are not in the mesh for topic
			gmap, ok = gs.fanout[topic]
			if !ok {
				// we don't have any yet, pick some
				var peers []peer.ID
				for p := range tmap {
					if gs.peers[p] == GossipSubID {
						peers = append(peers, p)
					}
				}

				if len(peers) > 0 {
					gmap = make(map[peer.ID]struct{})

					shufflePeers(peers)
					for _, p := range peers[:GossipSubD] {
						gmap[p] = struct{}{}
					}

					gs.fanout[topic] = gmap
				}
			}
		}

		for p := range gmap {
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg)
	for pid := range tosend {
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		mch, ok := gs.p.peers[pid]
		if !ok {
			continue
		}

		select {
		case mch <- out:
		default:
			log.Infof("dropping message to peer %s: queue full", pid)
			// Drop it. The peer is too slow.
		}
	}
}

func (gs *GossipSubRouter) Join(topic string) {
	// TODO
}

func (gs *GossipSubRouter) Leave(topic string) {
	// TODO
}

func (gs *GossipSubRouter) heartbeatTimer() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case gs.p.eval <- gs.heartbeat:
			case <-gs.p.ctx.Done():
				return
			}
		case <-gs.p.ctx.Done():
			return
		}
	}
}

func (gs *GossipSubRouter) heartbeat() {
	// TODO
}

func shufflePeers(peers []peer.ID) {
	// TODO
}
