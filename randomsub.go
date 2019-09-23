package pubsub

import (
	"context"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const (
	RandomSubID = protocol.ID("/randomsub/1.0.0")
)

var (
	RandomSubD = 6
)

// NewRandomSub returns a new PubSub object using RandomSubRouter as the router.
func NewRandomSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := &RandomSubRouter{
		peers: make(map[peer.ID]protocol.ID),
	}
	return NewPubSub(ctx, h, rt, opts...)
}

// RandomSubRouter is a router that implements a random propagation strategy.
// For each message, it selects RandomSubD peers and forwards the message to them.
type RandomSubRouter struct {
	p     PubSubControl
	peers map[peer.ID]protocol.ID
}

func (rs *RandomSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{RandomSubID, FloodSubID}
}

func (rs *RandomSubRouter) Attach(p PubSubControl) {
	rs.p = p
}

func (rs *RandomSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	rs.peers[p] = proto
}

func (rs *RandomSubRouter) RemovePeer(p peer.ID) {
	delete(rs.peers, p)
}

func (rs *RandomSubRouter) HandleRPC(rpc *RPC) {}

func (rs *RandomSubRouter) Publish(from peer.ID, msg *pb.Message) {
	tosend := make(map[peer.ID]struct{})
	rspeers := make(map[peer.ID]struct{})
	src := peer.ID(msg.GetFrom())

	for _, topic := range msg.GetTopicIDs() {
		tmap, ok := rs.p.PeersForTopic(topic)
		if !ok {
			continue
		}

		for p := range tmap {
			if p == from || p == src {
				continue
			}

			if rs.peers[p] == FloodSubID {
				tosend[p] = struct{}{}
			} else {
				rspeers[p] = struct{}{}
			}
		}
	}

	if len(rspeers) > RandomSubD {
		xpeers := peerMapToList(rspeers)
		shufflePeers(xpeers)
		xpeers = xpeers[:RandomSubD]
		for _, p := range xpeers {
			tosend[p] = struct{}{}
		}
	} else {
		for p := range rspeers {
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg)
	for p := range tosend {
		rs.p.SendMessage(p, out)
	}
}

func (rs *RandomSubRouter) Join(topic string) {}

func (rs *RandomSubRouter) Leave(topic string) {}
