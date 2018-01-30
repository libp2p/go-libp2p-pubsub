package floodsub

import (
	"context"

	pb "github.com/libp2p/go-floodsub/pb"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

const (
	FloodSubID = protocol.ID("/floodsub/1.0.0")
)

// NewFloodSub returns a new PubSub object using the FloodSubRouter
func NewFloodSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := &FloodSubRouter{}
	return NewPubSub(ctx, h, rt, opts...)
}

type FloodSubRouter struct {
	p *PubSub
}

func (fs *FloodSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{FloodSubID}
}

func (fs *FloodSubRouter) Attach(p *PubSub) {
	fs.p = p
}

func (fs *FloodSubRouter) AddPeer(peer.ID, protocol.ID) {}

func (fs *FloodSubRouter) RemovePeer(peer.ID) {}

func (fs *FloodSubRouter) HandleRPC(rpc *RPC) {}

func (fs *FloodSubRouter) Publish(from peer.ID, msg *pb.Message) {
	tosend := make(map[peer.ID]struct{})
	for _, topic := range msg.GetTopicIDs() {
		tmap, ok := fs.p.topics[topic]
		if !ok {
			continue
		}

		for p := range tmap {
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg)
	for pid := range tosend {
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		mch, ok := fs.p.peers[pid]
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
