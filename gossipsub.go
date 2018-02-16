package floodsub

import (
	"context"

	pb "github.com/libp2p/go-floodsub/pb"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

const (
	GossipSubID = protocol.ID("/meshsub/1.0.0")
)

func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := &GossipSubRouter{}
	return NewPubSub(ctx, h, rt, opts...)
}

type GossipSubRouter struct {
	p *PubSub
}

func (fs *GossipSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{GossipSubID, FloodSubID}
}

func (fs *GossipSubRouter) Attach(p *PubSub) {
	fs.p = p
}

func (fs *GossipSubRouter) AddPeer(peer.ID, protocol.ID) {

}

func (fs *GossipSubRouter) RemovePeer(peer.ID) {

}

func (fs *GossipSubRouter) HandleRPC(rpc *RPC) {

}

func (fs *GossipSubRouter) Publish(from peer.ID, msg *pb.Message) {

}

func (fs *GossipSubRouter) Join(topic string) {

}

func (fs *GossipSubRouter) Leave(topic string) {

}
