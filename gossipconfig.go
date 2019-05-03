package pubsub

import (
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type MessageCacher interface {
	MessageCacheReader
	Put(msg *pb.Message)
}

type GetFilteredPeers func(count int, filter func(peer.ID) bool) []peer.ID
type EmittingStrategy interface {
	GetEmitPeers(topicPeers GetFilteredPeers, meshPeers map[peer.ID]struct{}) map[peer.ID]struct{}
}

type noMeshPeersStrategy struct {
	numPeers int
}

func NewNoMeshPeersStrategy(numPeers int) EmittingStrategy {
	return &noMeshPeersStrategy{numPeers: numPeers}
}

func (s *noMeshPeersStrategy) GetEmitPeers(topicPeers GetFilteredPeers, meshPeers map[peer.ID]struct{}) map[peer.ID]struct{} {
	gpeers := topicPeers(s.numPeers, func(peer.ID) bool { return true })
	emitPeers := make(map[peer.ID]struct{})
	for _, p := range gpeers {
		// skip mesh peers
		_, ok := meshPeers[p]
		if !ok {
			emitPeers[p] = struct{}{}
		}
	}
	return emitPeers
}

type ClassicGossipSubStrategy struct {
	mcache             MessageCacher
	emitter            EmittingStrategy
	supportedProtocols []protocol.ID
	protocol           protocol.ID
}

func (gs *ClassicGossipSubStrategy) GetCacher() MessageCacheReader {
	return gs.mcache
}

func (gs *ClassicGossipSubStrategy) SupportedProtocols() []protocol.ID {
	return gs.supportedProtocols
}
func (gs *ClassicGossipSubStrategy) Protocol() protocol.ID {
	return gs.protocol
}

func (gs *ClassicGossipSubStrategy) Publish(rt *GossipSubRouter, from peer.ID, msg *pb.Message) {
	gs.mcache.Put(msg)

	tosend := make(map[peer.ID]struct{})
	rt.AddGossipPeers(tosend, msg.GetTopicIDs(), true)

	_, ok := tosend[from]
	if ok {
		delete(tosend, from)
	}

	calculatedFrom := peer.ID(msg.GetFrom())
	_, ok = tosend[calculatedFrom]
	if ok {
		delete(tosend, calculatedFrom)
	}

	rt.PropagateMSG(tosend, msg)
}

func (gs *ClassicGossipSubStrategy) GetEmitPeers(topicPeers GetFilteredPeers, meshPeers map[peer.ID]struct{}) map[peer.ID]struct{} {
	return gs.emitter.GetEmitPeers(topicPeers, meshPeers)
}

func (gs *ClassicGossipSubStrategy) OnGraft(rt *GossipSubRouter, topic string, peer peer.ID) {}
