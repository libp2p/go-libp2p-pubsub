package pubsub

import (
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

//HandleRPC, handleIHave, handleIWant, Publish, pushGossip

type MessageCacher interface {
	MessageCacheReader
	Put(msg *pb.Message)
}

type ClassicGossipSubConfiguration struct {
	mcache             MessageCacher
	supportedProtocols []protocol.ID
	protocol           protocol.ID
}

func (gs *ClassicGossipSubConfiguration) GetCacher() MessageCacheReader {
	return gs.mcache
}

func (gs *ClassicGossipSubConfiguration) SupportedProtocols() []protocol.ID {
	return gs.supportedProtocols
}
func (gs *ClassicGossipSubConfiguration) Protocol() protocol.ID {
	return gs.protocol
}

func (gs *ClassicGossipSubConfiguration) Publish(rt *GossipConfigurableRouter, from peer.ID, msg *pb.Message) {
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
