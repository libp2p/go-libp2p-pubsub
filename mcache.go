package floodsub

import (
	pb "github.com/libp2p/go-floodsub/pb"
)

func NewMessageCache(gossip, history int) *MessageCache {
	return &MessageCache{}
}

type MessageCache struct {
}

func (mc *MessageCache) Put(msg *pb.Message) {
	// TODO
}

func (mc *MessageCache) Get(mid string) (*pb.Message, bool) {
	// TODO
	return nil, false
}

func (mc *MessageCache) GetGossipIDs(topic string) []string {
	return nil
}

func (mc *MessageCache) Shift() {
	// TODO
}
