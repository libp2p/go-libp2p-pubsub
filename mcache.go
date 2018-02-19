package floodsub

import (
	pb "github.com/libp2p/go-floodsub/pb"
)

func NewMessageCache(win int) *MessageCache {
	return &MessageCache{}
}

type MessageCache struct {
}

func (mc *MessageCache) Add(msg *pb.Message) {
	// TODO
}
