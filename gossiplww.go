package pubsub

import (
	"context"

	host "github.com/libp2p/go-libp2p-host"
	protocol "github.com/libp2p/go-libp2p-protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type LWWMessageCache struct {
	IsNewerThan   func(currentMsg, incomingMsg *pb.Message) bool
	ComputeID     func(msg *pb.Message) string
	topicMsgIDMap map[string]string
	IDMsgMap      map[string]*pb.Message
}

func NewLWWMessageCache(IsNewerThan func(currentMsg, incomingMsg *pb.Message) bool, ComputeID func(msg *pb.Message) string) *LWWMessageCache {
	return &LWWMessageCache{
		topicMsgIDMap: make(map[string]string),
		IDMsgMap:      make(map[string]*pb.Message),
		IsNewerThan:   IsNewerThan,
		ComputeID:     ComputeID,
	}
}

func (mc *LWWMessageCache) Put(msg *pb.Message) {
	mid := mc.ComputeID(msg)
	_, ok := mc.IDMsgMap[mid]
	if !ok {
		mc.IDMsgMap[mid] = msg
	}

	for _, topic := range msg.TopicIDs {
		lastMsgID, ok := mc.topicMsgIDMap[topic]
		if !ok {
			mc.topicMsgIDMap[topic] = mid
			continue
		}
		lastMsg, ok := mc.IDMsgMap[lastMsgID]
		if !ok {
			continue
		}
		if mc.IsNewerThan(msg, lastMsg) {
			mc.topicMsgIDMap[topic] = mid
		}
	}
}

func (mc *LWWMessageCache) Get(mid string) (*pb.Message, bool) {
	m, ok := mc.IDMsgMap[mid]
	return m, ok
}

func (mc *LWWMessageCache) GetGossipIDs(topic string) []string {
	mid, ok := mc.topicMsgIDMap[topic]
	if ok {
		return []string{mid}
	}
	return []string{}
}

func (mc *LWWMessageCache) Shift() {}

var _ MessageCacheReader = (*LWWMessageCache)(nil)

// NewGossipBaseSub returns a new PubSub object using GossipSubRouter as the router.
func NewGossipSyncLWW(ctx context.Context, h host.Host, mcache *LWWMessageCache, protocolID protocol.ID, opts ...Option) (*PubSub, error) {
	rt := NewGossipConfigurableRouter(&ClassicGossipSubConfiguration{
		mcache:             mcache,
		supportedProtocols: []protocol.ID{protocolID},
		protocol:           protocolID,
	})
	return NewPubSub(ctx, h, rt, append([]Option{WithRouterConfiguration(rt)}, opts...)...)
}
