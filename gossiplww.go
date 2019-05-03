package pubsub

import (
	"context"
	record "github.com/libp2p/go-libp2p-record"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type LWWMessageCache struct {
	validator record.Validator
	IDMsgMap  map[string]*pb.Message
}

func NewLWWMessageCache(validator record.Validator) *LWWMessageCache {
	return &LWWMessageCache{
		validator: validator,
		IDMsgMap:  make(map[string]*pb.Message),
	}
}

func (mc *LWWMessageCache) Put(msg *pb.Message) {
	for _, topic := range msg.TopicIDs {
		lastMsg, ok := mc.IDMsgMap[topic]
		if !ok {
			mc.IDMsgMap[topic] = msg
			continue
		}

		msgBytes := [][]byte{msg.Data, lastMsg.Data}
		winningIndex, err := mc.validator.Select("", msgBytes)
		if err != nil {
			panic(err)
		}

		if winningIndex == 0 {
			mc.IDMsgMap[topic] = msg
		}
	}
}

func (mc *LWWMessageCache) Get(mid string) (*pb.Message, bool) {
	m, ok := mc.IDMsgMap[mid]
	return m, ok
}

func (mc *LWWMessageCache) GetGossipIDs(topic string) []string {
	_, ok := mc.IDMsgMap[topic]
	if ok {
		return []string{topic}
	}
	return []string{}
}

func (mc *LWWMessageCache) Shift() {}

var _ MessageCacheReader = (*LWWMessageCache)(nil)

type randomPeersStrategy struct {
	numPeers int
}

func NewRandomPeersStrategy(numPeers int) EmittingStrategy {
	return &randomPeersStrategy{numPeers: numPeers}
}

func (s *randomPeersStrategy) GetEmitPeers(topicPeers GetFilteredPeers, _ map[peer.ID]struct{}) map[peer.ID]struct{} {
	gpeers := topicPeers(s.numPeers, func(peer.ID) bool { return true })
	return peerListToMap(gpeers)
}

type LWWGossipSubConfig struct {
	ClassicGossipSubStrategy
}

func (cfg *LWWGossipSubConfig) OnGraft(rt *GossipSubRouter, topic string, peer peer.ID) {
	rt.RequestMessage(topic, peer)
}

// NewGossipBaseSub returns a new PubSub object using GossipSubRouter as the router.
func NewGossipSyncLWW(ctx context.Context, h host.Host, mcache *LWWMessageCache, protocolID protocol.ID, opts ...Option) (*PubSub, error) {
	rt := NewGossipSubRouterWithStrategies(&LWWGossipSubConfig{ClassicGossipSubStrategy{
		mcache:             mcache,
		emitter:            NewRandomPeersStrategy(GossipSubD),
		supportedProtocols: []protocol.ID{protocolID},
		protocol:           protocolID,
	}})
	return NewPubSub(ctx, h, rt, opts...)
}
