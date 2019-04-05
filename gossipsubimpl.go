package pubsub

import (
	"context"

	host "github.com/libp2p/go-libp2p-host"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

const (
	GossipSubID = protocol.ID("/meshsub/1.0.0")
)

// NewGossipSub returns a new PubSub object using a GossipConfigurableRouter as the router and configured with classic GossipSub parameters.
func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := NewGossipConfigurableRouter(&ClassicGossipSubConfiguration{
		mcache:             NewMessageCache(GossipSubHistoryGossip, GossipSubHistoryLength),
		supportedProtocols: []protocol.ID{GossipSubID, FloodSubID},
		protocol:           GossipSubID,
	})

	return NewPubSub(ctx, h, rt, append([]Option{WithRouterConfiguration(rt)}, opts...)...)
}