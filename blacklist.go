package pubsub

import (
	peer "github.com/libp2p/go-libp2p-peer"
)

// Blacklist is an interface for peer blacklisting.
type Blacklist interface {
	Add(peer.ID)
	Contains(peer.ID) bool
}

// MapBlacklist is a blacklist implementation using a perfect map
type MapBlacklist map[peer.ID]struct{}

// NewMapBlacklist creates a new MapBlacklist
func NewMapBlacklist() Blacklist {
	return MapBlacklist(make(map[peer.ID]struct{}))
}

func (b MapBlacklist) Add(p peer.ID) {
	b[p] = struct{}{}
}

func (b MapBlacklist) Contains(p peer.ID) bool {
	_, ok := b[p]
	return ok
}
