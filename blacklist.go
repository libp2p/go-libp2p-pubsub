package pubsub

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/whyrusleeping/timecache"
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

// TimeCachedBlacklist is a blacklist implementation using a time cache
type TimeCachedBlacklist struct {
	sync.RWMutex
	tc *timecache.TimeCache
}

// NewTimeCachedBlacklist creates a new TimeCachedBlacklist with the given expiry duration
func NewTimeCachedBlacklist(expiry time.Duration) (Blacklist, error) {
	b := &TimeCachedBlacklist{tc: timecache.NewTimeCache(expiry)}
	return b, nil
}

func (b *TimeCachedBlacklist) Add(p peer.ID) {
	b.Lock()
	defer b.Unlock()

	b.tc.Add(p.String())
}

func (b *TimeCachedBlacklist) Contains(p peer.ID) bool {
	b.RLock()
	defer b.RUnlock()

	return b.tc.Has(p.String())
}
