package timecache

import (
	"context"
	"sync"
	"time"
)

// FirstSeenCache is a time cache that only marks the expiry of a message when first added.
type FirstSeenCache struct {
	lk  sync.RWMutex
	m   map[string]time.Time
	ttl time.Duration

	done func()
}

var _ TimeCache = (*FirstSeenCache)(nil)

func newFirstSeenCache(ttl time.Duration) *FirstSeenCache {
	tc := &FirstSeenCache{
		m:   make(map[string]time.Time),
		ttl: ttl,
	}

	ctx, done := context.WithCancel(context.Background())
	tc.done = done
	go tc.background(ctx)

	return tc
}

func (tc *FirstSeenCache) Done() {
	tc.done()
}

func (tc *FirstSeenCache) Has(s string) bool {
	tc.lk.RLock()
	defer tc.lk.RUnlock()

	_, ok := tc.m[s]
	return ok
}

func (tc *FirstSeenCache) Add(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	if ok {
		return false
	}

	tc.m[s] = time.Now().Add(tc.ttl)
	return true
}

func (tc *FirstSeenCache) background(ctx context.Context) {
	ticker := time.NewTimer(backgroundSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			tc.sweep(now)

		case <-ctx.Done():
			return
		}
	}
}

func (tc *FirstSeenCache) sweep(now time.Time) {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	for k, expiry := range tc.m {
		if expiry.Before(now) {
			delete(tc.m, k)
		}
	}
}
