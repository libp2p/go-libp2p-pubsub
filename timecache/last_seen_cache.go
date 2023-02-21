package timecache

import (
	"context"
	"sync"
	"time"
)

// LastSeenCache is a time cache that marks the expiry of a message when added or cheked for presence.
type LastSeenCache struct {
	lk  sync.Mutex
	m   map[string]time.Time
	ttl time.Duration

	done func()
}

var _ TimeCache = (*LastSeenCache)(nil)

func newLastSeenCache(ttl time.Duration) *LastSeenCache {
	tc := &LastSeenCache{
		m:   make(map[string]time.Time),
		ttl: ttl,
	}

	ctx, done := context.WithCancel(context.Background())
	tc.done = done
	go tc.background(ctx)

	return tc
}

func (tc *LastSeenCache) Done() {
	tc.done()
}

func (tc *LastSeenCache) Add(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	tc.m[s] = time.Now()

	return !ok
}

func (tc *LastSeenCache) Has(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	if ok {
		tc.m[s] = time.Now()
	}

	return ok
}

func (tc *LastSeenCache) background(ctx context.Context) {
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

func (tc *LastSeenCache) sweep(now time.Time) {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	expired := now.Add(tc.ttl)
	for k, expiry := range tc.m {
		if expiry.Before(expired) {
			delete(tc.m, k)
		}
	}
}
