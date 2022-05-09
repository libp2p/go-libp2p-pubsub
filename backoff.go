package pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	MinBackoffDelay   = 100 * time.Millisecond
	MaxBackoffDelay   = 10 * time.Second
	TimeToLive             = 10 * time.Minute
	BackoffCleanupInterval = 1 * time.Minute
	BackoffMultiplier      = 2
)

type backoffHistory struct {
	duration  time.Duration
	lastTried time.Time
}

type backoff struct {
	mu   sync.Mutex
	info map[peer.ID]*backoffHistory
	ct   int           // size threshold that kicks off the cleaner
	ci   time.Duration // cleanup intervals
}

func newBackoff(ctx context.Context, sizeThreshold int, cleanupInterval time.Duration) *backoff {
	b := &backoff{
		mu:   sync.Mutex{},
		ct:   sizeThreshold,
		ci:   cleanupInterval,
		info: make(map[peer.ID]*backoffHistory),
	}

	go b.cleanupLoop(ctx)

	return b
}

func (b *backoff) updateAndGet(id peer.ID) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.info[id]
	if !ok || time.Since(h.lastTried) > TimeToLive {
		// first request goes immediately.
		h = &backoffHistory{
			duration: time.Duration(0),
		}
	} else if h.duration < MinBackoffDelay {
		h.duration = MinBackoffDelay
	} else if h.duration < MaxBackoffDelay {
		h.duration = time.Duration(BackoffMultiplier * h.duration)
		if h.duration > MaxBackoffDelay || h.duration < 0 {
			h.duration = MaxBackoffDelay
		}
	}

	h.lastTried = time.Now()
	b.info[id] = h

	return h.duration
}

func (b *backoff) cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, h := range b.info {
		if time.Since(h.lastTried) > TimeToLive {
			delete(b.info, id)
		}
	}
}

func (b *backoff) cleanupLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return // pubsub shutting down
		case <-time.Tick(b.ci):
			b.cleanup()
		}
	}
}
