package pubsub

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	MinBackoffDelay        = 100 * time.Millisecond
	MaxBackoffDelay        = 10 * time.Second
	TimeToLive             = 10 * time.Minute
	BackoffCleanupInterval = 1 * time.Minute
	BackoffMultiplier      = 2
	MaxBackoffJitterCoff   = 100
	MaxBackoffAttempts     = 4
)

type backoffHistory struct {
	duration  time.Duration
	lastTried time.Time
	attempts  int
}

type backoff struct {
	mu          sync.Mutex
	info        map[peer.ID]*backoffHistory
	ct          int           // size threshold that kicks off the cleaner
	ci          time.Duration // cleanup intervals
	maxAttempts int           // maximum backoff attempts prior to ejection
}

func newBackoff(ctx context.Context, sizeThreshold int, cleanupInterval time.Duration, maxAttempts int) *backoff {
	b := &backoff{
		mu:          sync.Mutex{},
		ct:          sizeThreshold,
		ci:          cleanupInterval,
		maxAttempts: maxAttempts,
		info:        make(map[peer.ID]*backoffHistory),
	}

	rand.Seed(time.Now().UnixNano()) // used for jitter
	go b.cleanupLoop(ctx)

	return b
}

func (b *backoff) updateAndGet(id peer.ID) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.info[id]
	switch {
	case !ok || time.Since(h.lastTried) > TimeToLive:
		// first request goes immediately.
		h = &backoffHistory{
			duration: time.Duration(0),
			attempts: 0,
		}

	case h.duration < MinBackoffDelay:
		h.duration = MinBackoffDelay

	case h.duration < MaxBackoffDelay:
		jitter := rand.Intn(MaxBackoffJitterCoff)
		h.duration = (BackoffMultiplier * h.duration) + time.Duration(jitter)*time.Millisecond
		if h.duration > MaxBackoffDelay || h.duration < 0 {
			h.duration = MaxBackoffDelay
		}
	}

	h.lastTried = time.Now()
	h.attempts += 1

	b.info[id] = h
	return h.duration
}

func (b *backoff) peerExceededBackoffThreshold(id peer.ID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.info[id]
	if !ok {
		return false // no record of this peer is still there, hence fine.
	}
	return h.attempts > b.maxAttempts
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
