package pubsub

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	MinBackoffDelay   = 100 * time.Millisecond
	MaxBackoffDelay   = 10 * time.Second
	TimeToLive        = 10 * time.Minute
	BackoffMultiplier = 2
)

type backoffHistory struct {
	duration  time.Duration
	lastTried time.Time
}

type backoff struct {
	mu   sync.Mutex
	info map[peer.ID]*backoffHistory
	ct   int // size threshold that kicks off the cleaner
}

func newBackoff(sizeThreshold int) *backoff {
	return &backoff{
		mu:   sync.Mutex{},
		ct:   sizeThreshold,
		info: make(map[peer.ID]*backoffHistory),
	}
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

	if len(b.info) > b.ct {
		b.cleanup()
	}

	return h.duration
}

func (b *backoff) cleanup() {
	for id, h := range b.info {
		if time.Since(h.lastTried) > TimeToLive {
			delete(b.info, id)
		}
	}
}
