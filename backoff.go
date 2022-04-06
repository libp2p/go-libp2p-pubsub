package pubsub

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	MinBackoffDelay   = 100 * time.Millisecond
	MaxBackoffDelay   = 10 * time.Second
	BackoffMultiplier = 2
)

type backoff struct {
	mu sync.Mutex
	info map[peer.ID]time.Duration
}

func newBackoff() *backoff{
	return &backoff{
		mu: sync.Mutex{},
		info: make(map[peer.ID]time.Duration),
	}
}

func (b *backoff) get(id peer.ID) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.info[id]
	if !ok {
		h = time.Duration(0)
		b.info[id] = h
	}

	return h
}

func (b *backoff) update(id peer.ID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	h := b.info[id]

	if h < MinBackoffDelay {
		h = MinBackoffDelay
	} else if h == MaxBackoffDelay {
		h = MaxBackoffDelay
	} else {
		h = time.Duration(BackoffMultiplier * h)
		if h > MaxBackoffDelay || h < 0 {
			h = MaxBackoffDelay
		}
	}

	b.info[id] = h
}
