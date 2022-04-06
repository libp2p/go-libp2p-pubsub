package pubsub

import (
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestBackoff(t *testing.T){
	id1 := peer.ID("peer-1")
	id2 := peer.ID("peer-2")
	b := newBackoff()

	if len(b.info) > 0 {
		t.Fatal("non-empty info map for backoff")
	}

	if d := b.get(id1); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %d", d)
	}
	if d := b.get(id2); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %d", d)
	}

	for i := 0; i < 10; i++{
		b.update(id1)

		got := b.get(id1)
		expected := time.Duration(math.Pow(BackoffMultiplier, float64(i)) * float64(MinBackoffDelay))
		if expected > MaxBackoffDelay {
			expected = MaxBackoffDelay
		}

		if expected != got {
			t.Fatalf("invalid backoff result, expected: %d, got: %d", expected, got)
		}
	}

	b.update(id2)
	got := b.get(id2)
	if got != MinBackoffDelay {
		t.Fatalf("invalid backoff result, expected: %d, got: %d", MinBackoffDelay, got)
	}
}