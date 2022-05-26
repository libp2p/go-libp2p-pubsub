package pubsub

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestBackoff_Update(t *testing.T) {
	id1 := peer.ID("peer-1")
	id2 := peer.ID("peer-2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	size := 10
	cleanupInterval := 5 * time.Second
	maxBackoffAttempts := 10

	b := newBackoff(ctx, size, cleanupInterval, maxBackoffAttempts)

	if len(b.info) > 0 {
		t.Fatal("non-empty info map for backoff")
	}

	if d := b.updateAndGet(id1); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %v", d)
	}
	if d := b.updateAndGet(id2); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %v", d)
	}

	for i := 0; i < maxBackoffAttempts-1; i++ {
		got := b.updateAndGet(id1)

		expected := time.Duration(math.Pow(BackoffMultiplier, float64(i)) *
			float64(MinBackoffDelay+MaxBackoffJitterCoff*time.Millisecond))
		if expected > MaxBackoffDelay {
			expected = MaxBackoffDelay
		}

		if expected < got { // considering jitter, expected backoff must always be greater than or equal to actual.
			t.Fatalf("invalid backoff result, expected: %v, got: %v", expected, got)
		}

		// update attempts on id1 are below threshold, hence peer should never go beyond backoff attempt threshold
		if b.peerExceededBackoffThreshold(id1) {
			t.Fatalf("invalid exceeding threshold status")
		}
	}

	// trying once more beyond the threshold, hence expecting exceeding threshold
	b.updateAndGet(id1)
	if !b.peerExceededBackoffThreshold(id1) {
		t.Fatal("update beyond max attempts does not reflect threshold")
	}

	got := b.updateAndGet(id2)
	if got != MinBackoffDelay {
		t.Fatalf("invalid backoff result, expected: %v, got: %v", MinBackoffDelay, got)
	}

	// sets last tried of id2 to long ago that it resets back upon next try.
	b.info[id2].lastTried = time.Now().Add(-TimeToLive)
	got = b.updateAndGet(id2)
	if got != time.Duration(0) {
		t.Fatalf("invalid ttl expiration, expected: %v, got: %v", time.Duration(0), got)
	}

	// update attempts on id2 are below threshold, hence peer should never go beyond backoff attempt threshold
	if b.peerExceededBackoffThreshold(id2) {
		t.Fatalf("invalid exceeding threshold status")
	}

	if len(b.info) != 2 {
		t.Fatalf("pre-invalidation attempt, info map size mismatch, expected: %d, got: %d", 2, len(b.info))
	}

}

func TestBackoff_Clean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	size := 10
	cleanupInterval := 2 * time.Second
	maxBackoffAttempts := 100 // setting attempts to a high number hence testing cleanup logic.
	b := newBackoff(ctx, size, cleanupInterval, maxBackoffAttempts)

	for i := 0; i < size; i++ {
		id := peer.ID(fmt.Sprintf("peer-%d", i))
		b.updateAndGet(id)
		b.info[id].lastTried = time.Now().Add(-TimeToLive) // enforces expiry
	}

	if len(b.info) != size {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", size, len(b.info))
	}

	// waits for a cleanup loop to kick-in
	time.Sleep(2 * cleanupInterval)

	// next update should trigger cleanup
	got := b.updateAndGet(peer.ID("some-new-peer"))
	if got != time.Duration(0) {
		t.Fatalf("invalid backoff result, expected: %v, got: %v", time.Duration(0), got)
	}

	// except "some-new-peer" every other records must be cleaned up
	if len(b.info) != 1 {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", 1, len(b.info))
	}
}
