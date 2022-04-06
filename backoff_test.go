package pubsub

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestBackoff_Update(t *testing.T){
	id1 := peer.ID("peer-1")
	id2 := peer.ID("peer-2")
	b := newBackoff(10)

	if len(b.info) > 0 {
		t.Fatal("non-empty info map for backoff")
	}

	if d := b.updateAndGet(id1); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %v", d)
	}
	if d := b.updateAndGet(id2); d != time.Duration(0) {
		t.Fatalf("invalid initialization: %v", d)
	}

	for i := 0; i < 10; i++{
		got := b.updateAndGet(id1)

		expected := time.Duration(math.Pow(BackoffMultiplier, float64(i)) * float64(MinBackoffDelay))
		if expected > MaxBackoffDelay {
			expected = MaxBackoffDelay
		}

		if expected != got {
			t.Fatalf("invalid backoff result, expected: %v, got: %v", expected, got)
		}
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

	if len(b.info) != 2 {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", 2, len(b.info))
	}
}

func TestBackoff_Clean(t *testing.T){
	size := 10
	b := newBackoff(size)

	for i := 0; i < size; i++{
		id := peer.ID(fmt.Sprintf("peer-%d", i))
		b.updateAndGet(id)
		b.info[id].lastTried = time.Now().Add(-TimeToLive) // enforces expiry
	}

	if len(b.info) != size {
		t.Fatalf("info map size mismatch, expected: %d, got: %d", size, len(b.info))
	}

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