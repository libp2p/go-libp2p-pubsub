package timecache

import (
	"fmt"
	"testing"
	"time"
)

func TestLastSeenCacheFound(t *testing.T) {
	tc := newLastSeenCache(time.Minute)

	tc.Add("test")

	if !tc.Has("test") {
		t.Fatal("should have this key")
	}
}

func TestLastSeenCacheExpire(t *testing.T) {
	tc := newLastSeenCacheWithSweepInterval(time.Second, time.Second)
	for i := 0; i < 11; i++ {
		tc.Add(fmt.Sprint(i))
		time.Sleep(time.Millisecond * 100)
	}

	time.Sleep(2 * time.Second)
	for i := 0; i < 11; i++ {
		if tc.Has(fmt.Sprint(i)) {
			t.Fatalf("should have dropped this key: %s from the cache already", fmt.Sprint(i))
		}
	}
}

func TestLastSeenCacheSlideForward(t *testing.T) {
	t.Skip("timing is too fine grained to run in CI")

	tc := newLastSeenCache(time.Second)
	i := 0

	// T0ms: Add 8 entries with a 100ms sleep after each
	for i < 8 {
		tc.Add(fmt.Sprint(i))
		time.Sleep(time.Millisecond * 100)
		i++
	}

	// T800ms: Lookup the first entry - this should slide the entry forward so that its expiration is a full second
	// later.
	if !tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have this key")
	}

	// T800ms: Wait till after the first and second entries would have normally expired (had we not looked the first
	// entry up).
	time.Sleep(time.Millisecond * 400)

	// T1200ms: The first entry should still be present in the cache - this will also slide the entry forward.
	if !tc.Has(fmt.Sprint(0)) {
		t.Fatal("should still have this key")
	}

	// T1200ms: The second entry should have expired
	if tc.Has(fmt.Sprint(1)) {
		t.Fatal("should have dropped this from the cache already")
	}

	// T1200ms: Sleep till the first entry actually expires
	time.Sleep(time.Millisecond * 1100)

	// T2300ms: Now the first entry should have expired
	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}

	// And it should not have been added back
	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}
}

func TestLastSeenCacheNotFoundAfterExpire(t *testing.T) {
	tc := newLastSeenCacheWithSweepInterval(time.Second, time.Second)
	tc.Add(fmt.Sprint(0))

	time.Sleep(2 * time.Second)
	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}
}
