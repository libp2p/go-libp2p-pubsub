package timecache

import (
	"fmt"
	"testing"
	"time"
)

func TestLastSeenCacheFound(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newLastSeenCache(time.Minute)
		defer tc.Done()

		tc.Add("test")

		if !tc.Has("test") {
			t.Fatal("should have this key")
		}
	})
}

func TestLastSeenCacheExpire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newLastSeenCacheWithSweepInterval(time.Second, time.Second)
		defer tc.Done()

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
	})
}

func TestLastSeenCacheSlideForward(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newLastSeenCacheWithSweepInterval(time.Second, 100*time.Millisecond)
		defer tc.Done()

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
		// entry up). Sleep a bit extra so the background sweep has time to remove expired entries.
		time.Sleep(time.Millisecond * 410)

		// ~T1210ms: The first entry should still be present in the cache - this will also slide the entry forward.
		if !tc.Has(fmt.Sprint(0)) {
			t.Fatal("should still have this key")
		}

		// ~T1210ms: The second entry should have expired (added at T100ms, expired at T1100ms, sweep removed it)
		if tc.Has(fmt.Sprint(1)) {
			t.Fatal("should have dropped this from the cache already")
		}

		// Sleep till the first entry actually expires (slid to ~T1210ms + 1s = T2210ms)
		time.Sleep(time.Millisecond * 1110)

		// Now the first entry should have expired
		if tc.Has(fmt.Sprint(0)) {
			t.Fatal("should have dropped this from the cache already")
		}

		// And it should not have been added back
		if tc.Has(fmt.Sprint(0)) {
			t.Fatal("should have dropped this from the cache already")
		}
	})
}

func TestLastSeenCacheNotFoundAfterExpire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newLastSeenCacheWithSweepInterval(time.Second, 500*time.Millisecond)
		defer tc.Done()

		tc.Add(fmt.Sprint(0))

		// Sleep past expiry + sweep interval to ensure the background sweep has removed the entry
		time.Sleep(2 * time.Second)
		if tc.Has(fmt.Sprint(0)) {
			t.Fatal("should have dropped this from the cache already")
		}
	})
}
