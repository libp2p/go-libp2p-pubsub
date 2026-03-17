package timecache

import (
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

// synctestTest wraps synctest.Test with GOMAXPROCS(1) to work around a Go
// runtime bug where concurrent bubble timer firing corrupts TSan state.
// https://github.com/golang/go/issues/78156
func synctestTest(t *testing.T, f func(t *testing.T)) {
	if raceEnabled {
		prev := runtime.GOMAXPROCS(1)
		t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	}
	synctest.Test(t, f)
}

func TestFirstSeenCacheFound(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newFirstSeenCache(time.Minute)
		defer tc.Done()

		tc.Add("test")

		if !tc.Has("test") {
			t.Fatal("should have this key")
		}
	})
}

func TestFirstSeenCacheExpire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newFirstSeenCacheWithSweepInterval(time.Second, time.Second)
		defer tc.Done()

		for i := 0; i < 10; i++ {
			tc.Add(fmt.Sprint(i))
			time.Sleep(time.Millisecond * 100)
		}

		time.Sleep(2 * time.Second)
		for i := 0; i < 10; i++ {
			if tc.Has(fmt.Sprint(i)) {
				t.Fatalf("should have dropped this key: %s from the cache already", fmt.Sprint(i))
			}
		}
	})
}

func TestFirstSeenCacheNotFoundAfterExpire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		tc := newFirstSeenCacheWithSweepInterval(time.Second, 500*time.Millisecond)
		defer tc.Done()

		tc.Add(fmt.Sprint(0))

		// Sleep past expiry + sweep interval to ensure the background sweep has removed the entry
		time.Sleep(2 * time.Second)
		if tc.Has(fmt.Sprint(0)) {
			t.Fatal("should have dropped this from the cache already")
		}
	})
}
