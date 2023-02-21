package timecache

import (
	"fmt"
	"testing"
	"time"
)

func TestFirstSeenCacheFound(t *testing.T) {
	tc := newFirstSeenCache(time.Minute)

	tc.Add("test")

	if !tc.Has("test") {
		t.Fatal("should have this key")
	}
}

func TestFirstSeenCacheExpire(t *testing.T) {
	backgroundSweepInterval = time.Second

	tc := newFirstSeenCache(time.Second)
	for i := 0; i < 10; i++ {
		tc.Add(fmt.Sprint(i))
		time.Sleep(time.Millisecond * 100)
	}

	time.Sleep(2 * time.Second)
	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}
}

func TestFirstSeenCacheNotFoundAfterExpire(t *testing.T) {
	backgroundSweepInterval = time.Second

	tc := newFirstSeenCache(time.Second)
	tc.Add(fmt.Sprint(0))

	time.Sleep(2 * time.Second)
	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}
}
