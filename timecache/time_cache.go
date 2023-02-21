package timecache

import (
	"time"

	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("pubsub/timecache")
var backgroundSweepInterval = time.Minute

type Strategy uint8

const (
	Strategy_FirstSeen Strategy = iota
	Strategy_LastSeen
)

type TimeCache interface {
	Add(string) bool
	Has(string) bool
	Done()
}

// NewTimeCache defaults to the original ("first seen") cache implementation
func NewTimeCache(ttl time.Duration) TimeCache {
	return NewTimeCacheWithStrategy(Strategy_FirstSeen, ttl)
}

func NewTimeCacheWithStrategy(strategy Strategy, ttl time.Duration) TimeCache {
	switch strategy {
	case Strategy_FirstSeen:
		return newFirstSeenCache(ttl)
	case Strategy_LastSeen:
		return newLastSeenCache(ttl)
	default:
		// Default to the original time cache implementation
		return newFirstSeenCache(ttl)
	}
}
