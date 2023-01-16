package timecache

import "time"

type Strategy uint8

const (
	Strategy_FirstSeen Strategy = iota
	Strategy_LastSeen
)

type TimeCache interface {
	Add(string)
	Has(string) bool
}

// NewTimeCache defaults to the new ("last seen") cache implementation
func NewTimeCache(span time.Duration) TimeCache {
	return NewTimeCacheWithStrategy(Strategy_LastSeen, span)
}

func NewTimeCacheWithStrategy(strategy Strategy, span time.Duration) TimeCache {
	switch strategy {
	case Strategy_FirstSeen:
		return newFirstSeenCache(span)
	case Strategy_LastSeen:
		return newLastSeenCache(span)
	default:
		// Default to the new time cache implementation
		return newLastSeenCache(span)
	}
}
