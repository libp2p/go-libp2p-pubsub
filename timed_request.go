package pubsub

import "time"

type TimedRequest[T any] struct {
	Request    T
	ReceivedAt time.Time
}

func NewTimedRequest[T any](req T, receivedAt time.Time) TimedRequest[T] {
	return TimedRequest[T]{
		Request:    req,
		ReceivedAt: receivedAt,
	}
}
