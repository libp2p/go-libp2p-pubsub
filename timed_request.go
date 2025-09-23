package pubsub

import "time"

type TimedRequest[T any] struct {
	Request        T
	ReceivedAt     time.Time
	EvalMethodName string
}

func NewTimedRequest[T any](req T, receivedAt time.Time) TimedRequest[T] {
	return TimedRequest[T]{
		Request:    req,
		ReceivedAt: receivedAt,
	}
}

func (t *TimedRequest[T]) AddEvalMethodName(methodName string) {
	t.EvalMethodName = methodName
}
