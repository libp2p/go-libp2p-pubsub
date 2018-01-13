package floodsub

import (
	"context"
	"time"
)

type Subscription struct {
	topic    string
	ch       chan *Message
	cancelCh chan<- *Subscription
	err      error

	validate         Validator
	validateTimeout  time.Duration
	validateThrottle chan struct{}
}

func (sub *Subscription) Topic() string {
	return sub.topic
}

func (sub *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case msg, ok := <-sub.ch:
		if !ok {
			return msg, sub.err
		}

		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sub *Subscription) Cancel() {
	sub.cancelCh <- sub
}

func (sub *Subscription) validateMsg(ctx context.Context, msg *Message) bool {
	vctx, cancel := context.WithTimeout(ctx, sub.validateTimeout)
	defer cancel()

	valid := sub.validate(vctx, msg)
	if !valid {
		log.Debugf("validation failed for topic %s", sub.topic)
	}

	return valid
}
