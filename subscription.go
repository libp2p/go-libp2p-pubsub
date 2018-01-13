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
	result := make(chan bool, 1)
	vctx, cancel := context.WithTimeout(ctx, sub.validateTimeout)
	defer cancel()

	go func() {
		result <- sub.validate(vctx, msg)
	}()

	select {
	case valid := <-result:
		if !valid {
			log.Debugf("validation failed for topic %s", sub.topic)
		}
		return valid
	case <-vctx.Done():
		log.Debugf("validation timeout for topic %s", sub.topic)
		return false
	}
}
