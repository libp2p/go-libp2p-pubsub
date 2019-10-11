package pubsub

import "context"

type Relay struct {
	topic    string
	cancelCh chan<- *Relay
	ctx      context.Context
}

func (r *Relay) Topic() string {
	return r.topic
}

func (r *Relay) Cancel() {
	select {
	case r.cancelCh <- r:
	case <-r.ctx.Done():
	}
}
