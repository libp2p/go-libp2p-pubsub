package pubsub

// Relay handles the details of a particular Topic relay.
// Only one Relay will exist per topic.
type Relay struct {
	sub *Subscription
}

// Topic returns the topic handle associated with the relay
func (r *Relay) Topic() *Topic {
	return r.sub.topicHandle
}

// Cancel closes the Relay & removes it from the topic handle it is relaying for
// If this is the last active subscription, then pubsub will send an unsubscribe
// announcement to the network.
func (r *Relay) Cancel() {
	select {
	case r.sub.cancelCh <- r.sub:
	case <-r.sub.ctx.Done():
	}
	r.sub.topicHandle.removeRelay(r)
}

type RelayOpt func(r *Relay) error
