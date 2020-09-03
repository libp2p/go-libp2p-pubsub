package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var PeerGaterRetainStats = 6 * time.Hour
var PeerGaterQuiet = time.Minute

type peerGater struct {
	sync.Mutex

	threshold, decay   float64
	validate, throttle float64

	lastThrottle time.Time

	stats map[peer.ID]*peerGaterStats
}

type peerGaterStats struct {
	connected bool
	expire    time.Time

	deliver, duplicate, ignore, reject float64
}

// WithPeerGater is a gossipsub router option that enables reactive validation queue
// management.
// The threshold parameter is the threshold of throttled/validated messages before the gating
// kicks in.
// The decay parameter is the (linear) decay of counters per second.
func WithPeerGater(threshold, decay float64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.gate = newPeerGater(ps.ctx, threshold, decay)

		// hook the tracer
		if ps.tracer != nil {
			ps.tracer.internal = append(ps.tracer.internal, gs.gate)
		} else {
			ps.tracer = &pubsubTracer{
				internal: []internalTracer{gs.gate},
				pid:      ps.host.ID(),
				msgID:    ps.msgID,
			}
		}

		return nil
	}
}

func newPeerGater(ctx context.Context, threshold, decay float64) *peerGater {
	pg := &peerGater{
		threshold: threshold,
		decay:     decay,
		stats:     make(map[peer.ID]*peerGaterStats),
	}
	go pg.background(ctx)
	return pg
}

func (pg *peerGater) background(ctx context.Context) {
	tick := time.NewTicker(DefaultDecayInterval)

	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			pg.decayStats()
		case <-ctx.Done():
			return
		}
	}
}

func (pg *peerGater) decayStats() {
	pg.Lock()
	defer pg.Unlock()

	pg.validate *= pg.decay
	if pg.validate < DefaultDecayToZero {
		pg.validate = 0
	}

	pg.throttle *= pg.throttle
	if pg.throttle < DefaultDecayToZero {
		pg.throttle = 0
	}

	now := time.Now()
	for p, st := range pg.stats {
		if st.connected {
			st.deliver *= pg.decay
			if st.deliver < DefaultDecayToZero {
				st.deliver = 0
			}

			st.duplicate *= pg.decay
			if st.duplicate < DefaultDecayToZero {
				st.duplicate = 0
			}

			st.ignore *= pg.decay
			if st.ignore < DefaultDecayToZero {
				st.ignore = 0
			}

			st.reject *= pg.decay
			if st.reject < DefaultDecayToZero {
				st.reject = 0
			}
		} else if st.expire.Before(now) {
			delete(pg.stats, p)
		}
	}
}

func (pg *peerGater) getPeerStats(p peer.ID) *peerGaterStats {
	st, ok := pg.stats[p]
	if !ok {
		st = &peerGaterStats{}
		pg.stats[p] = st
	}
	return st
}

func (pg *peerGater) AcceptFrom(p peer.ID) AcceptStatus {
	if pg == nil {
		return AcceptAll
	}

	pg.Lock()
	defer pg.Unlock()

	if time.Since(pg.lastThrottle) > PeerGaterQuiet {
		return AcceptAll
	}

	if pg.throttle == 0 {
		return AcceptAll
	}

	if pg.validate != 0 && pg.throttle/pg.validate < pg.threshold {
		return AcceptAll
	}

	st := pg.getPeerStats(p)

	total := st.deliver + 0.5*st.duplicate + st.ignore + 2*st.reject
	if total == 0 {
		return AcceptAll
	}

	// we make a randomized decision based on the goodput of the peer
	threshold := (1 + st.deliver) / (1 + total)
	if rand.Float64() < threshold {
		return AcceptAll
	}

	log.Debugf("throttling peer %s with threshold %f", p, threshold)
	return AcceptControl
}

func (pg *peerGater) AddPeer(p peer.ID, proto protocol.ID) {
	pg.Lock()
	defer pg.Unlock()

	st := pg.getPeerStats(p)
	st.connected = true
	st.expire = time.Time{}
}

func (pg *peerGater) RemovePeer(p peer.ID) {
	pg.Lock()
	defer pg.Unlock()

	st := pg.getPeerStats(p)
	st.connected = false
	st.expire = time.Now().Add(PeerGaterRetainStats)
}

func (pg *peerGater) Join(topic string)             {}
func (pg *peerGater) Leave(topic string)            {}
func (pg *peerGater) Graft(p peer.ID, topic string) {}
func (pg *peerGater) Prune(p peer.ID, topic string) {}

func (pg *peerGater) ValidateMessage(msg *Message) {
	pg.Lock()
	defer pg.Unlock()

	pg.validate++
}

func (pg *peerGater) DeliverMessage(msg *Message) {
	pg.Lock()
	defer pg.Unlock()

	st := pg.getPeerStats(msg.ReceivedFrom)
	st.deliver++
}

func (pg *peerGater) RejectMessage(msg *Message, reason string) {
	pg.Lock()
	defer pg.Unlock()

	switch reason {
	case rejectValidationQueueFull:
		fallthrough
	case rejectValidationThrottled:
		pg.lastThrottle = time.Now()
		pg.throttle++

	case rejectValidationIgnored:
		st := pg.getPeerStats(msg.ReceivedFrom)
		st.ignore++

	default:
		st := pg.getPeerStats(msg.ReceivedFrom)
		st.reject++
	}
}

func (pg *peerGater) DuplicateMessage(msg *Message) {
	pg.Lock()
	defer pg.Unlock()

	st := pg.getPeerStats(msg.ReceivedFrom)
	st.duplicate++
}

func (pg *peerGater) ThrottlePeer(p peer.ID) {}
