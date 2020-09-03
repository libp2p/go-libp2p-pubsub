package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	manet "github.com/multiformats/go-multiaddr-net"
)

var PeerGaterRetainStats = 6 * time.Hour
var PeerGaterQuiet = time.Minute

type peerGater struct {
	sync.Mutex

	threshold, decay   float64
	validate, throttle float64

	lastThrottle time.Time

	peerStats map[peer.ID]*peerGaterStats
	ipStats   map[string]*peerGaterStats

	host host.Host

	// for unit tests
	getIP func(peer.ID) string
}

type peerGaterStats struct {
	connected int
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

		gs.gate = newPeerGater(ps.ctx, ps.host, threshold, decay)

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

func newPeerGater(ctx context.Context, host host.Host, threshold, decay float64) *peerGater {
	pg := &peerGater{
		threshold: threshold,
		decay:     decay,
		peerStats: make(map[peer.ID]*peerGaterStats),
		ipStats:   make(map[string]*peerGaterStats),
		host:      host,
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
	for ip, st := range pg.ipStats {
		if st.connected > 0 {
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
			delete(pg.ipStats, ip)
		}
	}
}

func (pg *peerGater) getPeerStats(p peer.ID) *peerGaterStats {
	st, ok := pg.peerStats[p]
	if !ok {
		st = pg.getIPStats(p)
		pg.peerStats[p] = st
	}
	return st
}

func (pg *peerGater) getIPStats(p peer.ID) *peerGaterStats {
	ip := pg.getPeerIP(p)
	st, ok := pg.ipStats[ip]
	if !ok {
		st = &peerGaterStats{}
		pg.ipStats[ip] = st
	}
	return st
}

func (pg *peerGater) getPeerIP(p peer.ID) string {
	if pg.getIP != nil {
		return pg.getIP(p)
	}

	connToIP := func(c network.Conn) string {
		remote := c.RemoteMultiaddr()
		ip, err := manet.ToIP(remote)
		if err != nil {
			return "<unknown>"
		}
		return ip.String()
	}

	conns := pg.host.Network().ConnsToPeer(p)
	switch len(conns) {
	case 0:
		return "<unknown>"
	case 1:
		return connToIP(conns[0])
	default:
		// we have multiple connections -- order by number of streams and use the one with the
		// most streams; it's a nightmare to track multiple IPs per peer, so pick the best one.
		streams := make(map[string]int)
		for _, c := range conns {
			streams[c.ID()] = len(c.GetStreams())
		}
		sort.Slice(conns, func(i, j int) bool {
			return streams[conns[i].ID()] > streams[conns[j].ID()]
		})
		return connToIP(conns[0])
	}
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
	st.connected++
}

func (pg *peerGater) RemovePeer(p peer.ID) {
	pg.Lock()
	defer pg.Unlock()

	st := pg.getPeerStats(p)
	st.connected--
	st.expire = time.Now().Add(PeerGaterRetainStats)

	delete(pg.peerStats, p)
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
