package pubsub

import (
	"errors"
	"iter"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/quic-go/quic-go"
)

type peerBundle struct {
	p   peer.ID
	rtt time.Duration
	rpc *RPC
}

type rttLookup struct {
	h host.Host
}

func (r rttLookup) lookup(i peer.ID) (rtt time.Duration) {
	rtt = 200 * time.Millisecond

	for _, conn := range r.h.Network().ConnsToPeer(i) {
		// TODO add support for Yamux conns as well
		var quicConn *quic.Conn
		if conn.As(&quicConn) {
			rtt = quicConn.ConnectionStats().SmoothedRTT
			return
		}
	}
	return
}

func WithNFastestPeers(n int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return errors.New("not a gossipsub router")
		}
		rttLookup := rttLookup{ps.host}
		s := nFastestPeersScheduler{
			n:            n,
			peerSizeHint: gs.params.D,
			rttFn:        rttLookup.lookup,
		}
		gs.publishStrategy = &s
		return nil
	}
}

type nFastestPeersScheduler struct {
	n            int
	peerSizeHint int
	rttFn        func(peer.ID) time.Duration
	peers        []peerBundle
}

var _ RPCScheduler = &nFastestPeersScheduler{}

func (s *nFastestPeersScheduler) AddRPC(peer peer.ID, msgID string, rpc *RPC) {
	if s.peers == nil {
		s.peers = make([]peerBundle, 0, s.peerSizeHint)
	}
	s.peers = append(s.peers, peerBundle{
		p:   peer,
		rtt: s.rttFn(peer),
		rpc: rpc,
	})
}

func (s *nFastestPeersScheduler) All() iter.Seq2[peer.ID, *RPC] {
	slices.SortFunc(s.peers, func(a, b peerBundle) int {
		return int(a.rtt - b.rtt)
	})

	if len(s.peers) > s.n {
		s.peers = s.peers[:s.n]
	}

	return func(yield func(peer.ID, *RPC) bool) {
		defer func() {
			if len(s.peers) > 0 {
				s.peers = s.peers[:0]
			}
		}()
		for _, p := range s.peers {
			if !yield(p.p, p.rpc) {
				return
			}
		}
	}
}
