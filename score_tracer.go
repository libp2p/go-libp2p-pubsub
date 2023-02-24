package pubsub

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

type AppPeerStatsTracer struct {
	mu        sync.RWMutex
	peerStats map[peer.ID]PeerStats
}

func NewAppPeerStatsTracer() *AppPeerStatsTracer {
	return &AppPeerStatsTracer{
		peerStats: make(map[peer.ID]PeerStats),
	}
}

func (s *AppPeerStatsTracer) GetPeerStats(p peer.ID) (PeerStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stat, ok := s.peerStats[p]
	return stat, ok
}

func (s *AppPeerStatsTracer) updatePeerStats(p peer.ID, stats PeerStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.peerStats[p] = stats
}

func (s *AppPeerStatsTracer) removePeerStats(p peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.peerStats, p)
}
