package vivaldi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	hostpkg "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peerpkg "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ProtocolID is the vivaldi protocol used for exchanging coordinates.
const ProtocolID = "/libp2p/vivaldi/1.0.0"

// Service exposes a small request/response handler for exchanging Coord+Error and measuring RTT by timing the round-trip.
type Service struct {
	h          hostpkg.Host
	protocol   protocol.ID
	timeout    time.Duration
	lk         sync.RWMutex
	local      *VivaldiState
	peerStates map[peerpkg.ID]*VivaldiState
	peerRTTms  map[peerpkg.ID]float64

	// Newton invariant context.
	closePeers             map[peerpkg.ID]struct{}
	randomPeers            map[peerpkg.ID]struct{}
	peerMeta               map[peerpkg.ID]peerInvariantState
	forceClose             []float64
	forceRandom            []float64
	invariantWarmupUpdates int
	peerUpdateCounts       map[peerpkg.ID]int
}

type peerInvariantState struct {
	LastReported Coord
	HasLast      bool
	ExpectedMove Coord
}

// Config defines service options.
type Config struct {
	Protocol protocol.ID
	Timeout  time.Duration
}

// NewService creates a new Vivaldi service bound to the given host.
func NewService(h hostpkg.Host, cfg *Config) *Service {
	proto := protocol.ID(ProtocolID)
	t := 5 * time.Second
	if cfg != nil {
		if cfg.Protocol != "" {
			proto = cfg.Protocol
		}
		if cfg.Timeout > 0 {
			t = cfg.Timeout
		}
	}
	s := &Service{
		h:                      h,
		protocol:               proto,
		timeout:                t,
		peerStates:             make(map[peerpkg.ID]*VivaldiState),
		peerRTTms:              make(map[peerpkg.ID]float64),
		closePeers:             make(map[peerpkg.ID]struct{}),
		randomPeers:            make(map[peerpkg.ID]struct{}),
		peerMeta:               make(map[peerpkg.ID]peerInvariantState),
		invariantWarmupUpdates: 0,
		peerUpdateCounts:       make(map[peerpkg.ID]int),
	}
	h.SetStreamHandler(proto, s.handleStream)
	return s
}

// Close unregisters the stream handler.
func (s *Service) Close() {
	s.h.RemoveStreamHandler(s.protocol)
}

// response is the payload sent back to requesting peers.
type response struct {
	Coord     Coord   `json:"coord"`
	Error     float64 `json:"error"`
	Timestamp int64   `json:"ts"` // unix nanos
}

// handleStream answers incoming vivaldi requests with the local coord and error.
func (s *Service) handleStream(st network.Stream) {
	defer st.Close()
	// Read the (optional) request payload; we don't expect anything large.
	_ = st.SetDeadline(time.Now().Add(s.timeout))
	r := bufio.NewReader(st)
	// read until EOF or newline; allow empty requests
	_, _ = r.ReadBytes('\n')

	// Prepare response from an assumed global/local state provider.
	// For now, attempt to obtain state via a package-level accessor. If not set,
	// respond with zero coord and high error.
	var resp response
	s.lk.RLock()
	ls := s.local
	s.lk.RUnlock()
	if ls != nil {
		resp = response{Coord: ls.Coord, Error: ls.Error, Timestamp: time.Now().UnixNano()}
	} else {
		resp = response{Coord: Coord{0, 0, 0}, Error: 1e6, Timestamp: time.Now().UnixNano()}
	}

	enc := json.NewEncoder(st)
	_ = enc.Encode(&resp)
}

// ExchangeOnce opens a stream to peer, performs a request/response, measures RTT,
// and returns (rtt, remoteCoord, remoteError, error).
func (s *Service) ExchangeOnce(ctx context.Context, pid peerpkg.ID) (time.Duration, Coord, float64, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	st, err := s.h.NewStream(ctx, pid, s.protocol)
	if err != nil {
		return 0, Coord{}, 0, err
	}
	defer st.Close()
	// Set deadlines on the stream so slow peers don't block forever.
	_ = st.SetDeadline(time.Now().Add(s.timeout))

	// Send a small request (newline) to trigger the response.
	if _, err := st.Write([]byte("\n")); err != nil {
		return 0, Coord{}, 0, err
	}

	// Read response fully.
	data, err := io.ReadAll(st)
	rtt := time.Since(start)
	if err != nil {
		return rtt, Coord{}, 0, err
	}

	var resp response
	if err := json.Unmarshal(data, &resp); err != nil {
		return rtt, Coord{}, 0, err
	}
	return rtt, resp.Coord, resp.Error, nil
}

// SetLocalState sets the service's local Vivaldi state returned to peers.
func (s *Service) SetLocalState(st *VivaldiState) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.local = st
}

// GetLocalState returns a copy of the local Vivaldi state.
func (s *Service) GetLocalState() *VivaldiState {
	s.lk.RLock()
	defer s.lk.RUnlock()
	if s.local == nil {
		return nil
	}
	tmp := *s.local
	return &tmp
}

// GetPeerState returns the last known Vivaldi state for a peer, or nil.
func (s *Service) GetPeerState(pid peerpkg.ID) *VivaldiState {
	s.lk.RLock()
	defer s.lk.RUnlock()
	ps := s.peerStates[pid]
	if ps == nil {
		return nil
	}
	tmp := *ps
	return &tmp
}

// GetPeerRTTMS returns the most recently measured RTT to pid in milliseconds,
// and whether a measurement exists.
func (s *Service) GetPeerRTTMS(pid peerpkg.ID) (float64, bool) {
	s.lk.RLock()
	defer s.lk.RUnlock()
	v, ok := s.peerRTTms[pid]
	return v, ok
}

// SetNeighborSets records the close and random neighbour sets used to select
// peers for Newton-Vivaldi coordinate exchanges.
func (s *Service) SetNeighborSets(closePeers []peerpkg.ID, randomPeers []peerpkg.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.closePeers = make(map[peerpkg.ID]struct{}, len(closePeers))
	s.randomPeers = make(map[peerpkg.ID]struct{}, len(randomPeers))
	for _, p := range closePeers {
		s.closePeers[p] = struct{}{}
	}
	for _, p := range randomPeers {
		s.randomPeers[p] = struct{}{}
	}
}

func (s *Service) logInvariantReject(pid peerpkg.ID, invariant string, details string) {
	log.Printf("vivaldi[newton]: reject update peer=%s invariant=%s reason=%s", pid, invariant, details)
}
