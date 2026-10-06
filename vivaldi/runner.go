package vivaldi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	peerpkg "github.com/libp2p/go-libp2p/core/peer"
)

// UpdateConfig configures update and Newton guard behavior.
type UpdateConfig struct {
	Ce               float64
	Cc               float64
	Newton           bool
	OutlierThreshold float64
	Samples          int
	Interval         time.Duration

	// Newton checks
	IN1CentroidThresholdMS float64
	IN2ProjectionThreshold float64
	IN3MADKRandom          float64
	IN3MADKClose           float64
	IN3MinSamples          int
}

type sample struct {
	rtt    time.Duration
	coord  Coord
	err    float64
	errObj error
}

// ExchangeAndUpdate performs N exchanges with a peer, picks median RTT sample,
// applies Newton checks (if enabled), and updates local coordinates.
func (s *Service) ExchangeAndUpdate(ctx context.Context, pid peerpkg.ID, cfg UpdateConfig) (*VivaldiState, error) {
	if cfg.Samples <= 0 {
		cfg.Samples = 1
	}
	if cfg.Ce <= 0 {
		cfg.Ce = 0.25
	}
	if cfg.Cc <= 0 {
		cfg.Cc = 0.25
	}
	if cfg.IN3MinSamples <= 0 {
		cfg.IN3MinSamples = 8
	}

	smpls := make([]sample, 0, cfg.Samples)
	for i := 0; i < cfg.Samples; i++ {
		rtt, coord, remoteErr, err := s.ExchangeOnce(ctx, pid)
		smpls = append(smpls, sample{rtt: rtt, coord: coord, err: remoteErr, errObj: err})
		if i+1 < cfg.Samples {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	good := make([]sample, 0, len(smpls))
	for _, sp := range smpls {
		if sp.errObj == nil {
			good = append(good, sp)
		}
	}
	if len(good) == 0 {
		return nil, errors.New("all exchanges failed")
	}
	chosen := good[indexOfMedianDuration(good)]

	s.lk.Lock()
	updateCount := s.peerUpdateCounts[pid]
	s.peerUpdateCounts[pid] = updateCount + 1
	warmup := updateCount < s.invariantWarmupUpdates

	if s.local == nil {
		s.local = &VivaldiState{Coord: Coord{0, 0, 0}, Error: 1.0}
	}
	localCopy := *s.local
	s.lk.Unlock()

	params := VivaldiUpdateParams{
		Local:            localCopy,
		Remote:           VivaldiState{Coord: chosen.coord, Error: chosen.err},
		RTT:              float64(chosen.rtt.Milliseconds()),
		Ce:               cfg.Ce,
		Cc:               cfg.Cc,
		Newton:           cfg.Newton,
		OutlierThreshold: cfg.OutlierThreshold,
	}
	res, err := UpdateVivaldi(params)
	if err != nil {
		return nil, err
	}

	if cfg.Newton {
		// IN1, IN2 and IN3 are treated as soft invariants during warmup (only log),
		// and as hard invariants afterwards (log + reject).
		if err := s.checkIN2(pid, chosen.coord, cfg); err != nil {
			s.logInvariantReject(pid, "IN2", err.Error())
			if !warmup {
				return nil, err
			}
		}
		if err := s.checkIN1(pid, chosen.coord, cfg); err != nil {
			s.logInvariantReject(pid, "IN1", err.Error())
			if !warmup {
				return nil, err
			}
		}
		if err := s.checkIN3(pid, res.ForceMagnitude, cfg); err != nil {
			s.logInvariantReject(pid, "IN3", err.Error())
			if !warmup {
				return nil, err
			}
		}
	}

	s.lk.Lock()
	s.peerStates[pid] = &VivaldiState{Coord: chosen.coord, Error: chosen.err}
	s.peerRTTms[pid] = float64(chosen.rtt.Milliseconds())
	s.local = &VivaldiState{Coord: res.NewCoord, Error: res.NewError}

	// Update IN2 expected movement tracking for physically-close peers.
	if cfg.Newton {
		s.updateIN2Expectations(pid, res.ForceVector)
		meta := s.peerMeta[pid]
		meta.LastReported = chosen.coord
		meta.HasLast = true
		meta.ExpectedMove = Coord{}
		s.peerMeta[pid] = meta

		cls := s.peerClassLocked(pid)
		s.recordForceSampleLocked(cls, res.ForceMagnitude)
	}
	newLocal := *s.local
	s.lk.Unlock()
	return &newLocal, nil
}

func (s *Service) checkIN1(pid peerpkg.ID, incoming Coord, cfg UpdateConfig) error {
	if cfg.IN1CentroidThresholdMS <= 0 {
		return nil
	}
	s.lk.RLock()
	defer s.lk.RUnlock()

	// IN1: centroid of local + random neighbors should stay close to origin.
	count := 1.0
	sum := s.local.Coord
	for rp := range s.randomPeers {
		ps, ok := s.peerStates[rp]
		if !ok {
			continue
		}
		c := ps.Coord
		if rp == pid {
			c = incoming
		}
		sum = hvAdd(sum, c)
		count++
	}
	if count < 2 {
		return nil
	}
	centroid := hvScale(sum, 1.0/count)
	if hvNorm(centroid) > cfg.IN1CentroidThresholdMS {
		return fmt.Errorf("centroid drift %.2fms exceeds threshold %.2fms", hvNorm(centroid), cfg.IN1CentroidThresholdMS)
	}
	return nil
}

func (s *Service) checkIN2(pid peerpkg.ID, incoming Coord, cfg UpdateConfig) error {
	if cfg.IN2ProjectionThreshold <= 0 {
		return nil
	}
	s.lk.RLock()
	defer s.lk.RUnlock()
	if _, ok := s.closePeers[pid]; !ok {
		return nil
	}
	meta := s.peerMeta[pid]
	if !meta.HasLast {
		return nil
	}
	observed := hvDiff(incoming, meta.LastReported)
	diff := hvDiff(observed, meta.ExpectedMove)
	if hvNorm(diff) > cfg.IN2ProjectionThreshold {
		return fmt.Errorf("projection mismatch %.2fms exceeds threshold %.2fms", hvNorm(diff), cfg.IN2ProjectionThreshold)
	}
	return nil
}

func (s *Service) checkIN3(pid peerpkg.ID, forceMag float64, cfg UpdateConfig) error {
	s.lk.RLock()
	defer s.lk.RUnlock()
	class := s.peerClassLocked(pid)

	var hist []float64
	k := cfg.IN3MADKRandom
	if class == "close" {
		hist = s.forceClose
		k = cfg.IN3MADKClose
	} else {
		hist = s.forceRandom
	}
	if len(hist) < cfg.IN3MinSamples || k <= 0 {
		return nil
	}

	med := median(hist)
	madev := mad(hist, med)
	// Preserve robustness if all previous samples were identical.
	if madev < 1e-6 {
		madev = 1e-6
	}
	limit := med + k*madev
	if forceMag > limit {
		return fmt.Errorf("force %.2fms exceeds median+K*MAD %.2fms (median=%.2f, MAD=%.2f, K=%.2f)", forceMag, limit, med, madev, k)
	}
	return nil
}

func (s *Service) updateIN2Expectations(source peerpkg.ID, forceVec Coord) {
	for k := range s.closePeers {
		if k == source {
			continue
		}
		ps, ok := s.peerStates[k]
		if !ok {
			continue
		}
		src, ok := s.peerStates[source]
		if !ok {
			continue
		}
		dir := hvUnit(hvSub(src.Coord, ps.Coord))
		exp := hvProjection(forceVec, dir)
		meta := s.peerMeta[k]
		meta.ExpectedMove = hvAdd(meta.ExpectedMove, exp)
		s.peerMeta[k] = meta
	}
}

func (s *Service) peerClassLocked(pid peerpkg.ID) string {
	if _, ok := s.closePeers[pid]; ok {
		return "close"
	}
	return "random"
}

func (s *Service) recordForceSampleLocked(class string, v float64) {
	const maxHist = 256
	if class == "close" {
		s.forceClose = append(s.forceClose, v)
		if len(s.forceClose) > maxHist {
			s.forceClose = s.forceClose[len(s.forceClose)-maxHist:]
		}
		return
	}
	s.forceRandom = append(s.forceRandom, v)
	if len(s.forceRandom) > maxHist {
		s.forceRandom = s.forceRandom[len(s.forceRandom)-maxHist:]
	}
}

func indexOfMedianDuration(s []sample) int {
	n := len(s)
	idxs := make([]int, n)
	for i := 0; i < n; i++ {
		idxs[i] = i
	}
	sort.Slice(idxs, func(i, j int) bool { return s[idxs[i]].rtt < s[idxs[j]].rtt })
	return idxs[n/2]
}

// StartPeriodicRunner starts a background goroutine that periodically exchanges with
// the provided peers and updates local coordinates. It returns a stop function.
func (s *Service) StartPeriodicRunner(peers []peerpkg.ID, cfg UpdateConfig) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for _, p := range peers {
					ctx, cancel := context.WithTimeout(context.Background(), cfg.Interval)
					_, _ = s.ExchangeAndUpdate(ctx, p, cfg)
					cancel()
				}
			}
		}
	}()
	return func() { close(done) }
}
