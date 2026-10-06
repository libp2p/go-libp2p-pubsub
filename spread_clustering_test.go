package pubsub

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSplitIntoEqualRings(t *testing.T) {
	peers := []peer.ID{"p1", "p2", "p3", "p4", "p5"}
	rings := splitIntoEqualRings(peers, 3)
	if len(rings) != 3 {
		t.Fatalf("expected 3 rings, got %d", len(rings))
	}
	if got := len(rings[0]); got != 2 {
		t.Fatalf("expected first ring size 2, got %d", got)
	}
	if got := len(rings[1]); got != 2 {
		t.Fatalf("expected second ring size 2, got %d", got)
	}
	if got := len(rings[2]); got != 1 {
		t.Fatalf("expected third ring size 1, got %d", got)
	}
	flat := flattenRings(rings)
	if len(flat) != len(peers) {
		t.Fatalf("expected flattened len %d, got %d", len(peers), len(flat))
	}
	for i := range peers {
		if flat[i] != peers[i] {
			t.Fatalf("unexpected flatten order at %d: expected %s got %s", i, peers[i], flat[i])
		}
	}
}

func TestSanitizeSpreadClusteringConfig(t *testing.T) {
	cfg := sanitizeSpreadClusteringConfig(&SpreadClusteringConfig{
		ClusterPct: 0.4,
		NumRings:   5,
	})
	if cfg.ClusterPct != 0.4 {
		t.Fatalf("expected cluster pct 0.4, got %f", cfg.ClusterPct)
	}
	if cfg.NumRings != 5 {
		t.Fatalf("expected num rings 5, got %d", cfg.NumRings)
	}

	cfg = sanitizeSpreadClusteringConfig(&SpreadClusteringConfig{
		ClusterPct: 2,
		NumRings:   -1,
	})
	if cfg.ClusterPct != DefaultSpreadClusterPct {
		t.Fatalf("expected default cluster pct %f, got %f", DefaultSpreadClusterPct, cfg.ClusterPct)
	}
	if cfg.NumRings != DefaultSpreadNumRings {
		t.Fatalf("expected default num rings %d, got %d", DefaultSpreadNumRings, cfg.NumRings)
	}
}

func TestSpreadPropagationSkipsEmptyPeerID(t *testing.T) {
	sp := NewSpreadPropagation(&SpreadConfig{
		IntraFanout: 1,
		InterFanout: 0,
		IntraRho:    1,
		InterProb:   0,
	})
	got := sp.GetPeersForPropagation("from", nil, nil)
	if len(got) != 0 {
		t.Fatalf("expected no peers, got %v", got)
	}
}

func TestSanitizeSpreadConfigDuplicateRepropagation(t *testing.T) {
	cfg := sanitizeSpreadConfig(&SpreadConfig{
		DuplicateRepropagation: 3,
	})
	if cfg.DuplicateRepropagation != 3 {
		t.Fatalf("expected duplicate repropagation 3, got %d", cfg.DuplicateRepropagation)
	}

	cfg = sanitizeSpreadConfig(nil)
	if cfg.DuplicateRepropagation != DefaultSpreadDuplicateRepropagation {
		t.Fatalf("expected default duplicate repropagation %d, got %d", DefaultSpreadDuplicateRepropagation, cfg.DuplicateRepropagation)
	}
}
