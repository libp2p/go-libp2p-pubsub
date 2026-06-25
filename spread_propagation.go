package pubsub

import (
	"math/rand"

	"github.com/libp2p/go-libp2p/core/peer"
)

// SpreadConfig controls SPREAD peer selection: how many intra- and
// inter-cluster peers a message is forwarded to, and with what probability.
type SpreadConfig struct {
	// Intra-cluster fanout: number of intra-cluster peers to select
	IntraFanout int
	// Inter-cluster fanout: number of inter-cluster peers to select
	InterFanout int
	// Intra-cluster Cobra-walk rho: probability of selecting intra-cluster peers
	IntraRho float64
	// Inter-cluster communication probability: probability of selecting inter-cluster peers
	InterProb float64
	// Number of times a seen SPREAD message may be re-propagated as a duplicate.
	// This is the number of extra forwards after the first forward.
	DuplicateRepropagation int

	// UseAngularInterPeers toggles whether inter-cluster peers are drawn using
	// angular buckets from the spread clustering state instead of distance-only
	// ordering.
	UseAngularInterPeers bool
}

// Default configuration values
const (
	DefaultSpreadIntraFanout            = 3
	DefaultSpreadInterFanout            = 8
	DefaultSpreadIntraRho               = 0.6
	DefaultSpreadInterProb              = 0.8
	DefaultSpreadDuplicateRepropagation = 0
)

// SpreadPropagation selects the peers a SPREAD message is forwarded to, given
// the local cluster and inter-cluster peer sets computed by SpreadState.
type SpreadPropagation struct {
	config *SpreadConfig
}

// DefaultSpreadConfig returns a SpreadConfig populated with the package defaults.
func DefaultSpreadConfig() *SpreadConfig {
	return &SpreadConfig{
		IntraFanout:            DefaultSpreadIntraFanout,
		InterFanout:            DefaultSpreadInterFanout,
		IntraRho:               DefaultSpreadIntraRho,
		InterProb:              DefaultSpreadInterProb,
		DuplicateRepropagation: DefaultSpreadDuplicateRepropagation,
		UseAngularInterPeers:   false,
	}
}

func sanitizeSpreadConfig(cfg *SpreadConfig) *SpreadConfig {
	out := DefaultSpreadConfig()
	if cfg == nil {
		return out
	}
	if cfg.IntraFanout > 0 {
		out.IntraFanout = cfg.IntraFanout
	}
	if cfg.InterFanout > 0 {
		out.InterFanout = cfg.InterFanout
	}
	if cfg.IntraRho >= 0 && cfg.IntraRho <= 1 {
		out.IntraRho = cfg.IntraRho
	}
	if cfg.InterProb >= 0 && cfg.InterProb <= 1 {
		out.InterProb = cfg.InterProb
	}
	if cfg.DuplicateRepropagation >= 0 {
		out.DuplicateRepropagation = cfg.DuplicateRepropagation
	}
	out.UseAngularInterPeers = cfg.UseAngularInterPeers
	return out
}

// NewSpreadPropagation returns a SpreadPropagation using config, falling back
// to the package defaults for any unset or invalid field.
func NewSpreadPropagation(config *SpreadConfig) *SpreadPropagation {
	return &SpreadPropagation{
		config: sanitizeSpreadConfig(config),
	}
}

// GetPeersForPropagation returns the peers to forward a SPREAD message to,
// sampling intra- and inter-cluster peers according to the configured fanout
// and probabilities. The sender (from) is never included in the result.
func (sp *SpreadPropagation) GetPeersForPropagation(from peer.ID, clusterPeers []peer.ID, interClusterPeers []peer.ID) []peer.ID {

	selectedPeers := make(map[peer.ID]struct{})

	// Intra-cluster

	// Add mandatory cluster peer
	if mandatory := SelectRandomPeerID(clusterPeers); mandatory != "" {
		selectedPeers[mandatory] = struct{}{}
	}
	// Coin flip to decide whether to select more intra-cluster peers
	if rand.Float64() < sp.config.IntraRho {
		// Select [intra fanout - 1] more random ones
		for i := 0; i < sp.config.IntraFanout-1; i++ {
			peerID := SelectRandomPeerID(clusterPeers)
			if peerID == "" {
				break // No peers in the cluster
			}
			selectedPeers[peerID] = struct{}{}
		}
	}

	// Inter-cluster

	// Coin flip to decide whether to select any inter-cluster peers
	if rand.Float64() < sp.config.InterProb {
		// Select [inter fanout] random ones
		if len(interClusterPeers) > 0 {
			for i := 0; i < sp.config.InterFanout; i++ {
				peerID := SelectRandomPeerID(interClusterPeers)
				if peerID == "" {
					break // No peers in the cluster
				}
				selectedPeers[peerID] = struct{}{}
			}
		}
	}

	// Convert map to slice
	forwardingPeers := make([]peer.ID, 0, len(selectedPeers))
	for peerID := range selectedPeers {
		// don't send to sender
		if peerID == from {
			continue
		}
		forwardingPeers = append(forwardingPeers, peerID)
	}

	return forwardingPeers
}

// SelectRandomPeerID returns a uniformly random peer from peers, or the empty
// peer.ID if peers is empty.
func SelectRandomPeerID(peers []peer.ID) peer.ID {
	if len(peers) == 0 {
		return ""
	}
	return peers[rand.Intn(len(peers))]
}
