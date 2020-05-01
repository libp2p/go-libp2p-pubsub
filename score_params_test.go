package pubsub

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestPeerScoreThresholdsValidation(t *testing.T) {
	if (&PeerScoreThresholds{GossipThreshold: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{PublishThreshold: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{GossipThreshold: -1, PublishThreshold: 0}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{GossipThreshold: -1, PublishThreshold: -2, GraylistThreshold: 0}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{AcceptPXThreshold: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{OpportunisticGraftThreshold: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{GossipThreshold: -1, PublishThreshold: -2, GraylistThreshold: -3, AcceptPXThreshold: 1, OpportunisticGraftThreshold: 2}).validate() != nil {
		t.Fatal("expected validation success")
	}
}

func TestTopicScoreParamsValidation(t *testing.T) {
	if (&TopicScoreParams{}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TopicWeight: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TimeInMeshWeight: -1, TimeInMeshQuantum: time.Second}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshWeight: 1, TimeInMeshQuantum: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshWeight: 1, TimeInMeshQuantum: time.Second, TimeInMeshCap: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, FirstMessageDeliveriesWeight: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, FirstMessageDeliveriesWeight: 1, FirstMessageDeliveriesDecay: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, FirstMessageDeliveriesWeight: 1, FirstMessageDeliveriesDecay: 2}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, FirstMessageDeliveriesWeight: 1, FirstMessageDeliveriesDecay: .5, FirstMessageDeliveriesCap: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: 2}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: .5, MeshMessageDeliveriesCap: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: .5, MeshMessageDeliveriesCap: 5, MeshMessageDeliveriesThreshold: -3}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: .5, MeshMessageDeliveriesCap: 5, MeshMessageDeliveriesThreshold: 3, MeshMessageDeliveriesWindow: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesDecay: .5, MeshMessageDeliveriesCap: 5, MeshMessageDeliveriesThreshold: 3, MeshMessageDeliveriesWindow: time.Millisecond, MeshMessageDeliveriesActivation: time.Millisecond}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshFailurePenaltyWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshFailurePenaltyWeight: -1, MeshFailurePenaltyDecay: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, MeshFailurePenaltyWeight: -1, MeshFailurePenaltyDecay: 2}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, InvalidMessageDeliveriesWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, InvalidMessageDeliveriesWeight: -1, InvalidMessageDeliveriesDecay: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{TimeInMeshQuantum: time.Second, InvalidMessageDeliveriesWeight: -1, InvalidMessageDeliveriesDecay: 2}).validate() == nil {
		t.Fatal("expected validation error")
	}

	// Don't use these params in production!
	if (&TopicScoreParams{
		TopicWeight:                     1,
		TimeInMeshWeight:                0.01,
		TimeInMeshQuantum:               time.Second,
		TimeInMeshCap:                   10,
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     0.5,
		FirstMessageDeliveriesCap:       10,
		MeshMessageDeliveriesWeight:     -1,
		MeshMessageDeliveriesDecay:      0.5,
		MeshMessageDeliveriesCap:        10,
		MeshMessageDeliveriesThreshold:  5,
		MeshMessageDeliveriesWindow:     time.Millisecond,
		MeshMessageDeliveriesActivation: time.Second,
		MeshFailurePenaltyWeight:        -1,
		MeshFailurePenaltyDecay:         0.5,
		InvalidMessageDeliveriesWeight:  -1,
		InvalidMessageDeliveriesDecay:   0.5,
	}).validate() != nil {
		t.Fatal("expected validation success")
	}
}

func TestPeerScoreParamsValidation(t *testing.T) {
	appScore := func(peer.ID) float64 { return 0 }

	if (&PeerScoreParams{TopicScoreCap: -1, AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, DecayInterval: time.Second, DecayToZero: 0.01}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01, IPColocationFactorWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01, IPColocationFactorWeight: -1, IPColocationFactorThreshold: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, AppSpecificScore: appScore, DecayInterval: time.Millisecond, DecayToZero: 0.01, IPColocationFactorWeight: -1, IPColocationFactorThreshold: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: -1, IPColocationFactorWeight: -1, IPColocationFactorThreshold: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{TopicScoreCap: 1, AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 2, IPColocationFactorWeight: -1, IPColocationFactorThreshold: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01, BehaviourPenaltyWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01, BehaviourPenaltyWeight: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{AppSpecificScore: appScore, DecayInterval: time.Second, DecayToZero: 0.01, BehaviourPenaltyWeight: -1, BehaviourPenaltyDecay: 2}).validate() == nil {
		t.Fatal("expected validation error")
	}

	// don't use these params in production!
	if (&PeerScoreParams{
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		BehaviourPenaltyWeight:      -1,
		BehaviourPenaltyDecay:       0.999,
	}).validate() != nil {
		t.Fatal("expected validation success")
	}

	if (&PeerScoreParams{
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		BehaviourPenaltyWeight:      -1,
		BehaviourPenaltyDecay:       0.999,
	}).validate() != nil {
		t.Fatal("expected validation success")
	}

	if (&PeerScoreParams{
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		Topics: map[string]*TopicScoreParams{
			"test": &TopicScoreParams{
				TopicWeight:                     1,
				TimeInMeshWeight:                0.01,
				TimeInMeshQuantum:               time.Second,
				TimeInMeshCap:                   10,
				FirstMessageDeliveriesWeight:    1,
				FirstMessageDeliveriesDecay:     0.5,
				FirstMessageDeliveriesCap:       10,
				MeshMessageDeliveriesWeight:     -1,
				MeshMessageDeliveriesDecay:      0.5,
				MeshMessageDeliveriesCap:        10,
				MeshMessageDeliveriesThreshold:  5,
				MeshMessageDeliveriesWindow:     time.Millisecond,
				MeshMessageDeliveriesActivation: time.Second,
				MeshFailurePenaltyWeight:        -1,
				MeshFailurePenaltyDecay:         0.5,
				InvalidMessageDeliveriesWeight:  -1,
				InvalidMessageDeliveriesDecay:   0.5,
			},
		},
	}).validate() != nil {
		t.Fatal("expected validation success")
	}

	// don't use these params in production!
	if (&PeerScoreParams{
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		Topics: map[string]*TopicScoreParams{
			"test": &TopicScoreParams{
				TopicWeight:                     -1,
				TimeInMeshWeight:                0.01,
				TimeInMeshQuantum:               time.Second,
				TimeInMeshCap:                   10,
				FirstMessageDeliveriesWeight:    1,
				FirstMessageDeliveriesDecay:     0.5,
				FirstMessageDeliveriesCap:       10,
				MeshMessageDeliveriesWeight:     -1,
				MeshMessageDeliveriesDecay:      0.5,
				MeshMessageDeliveriesCap:        10,
				MeshMessageDeliveriesThreshold:  5,
				MeshMessageDeliveriesWindow:     time.Millisecond,
				MeshMessageDeliveriesActivation: time.Second,
				MeshFailurePenaltyWeight:        -1,
				MeshFailurePenaltyDecay:         0.5,
				InvalidMessageDeliveriesWeight:  -1,
				InvalidMessageDeliveriesDecay:   0.5,
			},
		},
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}

}

func TestScoreParameterDecay(t *testing.T) {
	decay1hr := ScoreParameterDecay(time.Hour)
	if decay1hr != .9987216039048303 {
		t.Fatalf("expected .9987216039048303, got %f", decay1hr)
	}
}
