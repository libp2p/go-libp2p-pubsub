package pubsub

import (
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPeerScoreThreshold_AtomicValidation(t *testing.T) {
	testPeerScoreThresholdsValidation(t, false)
}

func TestPeerScoreThreshold_SkipAtomicValidation(t *testing.T) {
	testPeerScoreThresholdsValidation(t, true)
}

func testPeerScoreThresholdsValidation(t *testing.T, skipAtomicValidation bool) {
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation,
		GossipThreshold:      1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation,
		PublishThreshold:     1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation,
		GossipThreshold:      -1,
		PublishThreshold:     0,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation,
		GossipThreshold:      -1,
		PublishThreshold:     -2,
		GraylistThreshold:    0,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation: skipAtomicValidation,
		AcceptPXThreshold:    -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		OpportunisticGraftThreshold: -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             -1,
		PublishThreshold:            -2,
		GraylistThreshold:           -3,
		AcceptPXThreshold:           1,
		OpportunisticGraftThreshold: 2}).validate() != nil {
		t.Fatal("expected validation success")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             math.Inf(-1),
		PublishThreshold:            -2,
		GraylistThreshold:           -3,
		AcceptPXThreshold:           1,
		OpportunisticGraftThreshold: 2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             -1,
		PublishThreshold:            math.Inf(-1),
		GraylistThreshold:           -3,
		AcceptPXThreshold:           1,
		OpportunisticGraftThreshold: 2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             -1,
		PublishThreshold:            -2,
		GraylistThreshold:           math.Inf(-1),
		AcceptPXThreshold:           1,
		OpportunisticGraftThreshold: 2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             -1,
		PublishThreshold:            -2,
		GraylistThreshold:           -3,
		AcceptPXThreshold:           math.NaN(),
		OpportunisticGraftThreshold: 2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreThresholds{
		SkipAtomicValidation:        skipAtomicValidation,
		GossipThreshold:             -1,
		PublishThreshold:            -2,
		GraylistThreshold:           -3,
		AcceptPXThreshold:           1,
		OpportunisticGraftThreshold: math.Inf(0),
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
}

func TestTopicScoreParamsValidation_InvalidParams_AtomicValidation(t *testing.T) {
	testTopicScoreParamsValidationWithInvalidParameters(t, false)
}

func TestTopicScoreParamsValidation_InvalidParams_SkipAtomicValidation(t *testing.T) {
	testTopicScoreParamsValidationWithInvalidParameters(t, true)
}

func testTopicScoreParamsValidationWithInvalidParameters(t *testing.T, skipAtomicValidation bool) {

	if skipAtomicValidation {
		if (&TopicScoreParams{
			SkipAtomicValidation: true}).validate() != nil {
			t.Fatal("expected validation success")
		}
	} else {
		if (&TopicScoreParams{}).validate() == nil {
			t.Fatal("expected validation failure")
		}
	}

	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation,
		TopicWeight:          -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation,
		TimeInMeshWeight:     -1,
		TimeInMeshQuantum:    time.Second,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation,
		TimeInMeshWeight:     1,
		TimeInMeshQuantum:    -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation: skipAtomicValidation,
		TimeInMeshWeight:     1,
		TimeInMeshQuantum:    time.Second,
		TimeInMeshCap:        -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation,
		TimeInMeshQuantum:            time.Second,
		FirstMessageDeliveriesWeight: -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation,
		TimeInMeshQuantum:            time.Second,
		FirstMessageDeliveriesWeight: 1,
		FirstMessageDeliveriesDecay:  -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation,
		TimeInMeshQuantum:            time.Second,
		FirstMessageDeliveriesWeight: 1,
		FirstMessageDeliveriesDecay:  2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:         skipAtomicValidation,
		TimeInMeshQuantum:            time.Second,
		FirstMessageDeliveriesWeight: 1,
		FirstMessageDeliveriesDecay:  .5,
		FirstMessageDeliveriesCap:    -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TimeInMeshQuantum:           time.Second,
		MeshMessageDeliveriesWeight: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TimeInMeshQuantum:           time.Second,
		MeshMessageDeliveriesWeight: -1,
		MeshMessageDeliveriesDecay:  -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TimeInMeshQuantum:           time.Second,
		MeshMessageDeliveriesWeight: -1,
		MeshMessageDeliveriesDecay:  2}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TimeInMeshQuantum:           time.Second,
		MeshMessageDeliveriesWeight: -1,
		MeshMessageDeliveriesDecay:  .5,
		MeshMessageDeliveriesCap:    -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation,
		TimeInMeshQuantum:              time.Second,
		MeshMessageDeliveriesWeight:    -1,
		MeshMessageDeliveriesDecay:     .5,
		MeshMessageDeliveriesCap:       5,
		MeshMessageDeliveriesThreshold: -3,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation,
		TimeInMeshQuantum:              time.Second,
		MeshMessageDeliveriesWeight:    -1,
		MeshMessageDeliveriesDecay:     .5,
		MeshMessageDeliveriesCap:       5,
		MeshMessageDeliveriesThreshold: 3,
		MeshMessageDeliveriesWindow:    -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:            skipAtomicValidation,
		TimeInMeshQuantum:               time.Second,
		MeshMessageDeliveriesWeight:     -1,
		MeshMessageDeliveriesDecay:      .5,
		MeshMessageDeliveriesCap:        5,
		MeshMessageDeliveriesThreshold:  3,
		MeshMessageDeliveriesWindow:     time.Millisecond,
		MeshMessageDeliveriesActivation: time.Millisecond}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation,
		TimeInMeshQuantum:        time.Second,
		MeshFailurePenaltyWeight: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation,
		TimeInMeshQuantum:        time.Second,
		MeshFailurePenaltyWeight: -1,
		MeshFailurePenaltyDecay:  -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:     skipAtomicValidation,
		TimeInMeshQuantum:        time.Second,
		MeshFailurePenaltyWeight: -1,
		MeshFailurePenaltyDecay:  2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation,
		TimeInMeshQuantum:              time.Second,
		InvalidMessageDeliveriesWeight: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation,
		TimeInMeshQuantum:              time.Second,
		InvalidMessageDeliveriesWeight: -1,
		InvalidMessageDeliveriesDecay:  -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&TopicScoreParams{
		SkipAtomicValidation:           skipAtomicValidation,
		TimeInMeshQuantum:              time.Second,
		InvalidMessageDeliveriesWeight: -1,
		InvalidMessageDeliveriesDecay:  2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
}

func TestTopicScoreParamsValidation_ValidParams_AtomicValidation(t *testing.T) {
	// Don't use these params in production!
	if (&TopicScoreParams{
		SkipAtomicValidation:            false,
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

func TestTopicScoreParamsValidation_NonAtomicValidation(t *testing.T) {
	// Don't use these params in production!
	// In non-atomic (selective) validation mode, the subset of parameters passes
	// validation if the individual parameters values pass validation.
	p := &TopicScoreParams{}
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.SkipAtomicValidation = true
	})
	// including topic weight.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.TopicWeight = 1
	})
	// including time in mesh parameters.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.TimeInMeshWeight = 0.01
		params.TimeInMeshQuantum = time.Second
		params.TimeInMeshCap = 10
	})
	// including first message delivery parameters.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.FirstMessageDeliveriesWeight = 1
		params.FirstMessageDeliveriesDecay = 0.5
		params.FirstMessageDeliveriesCap = 10
	})
	// including mesh message delivery parameters.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.MeshMessageDeliveriesWeight = -1
		params.MeshMessageDeliveriesDecay = 0.5
		params.MeshMessageDeliveriesCap = 10
		params.MeshMessageDeliveriesThreshold = 5
		params.MeshMessageDeliveriesWindow = time.Millisecond
		params.MeshMessageDeliveriesActivation = time.Second
	})
	// including mesh failure penalty parameters.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.MeshFailurePenaltyWeight = -1
		params.MeshFailurePenaltyDecay = 0.5
	})
	// including invalid message delivery parameters.
	setTopicParamAndValidate(t, p, func(params *TopicScoreParams) {
		params.InvalidMessageDeliveriesWeight = -1
		params.InvalidMessageDeliveriesDecay = 0.5
	})
}

func TestPeerScoreParamsValidation_InvalidParams_AtomicValidation(t *testing.T) {
	testPeerScoreParamsValidationWithInvalidParams(t, false)
}

func TestPeerScoreParamsValidation_InvalidParams_SkipAtomicValidation(t *testing.T) {
	testPeerScoreParamsValidationWithInvalidParams(t, true)
}

func testPeerScoreParamsValidationWithInvalidParams(t *testing.T, skipAtomicValidation bool) {
	appScore := func(peer.ID) float64 { return 0 }

	if (&PeerScoreParams{
		SkipAtomicValidation: skipAtomicValidation,
		TopicScoreCap:        -1,
		AppSpecificScore:     appScore,
		DecayInterval:        time.Second,
		DecayToZero:          0.01,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	if skipAtomicValidation {
		if (&PeerScoreParams{
			SkipAtomicValidation: skipAtomicValidation,
			TopicScoreCap:        1,
			DecayInterval:        time.Second,
			DecayToZero:          0.01,
		}).validate() != nil {
			t.Fatal("expected validation success")
		}
	} else {
		if (&PeerScoreParams{
			SkipAtomicValidation: skipAtomicValidation,
			TopicScoreCap:        1,
			DecayInterval:        time.Second,
			DecayToZero:          0.01,
		}).validate() == nil {
			t.Fatal("expected validation error")
		}
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:     skipAtomicValidation,
		TopicScoreCap:            1,
		AppSpecificScore:         appScore,
		DecayInterval:            time.Second,
		DecayToZero:              0.01,
		IPColocationFactorWeight: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: -1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Millisecond,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 -1,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 2,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation,
		AppSpecificScore:       appScore,
		DecayInterval:          time.Second,
		DecayToZero:            0.01,
		BehaviourPenaltyWeight: 1}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation,
		AppSpecificScore:       appScore,
		DecayInterval:          time.Second,
		DecayToZero:            0.01,
		BehaviourPenaltyWeight: -1,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}
	if (&PeerScoreParams{
		SkipAtomicValidation:   skipAtomicValidation,
		AppSpecificScore:       appScore,
		DecayInterval:          time.Second,
		DecayToZero:            0.01,
		BehaviourPenaltyWeight: -1,
		BehaviourPenaltyDecay:  2,
	}).validate() == nil {
		t.Fatal("expected validation error")
	}

	// Checks the topic parameters for invalid values such as infinite and
	// NaN numbers.
	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		Topics: map[string]*TopicScoreParams{
			"test": {
				TopicWeight:                     math.Inf(0),
				TimeInMeshWeight:                math.NaN(),
				TimeInMeshQuantum:               time.Second,
				TimeInMeshCap:                   10,
				FirstMessageDeliveriesWeight:    math.Inf(1),
				FirstMessageDeliveriesDecay:     0.5,
				FirstMessageDeliveriesCap:       10,
				MeshMessageDeliveriesWeight:     math.Inf(-1),
				MeshMessageDeliveriesDecay:      math.NaN(),
				MeshMessageDeliveriesCap:        math.Inf(0),
				MeshMessageDeliveriesThreshold:  5,
				MeshMessageDeliveriesWindow:     time.Millisecond,
				MeshMessageDeliveriesActivation: time.Second,
				MeshFailurePenaltyWeight:        -1,
				MeshFailurePenaltyDecay:         math.NaN(),
				InvalidMessageDeliveriesWeight:  math.Inf(0),
				InvalidMessageDeliveriesDecay:   math.NaN(),
			},
		},
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 math.Inf(0),
		IPColocationFactorWeight:    math.Inf(-1),
		IPColocationFactorThreshold: 1,
		BehaviourPenaltyWeight:      math.Inf(0),
		BehaviourPenaltyDecay:       math.NaN(),
	}).validate() == nil {
		t.Fatal("expected validation failure")
	}

	if (&PeerScoreParams{
		SkipAtomicValidation:        skipAtomicValidation,
		TopicScoreCap:               1,
		AppSpecificScore:            appScore,
		DecayInterval:               time.Second,
		DecayToZero:                 0.01,
		IPColocationFactorWeight:    -1,
		IPColocationFactorThreshold: 1,
		Topics: map[string]*TopicScoreParams{
			"test": {
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

func TestPeerScoreParamsValidation_ValidParams_AtomicValidation(t *testing.T) {
	appScore := func(peer.ID) float64 { return 0 }

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
			"test": {
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
}

func TestPeerScoreParamsValidation_ValidParams_SkipAtomicValidation(t *testing.T) {
	appScore := func(peer.ID) float64 { return 0 }

	// don't use these params in production!
	p := &PeerScoreParams{}
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.SkipAtomicValidation = true
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.AppSpecificScore = appScore
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.DecayInterval = time.Second
		params.DecayToZero = 0.01
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.IPColocationFactorWeight = -1
		params.IPColocationFactorThreshold = 1
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.BehaviourPenaltyWeight = -1
		params.BehaviourPenaltyDecay = 0.999
	})

	p = &PeerScoreParams{SkipAtomicValidation: true, AppSpecificScore: appScore}
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.TopicScoreCap = 1
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.DecayInterval = time.Second
		params.DecayToZero = 0.01
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.IPColocationFactorWeight = -1
		params.IPColocationFactorThreshold = 1
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.BehaviourPenaltyWeight = -1
		params.BehaviourPenaltyDecay = 0.999
	})
	setParamAndValidate(t, p, func(params *PeerScoreParams) {
		params.Topics = map[string]*TopicScoreParams{
			"test": {
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
		}
	})
}

func TestScoreParameterDecay(t *testing.T) {
	decay1hr := ScoreParameterDecay(time.Hour)
	if decay1hr != .9987216039048303 {
		t.Fatalf("expected .9987216039048303, got %f", decay1hr)
	}
}

func setParamAndValidate(t *testing.T, params *PeerScoreParams, set func(*PeerScoreParams)) {
	set(params)
	if err := params.validate(); err != nil {
		t.Fatalf("expected validation success, got: %s", err)
	}
}

func setTopicParamAndValidate(t *testing.T, params *TopicScoreParams, set func(topic *TopicScoreParams)) {
	set(params)
	if err := params.validate(); err != nil {
		t.Fatalf("expected validation success, got: %s", err)
	}
}
