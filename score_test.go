package pubsub

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestScoreTimeInMesh(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       0.5,
		TimeInMeshWeight:  1,
		TimeInMeshQuantum: time.Millisecond,
		TimeInMeshCap:     3600,
	}
	params.Topics[mytopic] = topicScoreParams

	peerA := peer.ID("A")

	// Peer score should start at 0
	ps := newPeerScore(params)
	ps.AddPeer(peerA, "myproto")

	aScore := ps.Score(peerA)
	if aScore != 0 {
		t.Fatal("expected score to start at zero")
	}

	// The time in mesh depends on how long the peer has been grafted
	ps.Graft(peerA, mytopic)
	elapsed := topicScoreParams.TimeInMeshQuantum * 40
	time.Sleep(elapsed)

	ps.refreshScores()
	aScore = ps.Score(peerA)
	expected := topicScoreParams.TopicWeight * topicScoreParams.TimeInMeshWeight * float64(elapsed/topicScoreParams.TimeInMeshQuantum)
	variance := 0.5
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}
}

func TestScoreTimeInMeshCap(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       0.5,
		TimeInMeshWeight:  1,
		TimeInMeshQuantum: time.Millisecond,
		TimeInMeshCap:     10,
	}

	params.Topics[mytopic] = topicScoreParams

	peerA := peer.ID("A")

	ps := newPeerScore(params)
	ps.AddPeer(peerA, "myproto")
	ps.Graft(peerA, mytopic)
	elapsed := topicScoreParams.TimeInMeshQuantum * 40
	time.Sleep(elapsed)

	// The time in mesh score has a cap
	ps.refreshScores()
	aScore := ps.Score(peerA)
	expected := topicScoreParams.TopicWeight * topicScoreParams.TimeInMeshWeight * topicScoreParams.TimeInMeshCap
	variance := 0.5
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}
}

func TestScoreFirstMessageDeliveries(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       1,
		FirstMessageDeliveriesWeight:  1,
		FirstMessageDeliveriesDecay: 1.0, // test without decay for now
		FirstMessageDeliveriesCap: 2000,
		TimeInMeshQuantum: time.Second,
	}

	params.Topics[mytopic] = topicScoreParams
	peerA := peer.ID("A")

	ps := newPeerScore(params)
	ps.AddPeer(peerA, "myproto")
	ps.Graft(peerA, mytopic)
	
	// deliver a bunch of messages from peer A
	nMessages := 100
	for i := 0; i < nMessages; i++ {
		pbMsg := makeTestMessage(i)
		pbMsg.TopicIDs = []string{mytopic}
		msg := Message{ReceivedFrom: peerA, Message: pbMsg}
		ps.ValidateMessage(&msg)
		ps.DeliverMessage(&msg)
	}

	ps.refreshScores()
	aScore := ps.Score(peerA)
	expected := topicScoreParams.TopicWeight * topicScoreParams.FirstMessageDeliveriesWeight * float64(nMessages)
	variance := 0.5
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}
}

func TestScoreFirstMessageDeliveriesCap(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       1,
		FirstMessageDeliveriesWeight:  1,
		FirstMessageDeliveriesDecay: 1.0, // test without decay for now
		FirstMessageDeliveriesCap: 50,
		TimeInMeshQuantum: time.Second,
	}

	params.Topics[mytopic] = topicScoreParams
	peerA := peer.ID("A")

	ps := newPeerScore(params)
	ps.AddPeer(peerA, "myproto")
	ps.Graft(peerA, mytopic)

	// deliver a bunch of messages from peer A
	nMessages := 100
	for i := 0; i < nMessages; i++ {
		pbMsg := makeTestMessage(i)
		pbMsg.TopicIDs = []string{mytopic}
		msg := Message{ReceivedFrom: peerA, Message: pbMsg}
		ps.ValidateMessage(&msg)
		ps.DeliverMessage(&msg)
	}

	ps.refreshScores()
	aScore := ps.Score(peerA)
	expected := topicScoreParams.TopicWeight * topicScoreParams.FirstMessageDeliveriesWeight * topicScoreParams.FirstMessageDeliveriesCap
	variance := 0.5
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}
}

func TestScoreFirstMessageDeliveriesDecay(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:                  1,
		FirstMessageDeliveriesWeight: 1,
		FirstMessageDeliveriesDecay:  0.9, // decay 10% per decay interval
		FirstMessageDeliveriesCap:    2000,
		TimeInMeshQuantum:            time.Second,
	}

	params.Topics[mytopic] = topicScoreParams
	peerA := peer.ID("A")

	ps := newPeerScore(params)
	ps.AddPeer(peerA, "myproto")
	ps.Graft(peerA, mytopic)

	// deliver a bunch of messages from peer A
	nMessages := 100
	for i := 0; i < nMessages; i++ {
		pbMsg := makeTestMessage(i)
		pbMsg.TopicIDs = []string{mytopic}
		msg := Message{ReceivedFrom: peerA, Message: pbMsg}
		ps.ValidateMessage(&msg)
		ps.DeliverMessage(&msg)
	}

	ps.refreshScores()
	aScore := ps.Score(peerA)
	expected := topicScoreParams.TopicWeight * topicScoreParams.FirstMessageDeliveriesWeight * topicScoreParams.FirstMessageDeliveriesDecay * float64(nMessages)
	variance := 0.1
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}

	// refreshing the scores applies the decay param
	decayIntervals := 10
	for i := 0; i < decayIntervals; i++ {
		ps.refreshScores()
		expected *= topicScoreParams.FirstMessageDeliveriesDecay
	}
	aScore = ps.Score(peerA)
	if !withinVariance(aScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", aScore, expected, variance*expected)
	}
}

func TestScoreMeshMessageDeliveries(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       1,
		MeshMessageDeliveriesWeight: -1,
		MeshMessageDeliveriesActivation: 500 * time.Millisecond,
		MeshMessageDeliveriesWindow: 10 * time.Millisecond,
		MeshMessageDeliveriesThreshold: 20,
		MeshMessageDeliveriesCap: 100,
		MeshMessageDeliveriesDecay: 1.0, // no decay for this test

		FirstMessageDeliveriesWeight: 0,
		TimeInMeshQuantum: time.Second,
	}

	params.Topics[mytopic] = topicScoreParams

	// peer A always delivers the message first.
	// peer B delivers next (within the delivery window).
	// peer C delivers outside the delivery window.
	// we expect peers A and B to have a score of zero, since all other parameter weights are zero.
	// Peer C should have a negative score.
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	peerC := peer.ID("C")
	peers := []peer.ID{peerA, peerB, peerC}

	ps := newPeerScore(params)
	for _, p := range peers {
		ps.AddPeer(p, "myproto")
		ps.Graft(p, mytopic)
	}

	// wait for half the activation time, and assert that nobody has been penalized yet for not delivering messages
	time.Sleep(topicScoreParams.MeshMessageDeliveriesActivation / time.Duration(2))
	ps.refreshScores()
	for _, p := range peers {
		score := ps.Score(p)
		if score < 0 {
			t.Fatalf("expected no mesh delivery penalty before activation time, got score %f", score)
		}
	}
	// wait the remainder of the activation time
	time.Sleep(topicScoreParams.MeshMessageDeliveriesActivation / time.Duration(2))

	// deliver a bunch of messages from peer A, with duplicates within the window from peer B,
	// and duplicates outside the window from peer C.
	nMessages := 100
	wg := sync.WaitGroup{}
	for i := 0; i < nMessages; i++ {
		pbMsg := makeTestMessage(i)
		pbMsg.TopicIDs = []string{mytopic}
		msg := Message{ReceivedFrom: peerA, Message: pbMsg}
		ps.ValidateMessage(&msg)
		ps.DeliverMessage(&msg)

		msg.ReceivedFrom = peerB
		ps.DuplicateMessage(&msg)

		// deliver duplicate from peerC after the window
		wg.Add(1)
		time.AfterFunc(topicScoreParams.MeshMessageDeliveriesWindow + (20 * time.Millisecond), func () {
			msg.ReceivedFrom = peerC
			ps.DuplicateMessage(&msg)
			wg.Done()
		})
	}
	wg.Wait()

	ps.refreshScores()
	aScore := ps.Score(peerA)
	bScore := ps.Score(peerB)
	cScore := ps.Score(peerC)
	if aScore < 0 {
		t.Fatalf("Expected non-negative score for peer A, got %f", aScore)
	}
	if bScore < 0 {
		t.Fatalf("Expected non-negative score for peer B, got %f", aScore)
	}

	// the penalty is the difference between the threshold and the actual mesh deliveries, squared.
	// since we didn't deliver anything, this is just the value of the threshold
	penalty := topicScoreParams.MeshMessageDeliveriesThreshold * topicScoreParams.MeshMessageDeliveriesThreshold
	expected := topicScoreParams.TopicWeight * topicScoreParams.MeshMessageDeliveriesWeight * penalty
	variance := 0.1
	if !withinVariance(cScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", cScore, expected, variance*expected)
	}
}

func TestScoreMeshFailurePenalty(t *testing.T) {
	// Create parameters with reasonable default values
	mytopic := "mytopic"
	params := &PeerScoreParams{
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		Topics:           make(map[string]*TopicScoreParams),
	}

	// the mesh failure penalty is applied when a peer is pruned while their
	// mesh deliveries are under the threshold.
	// for this test, we set the mesh delivery threshold, but set
	// MeshMessageDeliveriesWeight to zero, so the only affect on the score
	// is from the mesh failure penalty
	topicScoreParams := &TopicScoreParams{
		TopicWeight:       1,
		MeshFailurePenaltyWeight: -1,
		MeshFailurePenaltyDecay: 1.0,

		MeshMessageDeliveriesActivation: 0,
		MeshMessageDeliveriesWindow: 10 * time.Millisecond,
		MeshMessageDeliveriesThreshold: 20,
		MeshMessageDeliveriesCap: 100,
		MeshMessageDeliveriesDecay: 1.0,

		MeshMessageDeliveriesWeight: 0,
		FirstMessageDeliveriesWeight: 0,
		TimeInMeshQuantum: time.Second,
	}

	params.Topics[mytopic] = topicScoreParams

	peerA := peer.ID("A")
	peerB := peer.ID("B")
	peers := []peer.ID{peerA, peerB}

	ps := newPeerScore(params)
	for _, p := range peers {
		ps.AddPeer(p, "myproto")
		ps.Graft(p, mytopic)
	}

	// deliver messages from peer A. peer B does nothing
	nMessages := 100
	for i := 0; i < nMessages; i++ {
		pbMsg := makeTestMessage(i)
		pbMsg.TopicIDs = []string{mytopic}
		msg := Message{ReceivedFrom: peerA, Message: pbMsg}
		ps.ValidateMessage(&msg)
		ps.DeliverMessage(&msg)
	}

	// peers A and B should both have zero scores, since the failure penalty hasn't been applied yet
	ps.refreshScores()
	aScore := ps.Score(peerA)
	bScore := ps.Score(peerB)
	if aScore != 0 {
		t.Errorf("expected peer A to have score 0.0, got %f", aScore)
	}
	if bScore != 0 {
		t.Errorf("expected peer B to have score 0.0, got %f", bScore)
	}

	// prune peer B to apply the penalty
	ps.Prune(peerB, mytopic)
	ps.refreshScores()
	aScore = ps.Score(peerA)
	bScore = ps.Score(peerB)

	if aScore != 0 {
		t.Errorf("expected peer A to have score 0.0, got %f", aScore)
	}

	// penalty calculation is the same as for MeshMessageDeliveries, but multiplied by MeshFailurePenaltyWeight
	// instead of MeshMessageDeliveriesWeight
	penalty := topicScoreParams.MeshMessageDeliveriesThreshold * topicScoreParams.MeshMessageDeliveriesThreshold
	expected := topicScoreParams.TopicWeight * topicScoreParams.MeshFailurePenaltyWeight * penalty
	variance := 0.1
	if !withinVariance(bScore, expected, variance) {
		t.Fatalf("Score: %f. Expected %f ± %f", bScore, expected, variance*expected)
	}
}

func withinVariance(score float64, expected float64, variance float64) bool {
	if expected >= 0 {
		return score > expected*(1-variance) && score < expected*(1+variance)
	}
	return score > expected*(1+variance) && score < expected*(1-variance)
}
