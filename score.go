package pubsub

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type PeerScoreParams struct {
	// Score parameters per topic.
	Topics map[string]*TopicScoreParams

	// P6: IP-colocation factor.
	// The parameter has an associated counter which counts the number of peers with the same IP.
	// If the number of peers in the same IP exceeds IPColocationFactorThreshold, then the value
	// is the square of the difference, ie (PeersInSameIP - IPColocationThreshold)^2.
	// If the number of peers in the same IP is less than the threshold, then the value is 0.
	// The weight of the parameter MUST be negative, unless you want to disable for testing.
	// Note: In order to simulate many IPs in a managable manner when testing, you can set the weight to 0
	//       thus disabling the IP colocation penalty.
	IPColocationFactorWeight    float64
	IPColocationFactorThreshold int

	// Application-specific peer scoring
	AppSpecificScore  func(p peer.ID) float64
	AppSpecificWeight float64

	// the decay interval for parameter counters.
	DecayInterval time.Duration

	// counter value below which it is considered 0.
	DecayToZero float64

	// time to remember counters for a disconnected peer.
	RetainScore time.Duration
}

type TopicScoreParams struct {
	// The weight of the topic.
	TopicWeight float64

	// P1: time in the mesh
	// This is the time the peer has ben grafted in the mesh.
	// The value of of the parameter is the time/TimeInMeshQuantum, capped by TimeInMeshCap
	// The weight of the parameter MUST be positive.
	TimeInMeshWeight  float64
	TimeInMeshQuantum time.Duration
	TimeInMeshCap     int

	// P2: first message deliveries
	// This is the number of message deliveries in the topic.
	// The value of the parameter is a counter, decaying with FirstMessageDeliveriesDecay, and capped
	// by FirstMessageDeliveriesCap.
	// The weight of the parameter MUST be positive.
	FirstMessageDeliveriesWeight, FirstMessageDeliveriesDecay float64
	FirstMessageDeliveriesCap                                 int

	// P3: mesh message deliveries
	// This is the number of message deliveries in the mesh, within the MeshMessageDeliveriesLatency of
	// the first message delivery.
	// The parameter has an associated counter, decaying with MessageMessageDeliveriesDecay.
	// If the counter exceeds the threshold, its value is 0.
	// If the counter is below the MeshMessageDeliveriesThreshold, the value is the square of
	// the deficit, ie (MessageDeliveriesThreshold - counter)^2
	// The penalty is only activated after MeshMessageDeliveriesActivation time in the mesh.
	// The weight of the parameter MUST be negative.
	MeshMessageDeliveriesWeight, MeshMessageDeliveriesDecay       float64
	MeshMessageDeliveriesThreshold                                int
	MeshMessageDeliveriesLatency, MeshMessageDeliveriesActivation time.Duration

	// P4: invalid messages
	// This is the number of invalid messages in the topic.
	// The value of the parameter is a counter, decaying with InvalidMessageDeliveriesDecay.
	// The weight of the parameter MUST be negative.
	InvalidMessageDeliveriesWeight, InvalidMessageDeliveriesDecay float64
}

type peerScore struct {
}

func newPeerScore(gs *GossipSubRouter, params *PeerScoreParams) *peerScore {
	return nil
}

// router interface
func (ps *peerScore) Score(p peer.ID) float64 {
	return 0
}

// tracer interface
func (ps *peerScore) AddPeer(p peer.ID, proto protocol.ID) {

}

func (ps *peerScore) RemovePeer(p peer.ID) {

}

func (ps *peerScore) Join(topic string) {

}

func (ps *peerScore) Leave(topic string) {

}

func (ps *peerScore) Graft(p peer.ID, topic string) {

}

func (ps *peerScore) Prune(p peer.ID, topic string) {

}

func (ps *peerScore) DeliverMessage(msg *Message) {

}

func (ps *peerScore) RejectMessage(msg *Message, reason string) {

}

func (ps *peerScore) DuplicateMessage(msg *Message) {

}
