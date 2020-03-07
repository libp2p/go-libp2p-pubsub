package pubsub

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type PeerScoreParams struct {
	// Score parameters per topic.
	Topics map[string]*TopicScoreParams

	// P5: Application-specific peer scoring
	AppSpecificScore  func(p peer.ID) float64
	AppSpecificWeight float64

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
	TimeInMeshCap     float64

	// P2: first message deliveries
	// This is the number of message deliveries in the topic.
	// The value of the parameter is a counter, decaying with FirstMessageDeliveriesDecay, and capped
	// by FirstMessageDeliveriesCap.
	// The weight of the parameter MUST be positive.
	FirstMessageDeliveriesWeight, FirstMessageDeliveriesDecay float64
	FirstMessageDeliveriesCap                                 float64

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
	MeshMessageDeliveriesThreshold                                float64
	MeshMessageDeliveriesLatency, MeshMessageDeliveriesActivation time.Duration

	// P4: invalid messages
	// This is the number of invalid messages in the topic.
	// The value of the parameter is a counter, decaying with InvalidMessageDeliveriesDecay.
	// The weight of the parameter MUST be negative.
	InvalidMessageDeliveriesWeight, InvalidMessageDeliveriesDecay float64
}

type peerStats struct {
	// true if the peer is currently connected
	connected bool

	// expiration time of the score stats for disconnected peers
	expire time.Time

	// per topc stats
	topics map[string]*topicStats

	// IP tracking; store as string for easy processing
	ips []string
}

type topicStats struct {
	// true if the peer is in the mesh
	inMesh bool

	// time when the peer was (last) GRAFTed; valid only when in mesh
	graftTime time.Time

	// time in mesh (updated during refresh/decay to avoid calling gettimeofday on
	// every score invocation)
	meshTime time.Duration

	// first message deliveries
	firstMessageDeliveries float64

	// mesh message deliveries
	meshMessageDeliveries float64

	// true if the peer has been enough time in the mesh to activate mess message deliveries
	meshMessageDeliveriesActive bool

	// invalid message counter
	invalidMessageDeliveries float64
}

type peerScore struct {
	sync.Mutex

	// the score parameters
	params *PeerScoreParams

	// per peer stats for score calculation
	peerStats map[peer.ID]*peerStats

	// IP colocation tracking
	peerIPs map[string]peer.ID
}

func newPeerScore(params *PeerScoreParams) *peerScore {
	return nil
}

// router interface
func (ps *peerScore) Start(gs *GossipSubRouter) {

}

func (ps *peerScore) Score(p peer.ID) float64 {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return 0
	}

	var score float64

	// topic scores
	for topic, tstats := range pstats.topics {
		// the topic score
		var topicScore float64

		// the topic parameters
		topicParams, ok := ps.params.Topics[topic]
		if !ok {
			// we are not scoring this topic
			continue
		}

		// P1: time in Mesh
		if tstats.inMesh {
			p1 := float64(tstats.meshTime / topicParams.TimeInMeshQuantum)
			topicScore += p1 * topicParams.TimeInMeshWeight
		}

		// P2: first message deliveries
		p2 := tstats.firstMessageDeliveries
		topicScore += p2 * topicParams.FirstMessageDeliveriesWeight

		// P3: mesh message deliveries
		if tstats.meshMessageDeliveriesActive {
			if tstats.meshMessageDeliveries < topicParams.MeshMessageDeliveriesThreshold {
				deficit := topicParams.MeshMessageDeliveriesThreshold - tstats.meshMessageDeliveries
				p3 := deficit * deficit
				topicScore += p3 * topicParams.MeshMessageDeliveriesWeight
			}
		}

		// P4: invalid messages
		p4 := tstats.invalidMessageDeliveries
		topicScore += p4 * topicParams.InvalidMessageDeliveriesWeight

		// update score, mixing with topic weight
		score += topicScore * topicParams.TopicWeight
	}

	// P5: application-specific score
	p5 := ps.params.AppSpecificScore(p)
	score += p5 * ps.params.AppSpecificWeight

	// P6: IP collocation factor
	for _, ip := range pstats.ips {
		peersInIP := len(ps.peerIPs[ip])
		if peersInIP > ps.params.IPColocationFactorThreshold {
			surpluss := float64(peersInIP - ps.params.IPColocationFactorThreshold)
			p6 := surpluss * surpluss
			score += p6 * ps.params.IPColocationFactorWeight
		}
	}

	return score
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
