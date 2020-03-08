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
	// This is the number of message deliveries in the mesh, within the MeshMessageDeliveriesWindow of
	// the first message delivery.
	// The parameter has an associated counter, decaying with MessageMessageDeliveriesDecay.
	// If the counter exceeds the threshold, its value is 0.
	// If the counter is below the MeshMessageDeliveriesThreshold, the value is the square of
	// the deficit, ie (MessageDeliveriesThreshold - counter)^2
	// The penalty is only activated after MeshMessageDeliveriesActivation time in the mesh.
	// The weight of the parameter MUST be negative.
	MeshMessageDeliveriesWeight, MeshMessageDeliveriesDecay      float64
	MeshMessageDeliveriesCap, MeshMessageDeliveriesThreshold     float64
	MeshMessageDeliveriesWindow, MeshMessageDeliveriesActivation time.Duration

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
	peerIPs map[string]map[peer.ID]struct{}

	// message delivery tracking
	deliveries *messageDeliveries

	msgID MsgIdFunction
}

type messageDeliveries struct {
	// TODO
}

type deliveryRecord struct {
	firstSeen time.Time
	status    int
	peers     map[peer.ID]time.Time
}

// delivery record status
const (
	delivery_unknown = iota // we don't know (yet) if the message is valid
	delivery_valid
	delivery_invalid
	delivery_throttled
)

func newPeerScore(params *PeerScoreParams) *peerScore {
	// TODO
	return nil
}

// router interface
func (ps *peerScore) Start(gs *GossipSubRouter) {
	// TODO
}

func (ps *peerScore) Score(p peer.ID) float64 {
	if ps == nil {
		return 0
	}

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
			if p1 > topicParams.TimeInMeshCap {
				p1 = topicParams.TimeInMeshCap
			}
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

// periodic maintenance
func (ps *peerScore) refreshScores() {
	ps.Lock()
	defer ps.Unlock()

	now := time.Now()
	for p, pstats := range ps.peerStats {
		if !pstats.connected {
			// has the retention period expired?
			if now.After(pstats.expire) {
				// yes, throw it away (but clean up the IP tracking first)
				ps.removeIPs(p, pstats.ips)
				delete(ps.peerStats, p)
			}

			// we don't decay retained scores, as the peer is not active.
			// this way the peer cannot reset a negative score by simply disconnecting and reconnecting,
			// unless the retention period has ellapsed.
			// similarly, a well behaved peer does not lose its score by getting disconnected.
			continue
		}

		for topic, tstats := range pstats.topics {
			// the topic parameters
			topicParams, ok := ps.params.Topics[topic]
			if !ok {
				// we are not scoring this topic
				continue
			}

			// decay counters
			tstats.firstMessageDeliveries *= topicParams.FirstMessageDeliveriesDecay
			if tstats.firstMessageDeliveries < ps.params.DecayToZero {
				tstats.firstMessageDeliveries = 0
			}
			tstats.meshMessageDeliveries *= topicParams.MeshMessageDeliveriesDecay
			if tstats.meshMessageDeliveries < ps.params.DecayToZero {
				tstats.meshMessageDeliveries = 0
			}
			tstats.invalidMessageDeliveries *= topicParams.InvalidMessageDeliveriesDecay
			if tstats.invalidMessageDeliveries < ps.params.DecayToZero {
				tstats.invalidMessageDeliveries = 0
			}
			// update mesh time and activate mesh message delivery parameter if need be
			if tstats.inMesh {
				tstats.meshTime = now.Sub(tstats.graftTime)
				if tstats.meshTime > topicParams.MeshMessageDeliveriesActivation {
					tstats.meshMessageDeliveriesActive = true
				}
			}
		}
	}
}

func (ps *peerScore) resfreshIPs() {
	// peer IPs may change, so we periodically refresh them
	// TODO
}

// tracer interface
func (ps *peerScore) AddPeer(p peer.ID, proto protocol.ID) {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		pstats = &peerStats{topics: make(map[string]*topicStats)}
		ps.peerStats[p] = pstats
	}

	pstats.connected = true
	ips := ps.getIPs(p)
	ps.addIPs(p, ips, pstats.ips)
	pstats.ips = ips
}

func (ps *peerScore) RemovePeer(p peer.ID) {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	pstats.connected = false
	pstats.expire = time.Now().Add(ps.params.RetainScore)
}

func (ps *peerScore) Join(topic string)  {}
func (ps *peerScore) Leave(topic string) {}

func (ps *peerScore) Graft(p peer.ID, topic string) {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	tstats.inMesh = true
	tstats.graftTime = time.Now()
	tstats.meshTime = 0
}

func (ps *peerScore) Prune(p peer.ID, topic string) {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	tstats, ok := pstats.getTopicStats(topic, ps.params)
	if !ok {
		return
	}

	tstats.inMesh = false
}

func (ps *peerScore) DeliverMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	ps.markFirstMessageDelivery(msg.ReceivedFrom, msg)

	drec := ps.deliveries.getRecord(ps.msgID(msg.Message))

	// mark the message as valid and reward mesh peers that have forwarded it in time
	drec.status = delivery_valid
	for p, t := range drec.peers {
		ps.markDuplicateMessageDelivery(p, msg, drec.firstSeen, t)
	}
}

func (ps *peerScore) RejectMessage(msg *Message, reason string) {
	ps.Lock()
	defer ps.Unlock()

	if reason == "invalid signature" {
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)
		// if we reject with "invalid signature" we don't track this message delivery.
		return
	}

	drec := ps.deliveries.getRecord(ps.msgID(msg.Message))

	if reason == "validation throttled" {
		// if we reject with "validation throttled" we don't penalize the peer(s) that forward it
		// because we don't know if it was valid.
		drec.status = delivery_throttled
		// release the delivery time tracking map to free some memory early
		drec.peers = nil
		return
	}

	// mark the message as invalid and penalize peers that have already forwarded it.
	drec.status = delivery_invalid
	for p := range drec.peers {
		ps.markInvalidMessageDelivery(p, msg)
	}

	// release the delivery time tracking map to free some memory early
	drec.peers = nil
}

func (ps *peerScore) DuplicateMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	drec := ps.deliveries.getRecord(ps.msgID(msg.Message))

	_, ok := drec.peers[msg.ReceivedFrom]
	if ok {
		// we have already seen this duplicate!
		return
	}

	switch drec.status {
	case delivery_unknown:
		// the message is being validated; track the peer delivery time and wait for
		// the Deliver/Reject notification.
		now := time.Now()
		drec.peers[msg.ReceivedFrom] = now

	case delivery_valid:
		// mark the peer delivery time to only count a duplicate delivery once.
		now := time.Now()
		drec.peers[msg.ReceivedFrom] = now
		ps.markDuplicateMessageDelivery(msg.ReceivedFrom, msg, drec.firstSeen, now)

	case delivery_invalid:
		// we no longer track delivery time
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)

	case delivery_throttled:
		// the message ws throttled; do nothing (we don't know if it was valid)
	}
}

// message delivery records
func (d *messageDeliveries) getRecord(id string) *deliveryRecord {
	// TODO
	return nil
}

// utilities
func (pstats *peerStats) getTopicStats(topic string, params *PeerScoreParams) (*topicStats, bool) {
	tstats, ok := pstats.topics[topic]
	if ok {
		return tstats, true
	}

	_, scoredTopic := params.Topics[topic]
	if !scoredTopic {
		return nil, false
	}

	tstats = &topicStats{}
	pstats.topics[topic] = tstats

	return tstats, true
}

func (ps *peerScore) markInvalidMessageDelivery(p peer.ID, msg *Message) {
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	for _, topic := range msg.GetTopicIDs() {
		tstats, ok := pstats.getTopicStats(topic, ps.params)
		if !ok {
			continue
		}

		tstats.invalidMessageDeliveries += 1
	}
}

func (ps *peerScore) markFirstMessageDelivery(p peer.ID, msg *Message) {
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	for _, topic := range msg.GetTopicIDs() {
		tstats, ok := pstats.getTopicStats(topic, ps.params)
		if !ok {
			continue
		}

		cap := ps.params.Topics[topic].FirstMessageDeliveriesCap
		tstats.firstMessageDeliveries += 1
		if tstats.firstMessageDeliveries > cap {
			tstats.firstMessageDeliveries = cap
		}

		if !tstats.inMesh {
			continue
		}

		cap = ps.params.Topics[topic].MeshMessageDeliveriesCap
		tstats.meshMessageDeliveries += 1
		if tstats.meshMessageDeliveries > cap {
			tstats.meshMessageDeliveries = cap
		}
	}
}

func (ps *peerScore) markDuplicateMessageDelivery(p peer.ID, msg *Message, firstSeen, deliveryTime time.Time) {
	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	for _, topic := range msg.GetTopicIDs() {
		tstats, ok := pstats.getTopicStats(topic, ps.params)
		if !ok {
			continue
		}

		if !tstats.inMesh {
			continue
		}

		// check against the mesh delivery window
		if deliveryTime.After(firstSeen.Add(ps.params.Topics[topic].MeshMessageDeliveriesWindow)) {
			continue
		}

		cap := ps.params.Topics[topic].MeshMessageDeliveriesCap
		tstats.meshMessageDeliveries += 1
		if tstats.meshMessageDeliveries > cap {
			tstats.meshMessageDeliveries = cap
		}
	}
}

// gets the current IPs for a peer
func (ps *peerScore) getIPs(p peer.ID) []string {
	// TODO
	return nil
}

// merges two IP lists, adds tracking for the new IPs in the list, and removes tracking
// from the obsolete ips.
func (ps *peerScore) addIPs(p peer.ID, newips, oldips []string) {
addNewIPs:
	// add the new IPs to the tracking
	for _, ip := range newips {
		// check if it is in the old ips list
		for _, xip := range oldips {
			if ip == xip {
				continue addNewIPs
			}
		}
		// no, it's a new one -- add it to the tracker
		peers, ok := ps.peerIPs[ip]
		if !ok {
			peers = make(map[peer.ID]struct{})
			ps.peerIPs[ip] = peers
		}
		peers[p] = struct{}{}
	}

removeOldIPs:
	// remove the obsolete old IPs from the tracking
	for _, ip := range oldips {
		// check if it is in the new ips list
		for _, xip := range newips {
			if ip == xip {
				continue removeOldIPs
			}
			// no, it's obsolete -- remove it from the tracker
			peers, ok := ps.peerIPs[ip]
			if !ok {
				continue
			}
			delete(peers, p)
			if len(peers) == 0 {
				delete(ps.peerIPs, ip)
			}
		}
	}
}

// removes an IP list from the tracking list
func (ps *peerScore) removeIPs(p peer.ID, ips []string) {
	for _, ip := range ips {
		peers, ok := ps.peerIPs[ip]
		if !ok {
			continue
		}

		delete(peers, p)
		if len(peers) == 0 {
			delete(ps.peerIPs, ip)
		}
	}
}
