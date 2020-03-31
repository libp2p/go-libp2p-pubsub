package pubsub

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	manet "github.com/multiformats/go-multiaddr-net"
)

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

	// sticky mesh rate failure penalty counter
	meshFailurePenalty float64

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
	host  host.Host

	// debugging inspection
	inspect       PeerScoreInspectFn
	inspectPeriod time.Duration
}

type messageDeliveries struct {
	records map[string]*deliveryRecord

	// queue for cleaning up old delivery records
	head *deliveryEntry
	tail *deliveryEntry
}

type deliveryRecord struct {
	status    int
	validated time.Time
	peers     map[peer.ID]struct{}
}

type deliveryEntry struct {
	id     string
	expire time.Time
	next   *deliveryEntry
}

// delivery record status
const (
	deliveryUnknown   = iota // we don't know (yet) if the message is valid
	deliveryValid            // we know the message is valid
	deliveryInvalid          // we know the message is invalid
	deliveryThrottled        // we can't tell if it is valid because validation throttled
)

type PeerScoreInspectFn func(map[peer.ID]float64)

// WithPeerScoreInspect is a gossipsub router option that enables peer score debugging.
// When this option is enabled, the supplied function will be invoked periodically to allow
// the application to inspec or dump the scores for connected peers.
// This option must be passed _after_ the WithPeerScore option.
func WithPeerScoreInspect(inspect PeerScoreInspectFn, period time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		if gs.score == nil {
			return fmt.Errorf("peer scoring is not enabled")
		}

		gs.score.inspect = inspect
		gs.score.inspectPeriod = period

		return nil
	}
}

// implementation
func newPeerScore(params *PeerScoreParams) *peerScore {
	return &peerScore{
		params:     params,
		peerStats:  make(map[peer.ID]*peerStats),
		peerIPs:    make(map[string]map[peer.ID]struct{}),
		deliveries: &messageDeliveries{records: make(map[string]*deliveryRecord)},
		msgID:      DefaultMsgIdFn,
	}
}

// router interface
func (ps *peerScore) Start(gs *GossipSubRouter) {
	if ps == nil {
		return
	}

	ps.msgID = gs.p.msgID
	ps.host = gs.p.host
	go ps.background(gs.p.ctx)
}

func (ps *peerScore) Score(p peer.ID) float64 {
	if ps == nil {
		return 0
	}

	ps.Lock()
	defer ps.Unlock()

	return ps.score(p)
}

func (ps *peerScore) score(p peer.ID) float64 {
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

		// P3b:
		p3b := tstats.meshFailurePenalty
		topicScore += p3b * topicParams.MeshFailurePenaltyWeight

		// P4: invalid messages
		p4 := tstats.invalidMessageDeliveries
		topicScore += p4 * topicParams.InvalidMessageDeliveriesWeight

		// update score, mixing with topic weight
		score += topicScore * topicParams.TopicWeight
	}

	// apply the topic score cap, if any
	if ps.params.TopicScoreCap > 0 && score > ps.params.TopicScoreCap {
		score = ps.params.TopicScoreCap
	}

	// P5: application-specific score
	p5 := ps.params.AppSpecificScore(p)
	score += p5 * ps.params.AppSpecificWeight

	// P6: IP collocation factor
	for _, ip := range pstats.ips {
		_, whitelisted := ps.params.IPColocationFactorWhitelist[ip]
		if whitelisted {
			continue
		}

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
func (ps *peerScore) background(ctx context.Context) {
	refreshScores := time.NewTicker(ps.params.DecayInterval)
	defer refreshScores.Stop()

	refreshIPs := time.NewTicker(time.Minute)
	defer refreshIPs.Stop()

	gcDeliveryRecords := time.NewTicker(time.Minute)
	defer gcDeliveryRecords.Stop()

	var inspectScores <-chan time.Time
	if ps.inspect != nil {
		ticker := time.NewTicker(ps.inspectPeriod)
		defer ticker.Stop()
		// also dump at exit for one final sample
		defer ps.inspectScores()
		inspectScores = ticker.C
	}

	for {
		select {
		case <-refreshScores.C:
			ps.refreshScores()

		case <-refreshIPs.C:
			ps.refreshIPs()

		case <-gcDeliveryRecords.C:
			ps.gcDeliveryRecords()

		case <-inspectScores:
			ps.inspectScores()

		case <-ctx.Done():
			return
		}
	}
}

func (ps *peerScore) inspectScores() {
	ps.Lock()
	scores := make(map[peer.ID]float64, len(ps.peerStats))
	for p := range ps.peerStats {
		scores[p] = ps.score(p)
	}
	ps.Unlock()

	ps.inspect(scores)
}

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
			tstats.meshFailurePenalty *= topicParams.MeshFailurePenaltyDecay
			if tstats.meshFailurePenalty < ps.params.DecayToZero {
				tstats.meshFailurePenalty = 0
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

func (ps *peerScore) refreshIPs() {
	ps.Lock()
	defer ps.Unlock()

	// peer IPs may change, so we periodically refresh them
	for p, pstats := range ps.peerStats {
		if pstats.connected {
			ips := ps.getIPs(p)
			ps.setIPs(p, ips, pstats.ips)
			pstats.ips = ips
		}
	}
}

func (ps *peerScore) gcDeliveryRecords() {
	ps.Lock()
	defer ps.Unlock()

	ps.deliveries.gc()
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
	ps.setIPs(p, ips, pstats.ips)
	pstats.ips = ips
}

func (ps *peerScore) RemovePeer(p peer.ID) {
	ps.Lock()
	defer ps.Unlock()

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	// decide whether to retain the score; this currently only retains non-positive scores
	// to dissuade attacks on the score function.
	if ps.score(p) > 0 {
		ps.removeIPs(p, pstats.ips)
		delete(ps.peerStats, p)
		return
	}

	// furthermore, when we decide to retain the score, the firstMessageDelivery counters are
	// reset to 0 and mesh delivery penalties applied.
	for topic, tstats := range pstats.topics {
		tstats.firstMessageDeliveries = 0

		threshold := ps.params.Topics[topic].MeshMessageDeliveriesThreshold
		if tstats.inMesh && tstats.meshMessageDeliveriesActive && tstats.meshMessageDeliveries < threshold {
			deficit := threshold - tstats.meshMessageDeliveries
			tstats.meshFailurePenalty += deficit * deficit
		}

		tstats.inMesh = false
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
	tstats.meshMessageDeliveriesActive = false
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

	// sticky mesh delivery rate failure penalty
	threshold := ps.params.Topics[topic].MeshMessageDeliveriesThreshold
	if tstats.meshMessageDeliveriesActive && tstats.meshMessageDeliveries < threshold {
		deficit := threshold - tstats.meshMessageDeliveries
		tstats.meshFailurePenalty += deficit * deficit
	}

	tstats.inMesh = false
}

func (ps *peerScore) ValidateMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	// the pubsub subsystem is beginning validation; create a record to track time in
	// the validation pipeline with an accurate firstSeen time.
	_ = ps.deliveries.getRecord(ps.msgID(msg.Message))
}

func (ps *peerScore) DeliverMessage(msg *Message) {
	ps.Lock()
	defer ps.Unlock()

	ps.markFirstMessageDelivery(msg.ReceivedFrom, msg)

	drec := ps.deliveries.getRecord(ps.msgID(msg.Message))

	// mark the message as valid and reward mesh peers that have already forwarded it to us
	drec.status = deliveryValid
	drec.validated = time.Now()
	for p := range drec.peers {
		// this check is to make sure a peer can't send us a message twice and get a double count
		// if it is a first delivery.
		if p != msg.ReceivedFrom {
			ps.markDuplicateMessageDelivery(p, msg, time.Time{})
		}
	}
}

func (ps *peerScore) RejectMessage(msg *Message, reason string) {
	ps.Lock()
	defer ps.Unlock()

	switch reason {
	// we don't track those messages, but we penalize the peer as they are clearly invalid
	case rejectMissingSignature:
		fallthrough
	case rejectInvalidSignature:
		fallthrough
	case rejectSelfOrigin:
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)
		return

		// we ignore those messages, so do nothing.
	case rejectBlacklstedPeer:
		fallthrough
	case rejectBlacklistedSource:
		return

	case rejectValidationQueueFull:
		// the message was rejected before it entered the validation pipeline;
		// we don't know if this message has a valid signature, and thus we also don't know if
		// it has a valid message ID; all we can do is ignore it.
		return
	}

	drec := ps.deliveries.getRecord(ps.msgID(msg.Message))

	if reason == rejectValidationThrottled {
		// if we reject with "validation throttled" we don't penalize the peer(s) that forward it
		// because we don't know if it was valid.
		drec.status = deliveryThrottled
		// release the delivery time tracking map to free some memory early
		drec.peers = nil
		return
	}

	// mark the message as invalid and penalize peers that have already forwarded it.
	drec.status = deliveryInvalid
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
	case deliveryUnknown:
		// the message is being validated; track the peer delivery and wait for
		// the Deliver/Reject notification.
		drec.peers[msg.ReceivedFrom] = struct{}{}

	case deliveryValid:
		// mark the peer delivery time to only count a duplicate delivery once.
		drec.peers[msg.ReceivedFrom] = struct{}{}
		ps.markDuplicateMessageDelivery(msg.ReceivedFrom, msg, drec.validated)

	case deliveryInvalid:
		// we no longer track delivery time
		ps.markInvalidMessageDelivery(msg.ReceivedFrom, msg)

	case deliveryThrottled:
		// the message was throttled; do nothing (we don't know if it was valid)
	}
}

// message delivery records
func (d *messageDeliveries) getRecord(id string) *deliveryRecord {
	rec, ok := d.records[id]
	if ok {
		return rec
	}

	rec = &deliveryRecord{peers: make(map[peer.ID]struct{})}
	d.records[id] = rec

	entry := &deliveryEntry{id: id, expire: time.Now().Add(TimeCacheDuration)}
	if d.tail != nil {
		d.tail.next = entry
		d.tail = entry
	} else {
		d.head = entry
		d.tail = entry
	}

	return rec
}

func (d *messageDeliveries) gc() {
	if d.head == nil {
		return
	}

	now := time.Now()
	for d.head != nil && now.After(d.head.expire) {
		delete(d.records, d.head.id)
		d.head = d.head.next
	}

	if d.head == nil {
		d.tail = nil
	}
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

func (ps *peerScore) markDuplicateMessageDelivery(p peer.ID, msg *Message, validated time.Time) {
	var now time.Time

	pstats, ok := ps.peerStats[p]
	if !ok {
		return
	}

	if !validated.IsZero() {
		now = time.Now()
	}

	for _, topic := range msg.GetTopicIDs() {
		tstats, ok := pstats.getTopicStats(topic, ps.params)
		if !ok {
			continue
		}

		if !tstats.inMesh {
			continue
		}

		// check against the mesh delivery window -- if the validated time is passed as 0, then
		// the message was received before we finished validation and thus falls within the mesh
		// delivery window.
		if !validated.IsZero() && now.After(validated.Add(ps.params.Topics[topic].MeshMessageDeliveriesWindow)) {
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
	// in unit tests this can be nil
	if ps.host == nil {
		return nil
	}

	conns := ps.host.Network().ConnsToPeer(p)
	res := make([]string, 0, len(conns))
	for _, c := range conns {
		remote := c.RemoteMultiaddr()
		ip, err := manet.ToIP(remote)
		if err != nil {
			continue
		}

		if len(ip) == 4 {
			// IPv4 address
			ip4 := ip.String()
			res = append(res, ip4)
			continue
		}

		if len(ip) == 16 {
			// IPv6 address -- we add both the actual address and the /64 subnet
			ip6 := ip.String()
			res = append(res, ip6)

			ip6mask := ip.Mask(net.CIDRMask(64, 128)).String()
			res = append(res, ip6mask)
		}
	}

	return res
}

//  adds tracking for the new IPs in the list, and removes tracking from the obsolete ips.
func (ps *peerScore) setIPs(p peer.ID, newips, oldips []string) {
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