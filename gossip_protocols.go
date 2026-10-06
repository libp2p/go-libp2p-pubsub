package pubsub

// Gossip protocol choices selectable via WithProtocolChoice. GOSSIPSUB is the
// default behaviour; SPREAD enables the SPREAD anonymous-gossip selection for
// messages marked with msg.Spread.
const (
	GOSSIPSUB = 1
	SPREAD    = 2
)

// GossipProtocolChoice selects which forwarding strategy the GossipSub router
// uses. See the GOSSIPSUB and SPREAD constants.
type GossipProtocolChoice int
