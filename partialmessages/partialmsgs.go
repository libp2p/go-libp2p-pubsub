package partialmessages

import (
	"errors"
	"iter"
	"log/slog"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

const minGroupTTL = 3

// defaultPeerInitiatedGroupLimitPerTopic limits the total number (per topic) of
// *partialMessageStatePerTopicGroup we create in response to a incoming RPC.
// This only applies to groups that we haven't published for yet.
const defaultPeerInitiatedGroupLimitPerTopic = 255

const defaultPeerInitiatedGroupLimitPerTopicPerPeer = 8

// PartsMetadata returns metadata about the parts this partial message
// contains and, possibly implicitly, the parts it wants.
type PartsMetadata []byte

type PeerInfo struct {
	// If RequestedPartialMessage is false, the peer does not want encoded
	// partial messages. The encoded message in a PublishAction MUST be
	// empty for this peer. The implementation SHOULD still send relevant
	// partsMetadataToSend.
	RequestedPartialMessage bool
}

type PublishAction struct {
	// EncodedPartialMessage is the encoded PartialMessage that will be sent to
	// this peer.
	EncodedPartialMessage []byte
	// EncodedPartsMetadata is the PartsMetadata that will be sent to this peer.
	EncodedPartsMetadata []byte

	// Err signals a publishing error that will be bubbled up to the caller of PublishPartial
	Err error
}

// Message is a message that can be broken up into parts. It can be
// complete, partially complete, or empty. It is up to the application to define
// how a message is split into parts and recombined, as well as how missing and
// available parts are represented.
type Message[PeerState any] interface {
	GroupID() []byte

	// PublishActions takes an iterator of [peer IDs, PeerInfo] and a map of
	// peerStates and returns an iterator of PublishActions describing what
	// messages to send to whom.
	//
	// The implementation should update the peer's peerState to track the parts
	// we've sent to this peer.
	//
	// A peer's peerState will be the zero value if this is the first time
	// interacting with this peer. Applications can still eagerly push data in
	// this case by returning the appropriate PublishAction.
	//
	// Implementations SHOULD avoid returning duplicate or redundant
	// partsMetadataToSend. i.e. if a previously sent PartsMetadata is up to
	// date, implementations SHOULD return `nil` for `partsMetadataToSend`.
	PublishActions(peers iter.Seq2[peer.ID, PeerInfo], peerStates map[peer.ID]PeerState) iter.Seq2[peer.ID, PublishAction]
}
type partialMessageStatePerGroupPerTopic[P any] struct {
	peerState   map[peer.ID]P
	groupTTL    int
	initiatedBy peer.ID // zero value if we initiated the group
}

func newPartialMessageStatePerTopicGroup[P any](groupTTL int) *partialMessageStatePerGroupPerTopic[P] {
	return &partialMessageStatePerGroupPerTopic[P]{
		peerState: make(map[peer.ID]P),
		groupTTL:  max(groupTTL, minGroupTTL),
	}
}

func (s *partialMessageStatePerGroupPerTopic[P]) remotePeerInitiated() bool {
	return s.initiatedBy != ""
}

func (s *partialMessageStatePerGroupPerTopic[P]) IsZero() bool {
	return len(s.peerState) == 0
}

type PartialMessagesExtension[PeerState any] struct {
	Logger *slog.Logger

	// GossipActions is analogous to Message.PublishActions, except its used when
	// emitting Gossipsub gossip to peers not in our mesh. Implementations
	// should update the peerStates following the same guidelines outlined
	// in Message.PublishActions's doc comment.
	GossipActions func(topic, groupID string, peers iter.Seq2[peer.ID, PeerInfo], peerStates map[peer.ID]PeerState) iter.Seq2[peer.ID, PublishAction]

	// OnIncomingRPC is called whenever we receive an encoded
	// partial message from a peer. This function MUST be fast and non-blocking.
	// If you need to do slow work (e.g. validation), dispatch the work to your
	// own goroutine.
	//
	// This function SHOULD update the peer's PeerState with the received
	// rpc.PartsMetadata and rpc.PartialMessage.
	//
	// An implementation may be able to infer some peer state from the
	// rpc.PartialMessage, for example:
	//  - peer's received state (we can infer they have a part if they have given it us)
	//  - our last update to a peer (they can infer an update if they have given us a part)
	//
	// If the rpc should be ignored, the application can leave peerStates unmodified
	OnIncomingRPC func(from peer.ID, peerStates map[peer.ID]PeerState, rpc *pb.PartialMessagesExtension) error

	// PeerInitiatedGroupLimitPerTopic limits the number of Group states all
	// peers can initialize per topic. A group state is initialized by a peer if
	// the peer's message marks the first time we've seen a group id.
	PeerInitiatedGroupLimitPerTopic int

	// PeerInitiatedGroupLimitPerTopicPerPeer limits the number of Group states
	// a single peer can initialize per topic. A group state is initialized by a
	// peer if the peer's message marks the first time we've seen a group id.
	PeerInitiatedGroupLimitPerTopicPerPeer int

	// GroupTTLByHeatbeat is how many heartbeats we store Group state for after
	// publishing a partial message for the group.
	GroupTTLByHeatbeat int

	// map topic -> map[group]partialMessageStatePerGroupPerTopic
	statePerTopicPerGroup map[string]map[string]*partialMessageStatePerGroupPerTopic[PeerState]

	// map[topic]counter
	peerInitiatedGroupCounter map[string]*peerInitiatedGroupCounterState

	router Router
}

type PublishOptions struct {
	// PublishToPeers limits the publishing to only the specified peers.
	// If nil, will use the topic's mesh peers.
	PublishToPeers []peer.ID
}

type Router interface {
	SendRPC(p peer.ID, r *pb.PartialMessagesExtension, urgent bool)
	MeshPeers(topic string) iter.Seq[peer.ID]
	PeerRequestsPartial(peer peer.ID, topic string) bool
}

func (e *PartialMessagesExtension[PeerState]) groupState(topic string, groupID []byte, peerInitiated bool, from peer.ID) (*partialMessageStatePerGroupPerTopic[PeerState], error) {
	statePerTopic, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		statePerTopic = make(map[string]*partialMessageStatePerGroupPerTopic[PeerState])
		e.statePerTopicPerGroup[topic] = statePerTopic
	}
	if _, ok := e.peerInitiatedGroupCounter[topic]; !ok {
		e.peerInitiatedGroupCounter[topic] = &peerInitiatedGroupCounterState{}
	}
	state, ok := statePerTopic[string(groupID)]
	if !ok {
		if peerInitiated {
			err := e.peerInitiatedGroupCounter[topic].Inc(e.PeerInitiatedGroupLimitPerTopic, e.PeerInitiatedGroupLimitPerTopicPerPeer, from)
			if err != nil {
				return nil, err
			}
		}

		state = newPartialMessageStatePerTopicGroup[PeerState](e.GroupTTLByHeatbeat)
		statePerTopic[string(groupID)] = state
		state.initiatedBy = from
	}
	if !peerInitiated && state.remotePeerInitiated() {
		// We've tried to initiate this state as well, so it's no longer peer initiated.
		e.peerInitiatedGroupCounter[topic].Dec(state.initiatedBy)
		state.initiatedBy = ""
	}
	return state, nil
}

func (e *PartialMessagesExtension[PeerState]) Init(router Router) error {
	e.router = router
	if e.Logger == nil {
		return errors.New("field Logger must be set")
	}
	if e.OnIncomingRPC == nil {
		return errors.New("field OnIncomingRPC must be set")
	}
	if e.GossipActions == nil {
		return errors.New("field GossipActions must be set")
	}

	if e.PeerInitiatedGroupLimitPerTopic == 0 {
		e.PeerInitiatedGroupLimitPerTopic = defaultPeerInitiatedGroupLimitPerTopic
	}
	if e.PeerInitiatedGroupLimitPerTopicPerPeer == 0 {
		e.PeerInitiatedGroupLimitPerTopicPerPeer = defaultPeerInitiatedGroupLimitPerTopicPerPeer
	}

	e.statePerTopicPerGroup = make(map[string]map[string]*partialMessageStatePerGroupPerTopic[PeerState])
	e.peerInitiatedGroupCounter = make(map[string]*peerInitiatedGroupCounterState)

	return nil
}

func (e *PartialMessagesExtension[PeerState]) addPeerInfo(topic string, peers []peer.ID) iter.Seq2[peer.ID, PeerInfo] {
	return func(yield func(peer.ID, PeerInfo) bool) {
		for _, p := range peers {
			requestedPartial := e.router.PeerRequestsPartial(p, topic)
			if !yield(p, PeerInfo{RequestedPartialMessage: requestedPartial}) {
				return
			}
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) publish(topic string, groupID []byte, actions iter.Seq2[peer.ID, PublishAction]) error {
	var err error
	for p, action := range actions {
		if action.Err != nil {
			err = errors.Join(err, action.Err)
			continue
		}

		if len(action.EncodedPartialMessage) > 0 || len(action.EncodedPartsMetadata) > 0 {
			var rpc pb.PartialMessagesExtension
			rpc.TopicID = &topic
			rpc.GroupID = groupID
			rpc.PartialMessage = action.EncodedPartialMessage
			rpc.PartsMetadata = action.EncodedPartsMetadata
			e.sendRPC(p, &rpc)
		}
	}
	return err
}

func (e *PartialMessagesExtension[PeerState]) PublishPartial(topic string, partial Message[PeerState], opts PublishOptions) error {
	groupID := partial.GroupID()

	state, err := e.groupState(topic, groupID, false, "")
	if err != nil {
		return err
	}

	state.groupTTL = max(e.GroupTTLByHeatbeat, minGroupTTL)

	var peers iter.Seq2[peer.ID, PeerInfo]
	if len(opts.PublishToPeers) > 0 {
		peers = e.addPeerInfo(topic, opts.PublishToPeers)
	} else {
		peers = e.peersToPublishTo(topic, state)
	}

	return e.publish(topic, groupID, partial.PublishActions(peers, state.peerState))
}

// peersToPublishTo returns a iter.Seq of peers to publish to. It combines peers
// in the group state with mesh peers for the topic.
// Group state peers are used to cover the fanout and gossip message cases where
// the peer would not be in our mesh.
func (e *PartialMessagesExtension[PeerState]) peersToPublishTo(topic string, state *partialMessageStatePerGroupPerTopic[PeerState]) iter.Seq2[peer.ID, PeerInfo] {
	return func(yield func(peer.ID, PeerInfo) bool) {
		for p := range state.peerState {
			requestedPartial := e.router.PeerRequestsPartial(p, topic)
			if !yield(p, PeerInfo{RequestedPartialMessage: requestedPartial}) {
				return
			}
		}
		for p := range e.router.MeshPeers(topic) {
			if _, ok := state.peerState[p]; !ok {
				requestedPartial := e.router.PeerRequestsPartial(p, topic)
				if !yield(p, PeerInfo{RequestedPartialMessage: requestedPartial}) {
					return
				}
			}
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) RemovePeer(id peer.ID) {
	for topic, statePerTopic := range e.statePerTopicPerGroup {
		for _, state := range statePerTopic {
			delete(state.peerState, id)
		}
		if ctr, ok := e.peerInitiatedGroupCounter[topic]; ok {
			ctr.RemovePeer(id)
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) Heartbeat() {
	for topic, statePerTopic := range e.statePerTopicPerGroup {
		for group, s := range statePerTopic {
			if s.groupTTL == 0 || s.IsZero() {
				delete(statePerTopic, group)
				if len(statePerTopic) == 0 {
					delete(e.statePerTopicPerGroup, topic)
				}
				if s.remotePeerInitiated() {
					e.peerInitiatedGroupCounter[topic].Dec(s.initiatedBy)
				}
			} else {
				s.groupTTL--
			}
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) EmitGossip(topic string, peers []peer.ID) {
	topicState, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		return
	}

	for group, s := range topicState {
		if s.remotePeerInitiated() {
			continue
		}
		peersWithInfo := e.addPeerInfo(topic, peers)
		err := e.publish(topic, []byte(group), e.GossipActions(topic, group, peersWithInfo, s.peerState))
		if err != nil {
			e.Logger.Error("Failed to publish gossip message", "topic", topic, "group", group, "error", err)
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) sendRPC(to peer.ID, rpc *pb.PartialMessagesExtension) {
	e.Logger.Debug("Sending RPC", "to", to, "rpc", rpc)
	e.router.SendRPC(to, rpc, false)
}

func (e *PartialMessagesExtension[PeerState]) HandleRPC(from peer.ID, rpc *pb.PartialMessagesExtension) error {
	if rpc == nil {
		return nil
	}

	e.Logger.Debug("Received RPC", "from", from, "rpc", rpc)
	topic := rpc.GetTopicID()
	groupID := rpc.GroupID

	state, err := e.groupState(topic, groupID, true, from)
	if err != nil {
		return err
	}

	err = e.OnIncomingRPC(from, state.peerState, rpc)
	if err != nil {
		return err
	}

	return nil
}

type peerInitiatedGroupCounterState struct {
	// total number of peer initiated groups
	total int
	// number of groups initiated per peer
	perPeer map[peer.ID]int
}

var errPeerInitiatedGroupTotalLimitReached = errors.New("too many peer initiated group states")
var errPeerInitiatedGroupLimitReached = errors.New("too many peer initiated group states for this peer")

func (ctr *peerInitiatedGroupCounterState) Inc(totalLimit int, peerLimit int, id peer.ID) error {
	if ctr.total >= totalLimit {
		return errPeerInitiatedGroupTotalLimitReached
	}
	if ctr.perPeer == nil {
		ctr.perPeer = make(map[peer.ID]int)
	}
	if ctr.perPeer[id] >= peerLimit {
		return errPeerInitiatedGroupLimitReached
	}
	ctr.total++
	ctr.perPeer[id]++
	return nil
}

func (ctr *peerInitiatedGroupCounterState) Dec(id peer.ID) {
	if _, ok := ctr.perPeer[id]; ok {
		ctr.total--
		ctr.perPeer[id]--
		if ctr.perPeer[id] == 0 {
			delete(ctr.perPeer, id)
		}
	}
}

func (ctr *peerInitiatedGroupCounterState) RemovePeer(id peer.ID) {
	if n, ok := ctr.perPeer[id]; ok {
		ctr.total -= n
		delete(ctr.perPeer, id)
	}
}
