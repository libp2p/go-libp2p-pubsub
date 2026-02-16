package partialmessages

import (
	"errors"
	"iter"
	"log/slog"
	"slices"

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

type IsZeroer interface {
	IsZero() bool
}

// Message is a message that can be broken up into parts. It can be
// complete, partially complete, or empty. It is up to the application to define
// how a message is split into parts and recombined, as well as how missing and
// available parts are represented.
type Message[PeerState IsZeroer] interface {
	GroupID() []byte

	// ForPeer takes the remote's peer ID, if the peer requested partial
	// message data, and its peerState and returns an encoded partial message that
	// fulfills some or all of the request.
	//
	// If requestedMessage is false, the returned encoded message MUST be empty,
	// as the peer does not want encoded partial messages. The implementation
	// SHOULD still return relevant partsMetadataToSend.
	//
	// The implementation should return an updated PeerState to track:
	// - what parts we've sent to this peer.
	// - what parts we've sent.
	//
	// peerState will be the zero value if this is the first time interacting
	// with this peer. Applications can still eagerly push data in this case.
	//
	// The returned encodedMsg and partsMetadataToSend are sent to the peer. If
	// either is nil, that field will not be set. If both are nil, no message is
	// sent to the peer.
	//
	// Implementations SHOULD avoid returning duplicate or redundant
	// partsMetadataToSend. i.e. if a previously sent PartsMetadata is up to
	// date, implementations SHOULD return `nil` for `partsMetadataToSend`.
	ForPeer(remote peer.ID, requestedMessage bool, peerState PeerState) (next PeerState, encodedMsg []byte, partsMetadataToSend PartsMetadata, _ error)
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

type PartialMessagesExtension[PeerState IsZeroer] struct {
	Logger *slog.Logger

	// GossipForPeer is analogous to Message.ForPeer, except its used when
	// emitting Gossipsub gossip to peers not in our mesh. Implementations
	// should return an updated PeerState following the same guidelines outlined
	// in Message.ForPeer's doc comment.
	GossipForPeer func(topic string, groupID string, remote peer.ID, peerState PeerState) (next PeerState, encodedMsg []byte, partsMetadataToSend PartsMetadata, _ error)

	// OnIncomingRPC is called whenever we receive an encoded
	// partial message from a peer. This function MUST be fast and non-blocking.
	// If you need to do slow work (e.g. validation), dispatch the work to your
	// own goroutine.
	//
	// This function MUST return an updated PeerState updated with the received
	// rpc.PartsMetadata and rpc.PartialMessage.
	//
	// In particular it SHOULD use the rpc.PartialMessage contents to:
	//  - update PeerState.RecvdState (we can infer they have a part if they have given it us)
	//  - update PeerState.SentState (they can infer an update if they have given us a part)
	//
	// If PublishPartial is called within this callback, the ForPeer methods
	// will not see the latest PeerState.
	//
	// If the rpc should be ignored, the application can return an empty PeerState.
	OnIncomingRPC func(from peer.ID, peerState PeerState, rpc *pb.PartialMessagesExtension) (PeerState, error)

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
	if e.GossipForPeer == nil {
		return errors.New("field GossipForPeer must be set")
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

func (e *PartialMessagesExtension[PeerState]) PublishPartial(topic string, partial Message[PeerState], opts PublishOptions) error {
	groupID := partial.GroupID()

	state, err := e.groupState(topic, groupID, false, "")
	if err != nil {
		return err
	}

	state.groupTTL = max(e.GroupTTLByHeatbeat, minGroupTTL)

	var peers iter.Seq[peer.ID]
	if len(opts.PublishToPeers) > 0 {
		peers = slices.Values(opts.PublishToPeers)
	} else {
		peers = e.peersToPublishTo(topic, state)
	}

	for p := range peers {
		log := e.Logger.With("peer", p)
		requestedPartial := e.router.PeerRequestsPartial(p, topic)

		pState := state.peerState[p]
		var rpc pb.PartialMessagesExtension
		var sendRPC bool

		// Ask the application for message data
		pState, pm, partsMetadata, err := partial.ForPeer(p, requestedPartial, pState)
		if err != nil {
			log.Warn("partial message extension failed to get partial message bytes", "error", err)
			// Possibly a bad request, we'll delete the request as we will
			// likely error next time we try to handle it
			delete(state.peerState, p)
			continue
		}
		// If the application cleared the peerState, we don't need to track it anymore
		if pState.IsZero() {
			delete(state.peerState, p)
		} else {
			state.peerState[p] = pState
		}
		if requestedPartial && len(pm) > 0 {
			log.Debug("Respond to peer's partsMetadata")
			sendRPC = true
			rpc.PartialMessage = pm
		}
		if len(partsMetadata) > 0 {
			log.Debug("Including parts metadata")
			sendRPC = true
			rpc.PartsMetadata = partsMetadata
		}

		if sendRPC {
			rpc.TopicID = &topic
			rpc.GroupID = groupID
			e.sendRPC(p, &rpc)
		}
	}

	return nil
}

// peersToPublishTo returns a iter.Seq of peers to publish to. It combines peers
// in the group state with mesh peers for the topic.
// Group state peers are used to cover the fanout and gossip message cases where
// the peer would not be in our mesh.
func (e *PartialMessagesExtension[PeerState]) peersToPublishTo(topic string, state *partialMessageStatePerGroupPerTopic[PeerState]) iter.Seq[peer.ID] {
	return func(yield func(peer.ID) bool) {
		for p := range state.peerState {
			if !yield(p) {
				return
			}
		}
		for p := range e.router.MeshPeers(topic) {
			if _, ok := state.peerState[p]; !ok {
				if !yield(p) {
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
			if s.groupTTL == 0 {
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

		for _, peer := range peers {
			var rpc pb.PartialMessagesExtension
			pState := s.peerState[peer]
			pState, msg, partsMetadataToSend, err := e.GossipForPeer(topic, group, peer, pState)
			if err != nil {
				e.Logger.Error("Failed to encode partial message", "error", err)
				continue
			}
			// If the application cleared the peerState, we don't need to track it anymore
			if pState.IsZero() {
				delete(s.peerState, peer)
			} else {
				s.peerState[peer] = pState
			}
			if len(msg) > 0 || len(partsMetadataToSend) > 0 {
				rpc.TopicID = &topic
				rpc.GroupID = []byte(group)
				rpc.PartialMessage = msg
				rpc.PartsMetadata = partsMetadataToSend
				e.sendRPC(peer, &rpc)
			}
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

	var groupStateExists bool
	if statePerTopic, topicStateOk := e.statePerTopicPerGroup[topic]; topicStateOk {
		_, groupStateExists = statePerTopic[string(groupID)]
	}

	if !groupStateExists {
		// If we don't have the groupState, only create it if there is something
		// worth storing.
		var blankState PeerState
		pState, err := e.OnIncomingRPC(from, blankState, rpc)
		if err != nil {
			return err
		}
		if !pState.IsZero() {
			state, err := e.groupState(topic, groupID, true, from)
			if err != nil {
				return err
			}
			state.peerState[from] = pState
		}
		return nil
	}

	state, err := e.groupState(topic, groupID, true, from)
	if err != nil {
		return err
	}
	pState := state.peerState[from]

	pState, err = e.OnIncomingRPC(from, pState, rpc)
	if err != nil {
		return err
	}

	// If the application cleared the pState, we don't need to track it anymore
	if pState.IsZero() {
		delete(state.peerState, from)
	} else {
		state.peerState[from] = pState
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
