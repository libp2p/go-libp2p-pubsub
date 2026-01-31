package partialmessages

import (
	"errors"
	"iter"
	"log/slog"
	"slices"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TODO: Add gossip fallback (pick random connected peers and send ihave/iwants)

const minGroupTTL = 3

// defaultPeerInitiatedGroupLimitPerTopic limits the total number (per topic) of
// *partialMessageStatePerTopicGroup we create in response to a incoming RPC.
// This only applies to groups that we haven't published for yet.
const defaultPeerInitiatedGroupLimitPerTopic = 255

const defaultPeerInitiatedGroupLimitPerTopicPerPeer = 8

// PartsMetadata returns metadata about the parts this partial message
// contains and, possibly implicitly, the parts it wants.
//
// The following invariant must be true for any implementation:
//
// Given that A and B are the same concrete type that implement PartsMetadata,
//
// A.Merge(B)
// B.IsSubset(A) == true
//
// If A and B are separate concrete types, it may be impossible to merge them,
// so IsSubset is not expected to return true.
type PartsMetadata interface {
	Encode() []byte
	// Clone returns a deep clone
	Clone() PartsMetadata
	// Merge creates a union with the provided PartsMetadata.
	// Merge(nil) is a no-op.
	Merge(PartsMetadata)
	// Returns true if the object is a subset of B.
	IsSubset(B PartsMetadata) bool
}

// Message is a message that can be broken up into parts. It can be
// complete, partially complete, or empty. It is up to the application to define
// how a message is split into parts and recombined, as well as how missing and
// available parts are represented.
type Message interface {
	GroupID() []byte

	// PartialMessageBytes takes the opaque parts metadata and returns an
	// encoded partial message that fulfills as much of the request as possible.
	//
	// partsMetadata is nil if the node has not seen it's peer's partsMetadata,
	// applications can still eagerly push data in this case.
	//
	// The returned partsMetadata should represent the parts the remote peer
	// should have after decoding this message.
	PartialMessageBytes(from peer.ID, partsMetadata PartsMetadata) (msg []byte, nextPartsMetadata PartsMetadata, _ error)

	PartsMetadata() PartsMetadata
}

// peerState is the state we keep per peer. Used to make Publish
// Idempotent.
type peerState struct {
	// The parts metadata the peer has sent us
	partsMetadata PartsMetadata
	// The parts metadata this node has sent to the peer
	sentPartsMetadata PartsMetadata
}

func (ps *peerState) IsZero() bool {
	return ps.partsMetadata == nil && ps.sentPartsMetadata == nil
}

type partialMessageStatePerGroupPerTopic struct {
	peerState   map[peer.ID]*peerState
	groupTTL    int
	initiatedBy peer.ID // zero value if we initiated the group

	myLastPartsMetadata PartsMetadata
}

func newPartialMessageStatePerTopicGroup(groupTTL int) *partialMessageStatePerGroupPerTopic {
	return &partialMessageStatePerGroupPerTopic{
		peerState: make(map[peer.ID]*peerState),
		groupTTL:  max(groupTTL, minGroupTTL),
	}
}

func (s *partialMessageStatePerGroupPerTopic) remotePeerInitiated() bool {
	return s.initiatedBy != ""
}

func (s *partialMessageStatePerGroupPerTopic) clearPeerMetadata(peerID peer.ID) {
	if peerState, ok := s.peerState[peerID]; ok {
		peerState.partsMetadata = nil
		if peerState.IsZero() {
			delete(s.peerState, peerID)
		}
	}
}

type PartialMessagesExtension struct {
	Logger *slog.Logger

	// OnIncomingRPC is called whenever we receive an encoded
	// partial message from a peer. This function MUST be fast and non-blocking.
	// If you need to do slow work, dispatch the work to your own goroutine.
	OnIncomingRPC func(from peer.ID, rpc *pb.PartialMessagesExtension) error

	// ValidateRPC should be a fast function that performs some
	// basic sanity checks on incoming RPC.
	// Some example validations:
	//   - Is this a known topic?
	//   - Is the groupID well formed per application semantics?
	ValidateRPC func(from peer.ID, rpc *pb.PartialMessagesExtension) error

	// DecodePartsMetadata should return the decoded PartsMetadata from the RPC.
	// The RPC is guaranteed to have partsMetadata set.
	DecodePartsMetadata func(from peer.ID, rpc *pb.PartialMessagesExtension) (PartsMetadata, error)

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
	statePerTopicPerGroup map[string]map[string]*partialMessageStatePerGroupPerTopic

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

func (e *PartialMessagesExtension) groupState(topic string, groupID []byte, peerInitiated bool, from peer.ID) (*partialMessageStatePerGroupPerTopic, error) {
	statePerTopic, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		statePerTopic = make(map[string]*partialMessageStatePerGroupPerTopic)
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

		state = newPartialMessageStatePerTopicGroup(e.GroupTTLByHeatbeat)
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

func (e *PartialMessagesExtension) Init(router Router) error {
	e.router = router
	if e.Logger == nil {
		return errors.New("field Logger must be set")
	}
	if e.ValidateRPC == nil {
		return errors.New("field ValidateRPC must be set")
	}
	if e.DecodePartsMetadata == nil {
		return errors.New("field DecodePartsMetadata must be set")
	}
	if e.OnIncomingRPC == nil {
		return errors.New("field OnIncomingRPC must be set")
	}
	if e.PeerInitiatedGroupLimitPerTopic == 0 {
		e.PeerInitiatedGroupLimitPerTopic = defaultPeerInitiatedGroupLimitPerTopic
	}
	if e.PeerInitiatedGroupLimitPerTopicPerPeer == 0 {
		e.PeerInitiatedGroupLimitPerTopicPerPeer = defaultPeerInitiatedGroupLimitPerTopicPerPeer
	}

	e.statePerTopicPerGroup = make(map[string]map[string]*partialMessageStatePerGroupPerTopic)
	e.peerInitiatedGroupCounter = make(map[string]*peerInitiatedGroupCounterState)

	return nil
}

func (e *PartialMessagesExtension) PublishPartial(topic string, partial Message, opts PublishOptions) error {
	groupID := partial.GroupID()

	state, err := e.groupState(topic, groupID, false, "")
	if err != nil {
		return err
	}

	state.groupTTL = max(e.GroupTTLByHeatbeat, minGroupTTL)

	// Copy this defensively to avoid aliasing issues.
	myPartsMeta := partial.PartsMetadata().Clone()
	state.myLastPartsMetadata = myPartsMeta

	var peers iter.Seq[peer.ID]
	if len(opts.PublishToPeers) > 0 {
		peers = slices.Values(opts.PublishToPeers)
	} else {
		peers = e.peersToPublishTo(topic, state)
	}

	for p := range peers {
		log := e.Logger.With("peer", p)
		requestedPartial := e.router.PeerRequestsPartial(p, topic)

		var rpc pb.PartialMessagesExtension
		var sendRPC bool

		pState, peerStateOk := state.peerState[p]
		if !peerStateOk {
			pState = &peerState{}
			state.peerState[p] = pState
		}

		if requestedPartial {
			// This peer requested partial messages, we'll give them what we can
			pm, nextParts, err := partial.PartialMessageBytes(p, pState.partsMetadata)
			if err != nil {
				log.Warn("partial message extension failed to get partial message bytes", "error", err)
				// Possibly a bad request, we'll delete the request as we will likely error next time we try to handle it
				state.clearPeerMetadata(p)
				continue
			}
			if len(pm) > 0 {
				log.Debug("Respond to peer's IWant")
				sendRPC = true
				rpc.PartialMessage = pm
				// Only update peer's parts metadata if we actually sent data
				if nextParts != nil {
					pState.partsMetadata = nextParts
				}
			}
		}

		// Only send parts metadata if it was different then before
		if pState.sentPartsMetadata == nil || !myPartsMeta.IsSubset(pState.sentPartsMetadata) {
			log.Debug("Including parts metadata", "sent parts", pState.sentPartsMetadata, "my parts", myPartsMeta)
			sendRPC = true
			if pState.sentPartsMetadata == nil {
				pState.sentPartsMetadata = myPartsMeta.Clone()
			} else {
				pState.sentPartsMetadata.Merge(myPartsMeta)
			}
			rpc.PartsMetadata = pState.sentPartsMetadata.Encode()
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
func (e *PartialMessagesExtension) peersToPublishTo(topic string, state *partialMessageStatePerGroupPerTopic) iter.Seq[peer.ID] {
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

func (e *PartialMessagesExtension) AddPeer(id peer.ID) {
}

func (e *PartialMessagesExtension) RemovePeer(id peer.ID) {
	for topic, statePerTopic := range e.statePerTopicPerGroup {
		for _, state := range statePerTopic {
			delete(state.peerState, id)
		}
		if ctr, ok := e.peerInitiatedGroupCounter[topic]; ok {
			ctr.RemovePeer(id)
		}
	}
}

func (e *PartialMessagesExtension) Heartbeat() {
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

func (e *PartialMessagesExtension) EmitGossip(topic string, peers []peer.ID) {
	topicState, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		return
	}

	for group, s := range topicState {
		if s.remotePeerInitiated() || s.myLastPartsMetadata == nil {
			continue
		}

		rpc := &pb.PartialMessagesExtension{
			TopicID:       &topic,
			GroupID:       []byte(group),
			PartsMetadata: s.myLastPartsMetadata.Encode(),
		}

		for _, peer := range peers {
			pState, peerStateOk := s.peerState[peer]
			if !peerStateOk {
				pState = &peerState{}
				s.peerState[peer] = pState
			}

			if pState.sentPartsMetadata == nil || !s.myLastPartsMetadata.IsSubset(pState.sentPartsMetadata) {
				pState.sentPartsMetadata = s.myLastPartsMetadata
				if pState.sentPartsMetadata == nil {
					pState.sentPartsMetadata = s.myLastPartsMetadata.Clone()
				} else {
					pState.sentPartsMetadata.Merge(s.myLastPartsMetadata)
				}
				e.sendRPC(peer, rpc)
			}
		}
	}
}

func (e *PartialMessagesExtension) sendRPC(to peer.ID, rpc *pb.PartialMessagesExtension) {
	e.Logger.Debug("Sending RPC", "to", to, "rpc", rpc)
	e.router.SendRPC(to, rpc, false)
}

func (e *PartialMessagesExtension) HandleRPC(from peer.ID, rpc *pb.PartialMessagesExtension) error {
	if rpc == nil {
		return nil
	}

	err := e.ValidateRPC(from, rpc)
	if err != nil {
		return err
	}

	var peerPartsMetadata PartsMetadata

	if len(rpc.PartsMetadata) > 0 {
		peerPartsMetadata, err = e.DecodePartsMetadata(from, rpc)
		if err != nil {
			return err
		}
	}
	e.Logger.Debug("Received RPC", "from", from, "rpc", rpc)
	topic := rpc.GetTopicID()
	groupID := rpc.GroupID

	state, err := e.groupState(topic, groupID, true, from)
	if err != nil {
		return err
	}

	pState, peerStateOk := state.peerState[from]
	if peerPartsMetadata != nil {
		if !peerStateOk {
			peerStateOk = true
			pState = &peerState{}
			state.peerState[from] = pState
		}
		if pState.partsMetadata == nil {
			pState.partsMetadata = peerPartsMetadata
		} else {
			pState.partsMetadata.Merge(peerPartsMetadata)
		}
	}

	if peerStateOk && pState.sentPartsMetadata != nil && pState.partsMetadata != nil && len(rpc.PartialMessage) > 0 {
		// We have previously sent this peer our parts metadata and they have
		// sent us a partial message. We can update the peer's view of our parts
		// by merging our parts and their parts.
		//
		// This works if they are responding to our request or
		// if they send data eagerly. In the latter case, they will update our
		// view when they receive our parts metadata.
		pState.sentPartsMetadata.Merge(pState.partsMetadata)
	}

	err = e.OnIncomingRPC(from, rpc)
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
