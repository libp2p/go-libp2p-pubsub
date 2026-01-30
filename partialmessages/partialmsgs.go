package partialmessages

import (
	"bytes"
	"errors"
	"iter"
	"log/slog"
	"slices"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
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
type PartsMetadata []byte

// Message is a message that can be broken up into parts. It can be
// complete, partially complete, or empty. It is up to the application to define
// how a message is split into parts and recombined, as well as how missing and
// available parts are represented.
type Message interface {
	GroupID() []byte

	// PartialMessageBytes takes the opaque parts metadata and returns an
	// encoded partial message that fulfills as much of the request as possible.
	PartialMessageBytes(partsMetadata PartsMetadata) (msg []byte, _ error)

	// EagerPartialMessageBytes returns an encoded partial message to be sent to
	// peer whose parts metadata is unknown. It is valid for an implementation
	// to return `nil, nil, nil` if it has no eager push data it would like to
	// send. The returned partsMetadata should represent the parts the remote
	// peer should have after decoding this message.
	EagerPartialMessageBytes() (msg []byte, partsMetadata PartsMetadata, _ error)

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

	myLastPartsMetadata []byte
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

// MergeBitmap is a helper function for merging parts metadata if they are a
// bitmap.
func MergeBitmap(left, right PartsMetadata) PartsMetadata {
	return PartsMetadata(bitmap.Merge(bitmap.Bitmap(left), bitmap.Bitmap(right)))
}

type PartialMessagesExtension struct {
	Logger *slog.Logger

	MergePartsMetadata func(topic string, left, right PartsMetadata) PartsMetadata

	// OnIncomingRPC is called whenever we receive an encoded
	// partial message from a peer. This func MUST be fast and non-blocking.
	// If you need to do slow work, dispatch the work to your own goroutine.
	OnIncomingRPC func(from peer.ID, rpc *pb.PartialMessagesExtension) error

	// ValidateRPC should be a fast function that performs some
	// basic sanity checks on incoming RPC. For example:
	//   - Is this a known topic?
	//   - Is the groupID well formed per application semantics?
	//   - If this is a PartialIHAVE/PartialIWant, is the request metadata within
	//     expected bounds?
	ValidateRPC func(from peer.ID, rpc *pb.PartialMessagesExtension) error

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
	if e.OnIncomingRPC == nil {
		return errors.New("field OnIncomingRPC must be set")
	}
	if e.MergePartsMetadata == nil {
		return errors.New("field MergePartsMetadata must be set")
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

	// Copy this as the owner of the partial message if free to mutate the
	// returned metadata after this function returns.
	myPartsMeta := slices.Clone(partial.PartsMetadata())
	state.myLastPartsMetadata = myPartsMeta

	var peers iter.Seq[peer.ID]
	if len(opts.PublishToPeers) > 0 {
		peers = slices.Values(opts.PublishToPeers)
	} else {
		peers = e.peersToPublishTo(topic, state)
	}

	eagerData, eagerPartsMeta, err := partial.EagerPartialMessageBytes()
	if err != nil {
		return err
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

		havePeersPartsMetadata := pState.partsMetadata != nil
		// Try to fulfill any wants from the peer
		if requestedPartial && havePeersPartsMetadata {
			// This peer has previously asked for a certain part. We'll give
			// them what we can.
			pm, err := partial.PartialMessageBytes(pState.partsMetadata)
			if err != nil {
				log.Warn("partial message extension failed to get partial message bytes", "error", err)
				// Possibly a bad request, we'll delete the request as we will likely error next time we try to handle it
				state.clearPeerMetadata(p)
				continue
			}
			pState.partsMetadata = e.MergePartsMetadata(topic, pState.partsMetadata, myPartsMeta)
			if len(pm) > 0 {
				log.Debug("Respond to peer's IWant")
				sendRPC = true
				rpc.PartialMessage = pm
			}
		}

		// Only send the eager push to the peer if:
		//   - we don't have the peer's parts metadata
		//   - we have something to eager push
		if requestedPartial && !havePeersPartsMetadata && len(eagerData) > 0 {
			log.Debug("Eager pushing")
			sendRPC = true
			rpc.PartialMessage = eagerData
			// Merge the peer's empty partsMetadata with the parts we eagerly pushed.
			// This tracks what has been sent to the peer and avoids sending duplicates.
			pState.partsMetadata = e.MergePartsMetadata(topic, pState.partsMetadata, eagerPartsMeta)
		}

		// Only send parts metadata if it was different then before
		if pState.sentPartsMetadata == nil || !bytes.Equal(myPartsMeta, pState.sentPartsMetadata) {
			log.Debug("Including parts metadata")
			sendRPC = true
			pState.sentPartsMetadata = myPartsMeta
			rpc.PartsMetadata = myPartsMeta
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
		if s.remotePeerInitiated() || len(s.myLastPartsMetadata) == 0 {
			continue
		}

		rpc := &pb.PartialMessagesExtension{
			TopicID:       &topic,
			GroupID:       []byte(group),
			PartsMetadata: s.myLastPartsMetadata,
		}

		for _, peer := range peers {
			pState, peerStateOk := s.peerState[peer]
			if !peerStateOk {
				pState = &peerState{}
				s.peerState[peer] = pState
			}

			if !bytes.Equal(rpc.PartsMetadata, pState.sentPartsMetadata) {
				pState.sentPartsMetadata = rpc.PartsMetadata
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
	if err := e.ValidateRPC(from, rpc); err != nil {
		return err
	}
	e.Logger.Debug("Received RPC", "from", from, "rpc", rpc)
	topic := rpc.GetTopicID()
	groupID := rpc.GroupID

	state, err := e.groupState(topic, groupID, true, from)
	if err != nil {
		return err
	}

	if rpc.PartsMetadata != nil {
		pState, ok := state.peerState[from]
		if !ok {
			pState = &peerState{}
			state.peerState[from] = pState
		}
		pState.partsMetadata = e.MergePartsMetadata(rpc.GetTopicID(), pState.partsMetadata, rpc.PartsMetadata)
	}

	if pState, ok := state.peerState[from]; ok && len(pState.sentPartsMetadata) > 0 && len(rpc.PartialMessage) > 0 {
		// We have previously sent this peer our parts metadata and they have
		// sent us a partial message. We can update the peer's view of our parts
		// by merging our parts and their parts.
		//
		// This works if they are responding to our request or
		// if they send data eagerly. In the latter case, they will update our
		// view when they receive our parts metadata.
		pState.sentPartsMetadata = e.MergePartsMetadata(rpc.GetTopicID(), pState.sentPartsMetadata, pState.partsMetadata)
	}

	return e.OnIncomingRPC(from, rpc)
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
