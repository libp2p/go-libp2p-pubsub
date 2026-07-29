package pubsub

import (
	"errors"
	"iter"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type PeerExtensions struct {
	TestExtension   bool
	PartialMessages bool
	TopicStreams    bool
}

type TestExtensionConfig struct {
	OnReceiveTestExtension func(from peer.ID)
}

// WithTopicStreams advertises support for topic-scoped streams.
func WithTopicStreams() Option {
	return func(ps *PubSub) error {
		if rt, ok := ps.rt.(*GossipSubRouter); ok {
			rt.extensions.myExtensions.TopicStreams = true
		}
		return nil
	}
}

func WithTestExtension(c TestExtensionConfig) Option {
	return func(ps *PubSub) error {
		if rt, ok := ps.rt.(*GossipSubRouter); ok {
			rt.extensions.testExtension = &testExtension{
				sendRPC:                rt.extensions.sendRPC,
				onReceiveTestExtension: c.OnReceiveTestExtension,
			}
			rt.extensions.myExtensions.TestExtension = true
		}
		return nil
	}
}

func hasPeerExtensions(rpc *RPC) bool {
	if rpc != nil && rpc.Control != nil && rpc.Control.Extensions != nil {
		return true
	}
	return false
}

func peerExtensionsFromRPC(rpc *RPC) PeerExtensions {
	out := PeerExtensions{}
	if hasPeerExtensions(rpc) {
		out.TestExtension = rpc.Control.Extensions.GetTestExtension()
		out.PartialMessages = rpc.Control.Extensions.GetPartialMessages()
		out.TopicStreams = rpc.Control.Extensions.GetTopicStreams()
	}
	return out
}

func (pe *PeerExtensions) ExtendRPC(rpc *RPC) *RPC {
	if !pe.TestExtension && !pe.PartialMessages && !pe.TopicStreams {
		return rpc
	}
	if rpc.Control == nil {
		rpc.Control = &pubsub_pb.ControlMessage{}
	}
	if rpc.Control.Extensions == nil {
		rpc.Control.Extensions = &pubsub_pb.ControlExtensions{}
	}
	if pe.TestExtension {
		rpc.Control.Extensions.TestExtension = &pe.TestExtension
	}
	if pe.PartialMessages {
		rpc.Control.Extensions.PartialMessages = &pe.PartialMessages
	}
	if pe.TopicStreams {
		rpc.Control.Extensions.TopicStreams = &pe.TopicStreams
	}
	return rpc
}

// Using an interface type to avoid bubbling up PartialMessage's generics up to
// pubsub.
//
// Purposely not trying to make a generic extension interface as there is only
// one real consumer (partial messages). This may change in the future.
type partialMessageInterface interface {
	OnClosedOutboundStream(peer.ID)
	HandleRPC(from peer.ID, rpc *pubsub_pb.PartialMessagesExtension) error
	Heartbeat()
	EmitGossip(topic string, peers []peer.ID)
}

type peerExtensionState struct {
	received       bool
	receivedCaps   PeerExtensions
	sent           bool
	active         bool
	activeSnapshot PeerExtensions
}

type extensionsState struct {
	myExtensions      PeerExtensions
	peers             map[peer.ID]*peerExtensionState
	reportMisbehavior func(peer.ID)
	sendRPC           func(p peer.ID, r *RPC, urgent bool)
	testExtension     *testExtension

	partialMessagesExtension partialMessageInterface
	enableTopicStreams       func(peer.ID)
	disableTopicStreams      func(peer.ID)
	protocolViolation        func(peer.ID, network.Stream)
}

func newExtensionsState(myExtensions PeerExtensions, reportMisbehavior func(peer.ID), sendRPC func(peer.ID, *RPC, bool)) *extensionsState {
	return &extensionsState{
		myExtensions:      myExtensions,
		peers:             make(map[peer.ID]*peerExtensionState),
		reportMisbehavior: reportMisbehavior,
		sendRPC:           sendRPC,
		testExtension:     nil,
	}
}

func (es *extensionsState) HandleRPC(rpc *RPC) error {
	state := es.peers[rpc.from]
	var active PeerExtensions
	if state != nil && state.active {
		active = state.activeSnapshot
	}
	if active.TestExtension && es.testExtension != nil {
		es.testExtension.HandleRPC(rpc.from, rpc.TestExtension)
	}
	if active.PartialMessages && rpc.Partial != nil && es.partialMessagesExtension != nil {
		return es.partialMessagesExtension.HandleRPC(rpc.from, rpc.Partial)
	}
	return nil
}

func (es *extensionsState) peerState(id peer.ID) *peerExtensionState {
	state := es.peers[id]
	if state == nil {
		state = new(peerExtensionState)
		es.peers[id] = state
	}
	return state
}

func (es *extensionsState) observeExtensions(rpc *RPC) {
	state := es.peerState(rpc.from)
	if state.received {
		if hasPeerExtensions(rpc) {
			es.reportMisbehavior(rpc.from)
		}
		return
	}
	state.received = true
	state.receivedCaps = peerExtensionsFromRPC(rpc)
	es.reconcilePeerExtensions(rpc.from, state)
}

// Preprocess observes the first control hello and validates that
// payloads use the transport selected during extension negotiation.
func (es *extensionsState) Preprocess(rpc *RPC) bool {
	state := es.peers[rpc.from]
	if rpc.transport == peercomm.TransportTopic {
		return state != nil && state.active && state.activeSnapshot.TopicStreams
	}
	es.observeExtensions(rpc)
	state = es.peers[rpc.from]
	active := state.activeSnapshot
	if state.active && active.TopicStreams && (len(rpc.Publish) != 0 || (active.PartialMessages && rpc.Partial != nil)) {
		if es.protocolViolation != nil {
			es.protocolViolation(rpc.from, rpc.stream)
		}
		return false
	}
	return true
}

func (es *extensionsState) OnNewIncomingStream(peer.ID, protocol.ID) {}

func (es *extensionsState) OnClosedIncomingStream(id peer.ID, _ protocol.ID) {
	state := es.peers[id]
	if state == nil || !state.received {
		return
	}
	state.received = false
	state.receivedCaps = PeerExtensions{}
	es.reconcilePeerExtensions(id, state)
	es.prunePeerState(id, state)
}

func (es *extensionsState) OnNewOutboundStream(id peer.ID, helloPacket *RPC) *RPC {
	helloPacket = es.myExtensions.ExtendRPC(helloPacket)
	state := es.peerState(id)
	state.sent = true
	es.reconcilePeerExtensions(id, state)
	return helloPacket
}

func (es *extensionsState) OnClosedOutboundStream(id peer.ID) {
	state := es.peers[id]
	if state == nil || !state.sent {
		return
	}
	state.sent = false
	es.reconcilePeerExtensions(id, state)
	es.prunePeerState(id, state)
}

func (es *extensionsState) reconcilePeerExtensions(id peer.ID, state *peerExtensionState) {
	shouldActivate := state.received && state.sent
	if state.active && !shouldActivate {
		active := state.activeSnapshot
		state.active = false
		state.activeSnapshot = PeerExtensions{}
		if active.TopicStreams && es.disableTopicStreams != nil {
			es.disableTopicStreams(id)
		}
		if active.PartialMessages && es.partialMessagesExtension != nil {
			es.partialMessagesExtension.OnClosedOutboundStream(id)
		}
	}
	if !state.active && shouldActivate {
		active := PeerExtensions{
			TestExtension:   es.myExtensions.TestExtension && state.receivedCaps.TestExtension,
			PartialMessages: es.myExtensions.PartialMessages && state.receivedCaps.PartialMessages,
			TopicStreams:    es.myExtensions.TopicStreams && state.receivedCaps.TopicStreams,
		}
		state.active = true
		state.activeSnapshot = active
		if active.TestExtension && es.testExtension != nil {
			es.testExtension.OnNewOutboundStream(id)
		}
		if active.TopicStreams && es.enableTopicStreams != nil {
			es.enableTopicStreams(id)
		}
	}
}

func (es *extensionsState) prunePeerState(id peer.ID, state *peerExtensionState) {
	if !state.received && !state.sent && !state.active {
		delete(es.peers, id)
	}
}

func (es *extensionsState) activePeerExtensions(id peer.ID) PeerExtensions {
	state := es.peers[id]
	if state == nil || !state.active {
		return PeerExtensions{}
	}
	return state.activeSnapshot
}

func (es *extensionsState) Heartbeat() {
	if es.myExtensions.PartialMessages {
		es.partialMessagesExtension.Heartbeat()
	}
}

func WithPartialMessagesExtension[PeerState any](pm *partialmessages.PartialMessagesExtension[PeerState]) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return errors.New("pubsub router is not gossipsub")
		}
		err := pm.Init(partialMessageRouter{gs})
		if err != nil {
			return err
		}

		gs.extensions.myExtensions.PartialMessages = true
		gs.extensions.partialMessagesExtension = pm
		return nil
	}
}

// PublishPartial uses the given PubSub instance to publish partial messages.
// This is a standalone function rather a method on PubSub due to the generic
// type parameter.
func PublishPartial[PeerState any](ps *PubSub, topic string, groupID []byte, publishActionsFn partialmessages.PublishActionsFn[PeerState]) error {
	resp := make(chan error, 1)
	select {
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case ps.eval <- func() {
		defer close(resp)

		rt, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			resp <- errors.New("partial publishing is only supported by the GossipSub router")
			return
		}

		if rt.extensions.partialMessagesExtension == nil {
			resp <- errors.New("partial publishing is not enabled")
			return
		}
		pme, ok := rt.extensions.partialMessagesExtension.(*partialmessages.PartialMessagesExtension[PeerState])
		if !ok {
			resp <- errors.New("incompatible partial messages extension type")
			return
		}

		resp <- pme.PublishPartial(topic, groupID, publishActionsFn)
	}:
	}

	select {
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case r := <-resp:
		return r
	}
}

type partialMessageRouter struct {
	gs *GossipSubRouter
}

// PeerRequestsPartial returns true if a peer requested partial messages on this topic.
//
// It does not check if we support partial messages on the topic, because we may
// not be subscribed to that topic and thus not have that information.
// Callers should not use this if they don't support partial messages on this topic.
func (r partialMessageRouter) PeerRequestsPartial(peer peer.ID, topic string) bool {
	return r.gs.peerRequestsPartial(peer, topic)
}

// MeshPeers implements partialmessages.Router.
func (r partialMessageRouter) MeshPeers(topic string) iter.Seq[peer.ID] {
	return func(yield func(peer.ID) bool) {
		peerSet := r.gs.mesh[topic]
		if len(peerSet) == 0 {
			// Possibly a fanout topic, or no mesh peers are available yet.
			peerSet = r.gs.getFanoutPeersForPublishing(topic)
		}

		for peer := range peerSet {
			if r.gs.extensions.activePeerExtensions(peer).PartialMessages &&
				((r.gs.iRequestPartial(topic) && r.gs.peerSupportsSendingPartial(peer, topic)) ||
					(r.gs.iSupportSendingPartial(topic) && r.gs.peerRequestsPartial(peer, topic))) {
				if !yield(peer) {
					return
				}
			}
		}
	}
}

// SendRPC implements partialmessages.Router.
func (r partialMessageRouter) SendRPC(p peer.ID, rpc *pubsub_pb.PartialMessagesExtension, urgent bool) {
	r.gs.sendRPC(p, &RPC{
		RPC: pubsub_pb.RPC{
			Partial: rpc,
		},
	}, urgent)
}

var _ partialmessages.Router = partialMessageRouter{}
