package pubsub

import (
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

type lifecyclePartialMessages struct {
	closed []peer.ID
}

func (m *lifecyclePartialMessages) OnClosedOutboundStream(id peer.ID) {
	m.closed = append(m.closed, id)
}

func (*lifecyclePartialMessages) HandleRPC(peer.ID, *pubsub_pb.PartialMessagesExtension) error {
	return nil
}

func (*lifecyclePartialMessages) Heartbeat() {}

func (*lifecyclePartialMessages) EmitGossip(string, []peer.ID) {}

func newPartialLifecycleState(cleanup *lifecyclePartialMessages) *extensionsState {
	es := newExtensionsState(PeerExtensions{PartialMessages: true}, nil, nil)
	es.partialMessagesExtension = cleanup
	return es
}

func partialExtensionsHello(id peer.ID) *RPC {
	enabled := true
	return &RPC{
		RPC: pubsub_pb.RPC{Control: &pubsub_pb.ControlMessage{
			Extensions: &pubsub_pb.ControlExtensions{PartialMessages: &enabled},
		}},
		from: id,
	}
}

func activatePartialExtensions(t *testing.T, es *extensionsState, id peer.ID) {
	t.Helper()
	es.OnNewOutboundStream(id, &RPC{})
	if !es.Preprocess(partialExtensionsHello(id)) {
		t.Fatal("preprocess extensions hello")
	}
	if !es.activePeerExtensions(id).PartialMessages {
		t.Fatal("partial messages extension was not activated")
	}
}

func TestExtensionsDeactivateOnEitherHalfClosing(t *testing.T) {
	for _, test := range []struct {
		name  string
		close func(*extensionsState, peer.ID)
	}{
		{"incoming", func(es *extensionsState, id peer.ID) { es.OnClosedIncomingStream(id, "") }},
		{"outbound", func(es *extensionsState, id peer.ID) { es.OnClosedOutboundStream(id) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := new(lifecyclePartialMessages)
			es := newPartialLifecycleState(cleanup)
			id := peer.ID("peer")
			activatePartialExtensions(t, es, id)

			test.close(es, id)
			if es.activePeerExtensions(id).PartialMessages {
				t.Fatal("extension remained active after stream closure")
			}
			if len(cleanup.closed) != 1 || cleanup.closed[0] != id {
				t.Fatalf("expected one cleanup for %q, got %v", id, cleanup.closed)
			}

			test.close(es, id)
			if len(cleanup.closed) != 1 {
				t.Fatalf("duplicate closure triggered %d cleanups", len(cleanup.closed))
			}
		})
	}
}

func TestExtensionsReplacementHalfReactivates(t *testing.T) {
	for _, test := range []struct {
		name      string
		closeHalf func(*extensionsState, peer.ID)
		replace   func(*testing.T, *extensionsState, peer.ID)
	}{
		{
			name:      "incoming",
			closeHalf: func(es *extensionsState, id peer.ID) { es.OnClosedIncomingStream(id, "") },
			replace: func(t *testing.T, es *extensionsState, id peer.ID) {
				t.Helper()
				if !es.Preprocess(partialExtensionsHello(id)) {
					t.Fatal("preprocess replacement hello")
				}
			},
		},
		{
			name:      "outbound",
			closeHalf: func(es *extensionsState, id peer.ID) { es.OnClosedOutboundStream(id) },
			replace: func(t *testing.T, es *extensionsState, id peer.ID) {
				t.Helper()
				es.OnNewOutboundStream(id, &RPC{})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := new(lifecyclePartialMessages)
			es := newPartialLifecycleState(cleanup)
			id := peer.ID("peer")
			activatePartialExtensions(t, es, id)

			test.closeHalf(es, id)
			test.replace(t, es, id)
			if !es.activePeerExtensions(id).PartialMessages {
				t.Fatal("replacement half did not reactivate extension")
			}
			if len(cleanup.closed) != 1 {
				t.Fatalf("expected one cleanup before reactivation, got %d", len(cleanup.closed))
			}
		})
	}
}

func TestExtensionsPartialCleanupUsesActiveSnapshot(t *testing.T) {
	cleanup := new(lifecyclePartialMessages)
	es := newPartialLifecycleState(cleanup)
	id := peer.ID("peer")
	activatePartialExtensions(t, es, id)

	es.peers[id].receivedCaps = PeerExtensions{}
	es.myExtensions.PartialMessages = false
	es.OnClosedOutboundStream(id)

	if len(cleanup.closed) != 1 || cleanup.closed[0] != id {
		t.Fatalf("expected cleanup from negotiated snapshot, got %v", cleanup.closed)
	}
}

func TestPeerExtensionsTopicStreams(t *testing.T) {
	t.Run("advertise and parse", func(t *testing.T) {
		extensions := PeerExtensions{TopicStreams: true}
		rpc := extensions.ExtendRPC(&RPC{})

		if rpc.Control == nil || rpc.Control.Extensions == nil {
			t.Fatal("expected extensions envelope")
		}
		if !rpc.Control.Extensions.GetTopicStreams() {
			t.Fatal("expected topic streams capability")
		}
		if got := peerExtensionsFromRPC(rpc); !got.TopicStreams {
			t.Fatal("expected parsed topic streams capability")
		}
	})

	t.Run("empty capabilities leave RPC unchanged", func(t *testing.T) {
		extensions := PeerExtensions{}
		rpc := &RPC{}

		if got := extensions.ExtendRPC(rpc); got != rpc {
			t.Fatal("expected original RPC")
		}
		if rpc.Control != nil {
			t.Fatal("expected no empty control envelope")
		}
	})

	t.Run("empty capabilities preserve existing control", func(t *testing.T) {
		extensions := PeerExtensions{}
		control := &pubsub_pb.ControlMessage{
			Ihave: []*pubsub_pb.ControlIHave{{TopicID: proto.String("topic")}},
		}
		rpc := &RPC{RPC: pubsub_pb.RPC{Control: control}}

		if got := extensions.ExtendRPC(rpc); got != rpc {
			t.Fatal("expected original RPC")
		}
		if rpc.Control != control {
			t.Fatal("expected existing control to be preserved")
		}
		if rpc.Control.Extensions != nil {
			t.Fatal("expected no empty extensions envelope")
		}
		if len(rpc.Control.Ihave) != 1 || rpc.Control.Ihave[0].GetTopicID() != "topic" {
			t.Fatal("expected existing control content to be preserved")
		}
	})
}

func TestWithTopicStreamsAdvertisesOnly(t *testing.T) {
	router := &GossipSubRouter{
		extensions: newExtensionsState(PeerExtensions{}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {}),
	}
	ps := &PubSub{rt: router}

	if err := WithTopicStreams()(ps); err != nil {
		t.Fatal(err)
	}
	if !router.extensions.myExtensions.TopicStreams {
		t.Fatal("expected topic streams to be advertised")
	}

	rpc := router.extensions.OnNewOutboundStream("peer", &RPC{
		RPC: pubsub_pb.RPC{},
	})
	if !rpc.Control.Extensions.GetTopicStreams() {
		t.Fatal("expected first RPC to advertise topic streams")
	}
}

func TestTopicStreamsNegotiationActivatesAndRejectsControlPayload(t *testing.T) {
	peerID := peer.ID("peer")
	enabled := make(chan struct{}, 1)
	violations := 0
	es := newExtensionsState(PeerExtensions{TopicStreams: true, PartialMessages: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.enableTopicStreams = func(got peer.ID) {
		if got != peerID {
			t.Fatalf("enabled wrong peer: %s", got)
		}
		enabled <- struct{}{}
	}
	es.protocolViolation = func(got peer.ID, _ network.Stream) {
		if got != peerID {
			t.Fatalf("violated wrong peer: %s", got)
		}
		violations++
	}
	es.OnNewOutboundStream(peerID, &RPC{})
	rpc := &RPC{RPC: pubsub_pb.RPC{
		Control: &pubsub_pb.ControlMessage{Extensions: &pubsub_pb.ControlExtensions{
			TopicStreams: proto.Bool(true), PartialMessages: proto.Bool(true),
		}},
		Publish: []*pubsub_pb.Message{{Data: []byte("forbidden")}},
	}, from: peerID, transport: peercomm.TransportControl}
	if es.Preprocess(rpc) {
		t.Fatal("accepted payload on negotiated control stream")
	}
	<-enabled
	if violations != 1 {
		t.Fatalf("violations = %d, want 1", violations)
	}
}

func TestTopicStreamsUnsupportedPeerKeepsControlPayload(t *testing.T) {
	peerID := peer.ID("peer")
	es := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.OnNewOutboundStream(peerID, &RPC{})
	rpc := &RPC{RPC: pubsub_pb.RPC{
		Control: &pubsub_pb.ControlMessage{Extensions: &pubsub_pb.ControlExtensions{}},
		Publish: []*pubsub_pb.Message{{Data: []byte("fallback")}},
	}, from: peerID, transport: peercomm.TransportControl}
	if !es.Preprocess(rpc) {
		t.Fatal("rejected fallback control payload")
	}
}

func TestTopicTransportCannotSeedExtensionNegotiation(t *testing.T) {
	peerID := peer.ID("peer")
	enabled := 0
	es := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.enableTopicStreams = func(peer.ID) { enabled++ }
	rpc := &RPC{RPC: pubsub_pb.RPC{Control: &pubsub_pb.ControlMessage{Extensions: &pubsub_pb.ControlExtensions{TopicStreams: proto.Bool(true)}}}, from: peerID, transport: peercomm.TransportTopic}
	if es.Preprocess(rpc) {
		t.Fatal("accepted topic payload before control negotiation")
	}
	if state := es.peers[peerID]; state != nil && state.received {
		t.Fatal("topic payload seeded extension negotiation")
	}
	if enabled != 0 {
		t.Fatalf("topic streams enabled %d times", enabled)
	}
}

func TestTopicStreamsControlAllowsUnnegotiatedPartialFallback(t *testing.T) {
	peerID := peer.ID("peer")
	es := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.OnNewOutboundStream(peerID, &RPC{})
	rpc := &RPC{RPC: pubsub_pb.RPC{
		Control: &pubsub_pb.ControlMessage{Extensions: &pubsub_pb.ControlExtensions{TopicStreams: proto.Bool(true)}},
		Partial: &pubsub_pb.PartialMessagesExtension{TopicID: proto.String("topic"), PartialMessage: []byte("partial")},
	}, from: peerID, transport: peercomm.TransportControl}
	if !es.Preprocess(rpc) {
		t.Fatal("rejected unnegotiated partial fallback on control")
	}
}

func TestPeerExtensionsIncomingLifecycle(t *testing.T) {
	peerID := peer.ID("peer")
	enabled := 0
	disabled := 0
	partial := &recordingPartialMessageExtension{}
	es := newExtensionsState(PeerExtensions{TopicStreams: true, PartialMessages: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.enableTopicStreams = func(got peer.ID) {
		if got != peerID {
			t.Fatalf("enabled wrong peer: %s", got)
		}
		enabled++
	}
	es.disableTopicStreams = func(got peer.ID) {
		if got != peerID {
			t.Fatalf("disabled wrong peer: %s", got)
		}
		disabled++
	}
	es.partialMessagesExtension = partial

	es.OnNewOutboundStream(peerID, &RPC{})
	if !es.Preprocess(extensionHello(peerID, true, true)) {
		t.Fatal("rejected initial extension hello")
	}
	if enabled != 1 {
		t.Fatalf("topic stream activations = %d, want 1", enabled)
	}

	// Deactivation must use the activation snapshot, not current received state.
	es.peers[peerID].receivedCaps = PeerExtensions{}
	es.OnClosedIncomingStream(peerID, "")
	if state := es.peers[peerID]; state != nil && state.received {
		t.Fatal("incoming close did not clear received extensions")
	}
	if es.peers[peerID] != nil && es.peers[peerID].active {
		t.Fatal("incoming close left extensions active")
	}
	if disabled != 1 || partial.closed != 1 {
		t.Fatalf("deactivation counts = topic %d, partial %d; want 1, 1", disabled, partial.closed)
	}
	if es.Preprocess(&RPC{from: peerID, transport: peercomm.TransportTopic}) {
		t.Fatal("accepted topic RPC while extensions were inactive")
	}

	if !es.Preprocess(extensionHello(peerID, true, false)) {
		t.Fatal("rejected replacement extension hello")
	}
	if enabled != 2 {
		t.Fatalf("topic stream activations = %d, want 2", enabled)
	}
	if !es.Preprocess(&RPC{from: peerID, transport: peercomm.TransportTopic}) {
		t.Fatal("rejected topic RPC after replacement hello reactivated extensions")
	}

	es.OnClosedOutboundStream(peerID)
	if disabled != 2 || partial.closed != 1 {
		t.Fatalf("final deactivation counts = topic %d, partial %d; want 2, 1", disabled, partial.closed)
	}
	es.OnClosedOutboundStream(peerID)
	if disabled != 2 || partial.closed != 1 {
		t.Fatal("repeated outbound close deactivated extensions twice")
	}
}

func TestPeerExtensionsOutboundLifecycle(t *testing.T) {
	peerID := peer.ID("peer")
	enabled := 0
	disabled := 0
	es := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	es.enableTopicStreams = func(peer.ID) { enabled++ }
	es.disableTopicStreams = func(peer.ID) { disabled++ }

	if !es.Preprocess(extensionHello(peerID, true, false)) {
		t.Fatal("rejected incoming-first extension hello")
	}
	if enabled != 0 {
		t.Fatal("activated before outbound extensions were sent")
	}
	es.OnNewOutboundStream(peerID, &RPC{})
	if enabled != 1 {
		t.Fatalf("activations = %d, want 1", enabled)
	}

	es.OnClosedOutboundStream(peerID)
	if disabled != 1 {
		t.Fatalf("deactivations = %d, want 1", disabled)
	}
	if state := es.peers[peerID]; state == nil || !state.received {
		t.Fatal("outbound close unexpectedly cleared received extensions")
	}
	if es.Preprocess(&RPC{from: peerID, transport: peercomm.TransportTopic}) {
		t.Fatal("accepted topic RPC after outbound close")
	}

	es.OnNewOutboundStream(peerID, &RPC{})
	if enabled != 2 {
		t.Fatalf("replacement outbound stream activations = %d, want 2", enabled)
	}
	es.OnClosedIncomingStream(peerID, "")
	if disabled != 2 {
		t.Fatalf("incoming close deactivations = %d, want 2", disabled)
	}
	es.OnClosedIncomingStream(peerID, "")
	if disabled != 2 {
		t.Fatal("repeated incoming close deactivated extensions twice")
	}
}

func extensionHello(from peer.ID, topicStreams, partialMessages bool) *RPC {
	return &RPC{
		RPC: pubsub_pb.RPC{Control: &pubsub_pb.ControlMessage{Extensions: &pubsub_pb.ControlExtensions{
			TopicStreams:    proto.Bool(topicStreams),
			PartialMessages: proto.Bool(partialMessages),
		}}},
		from:      from,
		transport: peercomm.TransportControl,
	}
}

type recordingPartialMessageExtension struct {
	closed int
}

func (m *recordingPartialMessageExtension) OnClosedOutboundStream(peer.ID) {
	m.closed++
}

func (*recordingPartialMessageExtension) HandleRPC(peer.ID, *pubsub_pb.PartialMessagesExtension) error {
	return nil
}

func (*recordingPartialMessageExtension) Heartbeat() {}

func (*recordingPartialMessageExtension) EmitGossip(string, []peer.ID) {}
