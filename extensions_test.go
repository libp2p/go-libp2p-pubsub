package pubsub

import (
	"testing"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
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
	if err := es.HandleRPC(partialExtensionsHello(id)); err != nil {
		t.Fatalf("handle extensions hello: %v", err)
	}
	if !es.activeExtensions[id].PartialMessages {
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
			if es.activeExtensions[id].PartialMessages {
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
				if err := es.HandleRPC(partialExtensionsHello(id)); err != nil {
					t.Fatalf("handle replacement hello: %v", err)
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
			if !es.activeExtensions[id].PartialMessages {
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

	es.peerExtensions[id] = PeerExtensions{}
	es.myExtensions.PartialMessages = false
	es.OnClosedOutboundStream(id)

	if len(cleanup.closed) != 1 || cleanup.closed[0] != id {
		t.Fatalf("expected cleanup from negotiated snapshot, got %v", cleanup.closed)
	}
}
