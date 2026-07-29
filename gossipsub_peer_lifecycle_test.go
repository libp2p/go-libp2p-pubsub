package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

type outboundOpenFailureHost struct {
	host.Host

	started chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
	calls   atomic.Int32
}

func (h *outboundOpenFailureHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	h.calls.Add(1)
	h.once.Do(func() { close(h.started) })
	select {
	case <-h.release:
		return nil, h.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type outboundCloseCountingTracer struct {
	mockRawTracer
	closed atomic.Int32
}

func (t *outboundCloseCountingTracer) OnClosedOutboundStream(peer.ID) {
	t.closed.Add(1)
}

func waitForLifecycleCondition(t *testing.T, ps *PubSub, desc string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		result := make(chan bool, 1)
		ps.eval <- func() { result <- condition() }
		if <-result {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeLifecycleRPC(t *testing.T, stream network.Stream, rpc *pb.RPC) {
	t.Helper()
	b, err := proto.Marshal(rpc)
	if err != nil {
		t.Fatal(err)
	}
	if err := msgio.NewVarintWriter(stream).WriteMsg(b); err != nil {
		t.Fatal(err)
	}
}

func newOutboundOpenFailureTest(t *testing.T, ctx context.Context) (*PubSub, *outboundOpenFailureHost, host.Host, *outboundCloseCountingTracer) {
	t.Helper()
	hosts := getDefaultHosts(t, 2)
	local := &outboundOpenFailureHost{
		Host:    hosts[0],
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     errors.New("controlled outbound open failure"),
	}
	tracer := &outboundCloseCountingTracer{}
	ps := getGossipsub(ctx, local, WithRawTracer(tracer), WithMessageSignaturePolicy(StrictNoSign))
	hosts[1].SetStreamHandler(GossipSubID_v13, func(s network.Stream) { _ = s.Reset() })
	connect(t, hosts[1], local)
	select {
	case <-local.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial outbound open")
	}
	return ps, local, hosts[1], tracer
}

func TestInitialOutboundOpenFailureRetiresInboundPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps, local, remote, tracer := newOutboundOpenFailureTest(t, ctx)
	const topicID = "outbound-open-failure"
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := topic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	defer events.Cancel()

	inbound, err := remote.NewStream(ctx, local.ID(), GossipSubID_v13)
	if err != nil {
		t.Fatal(err)
	}
	defer inbound.Close()
	writeLifecycleRPC(t, inbound, &pb.RPC{
		Subscriptions: []*pb.RPC_SubOpts{{
			Topicid:   proto.String(topicID),
			Subscribe: proto.Bool(true),
		}},
		Control: &pb.ControlMessage{Extensions: &pb.ControlExtensions{
			TopicStreams: proto.Bool(true),
		}},
	})

	eventCtx, eventCancel := context.WithTimeout(ctx, 5*time.Second)
	join, err := events.NextPeerEvent(eventCtx)
	eventCancel()
	if err != nil {
		t.Fatal(err)
	}
	if join.Type != PeerJoin || join.Peer != remote.ID() {
		t.Fatalf("first event = %+v, want PeerJoin for %s", join, remote.ID())
	}

	reset := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := inbound.Read(b[:])
		reset <- err
	}()
	close(local.release)

	waitForLifecycleCondition(t, ps, "peer retirement and topic cleanup", func() bool {
		_, inRegistry := ps.peerComm.Lookup(remote.ID())
		_, inTopic := ps.topics[topicID][remote.ID()]
		gs := ps.rt.(*GossipSubRouter)
		_, hasExtensions := gs.extensions.peers[remote.ID()]
		return !inRegistry && !inTopic && !hasExtensions
	})
	select {
	case err := <-reset:
		if !errors.Is(err, network.ErrReset) {
			t.Fatalf("inbound read error = %v, want stream reset", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inbound stream reset")
	}

	eventCtx, eventCancel = context.WithTimeout(ctx, 5*time.Second)
	leave, err := events.NextPeerEvent(eventCtx)
	eventCancel()
	if err != nil {
		t.Fatal(err)
	}
	if leave.Type != PeerLeave || leave.Peer != join.Peer {
		t.Fatalf("second event = %+v, want matching PeerLeave for %s", leave, join.Peer)
	}
	eventCtx, eventCancel = context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = events.NextPeerEvent(eventCtx)
	eventCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected additional peer event: %v", err)
	}

	if got := tracer.closed.Load(); got != 0 {
		t.Fatalf("OnClosedOutboundStream calls = %d, want 0", got)
	}
	if got := local.calls.Load(); got != 1 {
		t.Fatalf("outbound open attempts = %d, want 1", got)
	}
	if connected := local.Network().Connectedness(remote.ID()); connected != network.Connected {
		t.Fatalf("host connectedness = %s, want connected", connected)
	}
}

func TestInitialOutboundOpenFailureWithoutInboundSubscriptionEmitsNoPeerLeave(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps, local, remote, _ := newOutboundOpenFailureTest(t, ctx)
	topic, err := ps.Join("outbound-open-failure-no-subscription")
	if err != nil {
		t.Fatal(err)
	}
	events, err := topic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	defer events.Cancel()

	close(local.release)
	waitForLifecycleCondition(t, ps, "peer retirement", func() bool {
		_, inRegistry := ps.peerComm.Lookup(remote.ID())
		return !inRegistry
	})
	if got := local.calls.Load(); got != 1 {
		t.Fatalf("outbound open attempts = %d, want 1", got)
	}

	eventCtx, eventCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer eventCancel()
	if event, err := events.NextPeerEvent(eventCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected peer event %+v: %v", event, err)
	}
}

// lifecycleSkeletonGossipsub is a minimal gossipsub peer for testing peer
// lifecycle edge cases. It manages two streams:
//
//   - Stream A (inbound): the stream the local peer opened to us.
//     The local writes RPCs on it; handlePeerDead reads from it.
//
//   - Stream B (outbound): the stream we opened back to the local.
//     We write RPCs on it; the local's handleNewStream reads from it.
type lifecycleSkeletonGossipsub struct {
	h      skeletonHost
	outRPC <-chan *pb.RPC // RPCs received from local (read from stream A)
	inRPC  chan<- *pb.RPC // RPCs to send to local (written to stream B)

	ready      chan struct{}
	remotePeer peer.ID

	// closeInbound signals the stream A reader to exit and close the stream.
	closeInbound chan struct{}
}

func newLifecycleSkeletonGossipsub(ctx context.Context, h skeletonHost) *lifecycleSkeletonGossipsub {
	recvRPC := make(chan *pb.RPC, 16)
	sendRPC := make(chan *pb.RPC, 16)

	skel := &lifecycleSkeletonGossipsub{
		h:            h,
		outRPC:       recvRPC,
		inRPC:        sendRPC,
		ready:        make(chan struct{}),
		closeInbound: make(chan struct{}),
	}

	var once sync.Once

	h.SetStreamHandler(GossipSubID_v13, func(s network.Stream) {
		first := false
		once.Do(func() {
			first = true
			skel.remotePeer = s.Conn().RemotePeer()

			// Open stream B back to the local peer for sending RPCs.
			outboundStream, err := h.NewStream(context.Background(), skel.remotePeer, GossipSubID_v13)
			if err != nil {
				panic(err)
			}
			writeCtx, cancel := context.WithCancel(ctx)
			close(skel.ready)

			go func() {
				defer outboundStream.Close()
				defer cancel()
				w := msgio.NewVarintWriter(outboundStream)
				for {
					select {
					case <-writeCtx.Done():
						return
					case r := <-sendRPC:
						b, err := proto.Marshal(r)
						if err != nil {
							panic(err)
						}
						if err := w.WriteMsg(b); err != nil {
							return
						}
					}
				}
			}()
		})

		if !first {
			return
		}

		// When closeInbound is signaled, reset stream A from a separate
		// goroutine. This interrupts ReadMsg below AND causes
		// handlePeerDead's s.Read() on the local side to return an error
		// → notifyPeerDead → handleDeadPeers.
		go func() {
			<-skel.closeInbound
			s.Reset()
		}()

		// Read RPCs from the local peer on stream A.
		r := msgio.NewVarintReaderSize(s, DefaultMaxMessageSize)
		for {
			msgbytes, err := r.ReadMsg()
			if err != nil {
				r.ReleaseMsg(msgbytes)
				return
			}
			if len(msgbytes) == 0 {
				continue
			}
			rpc := new(pb.RPC)
			err = proto.Unmarshal(msgbytes, rpc)
			r.ReleaseMsg(msgbytes)
			if err != nil {
				return
			}
			recvRPC <- rpc
		}
	})

	return skel
}

func (s *lifecycleSkeletonGossipsub) WaitReady(t *testing.T) {
	t.Helper()
	select {
	case <-s.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for skeleton to be ready")
	}
}

// CloseInbound resets stream A (the one the local opened to us) and removes
// the stream handler so the local's reconnect attempt fails.
// Stream B (our outbound) stays open for sending RPCs.
func (s *lifecycleSkeletonGossipsub) CloseInbound() {
	close(s.closeInbound)
	s.h.RemoveStreamHandler(GossipSubID_v13)
}

// skeletonHost is the subset of host.Host used by the skeleton.
type skeletonHost interface {
	ID() peer.ID
	SetStreamHandler(protocol.ID, network.StreamHandler)
	RemoveStreamHandler(protocol.ID)
	NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error)
}

// TestNoLeakFromDisconnectedPeer demonstrates the race condition where:
//  1. Two peers are connected. The local runs full gossipsub, the remote
//     runs the skeleton for manual control.
//  2. The remote resets stream A (the local's outbound stream) and removes
//     its stream handler. handlePeerDead fires on the local side.
//     handleDeadPeers removes the peer, sees the connection is still alive,
//     and tries to reconnect — but the reconnect fails because the remote
//     no longer has a stream handler. The peer ends up fully removed.
//  3. The remote sends a subscription RPC on stream B (still open).
//     handleNewStream on the local reads it and pushes it onto ps.incoming.
//  4. The remote disconnects.
//  5. The processLoop picks up the stale RPC and re-adds the disconnected
//     peer to the topic subscription map — leaked state never cleaned up.
func TestNoLeakFromDisconnectedPeer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		localHost := hosts[0]
		remoteHost := hosts[1]

		ps := getGossipsub(ctx, localHost, WithMessageSignaturePolicy(StrictNoSign))
		gs := ps.rt.(*GossipSubRouter)

		skel := newLifecycleSkeletonGossipsub(ctx, remoteHost)

		connect(t, localHost, remoteHost)
		skel.WaitReady(t)

		// Wait for local gossipsub to fully add the remote peer.
		waitForCond := func(desc string, cond func() bool) {
			t.Helper()
			deadline := time.Now().Add(5 * time.Second)
			for {
				ch := make(chan bool, 1)
				ps.eval <- func() { ch <- cond() }
				if <-ch {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("timed out: %s", desc)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		waitForCond("peer added", func() bool {
			_, inPeers := ps.peerComm.Lookup(remoteHost.ID())
			_, inRouter := gs.peers[remoteHost.ID()]
			return inPeers && inRouter
		})

		topic := "test-topic"

		// --- Step 1: Remote resets stream A and blocks reconnection. ---
		skel.CloseInbound()

		// Wait for the local to fully remove the peer. handleDeadPeers removes
		// it, tries to reconnect (fails because handler is gone), and newPeerError
		// cleans up the peer registry.
		waitForCond("peer removed", func() bool {
			_, inPeers := ps.peerComm.Lookup(remoteHost.ID())
			_, inGossipsub := gs.peers[remoteHost.ID()]
			return !inPeers && !inGossipsub
		})

		// --- Step 2: Remote sends a subscription RPC on stream B. ---
		skel.inRPC <- &pb.RPC{
			Subscriptions: []*pb.RPC_SubOpts{
				{
					Topicid:   proto.String(topic),
					Subscribe: proto.Bool(true),
				},
			},
		}

		// Give time for the RPC to be read by handleNewStream and processed.
		time.Sleep(500 * time.Millisecond)

		// --- Step 3: Remote disconnects. ---
		remoteHost.Network().ClosePeer(localHost.ID())

		// Wait for the disconnect to fully propagate through the local's
		// processLoop. Any dead-peer handling from the closed connection will
		// have completed by then.
		time.Sleep(time.Second)

		// --- Step 4: Observe leaked state. ---
		// The peer is not in the registry (it was removed and the reconnect failed),
		// but the stale RPC re-added it to ps.topics. Since the peer is not in
		// the registry, handleDeadPeers will never clean it up — it checks registry membership
		// first and skips unknown peers. This is permanently leaked state.
		checkDone := make(chan struct{})
		ps.eval <- func() {
			defer close(checkDone)

			_, inPeers := ps.peerComm.Lookup(remoteHost.ID())
			_, inTopics := ps.topics[topic][remoteHost.ID()]

			if inPeers || inTopics {
				t.Error("BUG: disconnected peer has leaked topic subscription state that will never be cleaned up")
			}
		}
		<-checkDone
	})
}
