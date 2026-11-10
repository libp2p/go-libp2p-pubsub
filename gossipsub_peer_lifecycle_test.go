package pubsub

import (
	"context"
	"fmt"
	"slices"
	"sync"
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
			_, inPeers := ps.peers[remoteHost.ID()]
			_, inRouter := gs.peers[remoteHost.ID()]
			return inPeers && inRouter
		})

		topic := "test-topic"

		// --- Step 1: Remote resets stream A and blocks reconnection. ---
		skel.CloseInbound()

		// Wait for the local to fully remove the peer. handleDeadPeers removes
		// it, tries to reconnect (fails because handler is gone), and newPeerError
		// cleans up ps.peers.
		waitForCond("peer removed", func() bool {
			_, inPeers := ps.peers[remoteHost.ID()]
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
		// The peer is not in ps.peers (it was removed and the reconnect failed),
		// but the stale RPC re-added it to ps.topics. Since the peer is not in
		// ps.peers, handleDeadPeers will never clean it up — it checks ps.peers
		// first and skips unknown peers. This is permanently leaked state.
		checkDone := make(chan struct{})
		ps.eval <- func() {
			defer close(checkDone)

			_, inPeers := ps.peers[remoteHost.ID()]
			_, inTopics := ps.topics[topic][remoteHost.ID()]

			if inPeers || inTopics {
				t.Error("BUG: disconnected peer has leaked topic subscription state that will never be cleaned up")
			}
		}
		<-checkDone
	})
}

// A peer announces its subscriptions on its own stream to us. When our stream
// to it is reset while the connection stays up, we open a new one, and the
// peer must keep counting as subscribed: its stream to us is still open and it
// will not announce its subscriptions again.
func TestGossipsubKeepsSubscriptionsWhenOutboundStreamIsReset(t *testing.T) {
	for _, flood := range []bool{true, false} {
		t.Run(fmt.Sprintf("flood publish %v", flood), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts, psubs, topics, subs := subscribedGossipsubPair(t, ctx, WithFloodPublish(flood))

			reset := resetGossipsubWriter(t, hosts[0], hosts[1])
			waitUntil(t, "host 0 never reopened its stream to host 1", func() bool {
				s := gossipsubWriter(hosts[0], hosts[1])
				return s != nil && s != reset
			})
			if !slices.Contains(psubs[0].ListPeers(subscriptionTestTopic), hosts[1].ID()) {
				t.Fatal("host 0 no longer counts host 1 as subscribed after reopening its stream")
			}
			expectDelivery(t, ctx, topics[0], subs[1])
		})
	}
}

// When we cannot open a new stream to such a peer at first, and manage to
// later, for example after identify runs on a new connection, the peer must
// count as subscribed again: it still will not repeat its subscriptions. This
// covers a respawned writer whose stream fails to open.
func TestGossipsubKeepsSubscriptionsWhenOutboundStreamReopensLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts, psubs, topics, subs := subscribedGossipsubPair(t, ctx)

	// Host 1 keeps its stream to host 0 but refuses new gossipsub streams.
	var removed []protocol.ID
	for _, id := range hosts[1].Mux().Protocols() {
		if slices.Contains(GossipSubDefaultProtocols, id) {
			hosts[1].RemoveStreamHandler(id)
			removed = append(removed, id)
		}
	}
	resetGossipsubWriter(t, hosts[0], hosts[1])
	waitUntil(t, "host 0 kept a writer to host 1 although no stream could open", func() bool {
		return !hasWriter(psubs[0], hosts[1].ID())
	})
	if enoughPeers(psubs[0], subscriptionTestTopic, 1) {
		t.Fatal("host 1 counts towards enough peers although host 0 has no stream to it")
	}

	for _, id := range removed {
		hosts[1].SetStreamHandler(id, psubs[1].handleNewStream)
	}
	// What identify on a new connection, or an identify push, does.
	psubs[0].notifyNewPeer(hosts[1].ID())
	waitUntil(t, "host 0 never reopened its stream to host 1", func() bool {
		return gossipsubWriter(hosts[0], hosts[1]) != nil
	})
	waitUntil(t, "host 0 does not count host 1 as subscribed after reopening its stream", func() bool {
		return slices.Contains(psubs[0].ListPeers(subscriptionTestTopic), hosts[1].ID())
	})
	expectDelivery(t, ctx, topics[0], subs[1])
}

// When the dead-peer backoff gives up on a still-connected peer, the peer
// keeps its subscriptions, and a writer opened later finds it subscribed.
func TestGossipsubKeepsSubscriptionsWhenBackoffGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts, psubs, topics, subs := subscribedGossipsubPair(t, ctx)

	for i := 0; hasWriter(psubs[0], hosts[1].ID()); i++ {
		if i > MaxBackoffAttempts {
			t.Fatalf("host 0 still respawns its writer after %d resets", i)
		}
		reset := resetGossipsubWriter(t, hosts[0], hosts[1])
		waitUntil(t, "host 0 neither reopened nor gave up on its stream to host 1", func() bool {
			s := gossipsubWriter(hosts[0], hosts[1])
			return (s != nil && s != reset) || !hasWriter(psubs[0], hosts[1].ID())
		})
	}
	if !inTopic(psubs[0], subscriptionTestTopic, hosts[1].ID()) {
		t.Fatal("host 0 dropped host 1's subscriptions when its backoff gave up")
	}
	if enoughPeers(psubs[0], subscriptionTestTopic, 1) {
		t.Fatal("host 1 counts towards enough peers although host 0 has no stream to it")
	}

	psubs[0].notifyNewPeer(hosts[1].ID())
	waitUntil(t, "host 0 does not count host 1 as subscribed after reopening its stream", func() bool {
		return slices.Contains(psubs[0].ListPeers(subscriptionTestTopic), hosts[1].ID())
	})
	expectDelivery(t, ctx, topics[0], subs[1])
}

// Blacklisting a subscribed peer clears its subscriptions also when we have no
// stream to it.
func TestGossipsubBlacklistClearsSubscriptionsOfPeerWithoutWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts, psubs, _, _ := subscribedGossipsubPair(t, ctx)

	for _, id := range hosts[1].Mux().Protocols() {
		if slices.Contains(GossipSubDefaultProtocols, id) {
			hosts[1].RemoveStreamHandler(id)
		}
	}
	resetGossipsubWriter(t, hosts[0], hosts[1])
	waitUntil(t, "host 0 kept a writer to host 1 although no stream could open", func() bool {
		return !hasWriter(psubs[0], hosts[1].ID())
	})

	psubs[0].BlacklistPeer(hosts[1].ID())
	waitUntil(t, "host 0 keeps a blacklisted peer subscribed", func() bool {
		return !inTopic(psubs[0], subscriptionTestTopic, hosts[1].ID())
	})
}

const subscriptionTestTopic = "subscriptions"

// onProcessLoop runs f on ps's event loop, where its state may be read.
func onProcessLoop[T any](ps *PubSub, f func() T) T {
	res := make(chan T, 1)
	ps.eval <- func() { res <- f() }
	return <-res
}

func hasWriter(ps *PubSub, p peer.ID) bool {
	return onProcessLoop(ps, func() bool { _, ok := ps.peers[p]; return ok })
}

func inTopic(ps *PubSub, topic string, p peer.ID) bool {
	return onProcessLoop(ps, func() bool { _, ok := ps.topics[topic][p]; return ok })
}

func enoughPeers(ps *PubSub, topic string, suggested int) bool {
	return onProcessLoop(ps, func() bool { return ps.rt.EnoughPeers(topic, suggested) })
}

func subscribedGossipsubPair(t *testing.T, ctx context.Context, opts ...Option) ([]host.Host, []*PubSub, []*Topic, []*Subscription) {
	t.Helper()
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts, opts...)
	connect(t, hosts[0], hosts[1])

	var topics []*Topic
	var subs []*Subscription
	for _, ps := range psubs {
		tp, err := ps.Join(subscriptionTestTopic)
		if err != nil {
			t.Fatal(err)
		}
		sub, err := tp.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, tp)
		subs = append(subs, sub)
	}
	waitUntil(t, "the hosts never saw each other's subscriptions", func() bool {
		return slices.Contains(psubs[0].ListPeers(subscriptionTestTopic), hosts[1].ID()) &&
			slices.Contains(psubs[1].ListPeers(subscriptionTestTopic), hosts[0].ID())
	})
	return hosts, psubs, topics, subs
}

func waitUntil(t *testing.T, failure string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gossipsubWriter returns from's outbound gossipsub stream to to, or nil.
func gossipsubWriter(from, to host.Host) network.Stream {
	for _, c := range from.Network().ConnsToPeer(to.ID()) {
		for _, s := range c.GetStreams() {
			if slices.Contains(GossipSubDefaultProtocols, s.Protocol()) && s.Stat().Direction == network.DirOutbound {
				return s
			}
		}
	}
	return nil
}

func resetGossipsubWriter(t *testing.T, from, to host.Host) network.Stream {
	t.Helper()
	s := gossipsubWriter(from, to)
	if s == nil {
		t.Fatal("no outbound gossipsub stream to reset")
	}
	s.Reset()
	return s
}

// expectDelivery publishes on from until sub receives a message. Without flood
// publish the router adds the peer back to its mesh at its next heartbeat, so
// a message published right away can miss it.
func expectDelivery(t *testing.T, ctx context.Context, from *Topic, sub *Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() {
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				from.Publish(ctx, []byte("after reset"))
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatalf("no message arrived after the reset: %v", err)
		}
		if string(msg.Data) == "after reset" {
			return
		}
	}
}
