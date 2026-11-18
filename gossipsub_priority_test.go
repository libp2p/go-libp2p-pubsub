package pubsub

import (
	"context"
	"slices"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

const (
	priorityTestNormalTopic = "normal"
	priorityTestTopic       = "priority"
)

func priorityTestIsPriorityTopic(msg *Message) bool {
	return msg.GetTopic() == priorityTestTopic
}

type priorityHarness struct {
	ps     *PubSub
	gs     *GossipSubRouter
	remote peer.ID
	queue  *rpcQueue
}

// newPriorityHarness returns a gossipsub router with one remote peer in the
// mesh of both test topics. Nothing drains the queue of the remote peer, so
// tests can assert the order in which the router queued the RPCs.
func newPriorityHarness(t *testing.T, queueSize int, opts ...Option) *priorityHarness {
	hosts := getDefaultHosts(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ps := getGossipsub(ctx, hosts[0], opts...)
	h := &priorityHarness{
		ps:     ps,
		gs:     ps.rt.(*GossipSubRouter),
		remote: peer.ID("remote"),
		queue:  newRpcQueue(queueSize),
	}
	h.onEventLoop(func() {
		ps.peers[h.remote] = h.queue
		h.gs.peers[h.remote] = GossipSubID_v12
		for _, topic := range []string{priorityTestNormalTopic, priorityTestTopic} {
			ps.topics[topic] = map[peer.ID]peerTopicState{h.remote: {}}
			h.gs.mesh[topic] = map[peer.ID]struct{}{h.remote: {}}
		}
	})
	return h
}

func (h *priorityHarness) onEventLoop(fn func()) {
	done := make(chan struct{})
	h.ps.eval <- func() {
		defer close(done)
		fn()
	}
	<-done
}

func newPriorityTestMessage(topic, data string, receivedFrom peer.ID) *Message {
	return &Message{
		Message:      &pb.Message{Topic: &topic, Data: []byte(data)},
		ID:           data,
		ReceivedFrom: receivedFrom,
	}
}

// dequeued pops the remote peer's queue until it is empty and returns the data
// of the queued messages in pop order.
func (h *priorityHarness) dequeued() []string {
	h.queue.queueMu.Lock()
	defer h.queue.queueMu.Unlock()

	var rpcs []*RPC
	for rpc := h.queue.queue.Pop(); rpc != nil; rpc = h.queue.queue.Pop() {
		rpcs = append(rpcs, rpc)
	}
	return priorityTestPublishedData(rpcs)
}

// lanes returns the RPCs queued in the priority and the normal lane without
// popping them.
func (h *priorityHarness) lanes() (priority, normal []*RPC) {
	h.queue.queueMu.Lock()
	defer h.queue.queueMu.Unlock()

	return slices.Clone(h.queue.queue.priority), slices.Clone(h.queue.queue.normal)
}

func priorityTestRequireDequeued(t *testing.T, h *priorityHarness, want ...string) {
	t.Helper()
	if got := h.dequeued(); !slices.Equal(got, want) {
		t.Fatalf("got queue order %v, want %v", got, want)
	}
}

func TestGossipSubMessagePriorityPublish(t *testing.T) {
	for _, tc := range []struct {
		name      string
		forwarded bool
	}{
		{name: "local", forwarded: false},
		{name: "forwarded", forwarded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				h := newPriorityHarness(t, 10, WithMessagePriority(priorityTestIsPriorityTopic))

				h.onEventLoop(func() {
					receivedFrom := h.ps.host.ID()
					if tc.forwarded {
						receivedFrom = peer.ID("sender")
					}
					h.gs.Publish(newPriorityTestMessage(priorityTestNormalTopic, "normal", receivedFrom))
					h.gs.Publish(newPriorityTestMessage(priorityTestTopic, "priority", receivedFrom))
				})

				priorityTestRequireDequeued(t, h, "priority", "normal")
			})
		})
	}
}

func TestGossipSubMessagePriorityUnset(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 10)

		h.onEventLoop(func() {
			self := h.ps.host.ID()
			h.gs.Publish(newPriorityTestMessage(priorityTestNormalTopic, "normal", self))
			h.gs.Publish(newPriorityTestMessage(priorityTestTopic, "priority", self))
		})

		if priority, _ := h.lanes(); len(priority) != 0 {
			t.Fatalf("got %d RPCs in the priority lane, want 0", len(priority))
		}
		priorityTestRequireDequeued(t, h, "normal", "priority")
	})
}

func TestGossipSubMessagePriorityPublishBatchKeepsNormalQueue(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 10, WithMessagePriority(priorityTestIsPriorityTopic))

		h.onEventLoop(func() {
			h.gs.PublishBatch([]*Message{
				newPriorityTestMessage(priorityTestTopic, "priority", h.ps.host.ID()),
			}, &BatchPublishOptions{Strategy: &RoundRobinMessageIDScheduler{}})
		})

		priority, normal := h.lanes()
		if len(priority) != 0 {
			t.Fatalf("got %d RPCs in the priority lane, want 0", len(priority))
		}
		if got := priorityTestPublishedData(normal); !slices.Equal(got, []string{"priority"}) {
			t.Fatalf("got normal lane messages %v, want [priority]", got)
		}
	})
}

func priorityTestGraftTopics(rpcs []*RPC) []string {
	var topics []string
	for _, rpc := range rpcs {
		for _, graft := range rpc.GetControl().GetGraft() {
			topics = append(topics, graft.GetTopicID())
		}
	}
	return topics
}

func priorityTestPublishedData(rpcs []*RPC) []string {
	var data []string
	for _, rpc := range rpcs {
		for _, msg := range rpc.Publish {
			data = append(data, string(msg.Data))
		}
	}
	return data
}

func priorityTestPendGraft(h *priorityHarness) {
	topic := priorityTestNormalTopic
	h.gs.control[h.remote] = &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}
}

func TestGossipSubMessagePriorityKeepsPiggybackedControlNormal(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 10, WithMessagePriority(priorityTestIsPriorityTopic))

		h.onEventLoop(func() {
			priorityTestPendGraft(h)
			h.gs.Publish(newPriorityTestMessage(priorityTestTopic, "priority", h.ps.host.ID()))
		})

		priority, normal := h.lanes()
		if got := priorityTestPublishedData(priority); !slices.Equal(got, []string{"priority"}) {
			t.Fatalf("got priority lane messages %v, want [priority]", got)
		}
		if got := priorityTestGraftTopics(priority); len(got) != 0 {
			t.Fatalf("got GRAFT %v in the priority lane, want none", got)
		}
		if got := priorityTestGraftTopics(normal); !slices.Equal(got, []string{priorityTestNormalTopic}) {
			t.Fatalf("got GRAFT %v in the normal lane, want [%s]", got, priorityTestNormalTopic)
		}
	})
}

func TestGossipSubMessagePriorityPublishTakesLastSlotFromPiggybackedControl(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 1, WithMessagePriority(priorityTestIsPriorityTopic))

		h.onEventLoop(func() {
			priorityTestPendGraft(h)
			h.gs.Publish(newPriorityTestMessage(priorityTestTopic, "priority", h.ps.host.ID()))
		})

		priority, normal := h.lanes()
		if got := priorityTestPublishedData(priority); !slices.Equal(got, []string{"priority"}) {
			t.Fatalf("got priority lane messages %v, want [priority]", got)
		}
		if len(normal) != 0 {
			t.Fatalf("got %d RPCs in the normal lane, want 0", len(normal))
		}
	})
}

func TestGossipSubMessagePriorityKeepsIDontWantUrgent(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 10, WithMessagePriority(priorityTestIsPriorityTopic))

		h.onEventLoop(func() {
			priorityTestPendGraft(h)
			big := string(make([]byte, h.gs.params.IDontWantMessageThreshold))
			h.gs.Preprocess(peer.ID("sender"), []*Message{
				newPriorityTestMessage(priorityTestNormalTopic, big, peer.ID("sender")),
			})
		})

		priority, normal := h.lanes()
		if len(normal) != 0 {
			t.Fatalf("got %d RPCs in the normal lane, want 0", len(normal))
		}
		if got := priorityTestGraftTopics(priority); !slices.Equal(got, []string{priorityTestNormalTopic}) {
			t.Fatalf("got GRAFT %v in the priority lane, want [%s]", got, priorityTestNormalTopic)
		}
		idontwants := 0
		for _, rpc := range priority {
			idontwants += len(rpc.GetControl().GetIdontwant())
		}
		if idontwants != 1 {
			t.Fatalf("got %d IDONTWANT in the priority lane, want 1", idontwants)
		}
	})
}

func TestGossipSubMessagePriorityRespectsQueueSize(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newPriorityHarness(t, 1, WithMessagePriority(priorityTestIsPriorityTopic))

		h.onEventLoop(func() {
			self := h.ps.host.ID()
			h.gs.Publish(newPriorityTestMessage(priorityTestNormalTopic, "normal", self))
			h.gs.Publish(newPriorityTestMessage(priorityTestTopic, "priority", self))
		})

		priorityTestRequireDequeued(t, h, "normal")
	})
}

func TestWithMessagePriorityRequiresGossipSub(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		hosts := getDefaultHosts(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if _, err := NewFloodSub(ctx, hosts[0], WithMessagePriority(priorityTestIsPriorityTopic)); err == nil {
			t.Fatal("expected an error for a router that is not gossipsub")
		}
	})
}
