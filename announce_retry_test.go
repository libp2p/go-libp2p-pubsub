package pubsub

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const announceTestPeer = peer.ID("announce-test-peer")

// announceHarness drives the announce retry machinery against a single peer with
// a controllable outbound queue. It stands in for processLoop: every closure sent
// to ps.eval runs here, and evals counts them, so tests can assert on how much
// work the retry path pushes through processLoop.
//
// Because every test runs inside a synctest bubble, and synctest.Test waits for
// all bubble goroutines to exit, each test also implicitly asserts that the retry
// loop shuts down when the pubsub context is cancelled.
type announceHarness struct {
	t     *testing.T
	ps    *PubSub
	pid   peer.ID
	queue *rpcQueue

	// evals counts the closures the retry loop pushed through processLoop.
	// Closures submitted by the harness itself cancel out their own increment.
	evals atomic.Int64

	pumped chan struct{}
}

func newAnnounceHarness(t *testing.T, queueSize int) *announceHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h := &announceHarness{
		t:      t,
		pid:    announceTestPeer,
		queue:  newRpcQueue(queueSize),
		pumped: make(chan struct{}),
	}
	h.ps = &PubSub{
		ctx:             ctx,
		eval:            make(chan func()),
		peers:           map[peer.ID]*rpcQueue{h.pid: h.queue},
		myTopics:        make(map[string]*Topic),
		mySubs:          make(map[string]map[*Subscription]struct{}),
		myRelays:        make(map[string]int),
		pendingAnnounce: make(map[peer.ID]map[string]bool),
		logger:          slog.New(slog.DiscardHandler),
	}

	go func() {
		defer close(h.pumped)
		for {
			select {
			case thunk := <-h.ps.eval:
				h.evals.Add(1)
				thunk()
			case <-ctx.Done():
				return
			}
		}
	}()

	t.Cleanup(func() {
		cancel()
		<-h.pumped
	})

	return h
}

// do runs f on the processLoop stand-in, which owns all of the announce state.
func (h *announceHarness) do(f func()) {
	done := make(chan struct{})
	h.ps.eval <- func() {
		h.evals.Add(-1) // harness traffic is not retry work
		f()
		close(done)
	}
	<-done
}

// subscribe and unsubscribe edit local subscription state directly; the real
// Subscribe path needs a live processLoop, which this harness deliberately does
// not run.
func (h *announceHarness) subscribe(topics ...string) {
	h.do(func() {
		for _, topic := range topics {
			h.ps.mySubs[topic] = map[*Subscription]struct{}{}
		}
	})
}

func (h *announceHarness) unsubscribe(topics ...string) {
	h.do(func() {
		for _, topic := range topics {
			delete(h.ps.mySubs, topic)
		}
	})
}

func (h *announceHarness) announce(topic string, sub bool) {
	h.do(func() { h.ps.announce(topic, sub) })
}

// pendingTopics snapshots the peer's pending announcements as topic -> subscribe.
func (h *announceHarness) pendingTopics() map[string]bool {
	out := map[string]bool{}
	h.do(func() {
		for topic, sub := range h.ps.pendingAnnounce[h.pid] {
			out[topic] = sub
		}
	})
	return out
}

// fill saturates the peer's outbound queue so every subsequent Push fails.
func (h *announceHarness) fill() {
	h.t.Helper()
	for h.queue.Len() < h.queue.maxSize {
		if err := h.queue.Push(rpcWithMessages(), false); err != nil {
			h.t.Fatalf("filling queue: %v", err)
		}
	}
}

// drain empties the queue, freeing it for retries, and returns what was in it.
func (h *announceHarness) drain() []*RPC {
	h.t.Helper()
	var out []*RPC
	for h.queue.Len() > 0 {
		rpc, err := h.queue.Pop(h.ps.ctx)
		if err != nil {
			h.t.Fatalf("popping queue: %v", err)
		}
		out = append(out, rpc)
	}
	return out
}

// announcements picks the subscription RPCs out of a drained queue; the filler
// RPCs carry no subscriptions.
func announcements(rpcs []*RPC) []*RPC {
	var out []*RPC
	for _, rpc := range rpcs {
		if len(rpc.GetSubscriptions()) > 0 {
			out = append(out, rpc)
		}
	}
	return out
}

func subsOf(rpc *RPC) map[string]bool {
	out := map[string]bool{}
	for _, s := range rpc.GetSubscriptions() {
		out[s.GetTopicid()] = s.GetSubscribe()
	}
	return out
}

// assertAnnounceRetryStopped fails if the retry loop is still pushing work
// through processLoop.
func (h *announceHarness) assertRetryStopped() {
	h.t.Helper()
	before := h.evals.Load()
	time.Sleep(4 * announceRetryMaxBackoff)
	synctest.Wait()
	if after := h.evals.Load(); after != before {
		h.t.Fatalf("announce retry loop still running: %d extra processLoop evals", after-before)
	}
}

// TestAnnounceRetryDeliversAfterProlongedCongestion pins down the property that
// matters. Subscriptions are propagated as deltas and the full set is only ever
// resent in the hello packet of a new outbound stream, so an announcement that
// is abandoned leaves the peer with a stale view of our topics for the lifetime
// of that stream. Congestion lasting minutes must therefore still converge.
func TestAnnounceRetryDeliversAfterProlongedCongestion(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newAnnounceHarness(t, 1)
		h.fill()
		h.subscribe("test")
		h.announce("test", true)

		if got := h.pendingTopics(); len(got) != 1 || !got["test"] {
			t.Fatalf("expected a pending subscribe for \"test\", got %v", got)
		}

		// Stay congested far longer than any fixed attempt budget would survive.
		time.Sleep(5 * time.Minute)
		synctest.Wait()

		// Nothing got through, and the announcement is still outstanding.
		// Draining also frees the queue for the next retry.
		if got := announcements(h.drain()); len(got) != 0 {
			t.Fatalf("expected nothing delivered while congested, got %d announcements", len(got))
		}
		if got := h.pendingTopics(); len(got) != 1 || !got["test"] {
			t.Fatalf("expected subscribe for \"test\" still pending after 5m, got %v", got)
		}

		time.Sleep(2 * announceRetryMaxBackoff)
		synctest.Wait()

		got := announcements(h.drain())
		if len(got) != 1 {
			t.Fatalf("expected 1 announcement once congestion cleared, got %d", len(got))
		}
		if subs := subsOf(got[0]); len(subs) != 1 || !subs["test"] {
			t.Fatalf("expected subscribe for \"test\", got %v", subs)
		}
		if got := h.pendingTopics(); len(got) != 0 {
			t.Fatalf("expected nothing pending after delivery, got %v", got)
		}
		h.assertRetryStopped()
	})
}

// TestAnnounceRetryCoalescesPerPeer checks the cost side: however many topics
// fail to announce, a peer accumulates one pending set served by one retry loop,
// and they are delivered as a single RPC.
func TestAnnounceRetryCoalescesPerPeer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		topics := []string{"a", "b", "c", "d", "e"}

		// Room for a separate RPC per topic, so uncoalesced retries would show up
		// as one announcement each.
		h := newAnnounceHarness(t, len(topics)+1)
		h.fill()
		h.subscribe(topics...)
		for _, topic := range topics {
			h.announce(topic, true)
		}

		h.do(func() {
			if got := len(h.ps.pendingAnnounce); got != 1 {
				t.Errorf("expected pending announcements for 1 peer, got %d", got)
			}
		})
		if got := h.pendingTopics(); len(got) != len(topics) {
			t.Fatalf("expected %d pending topics, got %v", len(topics), got)
		}

		h.drain()
		time.Sleep(2 * announceRetryMaxBackoff)
		synctest.Wait()

		got := announcements(h.drain())
		if len(got) != 1 {
			t.Fatalf("expected the pending announcements coalesced into 1 RPC, got %d", len(got))
		}
		subs := subsOf(got[0])
		if len(subs) != len(topics) {
			t.Fatalf("expected %d subscriptions in the coalesced RPC, got %v", len(topics), subs)
		}
		for _, topic := range topics {
			if !subs[topic] {
				t.Errorf("coalesced RPC missing subscribe for %q: %v", topic, subs)
			}
		}
		h.assertRetryStopped()
	})
}

// TestAnnounceRetrySupersedesEarlierState checks that a peer is only ever told
// our latest intent for a topic, never a superseded one.
func TestAnnounceRetrySupersedesEarlierState(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newAnnounceHarness(t, 4)
		h.fill()

		h.subscribe("test")
		h.announce("test", true)
		h.unsubscribe("test")
		h.announce("test", false)

		if got := h.pendingTopics(); len(got) != 1 || got["test"] {
			t.Fatalf("expected a single pending unsubscribe for \"test\", got %v", got)
		}

		h.drain()
		time.Sleep(2 * announceRetryMaxBackoff)
		synctest.Wait()

		got := announcements(h.drain())
		if len(got) != 1 {
			t.Fatalf("expected 1 announcement, got %d", len(got))
		}
		subs := subsOf(got[0])
		if len(subs) != 1 {
			t.Fatalf("expected 1 subscription entry, got %v", subs)
		}
		if subs["test"] {
			t.Fatalf("expected the superseded subscribe replaced by an unsubscribe, got %v", subs)
		}
	})
}

// TestAnnounceRetryDiscardsStaleAnnouncement keeps the staleness check the
// per-topic retry used to perform: a queued announcement that no longer matches
// local state is dropped rather than sent.
func TestAnnounceRetryDiscardsStaleAnnouncement(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newAnnounceHarness(t, 4)
		h.fill()
		h.subscribe("test")
		h.announce("test", true)

		// Leave the topic without announcing it: the queued subscribe no longer
		// describes our state.
		h.unsubscribe("test")

		h.drain()
		time.Sleep(2 * announceRetryMaxBackoff)
		synctest.Wait()

		if got := announcements(h.drain()); len(got) != 0 {
			t.Fatalf("expected the stale subscribe discarded, got %d announcements", len(got))
		}
		if got := h.pendingTopics(); len(got) != 0 {
			t.Fatalf("expected nothing pending, got %v", got)
		}
		h.assertRetryStopped()
	})
}

// TestAnnounceRetryStopsWhenPeerDisconnects checks that a departed peer's retry
// state and goroutine are reclaimed.
func TestAnnounceRetryStopsWhenPeerDisconnects(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newAnnounceHarness(t, 1)
		h.fill()
		h.subscribe("test")
		h.announce("test", true)

		h.do(func() { delete(h.ps.peers, h.pid) })

		time.Sleep(2 * announceRetryMaxBackoff)
		synctest.Wait()

		if got := h.pendingTopics(); len(got) != 0 {
			t.Fatalf("expected pending announcements cleared for a departed peer, got %v", got)
		}
		h.assertRetryStopped()
	})
}

// TestAnnounceRetryBacksOff bounds what a permanently congested peer costs
// processLoop.
func TestAnnounceRetryBacksOff(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		h := newAnnounceHarness(t, 1)
		h.fill()
		h.subscribe("test")
		h.announce("test", true)

		const window = 10 * time.Minute

		before := h.evals.Load()
		time.Sleep(window)
		synctest.Wait()
		flushes := h.evals.Load() - before

		// The backoff doubles from at most announceRetryInitialBackoff up to
		// announceRetryMaxBackoff, so the window admits roughly
		// window/announceRetryMaxBackoff flushes plus the short attempts made
		// while it ramps. Without any backoff the same window would admit
		// ~window/announceRetryInitialBackoff, an order of magnitude more.
		maxFlushes := int64(window/announceRetryMaxBackoff) + 20
		unbounded := int64(window / announceRetryInitialBackoff)

		if flushes == 0 {
			t.Fatal("expected the retry loop to keep retrying while congested")
		}
		if flushes > maxFlushes {
			t.Fatalf("expected at most %d retry flushes in %v, got %d", maxFlushes, window, flushes)
		}
		if flushes >= unbounded/2 {
			t.Fatalf("retry rate looks unbounded: %d flushes in %v (no-backoff rate would be ~%d)",
				flushes, window, unbounded)
		}
	})
}
