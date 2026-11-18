package pubsub

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/peer"
)

func subRPC(from peer.ID, subscribe bool, topics ...string) *RPC {
	opts := make([]*pb.RPC_SubOpts, len(topics))
	for i := range topics {
		opts[i] = &pb.RPC_SubOpts{Topicid: &topics[i], Subscribe: &subscribe}
	}
	return &RPC{RPC: pb.RPC{Subscriptions: opts}, from: from}
}

func peerTopicCount(ps *PubSub, pid peer.ID) int {
	n := 0
	for _, tmap := range ps.topics {
		if _, ok := tmap[pid]; ok {
			n++
		}
	}
	return n
}

// One small SUBSCRIBE per RPC must not grow p.topics past the per-peer cap.
func TestMaxSubscriptionsPerPeer(t *testing.T) {
	const limit = 5
	noisy, honest := peer.ID("noisy"), peer.ID("honest")
	ps := &PubSub{
		rt:                      &FloodSubRouter{},
		topics:                  make(map[string]map[peer.ID]peerTopicState),
		peerSubCounts:           make(map[peer.ID]int),
		myTopics:                make(map[string]*Topic),
		maxSubscriptionsPerPeer: limit,
	}

	for i := range 1000 {
		ps.handleIncomingRPC(subRPC(noisy, true, fmt.Sprintf("t%d", i)))
	}
	if got := peerTopicCount(ps, noisy); got != limit {
		t.Fatalf("noisy tracked in %d topics, want %d", got, limit)
	}
	if len(ps.topics) != limit {
		t.Fatalf("len(p.topics) = %d, want %d", len(ps.topics), limit)
	}

	// Re-subscribing to an already tracked topic at the cap is not a new membership.
	ps.handleIncomingRPC(subRPC(noisy, true, "t0"))
	if ps.peerSubCounts[noisy] != limit {
		t.Fatalf("count after re-subscribe = %d, want %d", ps.peerSubCounts[noisy], limit)
	}

	// The cap is per peer: another peer is unaffected.
	ps.handleIncomingRPC(subRPC(honest, true, "t999"))
	if peerTopicCount(ps, honest) != 1 {
		t.Fatal("honest peer's subscription was dropped")
	}

	// Unsubscribing frees a slot.
	ps.handleIncomingRPC(subRPC(noisy, false, "t0"))
	ps.handleIncomingRPC(subRPC(noisy, true, "fresh"))
	if _, ok := ps.topics["fresh"][noisy]; !ok {
		t.Fatal("subscription after unsubscribe was dropped")
	}
	if got := peerTopicCount(ps, noisy); got != ps.peerSubCounts[noisy] {
		t.Fatalf("peerSubCounts=%d disagrees with p.topics=%d", ps.peerSubCounts[noisy], got)
	}

	// Removing the peer clears both its memberships and its count.
	ps.clearPeerFromTopicsState(noisy)
	if _, ok := ps.peerSubCounts[noisy]; ok || peerTopicCount(ps, noisy) != 0 {
		t.Fatal("peer state not cleared on removal")
	}
}

func TestMaxTopicLength(t *testing.T) {
	pid := peer.ID("p")
	ps := &PubSub{
		rt:             &FloodSubRouter{},
		topics:         make(map[string]map[peer.ID]peerTopicState),
		peerSubCounts:  make(map[peer.ID]int),
		myTopics:       make(map[string]*Topic),
		maxTopicLength: 8,
	}
	ps.handleIncomingRPC(subRPC(pid, true, "12345678", "123456789"))
	if _, ok := ps.topics["12345678"][pid]; !ok {
		t.Fatal("topic at the limit was dropped")
	}
	if _, ok := ps.topics["123456789"]; ok {
		t.Fatal("topic over the limit was tracked")
	}
	if ps.peerSubCounts[pid] != 1 {
		t.Fatalf("count = %d, want 1", ps.peerSubCounts[pid])
	}
}

func TestSubscriptionLimitDefaults(t *testing.T) {
	hosts := getDefaultHosts(t, 5)
	allow := NewAllowlistSubscriptionFilter("a")
	for _, tc := range []struct {
		name              string
		opts              []Option
		wantSubs, wantLen int
	}{
		{"no filter", nil, DefaultMaxSubscriptionsPerPeer, DefaultMaxTopicLength},
		{"filter set", []Option{WithSubscriptionFilter(allow)}, 0, 0},
		{"filter + explicit", []Option{WithSubscriptionFilter(allow), WithMaxSubscriptionsPerPeer(500), WithMaxTopicLength(256)}, 500, 256},
		{"explicit zero", []Option{WithMaxSubscriptionsPerPeer(0), WithMaxTopicLength(0)}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := hosts[0]
			hosts = hosts[1:]
			ps, err := NewFloodSub(context.Background(), h, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if ps.maxSubscriptionsPerPeer != tc.wantSubs || ps.maxTopicLength != tc.wantLen {
				t.Fatalf("limits = (%d, %d), want (%d, %d)", ps.maxSubscriptionsPerPeer, ps.maxTopicLength, tc.wantSubs, tc.wantLen)
			}
		})
	}
	for _, opt := range []Option{WithMaxSubscriptionsPerPeer(-1), WithMaxTopicLength(-1)} {
		if _, err := NewFloodSub(context.Background(), hosts[0], opt); err == nil {
			t.Fatal("negative limit accepted")
		}
	}
}
