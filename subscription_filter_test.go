package pubsub

import (
	"context"
	"regexp"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestBasicSubscriptionFilter(t *testing.T) {
	peerA := peer.ID("A")

	topic1 := "test1"
	topic2 := "test2"
	topic3 := "test3"
	yes := true
	subs := []*pb.RPC_SubOpts{
		{
			Topicid:   &topic1,
			Subscribe: &yes,
		},
		{
			Topicid:   &topic2,
			Subscribe: &yes,
		},
		{
			Topicid:   &topic3,
			Subscribe: &yes,
		},
	}

	filter := NewAllowlistSubscriptionFilter(topic1, topic2)
	canSubscribe := filter.CanSubscribe(topic1)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic2)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic3)
	if canSubscribe {
		t.Fatal("expected disallowed subscription")
	}
	allowedSubs, err := filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 2 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 {
			t.Fatal("unpexted subscription to test3")
		}
	}

	limitFilter := WrapLimitSubscriptionFilter(filter, 2)
	_, err = limitFilter.FilterIncomingSubscriptions(peerA, subs)
	if err != ErrTooManySubscriptions {
		t.Fatal("expected rejection because of too many subscriptions")
	}

	filter = NewRegexpSubscriptionFilter(regexp.MustCompile("test[12]"))
	canSubscribe = filter.CanSubscribe(topic1)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic2)
	if !canSubscribe {
		t.Fatal("expected allowed subscription")
	}
	canSubscribe = filter.CanSubscribe(topic3)
	if canSubscribe {
		t.Fatal("expected disallowed subscription")
	}
	allowedSubs, err = filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 2 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 {
			t.Fatal("unexpected subscription")
		}
	}

	limitFilter = WrapLimitSubscriptionFilter(filter, 2)
	_, err = limitFilter.FilterIncomingSubscriptions(peerA, subs)
	if err != ErrTooManySubscriptions {
		t.Fatal("expected rejection because of too many subscriptions")
	}

}

func TestSubscriptionFilterDeduplication(t *testing.T) {
	peerA := peer.ID("A")

	topic1 := "test1"
	topic2 := "test2"
	topic3 := "test3"
	yes := true
	no := false
	subs := []*pb.RPC_SubOpts{
		{
			Topicid:   &topic1,
			Subscribe: &yes,
		},
		{
			Topicid:   &topic1,
			Subscribe: &yes,
		},

		{
			Topicid:   &topic2,
			Subscribe: &yes,
		},
		{
			Topicid:   &topic2,
			Subscribe: &no,
		},
		{
			Topicid:   &topic3,
			Subscribe: &yes,
		},
	}

	filter := NewAllowlistSubscriptionFilter(topic1, topic2)
	allowedSubs, err := filter.FilterIncomingSubscriptions(peerA, subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(allowedSubs) != 1 {
		t.Fatalf("expected 2 allowed subscriptions but got %d", len(allowedSubs))
	}
	for _, sub := range allowedSubs {
		if sub.GetTopicid() == topic3 || sub.GetTopicid() == topic2 {
			t.Fatal("unexpected subscription")
		}
	}
}

func TestSubscriptionFilterRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 2)
	ps1 := getPubsub(ctx, hosts[0], WithSubscriptionFilter(NewAllowlistSubscriptionFilter("test1", "test2")))
	ps2 := getPubsub(ctx, hosts[1], WithSubscriptionFilter(NewAllowlistSubscriptionFilter("test2", "test3")))

	_ = mustSubscribe(t, ps1, "test1")
	_ = mustSubscribe(t, ps1, "test2")
	_ = mustSubscribe(t, ps2, "test2")
	_ = mustSubscribe(t, ps2, "test3")

	// check the rejection as well
	_, err := ps1.Join("test3")
	if err == nil {
		t.Fatal("expected subscription error")
	}

	connect(t, hosts[0], hosts[1])

	time.Sleep(time.Second)

	var sub1, sub2, sub3 bool
	ready := make(chan struct{})

	ps1.eval <- func() {
		_, sub1 = ps1.topics["test1"][hosts[1].ID()]
		_, sub2 = ps1.topics["test2"][hosts[1].ID()]
		_, sub3 = ps1.topics["test3"][hosts[1].ID()]
		ready <- struct{}{}
	}
	<-ready

	if sub1 {
		t.Fatal("expected no subscription for test1")
	}
	if !sub2 {
		t.Fatal("expected subscription for test2")
	}
	if sub3 {
		t.Fatal("expected no subscription for test1")
	}

	ps2.eval <- func() {
		_, sub1 = ps2.topics["test1"][hosts[0].ID()]
		_, sub2 = ps2.topics["test2"][hosts[0].ID()]
		_, sub3 = ps2.topics["test3"][hosts[0].ID()]
		ready <- struct{}{}
	}
	<-ready

	if sub1 {
		t.Fatal("expected no subscription for test1")
	}
	if !sub2 {
		t.Fatal("expected subscription for test1")
	}
	if sub3 {
		t.Fatal("expected no subscription for test1")
	}
}
