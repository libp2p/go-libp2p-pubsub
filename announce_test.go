package pubsub

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestAnnounceStorage(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-storage"
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// Host 1 subscribes
	_, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	// Host 0 announces
	payload := []byte("test storage")
	expiry := time.Now().Add(time.Second * 10)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the message is stored in host 0's message cache announcements
	gs0, ok := psubs[0].rt.(*GossipSubRouter)
	if !ok {
		t.Fatal("expected GossipSubRouter")
	}

	resultChan := make(chan int, 1)
	psubs[0].eval <- func() {
		// Count total announcements across all buckets in the wheel
		count := 0
		for _, bucket := range gs0.mcache.annWheel {
			count += len(bucket)
		}
		resultChan <- count
	}

	count := <-resultChan
	if count != 1 {
		t.Fatalf("expected 1 announcement stored, got %d", count)
	}
}

func TestAnnounceBasic(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce"
	hosts := getDefaultHosts(t, 3)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	// Get topics for all hosts
	topics := getTopics(psubs, topic)

	// Subscribe on host 1 and 2
	sub1, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	sub2, err := topics[2].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Wait for mesh to form and subscriptions to propagate
	time.Sleep(time.Second * 2)

	// Host 0 announces a message (not subscribed)
	payload := []byte("announced message")
	expiry := time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribers should receive the message via IWANT
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	msg1, err := sub1.Next(timeoutCtx)
	if err != nil {
		t.Fatalf("host 1 failed to receive message: %v", err)
	}
	if !bytes.Equal(msg1.Data, payload) {
		t.Fatalf("received incorrect message: got %s, want %s", msg1.Data, payload)
	}

	msg2, err := sub2.Next(timeoutCtx)
	if err != nil {
		t.Fatalf("host 2 failed to receive message: %v", err)
	}
	if !bytes.Equal(msg2.Data, payload) {
		t.Fatalf("received incorrect message: got %s, want %s", msg2.Data, payload)
	}
}

func TestAnnounceWhenSubscribed(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-subscribed"
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// Both hosts subscribe
	sub0, err := topics[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	// Host 0 announces while subscribed
	payload := []byte("announced while subscribed")
	expiry := time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Host 0 should NOT receive its own announcement (marked as seen)
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	msg, err := sub0.Next(timeoutCtx)
	if err != context.DeadlineExceeded {
		if msg != nil {
			t.Fatal("announcer should not receive own announcement when subscribed")
		}
		t.Fatalf("expected timeout, got error: %v", err)
	}

	// Host 1 should receive it
	msg1, err := sub1.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg1.Data, payload) {
		t.Fatalf("received incorrect message: got %s, want %s", msg1.Data, payload)
	}
}

func TestAnnounceDuplicate(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-duplicate"
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts, WithMessageIdFn(func(msg *pb.Message) string {
		// use a content addressed ID function
		return string(msg.Data)
	}))
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// Host 0 subscribes
	_, err := topics[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Host 1 subscribes
	sub1, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	payload := []byte("duplicate test")
	expiry := time.Now().Add(time.Second * 5)

	// First announcement should succeed
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatalf("first announce failed: %v", err)
	}

	// Host 1 receives the message
	msg1, err := sub1.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg1.Data, payload) {
		t.Fatal("received incorrect message")
	}

	// Try announcing the exact same payload again - this is a duplicate
	expiry = time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatalf("second announce failed: %v", err)
	}

	// Host 1 should NOT receive the duplicate message (it should be filtered)
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
	defer cancel()
	msg2, err := sub1.Next(timeoutCtx)
	if err != context.DeadlineExceeded {
		if msg2 != nil {
			t.Fatal("host 1 should not receive duplicate announcement")
		}
		t.Fatalf("expected timeout for duplicate message, got error: %v", err)
	}
}

func TestAnnounceExpiry(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-expiry"
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// Only host 1 subscribes
	_, err := topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	// Announce with very short expiry
	payload := []byte("expires soon")
	expiry := time.Now().Add(time.Millisecond * 100)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for expiry plus heartbeat
	time.Sleep(time.Millisecond*100 + time.Second*2)

	// Try to access the gossipsub router to verify cleanup
	gs0, ok := psubs[0].rt.(*GossipSubRouter)
	if !ok {
		t.Fatal("expected GossipSubRouter")
	}

	// Check that the announcement was cleaned up
	resultChan := make(chan int, 1)
	psubs[0].eval <- func() {
		// Count total announcements across all buckets in the wheel
		count := 0
		for _, bucket := range gs0.mcache.annWheel {
			count += len(bucket)
		}
		resultChan <- count
	}

	announcementCount := <-resultChan
	if announcementCount != 0 {
		t.Fatalf("expected 0 announcements after expiry, got %d", announcementCount)
	}
}

func TestAnnounceNoSubscribers(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-no-subs"
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// No one subscribes
	time.Sleep(time.Millisecond * 500)

	// Announce should succeed even without subscribers (it's a no-op)
	payload := []byte("no subscribers")
	expiry := time.Now().Add(time.Second * 5)
	err := topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Since no one is subscribed, the message is not stored and no IHAVE is sent
	_, err = topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 500)

	// Now announce another message - this one should be received
	payload2 := []byte("with subscriber")
	expiry2 := time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload2, expiry2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the announcement was stored
	gs0, ok := psubs[0].rt.(*GossipSubRouter)
	if !ok {
		t.Fatal("expected GossipSubRouter")
	}

	resultChan := make(chan int, 1)
	psubs[0].eval <- func() {
		// Count total announcements across all buckets in the wheel
		count := 0
		for _, bucket := range gs0.mcache.annWheel {
			count += len(bucket)
		}
		resultChan <- count
	}

	count := <-resultChan
	if count != 1 {
		t.Fatalf("expected 1 announcements stored, got %d", count)
	}
}

func TestAnnounceMultipleMessages(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-multiple"
	hosts := getDefaultHosts(t, 3)
	psubs := getGossipsubs(ctx, hosts)
	connectAll(t, hosts)

	topics := getTopics(psubs, topic)

	// All hosts subscribe
	subs := make([]*Subscription, 3)
	for i := range 3 {
		sub, err := topics[i].Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = sub
	}

	time.Sleep(time.Millisecond * 500)

	// Host 0 announces multiple messages
	numMessages := 5
	payloads := make([][]byte, numMessages)
	expiry := time.Now().Add(time.Second * 10)

	for i := range numMessages {
		payloads[i] = []byte("message " + string(rune('0'+i)))
		err := topics[0].Announce(ctx, payloads[i], expiry)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 50)
	}

	// Host 1 and 2 should receive all messages
	for hostIdx := 1; hostIdx < 3; hostIdx++ {
		receivedCount := 0
		for receivedCount < numMessages {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*2)
			_, err := subs[hostIdx].Next(timeoutCtx)
			cancel()
			if err != nil {
				t.Fatalf("host %d: failed to receive message %d: %v", hostIdx, receivedCount, err)
			}
			receivedCount++
		}
	}
}

func TestAnnounceWithClosedTopic(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-closed"
	hosts := getDefaultHosts(t, 1)
	psubs := getGossipsubs(ctx, hosts)

	topics := getTopics(psubs, topic)

	// Close the topic
	err := topics[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	// Announce should fail with ErrTopicClosed
	payload := []byte("should fail")
	expiry := time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != ErrTopicClosed {
		t.Fatalf("expected ErrTopicClosed, got %v", err)
	}
}

func TestAnnounceWithFloodsub(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-floodsub"
	hosts := getDefaultHosts(t, 1)

	// Create a floodsub instance instead of gossipsub
	psubs := getPubsubs(ctx, hosts) // This creates floodsub

	topics := getTopics(psubs, topic)

	// Announce should fail with non-GossipSub router
	payload := []byte("floodsub test")
	expiry := time.Now().Add(time.Second * 5)
	err := topics[0].Announce(ctx, payload, expiry)
	if err == nil {
		t.Fatal("expected error with floodsub router, got nil")
	}
}

func TestAnnounceGossipThreshold(t *testing.T) {
	ctx := t.Context()

	const topic = "test-announce-threshold"
	hosts := getDefaultHosts(t, 3)

	// Setup peer scoring with gossip threshold
	psubs := getGossipsubs(ctx, hosts,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 {
					// Give host 2 a very low score
					if p == hosts[2].ID() {
						return -1000
					}
					return 0
				},
				AppSpecificWeight: 1.0,
				DecayInterval:     time.Second,
				DecayToZero:       0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -500,
				PublishThreshold:  -1000,
				GraylistThreshold: -2000,
			},
		),
	)

	connectAll(t, hosts)
	topics := getTopics(psubs, topic)

	// All hosts subscribe
	_, err := topics[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	_, err = topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	sub2, err := topics[2].Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 1)

	// Host 0 announces
	payload := []byte("threshold test")
	expiry := time.Now().Add(time.Second * 5)
	err = topics[0].Announce(ctx, payload, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Host 2 with low score should not receive IHAVE
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
	defer cancel()
	msg, err := sub2.Next(timeoutCtx)
	if err != context.DeadlineExceeded {
		if msg != nil {
			t.Fatal("host with low score should not receive announcement")
		}
		t.Fatalf("expected timeout for low-score peer, got error: %v", err)
	}
}
