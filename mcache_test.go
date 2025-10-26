package pubsub

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestMessageCache(t *testing.T) {
	mcache := NewMessageCache(3, 5, time.Second, 60*time.Second) // 3 gossip, 5 history, 1s heartbeat, 60s max TTL
	msgID := DefaultMsgIdFn

	msgs := make([]*pb.Message, 60)
	for i := range msgs {
		msgs[i] = makeTestMessage(i)
	}

	for i := range 10 {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	for i := range 10 {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	gids := mcache.GossipForTopic("test")
	if len(gids) != 10 {
		t.Fatalf("Expected 10 gossip IDs; got %d", len(gids))
	}

	for i := range 10 {
		mid := msgID(msgs[i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	mcache.ShiftWindow()
	for i := 10; i < 20; i++ {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	for i := 0; i < 20; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	gids = mcache.GossipForTopic("test")
	if len(gids) != 20 {
		t.Fatalf("Expected 20 gossip IDs; got %d", len(gids))
	}

	for i := range 10 {
		mid := msgID(msgs[i])
		if mid != gids[10+i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[i])
		if mid != gids[i-10] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	mcache.ShiftWindow()
	for i := 20; i < 30; i++ {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	mcache.ShiftWindow()
	for i := 30; i < 40; i++ {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	mcache.ShiftWindow()
	for i := 40; i < 50; i++ {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	mcache.ShiftWindow()
	for i := 50; i < 60; i++ {
		mcache.AppendWindow(&Message{Message: msgs[i]})
	}

	if len(mcache.msgs) != 50 {
		t.Fatalf("Expected 50 messages in the cache; got %d", len(mcache.msgs))
	}

	for i := range 10 {
		mid := msgID(msgs[i])
		_, ok := mcache.Get(mid)
		if ok {
			t.Fatalf("Message %d still in cache", i)
		}
	}

	for i := 10; i < 60; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	gids = mcache.GossipForTopic("test")
	if len(gids) != 30 {
		t.Fatalf("Expected 30 gossip IDs; got %d", len(gids))
	}

	for i := range 10 {
		mid := msgID(msgs[50+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[30+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 20; i < 30; i++ {
		mid := msgID(msgs[10+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

}

func makeTestMessage(n int) *pb.Message {
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(n))
	data := []byte(fmt.Sprintf("%d", n))
	topic := "test"
	return &pb.Message{
		Data:  data,
		Topic: &topic,
		From:  []byte("test"),
		Seqno: seqno,
	}
}

func TestAnnouncementTimeWheel(t *testing.T) {
	// Create cache with 60 buckets for announcements (simulating 60 heartbeat intervals)
	mcache := NewMessageCache(3, 5, time.Second, 60*time.Second)
	msgID := DefaultMsgIdFn

	// Test basic insertion
	msg1 := makeTestMessage(1)
	expiry1 := time.Now().Add(5 * time.Second)
	mcache.TrackAnn(&Message{Message: msg1}, expiry1)

	mid1 := msgID(msg1)

	// Verify message is in cache (announcements are stored in msgs)
	if _, ok := mcache.Get(mid1); !ok {
		t.Fatal("Message not in announcement cache")
	}

	// Verify message can be retrieved
	m, _, ok := mcache.GetForPeer(mid1, "peer1")
	if !ok {
		t.Fatal("Failed to retrieve announced message")
	}
	if m.Message != msg1 {
		t.Fatal("Retrieved message doesn't match")
	}

	// Test multiple messages with different expiries
	msg2 := makeTestMessage(2)
	msg3 := makeTestMessage(3)
	expiry2 := time.Now().Add(10 * time.Second)
	expiry3 := time.Now().Add(15 * time.Second)

	mcache.TrackAnn(&Message{Message: msg2}, expiry2)
	mcache.TrackAnn(&Message{Message: msg3}, expiry3)

	mid2 := msgID(msg2)
	mid3 := msgID(msg3)

	// Verify all messages are in cache
	if _, ok := mcache.Get(mid1); !ok {
		t.Fatal("Message 1 should be in cache")
	}
	if _, ok := mcache.Get(mid2); !ok {
		t.Fatal("Message 2 should be in cache")
	}
	if _, ok := mcache.Get(mid3); !ok {
		t.Fatal("Message 3 should be in cache")
	}

	// Test wheel advancement (cleanup)
	// Advance 6 ticks (6 seconds) - msg1 should be cleaned up
	for i := 0; i < 6; i++ {
		mcache.PruneAnns()
	}

	// msg1 should be gone
	if _, ok := mcache.Get(mid1); ok {
		t.Fatal("Message 1 should have been cleaned up")
	}

	// msg2 and msg3 should still exist
	if _, ok := mcache.Get(mid2); !ok {
		t.Fatal("Message 2 should still exist")
	}
	if _, ok := mcache.Get(mid3); !ok {
		t.Fatal("Message 3 should still exist")
	}

	// Test expired message insertion (shouldn't be added)
	msg4 := makeTestMessage(4)
	expiry4 := time.Now().Add(-1 * time.Second) // Already expired
	mcache.TrackAnn(&Message{Message: msg4}, expiry4)

	mid4 := msgID(msg4)
	if _, ok := mcache.Get(mid4); ok {
		t.Fatal("Expired message should not have been added")
	}

	// Test wraparound (TTL > wheel size)
	msg5 := makeTestMessage(5)
	expiry5 := time.Now().Add(70 * time.Second) // Exceeds 60s max
	mcache.TrackAnn(&Message{Message: msg5}, expiry5)

	mid5 := msgID(msg5)
	if _, ok := mcache.Get(mid5); !ok {
		t.Fatal("Long TTL message should still be added (clamped to last bucket)")
	}

	// Verify we still have at least 3 messages in cache (msg2, msg3, msg5)
	if len(mcache.msgs) < 3 {
		t.Fatalf("Expected at least 3 messages in cache, got %d", len(mcache.msgs))
	}
}
