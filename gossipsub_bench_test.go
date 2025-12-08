package pubsub

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func BenchmarkGossipSub_twoHosts(b *testing.B) {
	benchmarkGossipSub(b, 2)
}
func BenchmarkGossipSub_full(b *testing.B) {
	benchmarkGossipSub(b, 10)
}

func BenchmarkGossipSubBatch_twoHosts(b *testing.B) {
	benchmarkGossipSubBatch(b, 2)
}

func BenchmarkGossipSubBatch_full(b *testing.B) {
	benchmarkGossipSubBatch(b, 10)
}

func benchmarkGossipSub(b *testing.B, hostCount int) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs := getDefaultHosts(b, hostCount)
	gs := getGossipsubs(ctx, hs, WithMessageSignaturePolicy(StrictNoSign))
	denseConnect(b, hs)
	topicStr := "test-topic"
	var topics []*Topic
	var subs []*Subscription
	for _, g := range gs {
		t, err := g.Join(topicStr)
		if err != nil {
			b.Fatal(err)
		}
		topics = append(topics, t)
		s, err := t.Subscribe()
		if err != nil {
			b.Fatal(err)
		}
		subs = append(subs, s)
		_ = subs
	}

	time.Sleep(time.Second)
	fmt.Println("peer count", len(topics[0].ListPeers()))

	b.ResetTimer()
	msg := make([]byte, 64*1024)
	for b.Loop() {
		ctr := binary.BigEndian.Uint64(msg[0:8])
		ctr++
		binary.BigEndian.AppendUint64(msg[:0], ctr)
		topics[0].Publish(ctx, msg)
	}
}
func benchmarkGossipSubBatch(b *testing.B, hostCount int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs := getDefaultHosts(b, hostCount)
	gs := getGossipsubs(ctx, hs, WithMessageSignaturePolicy(StrictNoSign))
	denseConnect(b, hs)
	topicStr := "test-topic-%d"
	var topics []*Topic
	var subs []*Subscription

	const topicCount = 32
	for _, g := range gs {
		for i := range topicCount {
			t, err := g.Join(fmt.Sprintf(topicStr, i))
			if err != nil {
				b.Fatal(err)
			}
			topics = append(topics, t)
			s, err := t.Subscribe()
			if err != nil {
				b.Fatal(err)
			}
			subs = append(subs, s)
			_ = subs
		}
	}

	time.Sleep(time.Second)
	fmt.Println("peer count", len(topics[0].ListPeers()))
	msg := make([]byte, 64*1024)
	start := time.Now()

	for b.Loop() {
		var batch MessageBatch
		for i := range topicCount {
			ctr := binary.BigEndian.Uint64(msg[0:8])
			ctr++
			binary.BigEndian.AppendUint64(msg[:0], ctr)
			err := topics[i].AddToBatch(ctx, &batch, msg)
			if err != nil {
				b.Fatal(err)
			}
		}
		gs[0].PublishBatch(&batch)
	}

	took := time.Since(start)
	fmt.Println("Per topic:", took/(time.Duration(b.N*topicCount)), "per op")
}
