package pubsub

import (
	"context"
	"io"
	"iter"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

func waitForTopicStreams(t *testing.T, ps *PubSub, id peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done := make(chan bool, 1)
		select {
		case ps.eval <- func() {
			a, ok := ps.peerComm.Lookup(id)
			done <- ok && a.TopicStreamsEnabled()
		}:
		case <-ps.ctx.Done():
			t.Fatal(ps.ctx.Err())
		}
		if <-done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("topic streams negotiation timed out")
}

func waitForTopicPeer(t *testing.T, ps *PubSub, topic string, id peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done := make(chan bool, 1)
		select {
		case ps.eval <- func() {
			_, ok := ps.topics[topic][id]
			done <- ok
		}:
		case <-ps.ctx.Done():
			t.Fatal(ps.ctx.Err())
		}
		if <-done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer %s subscription to %q timed out", id, topic)
}

func TestTopicStreamsV12FallbackStaysOnControl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getDefaultHosts(t, 2)
	ps0 := getGossipsub(ctx, hosts[0], WithTopicStreams())
	ps1 := getGossipsub(ctx, hosts[1], WithGossipSubProtocols([]protocol.ID{GossipSubID_v12}, GossipSubDefaultFeatures))
	sub, err := ps1.Subscribe("topic-stream-v12-fallback")
	if err != nil {
		t.Fatal(err)
	}
	connect(t, hosts[0], hosts[1])
	waitForTopicPeer(t, ps0, "topic-stream-v12-fallback", hosts[1].ID())
	data := []byte("control-fallback")
	if err := ps0.Publish("topic-stream-v12-fallback", data); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	got, err := sub.Next(readCtx)
	if err != nil || string(got.Data) != string(data) {
		t.Fatalf("v1.2 fallback delivery = %q, %v", got.GetData(), err)
	}
}

func TestTopicStreamsRejectsUnnegotiatedAndControlPayload(t *testing.T) {
	t.Run("unnegotiated stream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		_ = getGossipsub(ctx, hosts[0], WithTopicStreams())
		_ = getGossipsub(ctx, hosts[1])
		connect(t, hosts[0], hosts[1])
		s, err := hosts[1].NewStream(ctx, hosts[0].ID(), peercomm.TopicStreamsProtocol)
		if err != nil {
			t.Fatal(err)
		}
		w := msgio.NewVarintWriter(s)
		topic := "not-negotiated"
		if err := w.WriteMsg(mustMarshalTopicStream(t, &pb.TopicRPCHeader{Topic: &topic})); err != nil {
			t.Fatal(err)
		}
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		var one [1]byte
		if _, err := s.Read(one[:]); err == nil {
			t.Fatal("unnegotiated topic stream remained open")
		}
	})

	t.Run("payload on control", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		ps := getGossipsubs(ctx, hosts, WithTopicStreams())
		connect(t, hosts[0], hosts[1])
		waitForTopicStreams(t, ps[0], hosts[1].ID())
		s, err := hosts[1].NewStream(ctx, hosts[0].ID(), GossipSubID_v13)
		if err != nil {
			t.Fatal(err)
		}
		w := msgio.NewVarintWriter(s)
		topic := "forbidden-control"
		rpc := &pb.RPC{Publish: []*pb.Message{{Topic: &topic, Data: []byte("bad")}}}
		if err := w.WriteMsg(mustMarshalTopicStream(t, rpc)); err != nil {
			t.Fatal(err)
		}
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		var one [1]byte
		if _, err := s.Read(one[:]); err == nil {
			t.Fatal("control payload violation did not close connection")
		}
	})
}

func TestTopicStreamsRejectsUnwantedTopicAndPenalizesPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getDefaultHosts(t, 2)
	ps0 := getGossipsub(ctx, hosts[0], WithTopicStreams(), WithPeerScore(
		&PeerScoreParams{
			AppSpecificScore:       func(peer.ID) float64 { return 0 },
			BehaviourPenaltyWeight: -1,
			BehaviourPenaltyDecay:  ScoreParameterDecay(time.Minute),
			DecayInterval:          DefaultDecayInterval,
			DecayToZero:            DefaultDecayToZero,
		},
		&PeerScoreThresholds{
			GossipThreshold:   -100,
			PublishThreshold:  -500,
			GraylistThreshold: -1000,
		},
	))
	ps1 := getGossipsub(ctx, hosts[1], WithTopicStreams())
	connect(t, hosts[0], hosts[1])
	waitForTopicStreams(t, ps0, hosts[1].ID())
	waitForTopicStreams(t, ps1, hosts[0].ID())

	s, err := hosts[1].NewStream(ctx, hosts[0].ID(), peercomm.TopicStreamsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	w := msgio.NewVarintWriter(s)
	topic := "unwanted-topic"
	if err := w.WriteMsg(mustMarshalTopicStream(t, &pb.TopicRPCHeader{Topic: &topic})); err != nil {
		t.Fatal(err)
	}
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	var one [1]byte
	if _, err := s.Read(one[:]); err == nil {
		t.Fatal("unwanted topic stream remained open")
	}

	score := make(chan float64, 1)
	select {
	case ps0.eval <- func() {
		score <- ps0.rt.(*GossipSubRouter).score.Score(hosts[1].ID())
	}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := <-score; got >= 0 {
		t.Fatalf("peer score = %f, want negative after unwanted topic stream", got)
	}
}

func mustMarshalTopicStream(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func waitForTopicWriter(t *testing.T, ps *PubSub, id peer.ID, topic string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result := make(chan bool, 1)
		select {
		case ps.eval <- func() {
			actor, ok := ps.peerComm.Lookup(id)
			result <- ok && actor.HasTopicWriter(topic) == want
		}:
		case <-ps.ctx.Done():
			t.Fatal(ps.ctx.Err())
		}
		if <-result {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("topic writer for peer %s and topic %q did not become %t", id, topic, want)
}

func nextTopicMessage(t *testing.T, ctx context.Context, sub *Subscription, want []byte) {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	message, err := sub.Next(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	if string(message.Data) != string(want) {
		t.Fatalf("message data = %q, want %q", message.Data, want)
	}
}

func setupNegotiatedTopicStreams(t *testing.T, topics0, topics1 []string) (context.Context, context.CancelFunc, []peer.ID, []*PubSub, map[string]*Subscription, map[string]*Subscription) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hosts := getDefaultHosts(t, 2)
	ps := getGossipsubs(ctx, hosts, WithTopicStreams(), WithFloodPublish(true))
	subs0 := make(map[string]*Subscription)
	for _, topic := range topics0 {
		sub, err := ps[0].Subscribe(topic)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		subs0[topic] = sub
	}
	subs1 := make(map[string]*Subscription)
	for _, topic := range topics1 {
		sub, err := ps[1].Subscribe(topic)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		subs1[topic] = sub
	}
	connect(t, hosts[0], hosts[1])
	waitForTopicStreams(t, ps[0], hosts[1].ID())
	waitForTopicStreams(t, ps[1], hosts[0].ID())
	for _, topic := range topics1 {
		waitForTopicPeer(t, ps[0], topic, hosts[1].ID())
	}
	for _, topic := range topics0 {
		waitForTopicPeer(t, ps[1], topic, hosts[0].ID())
	}
	return ctx, cancel, []peer.ID{hosts[0].ID(), hosts[1].ID()}, ps, subs0, subs1
}

func TestTopicStreamsNegotiatedPublishDelivery(t *testing.T) {
	topic := "topic-stream-negotiated-publish"
	ctx, cancel, ids, ps, _, subs1 := setupNegotiatedTopicStreams(t, nil, []string{topic})
	defer cancel()

	data := []byte("delivered over topic writer")
	if err := ps[0].Publish(topic, data); err != nil {
		t.Fatal(err)
	}
	nextTopicMessage(t, ctx, subs1[topic], data)
	waitForTopicWriter(t, ps[0], ids[1], topic, true)
}

func TestTopicStreamsDistinctTopicsUseDistinctWriters(t *testing.T) {
	topics := []string{"topic-stream-distinct-a", "topic-stream-distinct-b"}
	ctx, cancel, ids, ps, _, subs1 := setupNegotiatedTopicStreams(t, nil, topics)
	defer cancel()

	for i, topic := range topics {
		data := []byte{byte('a' + i)}
		if err := ps[0].Publish(topic, data); err != nil {
			t.Fatal(err)
		}
		nextTopicMessage(t, ctx, subs1[topic], data)
		waitForTopicWriter(t, ps[0], ids[1], topic, true)
	}
}

func TestTopicStreamsBidirectionalPublishUsesIndependentWriters(t *testing.T) {
	topic := "topic-stream-bidirectional"
	ctx, cancel, ids, ps, subs0, subs1 := setupNegotiatedTopicStreams(t, []string{topic}, []string{topic})
	defer cancel()

	if err := ps[0].Publish(topic, []byte("zero to one")); err != nil {
		t.Fatal(err)
	}
	nextTopicMessage(t, ctx, subs0[topic], []byte("zero to one"))
	nextTopicMessage(t, ctx, subs1[topic], []byte("zero to one"))
	if err := ps[1].Publish(topic, []byte("one to zero")); err != nil {
		t.Fatal(err)
	}
	nextTopicMessage(t, ctx, subs0[topic], []byte("one to zero"))
	waitForTopicWriter(t, ps[0], ids[1], topic, true)
	waitForTopicWriter(t, ps[1], ids[0], topic, true)
}

func TestTopicStreamsRemoteUnsubscribeClosesSenderWriter(t *testing.T) {
	topic := "topic-stream-unsubscribe"
	ctx, cancel, ids, ps, _, subs1 := setupNegotiatedTopicStreams(t, nil, []string{topic})
	defer cancel()

	if err := ps[0].Publish(topic, []byte("open writer")); err != nil {
		t.Fatal(err)
	}
	nextTopicMessage(t, ctx, subs1[topic], []byte("open writer"))
	waitForTopicWriter(t, ps[0], ids[1], topic, true)

	subs1[topic].Cancel()
	waitForTopicWriter(t, ps[0], ids[1], topic, false)
}

type topicStreamsPartialState struct{}

func newTopicStreamsPartialExtension(received chan<- *pb.PartialMessagesExtension) *partialmessages.PartialMessagesExtension[topicStreamsPartialState] {
	return &partialmessages.PartialMessagesExtension[topicStreamsPartialState]{
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEmitGossip: func(string, []byte, []peer.ID, map[peer.ID]topicStreamsPartialState) {},
		OnIncomingRPC: func(_ peer.ID, _ map[peer.ID]topicStreamsPartialState, rpc *pb.PartialMessagesExtension) error {
			if received != nil {
				received <- proto.Clone(rpc).(*pb.PartialMessagesExtension)
			}
			return nil
		},
	}
}

func TestTopicStreamsNegotiatedPartialMessageDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getDefaultHosts(t, 2)
	received := make(chan *pb.PartialMessagesExtension, 1)
	ps0 := getGossipsub(ctx, hosts[0], WithTopicStreams(), WithPartialMessagesExtension(newTopicStreamsPartialExtension(nil)))
	ps1 := getGossipsub(ctx, hosts[1], WithTopicStreams(), WithPartialMessagesExtension(newTopicStreamsPartialExtension(received)))
	topic := "topic-stream-partial"
	t0, err := ps0.Join(topic, SupportsPartialMessages())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = t0.Subscribe(); err != nil {
		t.Fatal(err)
	}
	t1, err := ps1.Join(topic, RequestPartialMessages())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = t1.Subscribe(); err != nil {
		t.Fatal(err)
	}
	connect(t, hosts[0], hosts[1])
	waitForTopicStreams(t, ps0, hosts[1].ID())
	waitForTopicStreams(t, ps1, hosts[0].ID())
	waitForTopicPeer(t, ps0, topic, hosts[1].ID())
	waitForTopicPeer(t, ps1, topic, hosts[0].ID())

	group := []byte("group")
	payload := []byte("partial payload")
	err = PublishPartial(ps0, topic, group, func(states map[peer.ID]topicStreamsPartialState, _ func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			for id := range states {
				if !yield(id, partialmessages.PublishAction{EncodedPartialMessage: payload, EncodedPartsMetadata: []byte{1}}) {
					return
				}
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case rpc := <-received:
		if rpc.GetTopicID() != topic || string(rpc.GetGroupID()) != string(group) || string(rpc.GetPartialMessage()) != string(payload) {
			t.Fatalf("partial RPC = %v", rpc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("partial message delivery timed out")
	}
	waitForTopicWriter(t, ps0, hosts[1].ID(), topic, true)
}
