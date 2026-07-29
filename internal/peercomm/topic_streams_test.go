package peercomm

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type topicHost struct{ streams chan *testStream }

func (h *topicHost) NewStream(_ context.Context, p peer.ID, protocols ...protocol.ID) (network.Stream, error) {
	if len(protocols) != 1 || protocols[0] != TopicStreamsProtocol {
		panic("unexpected protocol")
	}
	s := newTestStream(p)
	h.streams <- s
	return s, nil
}

type fixedTopicHost struct{ stream *testStream }

func (h *fixedTopicHost) NewStream(_ context.Context, _ peer.ID, _ ...protocol.ID) (network.Stream, error) {
	return h.stream, nil
}

type gatedTopicHost struct {
	started chan struct{}
	release <-chan struct{}
	stream  *testStream
	once    sync.Once
}

func (h *gatedTopicHost) NewStream(_ context.Context, p peer.ID, protocols ...protocol.ID) (network.Stream, error) {
	if len(protocols) != 1 || protocols[0] != TopicStreamsProtocol {
		panic("unexpected protocol")
	}
	h.once.Do(func() { close(h.started) })
	<-h.release
	if h.stream == nil {
		h.stream = newTestStream(p)
	}
	return h.stream, nil
}

func requirePromptReturn(t *testing.T, call func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		call()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("topic stream teardown blocked")
	}
}

func TestCloseTopicDoesNotWaitForBlockedWrite(t *testing.T) {
	gate := make(chan struct{})
	stream := newTestStream(peer.ID("peer"))
	stream.writeStarted = make(chan struct{})
	stream.writeGate = gate
	r, _ := NewRegistry(context.Background(), testConfig(&fixedTopicHost{stream: stream}, Hooks{}))
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	topic := "alpha"
	if err := a.Send(&pb.RPC{Publish: []*pb.Message{{Topic: &topic, Data: []byte("data")}}}, false); err != nil {
		t.Fatal(err)
	}
	waitDone(t, stream.writeStarted, "blocked topic write")
	requirePromptReturn(t, func() { a.CloseTopic(topic) })
	if a.HasTopicWriter(topic) {
		t.Fatal("closed writer remained registered")
	}
	close(gate)
}

func TestDisableTopicStreamsDoesNotWaitForBlockedOpen(t *testing.T) {
	gate := make(chan struct{})
	h := &gatedTopicHost{started: make(chan struct{}), release: gate}
	r, _ := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	topic := "alpha"
	if err := a.Send(&pb.RPC{Publish: []*pb.Message{{Topic: &topic, Data: []byte("data")}}}, false); err != nil {
		t.Fatal(err)
	}
	waitDone(t, h.started, "blocked topic open")
	requirePromptReturn(t, a.DisableTopicStreams)
	if a.TopicStreamsEnabled() || a.HasTopicWriter(topic) {
		t.Fatal("disabled topic writer remained active")
	}
	close(gate)
}

func TestTopicSendSplitsWithoutMutationAndWritesHeaderFirst(t *testing.T) {
	gate := make(chan struct{})
	stream := newTestStream(peer.ID("peer"))
	stream.writeStarted = make(chan struct{})
	stream.writeGate = gate
	r, err := NewRegistry(context.Background(), testConfig(&fixedTopicHost{stream: stream}, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	topic := "alpha"
	extension := &pb.TestExtension{}
	extension.ProtoReflect().SetUnknown([]byte{0x08, 0x01})
	rpc := &pb.RPC{
		Publish:       []*pb.Message{{From: []byte("from"), Data: []byte("data"), Topic: &topic}},
		Subscriptions: []*pb.RPC_SubOpts{{Subscribe: proto.Bool(true), Topicid: &topic}},
		Control:       &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"message"}}}},
		TestExtension: extension,
	}
	before := proto.Clone(rpc).(*pb.RPC)
	if err := a.Send(rpc, false); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(rpc, before) {
		t.Fatal("Send mutated caller RPC")
	}
	waitDone(t, stream.writeStarted, "blocked topic header")

	rpc.Subscriptions[0].Topicid = proto.String("mutated")
	rpc.Control.Ihave[0].MessageIDs[0] = "mutated"
	rpc.TestExtension.ProtoReflect().SetUnknown([]byte{0x08, 0x02})
	rpc.Publish[0].Data[0] = 'X'

	control, err := a.queue.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	expectedControl := proto.Clone(before).(*pb.RPC)
	expectedControl.Publish = nil
	if !proto.Equal(control, expectedControl) {
		t.Fatalf("control snapshot = %v, want %v", control, expectedControl)
	}

	release()
	headerFrame := receive(t, stream.writes, "topic header")
	var header pb.TopicRPCHeader
	decodeFrameInto(t, headerFrame, &header)
	if header.GetTopic() != topic {
		t.Fatalf("header topic = %q", header.GetTopic())
	}
	payloadFrame := receive(t, stream.writes, "topic payload")
	var payload pb.TopicRPC
	decodeFrameInto(t, payloadFrame, &payload)
	if payload.GetPublish() == nil || payload.GetPublish().Topic != nil || string(payload.GetPublish().Data) != "data" {
		t.Fatalf("bad payload snapshot: %v", &payload)
	}
}

func TestTopicSendRejectsMissingTopicWithoutPartialEnqueue(t *testing.T) {
	h := &topicHost{streams: make(chan *testStream, 1)}
	r, err := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	topic := "alpha"
	rpc := &pb.RPC{
		Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic}},
		Publish: []*pb.Message{
			{Topic: &topic, Data: []byte("valid")},
			{Data: []byte("missing-topic")},
		},
	}
	if err := a.Send(rpc, false); !errors.Is(err, ErrInvalidTopicRPC) {
		t.Fatalf("send error = %v", err)
	}
	if got := len(a.queue.normal); got != 0 {
		t.Fatalf("control queue length = %d, want 0", got)
	}
	select {
	case <-h.streams:
		t.Fatal("opened stream after rejecting RPC")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestTopicSendQueueFullDoesNotPartiallyEnqueue(t *testing.T) {
	h := &topicHost{streams: make(chan *testStream, 1)}
	cfg := testConfig(h, Hooks{})
	cfg.QueueSize = 1
	r, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	if err := a.queue.push(&pb.RPC{}, false); err != nil {
		t.Fatal(err)
	}
	topic := "alpha"
	rpc := &pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic}}, Publish: []*pb.Message{{Topic: &topic, Data: []byte("data")}}}
	if err := a.Send(rpc, false); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("send error = %v", err)
	}
	if len(a.topicWriters) != 0 {
		t.Fatal("topic payload enqueued despite full control queue")
	}
}

func TestTopicSendReusesStreamAndCloseTopicReplacesAfterClose(t *testing.T) {
	h := &topicHost{streams: make(chan *testStream, 3)}
	r, _ := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	a.EnableTopicStreams()
	topic := "alpha"
	makeRPC := func(data string) *pb.RPC { return &pb.RPC{Publish: []*pb.Message{{Data: []byte(data), Topic: &topic}}} }
	if err := a.Send(makeRPC("one"), false); err != nil {
		t.Fatal(err)
	}
	first := receive(t, h.streams, "first stream")
	receive(t, first.writes, "header")
	receive(t, first.writes, "first payload")
	if err := a.Send(makeRPC("two"), false); err != nil {
		t.Fatal(err)
	}
	receive(t, first.writes, "second payload")
	a.CloseTopic(topic)
	waitDone(t, first.reset, "first close")
	if err := a.Send(makeRPC("three"), false); err != nil {
		t.Fatal(err)
	}
	second := receive(t, h.streams, "replacement stream")
	if second == first {
		t.Fatal("stream not replaced")
	}
}

func TestTopicInboundReconstructsPublishAndPartial(t *testing.T) {
	p := peer.ID("peer")
	inbound := make(chan *pb.RPC, 2)
	r, _ := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{InboundRPC: func(_ *Actor, _ network.Stream, transport Transport, rpc *pb.RPC) {
		if transport != TransportTopic {
			t.Errorf("transport = %v, want topic", transport)
		}
		inbound <- rpc
	}}))
	defer r.Stop()
	a := r.GetOrCreate(p)
	a.EnableTopicStreams()
	topic := "alpha"
	s := newTestStream(p)
	s.reads <- streamRead{data: frameProto(t, &pb.TopicRPCHeader{Topic: &topic})}
	s.reads <- streamRead{data: frameProto(t, &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{Publish: &pb.Message{Data: []byte("data")}}})}
	s.reads <- streamRead{data: frameProto(t, &pb.TopicRPC{Payload: &pb.TopicRPC_Partial{Partial: &pb.PartialMessagesExtension{PartialMessage: []byte("part")}}})}
	s.reads <- streamRead{err: io.EOF}
	done := make(chan struct{})
	go func() { a.HandleTopicInbound(s); close(done) }()
	publish := receive(t, inbound, "publish")
	if len(publish.Publish) != 1 || publish.Publish[0].GetTopic() != topic {
		t.Fatalf("publish not reconstructed: %v", publish)
	}
	partial := receive(t, inbound, "partial")
	if partial.Partial == nil || partial.Partial.GetTopicID() != topic {
		t.Fatalf("partial not reconstructed: %v", partial)
	}
	waitDone(t, done, "topic inbound")
}

func TestTopicInboundRejectsEmptyDataAndWireTopic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload *pb.TopicRPC
	}{
		{name: "empty data", payload: &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{Publish: &pb.Message{}}}},
		{name: "wire topic", payload: &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{Publish: &pb.Message{Data: []byte("x"), Topic: proto.String("bad")}}}},
		{name: "empty oneof", payload: &pb.TopicRPC{}},
		{name: "nil publish", payload: &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := peer.ID("peer")
			r, _ := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
			defer r.Stop()
			a := r.GetOrCreate(p)
			a.EnableTopicStreams()
			topic := "alpha"
			s := newTestStream(p)
			s.reads <- streamRead{data: frameProto(t, &pb.TopicRPCHeader{Topic: &topic})}
			s.reads <- streamRead{data: frameProto(t, tc.payload)}
			done := make(chan struct{})
			go func() { a.HandleTopicInbound(s); close(done) }()
			waitDone(t, done, "violation")
			conn := s.conn.(*testConn)
			conn.mu.Lock()
			code, closed := conn.closeCode, conn.closed
			conn.mu.Unlock()
			if closed != 1 || code != TopicStreamsViolation {
				t.Fatalf("close = %d/%x", closed, code)
			}
		})
	}
}

func TestTopicHeaderDeadline(t *testing.T) {
	p := peer.ID("peer")
	r, _ := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
	defer r.Stop()
	a := r.GetOrCreate(p)
	a.EnableTopicStreams()
	s := newTestStream(p)
	done := make(chan struct{})
	go func() { a.HandleTopicInbound(s); close(done) }()
	select {
	case <-done:
		t.Fatal("returned before input")
	case <-time.After(20 * time.Millisecond):
	}
	a.Retire()
	waitDone(t, done, "retirement")
}

func TestTopicViolationClosesOffendingConnection(t *testing.T) {
	p := peer.ID("peer")
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(p)
	offending := newTestStream(p)
	a.ProtocolViolation(offending)
	conn := offending.conn.(*testConn)
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.closed != 1 || conn.closeCode != TopicStreamsViolation {
		t.Fatalf("offending connection close = %d/%x", conn.closed, conn.closeCode)
	}
}
