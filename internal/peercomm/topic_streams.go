package peercomm

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

const (
	TopicStreamsProtocol           protocol.ID           = "/gsts/v0beta"
	TopicStreamsViolation          network.ConnErrorCode = 0xd52505
	maxInboundTopicStreamsPerTopic                       = 3
	maxInboundTopicStreamsPerPeer                        = 24
)

var (
	ErrInvalidTopicRPC = errors.New("peercomm: topic payload has no topic")
)

type topicWriter struct {
	topic  string
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *pb.TopicRPC
}

type topicInboundState struct {
	active  int
	deliver sync.Mutex
}

// EnableTopicStreams enables the negotiated transport. It is safe to call from
// the PubSub process loop and does not wait for actor work.
func (a *Actor) EnableTopicStreams() {
	a.topicMu.Lock()
	if a.ctx.Err() == nil {
		a.topicEnabled = true
	}
	a.topicMu.Unlock()
}

// DisableTopicStreams tears down all topic streams without affecting the
// control stream.
func (a *Actor) DisableTopicStreams() {
	a.topicMu.Lock()
	a.topicEnabled = false
	a.stopTopicWritersLocked()
	a.topicMu.Unlock()
	a.closeInboundTopics()
}

func (a *Actor) TopicStreamsEnabled() bool {
	a.topicMu.Lock()
	defer a.topicMu.Unlock()
	return a.topicEnabled
}

// HasTopicWriter reports whether this actor currently owns an outbound writer
// for topic. It is intended for transport diagnostics and tests.
func (a *Actor) HasTopicWriter(topic string) bool {
	a.topicMu.Lock()
	defer a.topicMu.Unlock()
	_, ok := a.topicWriters[topic]
	return ok
}

// CloseTopic closes the initiator-owned stream for topic. A later send lazily
// creates a replacement.
func (a *Actor) CloseTopic(topic string) {
	a.topicMu.Lock()
	writer := a.topicWriters[topic]
	delete(a.topicWriters, topic)
	if writer != nil {
		writer.cancel()
	}
	a.topicMu.Unlock()
}

func (a *Actor) stopTopicWriters() {
	a.topicMu.Lock()
	a.stopTopicWritersLocked()
	a.topicMu.Unlock()
}

func (a *Actor) stopTopicWritersLocked() {
	for topic, writer := range a.topicWriters {
		delete(a.topicWriters, topic)
		writer.cancel()
	}
}

func (a *Actor) splitAndSend(rpc *pb.RPC, urgent bool) error {
	a.topicMu.Lock()
	defer a.topicMu.Unlock()
	if a.ctx.Err() != nil {
		return ErrQueueClosed
	}
	if !a.topicEnabled {
		return a.queue.push(rpc, urgent)
	}

	control := &pb.RPC{
		Subscriptions: rpc.Subscriptions,
		Control:       rpc.Control,
		TestExtension: rpc.TestExtension,
	}
	byTopic := make(map[string][]*pb.TopicRPC)
	for _, message := range rpc.Publish {
		if message == nil || message.GetTopic() == "" {
			return ErrInvalidTopicRPC
		}
		topic := message.GetTopic()
		publish := proto.Clone(message).(*pb.Message)
		publish.Topic = nil
		byTopic[topic] = append(byTopic[topic], &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{Publish: publish}})
	}
	if rpc.Partial != nil {
		if rpc.Partial.GetTopicID() == "" {
			return ErrInvalidTopicRPC
		}
		partial := proto.Clone(rpc.Partial).(*pb.PartialMessagesExtension)
		topic := partial.GetTopicID()
		partial.TopicID = nil
		byTopic[topic] = append(byTopic[topic], &pb.TopicRPC{Payload: &pb.TopicRPC_Partial{Partial: partial}})
	}

	// Reserve every destination before publishing any part of the RPC. This
	// keeps a full control or topic queue from producing a partial send.
	a.queue.mu.Lock()
	defer a.queue.mu.Unlock()
	if a.queue.closed {
		return ErrQueueClosed
	}
	controlPending := proto.Size(control) > 0
	if controlPending && len(a.queue.urgent)+len(a.queue.normal) >= a.queue.capacity {
		return ErrQueueFull
	}
	for topic, items := range byTopic {
		writer := a.topicWriters[topic]
		queued := 0
		if writer != nil {
			queued = len(writer.queue)
		}
		if queued+len(items) > a.registry.config.QueueSize {
			return ErrQueueFull
		}
	}

	if controlPending {
		if urgent {
			a.queue.urgent = append(a.queue.urgent, control)
		} else {
			a.queue.normal = append(a.queue.normal, control)
		}
		a.queue.available.Signal()
	}
	for topic, items := range byTopic {
		writer := a.topicWriters[topic]
		if writer == nil {
			ctx, cancel := context.WithCancel(a.ctx)
			writer = &topicWriter{topic: topic, ctx: ctx, cancel: cancel, queue: make(chan *pb.TopicRPC, a.registry.config.QueueSize)}
			a.topicWriters[topic] = writer
			a.topicWritersWG.Add(1)
			go a.runTopicWriter(writer)
		}
		for _, item := range items {
			writer.queue <- item
		}
	}
	return nil
}

func (a *Actor) runTopicWriter(w *topicWriter) {
	defer a.topicWritersWG.Done()
	var stream network.Stream
	defer func() {
		if stream != nil {
			_ = stream.Close()
		}
	}()
	for {
		select {
		case <-w.ctx.Done():
			return
		case item := <-w.queue:
			var err error
			stream, err = a.writeTopicItem(w, stream, item)
			if err != nil && stream != nil {
				_ = stream.Close()
				stream = nil
			}
		}
	}
}

func (a *Actor) writeTopicItem(w *topicWriter, stream network.Stream, item *pb.TopicRPC) (network.Stream, error) {
	if err := w.ctx.Err(); err != nil {
		return stream, err
	}
	if stream == nil {
		s, err := a.registry.config.Host.NewStream(w.ctx, a.peer, TopicStreamsProtocol)
		if err != nil {
			return nil, err
		}
		stream = s
		if err := w.ctx.Err(); err != nil {
			_ = s.Reset()
			return nil, err
		}
		if err := writeProto(s, &pb.TopicRPCHeader{Topic: proto.String(w.topic)}); err != nil {
			_ = s.Close()
			return nil, err
		}
		go a.watchTopicResponder(w.ctx, s)
	}
	if err := writeProto(stream, item); err != nil {
		return stream, err
	}
	return stream, nil
}

func (a *Actor) watchTopicResponder(ctx context.Context, s network.Stream) {
	var one [1]byte
	n, _ := s.Read(one[:])
	if ctx.Err() == nil && n != 0 {
		a.protocolViolation(s)
	}
}

// HandleTopicInbound validates and consumes a responder-side topic stream.
func (a *Actor) HandleTopicInbound(s network.Stream) {
	if s == nil || s.Conn().RemotePeer() != a.peer {
		if s != nil {
			_ = s.Reset()
		}
		return
	}
	if !a.TopicStreamsEnabled() {
		a.protocolViolation(s)
		return
	}
	a.topicMu.Lock()
	a.topicInboundStreams[s] = struct{}{}
	a.topicMu.Unlock()
	defer func() { a.topicMu.Lock(); delete(a.topicInboundStreams, s); a.topicMu.Unlock() }()
	_ = s.SetReadDeadline(time.Now().Add(time.Second))
	r := msgio.NewVarintReaderSize(s, a.registry.config.MaxMessageSize)
	b, err := r.ReadMsg()
	if err != nil {
		r.ReleaseMsg(b)
		_ = s.Reset()
		return
	}
	header := new(pb.TopicRPCHeader)
	err = proto.Unmarshal(b, header)
	r.ReleaseMsg(b)
	if err != nil || header.GetTopic() == "" {
		a.protocolViolation(s)
		return
	}
	_ = s.SetReadDeadline(time.Time{})
	topic := header.GetTopic()

	a.topicMu.Lock()
	state := a.topicInbound[topic]
	if state == nil {
		state = new(topicInboundState)
		a.topicInbound[topic] = state
	}
	if state.active >= maxInboundTopicStreamsPerTopic || a.topicInboundTotal >= maxInboundTopicStreamsPerPeer {
		a.topicMu.Unlock()
		if h := a.registry.config.Hooks.TopicMisbehavior; h != nil {
			h(a, topic)
		}
		_ = s.Reset()
		return
	}
	state.active++
	a.topicInboundTotal++
	a.topicMu.Unlock()
	defer func() {
		a.topicMu.Lock()
		state.active--
		a.topicInboundTotal--
		if state.active == 0 {
			delete(a.topicInbound, topic)
		}
		a.topicMu.Unlock()
	}()
	state.deliver.Lock()
	defer state.deliver.Unlock()
	if h := a.registry.config.Hooks.TopicAllowed; h != nil && !h(a, topic) {
		if mh := a.registry.config.Hooks.TopicMisbehavior; mh != nil {
			mh(a, topic)
		}
		_ = s.Reset()
		return
	}
	for {
		b, err = r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			if errors.Is(err, io.EOF) {
				_ = s.Close()
			} else {
				_ = s.Reset()
			}
			return
		}
		trpc := new(pb.TopicRPC)
		err = proto.Unmarshal(b, trpc)
		r.ReleaseMsg(b)
		if err != nil || trpc.GetPayload() == nil {
			a.protocolViolation(s)
			return
		}
		var rpc pb.RPC
		switch payload := trpc.GetPayload().(type) {
		case *pb.TopicRPC_Publish:
			m := payload.Publish
			if m == nil || len(m.Data) == 0 || m.Topic != nil {
				a.protocolViolation(s)
				return
			}
			m.Topic = proto.String(topic)
			rpc.Publish = []*pb.Message{m}
		case *pb.TopicRPC_Partial:
			if payload.Partial == nil || payload.Partial.TopicID != nil {
				a.protocolViolation(s)
				return
			}
			partial := proto.Clone(payload.Partial).(*pb.PartialMessagesExtension)
			partial.TopicID = proto.String(topic)
			rpc.Partial = partial
		default:
			a.protocolViolation(s)
			return
		}
		if h := a.registry.config.Hooks.InboundRPC; h != nil {
			h(a, s, TransportTopic, &rpc)
		}
	}
}

func (a *Actor) closeInboundTopics() {
	a.topicMu.Lock()
	streams := make([]network.Stream, 0, len(a.topicInboundStreams))
	for s := range a.topicInboundStreams {
		streams = append(streams, s)
	}
	a.topicMu.Unlock()
	for _, s := range streams {
		_ = s.Reset()
	}
}

// ProtocolViolation closes the connection carrying s. If s is nil, it closes
// the current inbound control connection.
func (a *Actor) ProtocolViolation(s network.Stream) {
	if s == nil {
		a.inboundMu.Lock()
		if a.currentInbound != nil {
			s = a.currentInbound.stream
		}
		a.inboundMu.Unlock()
	} else if s.Conn().RemotePeer() != a.peer {
		return
	}
	if s != nil {
		a.protocolViolation(s)
	}
}

func (a *Actor) protocolViolation(s network.Stream) {
	_ = s.Conn().CloseWithError(TopicStreamsViolation)
}
