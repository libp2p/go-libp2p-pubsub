package peercomm

import (
	"context"
	"encoding/binary"
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

func TestQueuePriorityFullAndClose(t *testing.T) {
	q := newRPCQueue(3)
	n1 := &pb.RPC{Publish: []*pb.Message{{Data: []byte("normal-1")}}}
	u1 := &pb.RPC{Publish: []*pb.Message{{Data: []byte("urgent-1")}}}
	u2 := &pb.RPC{Publish: []*pb.Message{{Data: []byte("urgent-2")}}}
	if err := q.push(n1, false); err != nil {
		t.Fatal(err)
	}
	if err := q.push(u1, true); err != nil {
		t.Fatal(err)
	}
	if err := q.push(u2, true); err != nil {
		t.Fatal(err)
	}
	if err := q.push(&pb.RPC{}, false); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full push: %v", err)
	}

	ctx := context.Background()
	for i, want := range []*pb.RPC{u1, u2, n1} {
		got, err := q.pop(ctx)
		if err != nil {
			t.Fatalf("pop %d: %v", i, err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("pop %d returned wrong priority: got %v, want %v", i, got, want)
		}
	}
	q.close()
	if _, err := q.pop(ctx); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("closed pop: %v", err)
	}
	if err := q.push(&pb.RPC{}, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("closed push: %v", err)
	}
}

func TestQueuePopCancellation(t *testing.T) {
	q := newRPCQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.pop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pop: %v", err)
	}
}

func TestActorSendSnapshotsRPC(t *testing.T) {
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	a := r.GetOrCreate(peer.ID("peer"))
	rpc := &pb.RPC{Publish: []*pb.Message{{Data: []byte("original")}}}
	if err := a.Send(rpc, false); err != nil {
		t.Fatal(err)
	}
	got, err := a.queue.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got == rpc {
		t.Fatal("Send enqueued the supplied RPC pointer")
	}
	if !proto.Equal(got, rpc) {
		t.Fatalf("queued RPC differs from original: got %v, want %v", got, rpc)
	}

	rpc.Publish[0].Data[0] = 'O'
	if string(got.Publish[0].Data) != "original" {
		t.Fatalf("queued nested data changed with original: %q", got.Publish[0].Data)
	}
}

type failingHost struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (h *failingHost) NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return nil, h.err
}

func testConfig(h StreamOpener, hooks Hooks) Config {
	return Config{Host: h, Hooks: hooks, QueueSize: 4, MaxMessageSize: 1 << 20, MaxControlMessageSize: 1 << 20}
}

func TestRegistryReturnsOneActorAndRetires(t *testing.T) {
	h := &failingHost{err: errors.New("open failed")}
	r, err := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	p := peer.ID("peer")
	a := r.GetOrCreate(p)
	if got := r.GetOrCreate(p); got != a {
		t.Fatal("registry created a duplicate actor")
	}
	r.Retire(p)
	if err := a.Send(&pb.RPC{}, false); !errors.Is(err, ErrActorRetired) {
		t.Fatalf("retired send: %v", err)
	}
	if got := r.GetOrCreate(p); got == a {
		t.Fatal("registry reused retired actor")
	}
	r.Stop()
}

func TestRegistryRejectsStaleActorRetirement(t *testing.T) {
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	p := peer.ID("peer")
	stale := r.GetOrCreate(p)
	if _, ok := r.RetireActor(stale); !ok {
		t.Fatal("current actor was not retired")
	}
	current := r.GetOrCreate(p)
	if _, ok := r.RetireActor(stale); ok {
		t.Fatal("stale actor retired current generation")
	}
	if !r.IsCurrent(current) {
		t.Fatal("replacement actor is not current")
	}
}

func TestStartCoalescesWhileOpening(t *testing.T) {
	ctxSeen := make(chan struct{})
	h := &blockingHost{started: ctxSeen}
	r, err := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	a := r.GetOrCreate(peer.ID("peer"))
	if err := a.Start(testOpen(0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctxSeen:
	case <-time.After(time.Second):
		t.Fatal("open did not start")
	}
	for i := 0; i < 10; i++ {
		if err := a.Start(testOpen(0)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := h.count(); got != 1 {
		t.Fatalf("open calls = %d, want 1", got)
	}
	r.Stop()
}

type blockingHost struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	once    sync.Once
}

func (h *blockingHost) NewStream(ctx context.Context, _ peer.ID, _ ...protocol.ID) (network.Stream, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	h.once.Do(func() { close(h.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func (h *blockingHost) count() int { h.mu.Lock(); defer h.mu.Unlock(); return h.calls }

type streamRead struct {
	data []byte
	err  error
}

type testConn struct {
	network.Conn
	remote peer.ID
}

func (c *testConn) RemotePeer() peer.ID { return c.remote }

type testStream struct {
	network.Stream
	conn network.Conn

	reads  chan streamRead
	writes chan []byte

	mu           sync.Mutex
	pending      []byte
	terminalErr  error
	writeErr     error
	readStarted  chan struct{}
	writeStarted chan struct{}
	readGate     <-chan struct{}
	writeGate    <-chan struct{}
	readOnce     sync.Once
	writeOnce    sync.Once
	resetCount   int
	closeCount   int
	reset        chan struct{}
	resetOnce    sync.Once
	protocol     protocol.ID
}

func newTestStream(p peer.ID) *testStream {
	return &testStream{
		conn:   &testConn{remote: p},
		reads:  make(chan streamRead, 8),
		writes: make(chan []byte, 8),
		reset:  make(chan struct{}),
	}
}

func (s *testStream) Conn() network.Conn               { return s.conn }
func (s *testStream) Protocol() protocol.ID            { return s.protocol }
func (s *testStream) SetProtocol(id protocol.ID) error { s.protocol = id; return nil }

func (s *testStream) Read(p []byte) (int, error) {
	if s.readStarted != nil {
		s.readOnce.Do(func() { close(s.readStarted) })
	}
	if s.readGate != nil {
		select {
		case <-s.readGate:
		case <-s.reset:
			return 0, network.ErrReset
		}
	}
	for {
		s.mu.Lock()
		if len(s.pending) > 0 {
			n := copy(p, s.pending)
			s.pending = s.pending[n:]
			s.mu.Unlock()
			return n, nil
		}
		if s.terminalErr != nil {
			err := s.terminalErr
			s.mu.Unlock()
			return 0, err
		}
		s.mu.Unlock()
		select {
		case r := <-s.reads:
			if len(r.data) == 0 {
				s.mu.Lock()
				s.terminalErr = r.err
				s.mu.Unlock()
				return 0, r.err
			}
			s.mu.Lock()
			s.pending = append(s.pending, r.data...)
			s.mu.Unlock()
		case <-s.reset:
			return 0, network.ErrReset
		}
	}
}

func (s *testStream) Write(p []byte) (int, error) {
	if s.writeStarted != nil {
		s.writeOnce.Do(func() { close(s.writeStarted) })
	}
	if s.writeGate != nil {
		select {
		case <-s.writeGate:
		case <-s.reset:
			return 0, network.ErrReset
		}
	}
	s.mu.Lock()
	err := s.writeErr
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	b := append([]byte(nil), p...)
	s.writes <- b
	return len(p), nil
}

func (s *testStream) Close() error {
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	s.resetOnce.Do(func() { close(s.reset) })
	return nil
}

func (s *testStream) Reset() error {
	s.mu.Lock()
	s.resetCount++
	s.mu.Unlock()
	s.resetOnce.Do(func() { close(s.reset) })
	return nil
}

func (s *testStream) SetWriteDeadline(time.Time) error { return nil }

func (s *testStream) counts() (closed, reset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount, s.resetCount
}

func frameProto(t *testing.T, rpc proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(rpc)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, binary.MaxVarintLen64+len(payload))
	n := binary.PutUvarint(buf, uint64(len(payload)))
	copy(buf[n:], payload)
	return buf[:n+len(payload)]
}

func decodeFrameInto(t *testing.T, frame []byte, message proto.Message) {
	t.Helper()
	size, n := binary.Uvarint(frame)
	if n <= 0 || int(size) != len(frame)-n {
		t.Fatalf("invalid frame: size=%d prefix=%d bytes=%d", size, n, len(frame))
	}
	if err := proto.Unmarshal(frame[n:], message); err != nil {
		t.Fatal(err)
	}
}

func decodeFrame(t *testing.T, frame []byte) *pb.RPC {
	rpc := new(pb.RPC)
	decodeFrameInto(t, frame, rpc)
	return rpc
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func waitDone(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	receive(t, ch, what)
}

func TestInboundReplacementCallbackOrdering(t *testing.T) {
	p := peer.ID("peer")
	events := make(chan string, 4)
	oldClosing := make(chan struct{})
	releaseClose := make(chan struct{})
	old := newTestStream(p)
	fresh := newTestStream(p)

	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{
		InboundOpened: func(_ *Actor, s network.Stream) {
			if s == old {
				events <- "open-old"
			} else {
				events <- "open-new"
			}
		},
		InboundClosed: func(_ *Actor, s network.Stream) {
			if s == old {
				close(oldClosing)
				<-releaseClose
				events <- "close-old"
			} else {
				events <- "close-new"
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	a := r.GetOrCreate(p)
	oldDone := make(chan struct{})
	go func() { a.HandleInbound(old); close(oldDone) }()
	if got := receive(t, events, "old open"); got != "open-old" {
		t.Fatalf("first event = %q", got)
	}
	newDone := make(chan struct{})
	go func() { a.HandleInbound(fresh); close(newDone) }()
	waitDone(t, oldClosing, "old close callback")
	select {
	case got := <-events:
		t.Fatalf("replacement opened before old close completed: %q", got)
	default:
	}
	close(releaseClose)
	if got := receive(t, events, "old close"); got != "close-old" {
		t.Fatalf("second event = %q", got)
	}
	if got := receive(t, events, "new open"); got != "open-new" {
		t.Fatalf("third event = %q", got)
	}
	waitDone(t, oldDone, "old handler")

	// Completion of the stale old run must not clear the replacement.
	a.inboundMu.Lock()
	if a.currentInbound == nil || a.currentInbound.stream != fresh {
		t.Fatal("old close cleared replacement state")
	}
	a.inboundMu.Unlock()
	r.Stop()
	waitDone(t, newDone, "new handler")
}

func TestInboundCloseClaimedExactlyOnce(t *testing.T) {
	p := peer.ID("peer")
	const protoID = protocol.ID("/inbound/1")
	opened := make(chan struct{})
	closed := make(chan struct{}, 1)
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{
		InboundOpened: func(*Actor, network.Stream) { close(opened) },
		InboundClosed: func(*Actor, network.Stream) { closed <- struct{}{} },
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(p)
	s := newTestStream(p)
	s.protocol = protoID
	done := make(chan struct{})
	go func() { a.HandleInbound(s); close(done) }()
	waitDone(t, opened, "inbound open")
	s.reads <- streamRead{err: io.EOF}
	waitDone(t, closed, "inbound close")
	waitDone(t, done, "inbound handler")
	retirement, ok := r.RetireActor(a)
	if !ok {
		t.Fatal("actor was not retired")
	}
	if retirement.HadInbound {
		t.Fatalf("retirement reclaimed closed inbound protocol %q", retirement.InboundProtocol)
	}
}

func TestRetirementClaimsInboundAndSuppressesCloseHook(t *testing.T) {
	p := peer.ID("peer")
	const protoID = protocol.ID("/inbound/1")
	opened := make(chan struct{})
	closed := make(chan struct{}, 1)
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{
		InboundOpened: func(*Actor, network.Stream) { close(opened) },
		InboundClosed: func(*Actor, network.Stream) { closed <- struct{}{} },
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(p)
	s := newTestStream(p)
	s.protocol = protoID
	done := make(chan struct{})
	go func() { a.HandleInbound(s); close(done) }()
	waitDone(t, opened, "inbound open")
	retirement, ok := r.RetireActor(a)
	if !ok {
		t.Fatal("actor was not retired")
	}
	if !retirement.HadInbound || retirement.InboundProtocol != protoID {
		t.Fatalf("retirement inbound = %q/%t, want %q/true", retirement.InboundProtocol, retirement.HadInbound, protoID)
	}
	waitDone(t, done, "retired inbound handler")
	select {
	case <-closed:
		t.Fatal("retired inbound emitted duplicate close hook")
	default:
	}
}

func TestInboundValidFrameAuthenticatesPeer(t *testing.T) {
	p := peer.ID("authenticated-peer")
	rpcs := make(chan *pb.RPC, 1)
	actors := make(chan peer.ID, 1)
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{
		InboundRPC: func(a *Actor, _ network.Stream, transport Transport, rpc *pb.RPC) {
			if transport != TransportControl {
				t.Errorf("transport = %v, want control", transport)
			}
			actors <- a.Peer()
			rpcs <- rpc
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(p)
	s := newTestStream(p)
	want := &pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: proto.String("topic"), Subscribe: proto.Bool(true)}}}
	s.reads <- streamRead{data: frameProto(t, want)}
	s.reads <- streamRead{err: io.EOF}
	done := make(chan struct{})
	go func() { a.HandleInbound(s); close(done) }()
	got := receive(t, rpcs, "inbound RPC")
	if !proto.Equal(got, want) {
		t.Fatalf("RPC mismatch: got %v want %v", got, want)
	}
	if gotPeer := receive(t, actors, "authenticated actor"); gotPeer != p {
		t.Fatalf("actor peer = %q, want %q", gotPeer, p)
	}
	waitDone(t, done, "inbound EOF")
	closed, reset := s.counts()
	if closed != 1 || reset != 0 {
		t.Fatalf("EOF close/reset = %d/%d, want 1/0", closed, reset)
	}
}

func TestInboundFramingFailuresReset(t *testing.T) {
	tests := []struct {
		name   string
		config func(Config) Config
		input  func(*testing.T) []byte
	}{
		{name: "malformed protobuf", input: func(*testing.T) []byte { return []byte{1, 0xff} }},
		{name: "oversized envelope", config: func(c Config) Config { c.MaxMessageSize = 2; return c }, input: func(*testing.T) []byte { return []byte{3} }},
		{name: "oversized control", config: func(c Config) Config { c.MaxControlMessageSize = 1; return c }, input: func(t *testing.T) []byte {
			return frameProto(t, &pb.RPC{Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: proto.String("too-large")}}}})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := peer.ID("peer")
			cfg := testConfig(&failingHost{}, Hooks{})
			if tt.config != nil {
				cfg = tt.config(cfg)
			}
			r, err := NewRegistry(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Stop()
			s := newTestStream(p)
			s.reads <- streamRead{data: tt.input(t)}
			done := make(chan struct{})
			go func() { r.GetOrCreate(p).HandleInbound(s); close(done) }()
			waitDone(t, done, "framing failure")
			closed, reset := s.counts()
			if closed != 0 || reset != 1 {
				t.Fatalf("close/reset = %d/%d, want 0/1", closed, reset)
			}
		})
	}
}

type openRequest struct {
	ctx    context.Context
	result chan openResult
}

type openResult struct {
	stream network.Stream
	err    error
}

type controlledHost struct{ calls chan openRequest }

func (h *controlledHost) NewStream(ctx context.Context, _ peer.ID, _ ...protocol.ID) (network.Stream, error) {
	req := openRequest{ctx: ctx, result: make(chan openResult, 1)}
	h.calls <- req
	result := <-req.result
	return result.stream, result.err
}

func outboundConfig(h StreamOpener, hooks Hooks) Config {
	if hooks.OutboundReady == nil {
		hooks.OutboundReady = func(a *Actor, stream network.Stream) { _ = a.Activate(stream, nil) }
	}
	return testConfig(h, hooks)
}

func testOpen(backoff time.Duration) OpenRequest {
	return OpenRequest{Protocols: []protocol.ID{"/test/1"}, Backoff: backoff}
}

func TestHelloWrittenBeforeQueuedRPC(t *testing.T) {
	h := &controlledHost{calls: make(chan openRequest, 1)}
	hello := &pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: proto.String("hello")}}}
	queued := &pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: proto.String("queued")}}}
	r, err := NewRegistry(context.Background(), outboundConfig(h, Hooks{OutboundReady: func(a *Actor, stream network.Stream) { _ = a.Activate(stream, hello) }}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	if err := a.Send(queued, false); err != nil {
		t.Fatal(err)
	}
	if err := a.Start(testOpen(0)); err != nil {
		t.Fatal(err)
	}
	req := receive(t, h.calls, "open request")
	s := newTestStream(a.Peer())
	req.result <- openResult{stream: s}
	if got := decodeFrame(t, receive(t, s.writes, "hello write")); !proto.Equal(got, hello) {
		t.Fatalf("first write = %v", got)
	}
	if got := decodeFrame(t, receive(t, s.writes, "queued write")); !proto.Equal(got, queued) {
		t.Fatalf("second write = %v", got)
	}
}

func TestWriteAndDeathRaceNotifiesTerminalOnce(t *testing.T) {
	h := &controlledHost{calls: make(chan openRequest, 1)}
	writeErr := errors.New("write failed")
	dead := make(chan error, 2)
	failed := make(chan error, 2)
	r, err := NewRegistry(context.Background(), outboundConfig(h, Hooks{
		OutboundDead:       func(_ *Actor, _ network.Stream, err error) { dead <- err },
		OutboundSendFailed: func(_ *Actor, _ network.Stream, _ *pb.RPC, err error) { failed <- err },
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	a := r.GetOrCreate(peer.ID("peer"))
	if err := a.Send(&pb.RPC{}, false); err != nil {
		t.Fatal(err)
	}
	if err := a.Start(testOpen(0)); err != nil {
		t.Fatal(err)
	}
	req := receive(t, h.calls, "open request")
	s := newTestStream(a.Peer())
	readGate := make(chan struct{})
	writeGate := make(chan struct{})
	s.readStarted = make(chan struct{})
	s.writeStarted = make(chan struct{})
	s.readGate = readGate
	s.writeGate = writeGate
	s.mu.Lock()
	s.writeErr = writeErr
	s.terminalErr = errors.New("read failed")
	s.mu.Unlock()
	req.result <- openResult{stream: s}
	waitDone(t, s.readStarted, "death watcher read")
	waitDone(t, s.writeStarted, "writer")
	close(readGate)
	close(writeGate)
	if !errors.Is(receive(t, failed, "send failure"), writeErr) {
		t.Fatal("wrong send failure")
	}
	receive(t, dead, "terminal notification")
	select {
	case err := <-dead:
		t.Fatalf("duplicate terminal notification: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRetirementDuringOpenAndSendRace(t *testing.T) {
	h := &controlledHost{calls: make(chan openRequest, 1)}
	r, err := NewRegistry(context.Background(), outboundConfig(h, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	a := r.GetOrCreate(peer.ID("peer"))
	if err := a.Start(testOpen(0)); err != nil {
		t.Fatal(err)
	}
	req := receive(t, h.calls, "open request")

	const senders = 32
	start := make(chan struct{})
	errs := make(chan error, senders)
	for i := 0; i < senders; i++ {
		go func() { <-start; errs <- a.Send(&pb.RPC{}, false) }()
	}
	close(start)
	r.Retire(a.Peer())
	for i := 0; i < senders; i++ {
		err := receive(t, errs, "racing send")
		if err != nil && !errors.Is(err, ErrActorRetired) && !errors.Is(err, ErrQueueFull) {
			t.Fatalf("send error = %v", err)
		}
	}
	stale := newTestStream(a.Peer())
	req.result <- openResult{stream: stale}
	waitDone(t, a.Done(), "actor retirement")
	if err := a.Send(&pb.RPC{}, false); !errors.Is(err, ErrActorRetired) {
		t.Fatalf("post-retirement send = %v", err)
	}
	waitDone(t, stale.reset, "retired open result reset")
	_, resets := stale.counts()
	if resets != 1 {
		t.Fatalf("retired open result resets = %d, want 1", resets)
	}
}

func TestStaleActorCallbacksAndRegistryShutdown(t *testing.T) {
	h := &failingHost{err: errors.New("open failed")}
	r, err := NewRegistry(context.Background(), testConfig(h, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	p := peer.ID("peer")
	old := r.GetOrCreate(p)
	r.Retire(p)
	fresh := r.GetOrCreate(p)
	if fresh == old {
		t.Fatal("actor was not replaced")
	}
	if _, ok := r.RetireActor(old); ok {
		t.Fatal("stale actor callback retired replacement")
	}
	if got, ok := r.Lookup(p); !ok || got != fresh {
		t.Fatal("replacement missing after stale callback")
	}

	r.Stop()
	waitDone(t, fresh.Done(), "registry shutdown")
	if _, ok := r.Lookup(p); ok {
		t.Fatal("registry retained actor after shutdown")
	}
	postStop := r.GetOrCreate(p)
	waitDone(t, postStop.Done(), "post-stop actor")
	if err := postStop.Start(testOpen(0)); !errors.Is(err, ErrActorRetired) {
		t.Fatalf("post-stop start = %v", err)
	}
	if err := postStop.Send(&pb.RPC{}, false); !errors.Is(err, ErrActorRetired) {
		t.Fatalf("post-stop send = %v", err)
	}
}

func TestRetirementDuringBackoffReadAndWrite(t *testing.T) {
	t.Run("backoff", func(t *testing.T) {
		h := &controlledHost{calls: make(chan openRequest, 1)}
		r, err := NewRegistry(context.Background(), outboundConfig(h, Hooks{}))
		if err != nil {
			t.Fatal(err)
		}
		a := r.GetOrCreate(peer.ID("peer"))
		if err := a.Start(testOpen(time.Hour)); err != nil {
			t.Fatal(err)
		}
		a.Retire()
		waitDone(t, a.Done(), "backoff retirement")
		select {
		case <-h.calls:
			t.Fatal("open started after retirement during backoff")
		default:
		}
	})

	t.Run("inbound read", func(t *testing.T) {
		p := peer.ID("peer")
		closed := make(chan struct{})
		r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{InboundClosed: func(*Actor, network.Stream) { close(closed) }}))
		if err != nil {
			t.Fatal(err)
		}
		a := r.GetOrCreate(p)
		s := newTestStream(p)
		started := make(chan struct{})
		s.readStarted = started
		done := make(chan struct{})
		go func() { a.HandleInbound(s); close(done) }()
		waitDone(t, started, "inbound read")
		a.Retire()
		waitDone(t, closed, "inbound close callback")
		waitDone(t, done, "inbound read retirement")
		_, resets := s.counts()
		if resets == 0 {
			t.Fatal("retirement did not reset inbound read")
		}
	})

	t.Run("outbound write", func(t *testing.T) {
		h := &controlledHost{calls: make(chan openRequest, 1)}
		r, err := NewRegistry(context.Background(), outboundConfig(h, Hooks{}))
		if err != nil {
			t.Fatal(err)
		}
		a := r.GetOrCreate(peer.ID("peer"))
		if err := a.Send(&pb.RPC{}, false); err != nil {
			t.Fatal(err)
		}
		if err := a.Start(testOpen(0)); err != nil {
			t.Fatal(err)
		}
		req := receive(t, h.calls, "open request")
		s := newTestStream(a.Peer())
		s.writeStarted = make(chan struct{})
		s.writeGate = make(chan struct{})
		req.result <- openResult{stream: s}
		waitDone(t, s.writeStarted, "outbound write")
		a.Retire()
		waitDone(t, a.Done(), "outbound write retirement")
		waitDone(t, s.reset, "outbound write reset")
	})
}

func TestInboundRejectsMismatchedPeer(t *testing.T) {
	r, err := NewRegistry(context.Background(), testConfig(&failingHost{}, Hooks{}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	s := newTestStream(peer.ID("impostor"))
	done := make(chan struct{})
	go func() { r.GetOrCreate(peer.ID("expected")).HandleInbound(s); close(done) }()
	waitDone(t, done, "peer mismatch rejection")
	_, resets := s.counts()
	if resets != 1 {
		t.Fatalf("mismatched peer resets = %d, want 1", resets)
	}
}
