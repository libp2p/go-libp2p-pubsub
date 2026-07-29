// Package peercomm owns pubsub control transport communication for each peer.
package peercomm

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"sync"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

const WriteTimeout = 30 * time.Second

var (
	ErrQueueFull    = errors.New("peercomm: outbound queue full")
	ErrQueueClosed  = errors.New("peercomm: outbound queue closed")
	ErrActorRetired = errors.New("peercomm: actor retired")
	ErrNoProtocols  = errors.New("peercomm: no protocols available")
)

// StreamOpener is the subset of host.Host needed by peer communication.
type StreamOpener interface {
	NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error)
}

// Transport identifies the control or topic stream carrying an inbound RPC.
type Transport uint8

const (
	TransportControl Transport = iota
	TransportTopic
)

// Hooks emits transport events without calling back into root PubSub for policy decisions.
type Hooks struct {
	InboundOpened      func(*Actor, network.Stream)
	InboundRPC         func(*Actor, network.Stream, Transport, *pb.RPC)
	InboundClosed      func(*Actor, network.Stream)
	OutboundReady      func(*Actor, network.Stream)
	OutboundSent       func(*Actor, network.Stream, *pb.RPC)
	OutboundSendFailed func(*Actor, network.Stream, *pb.RPC, error)
	OutboundOpenFailed func(*Actor, error)
	OutboundDead       func(*Actor, network.Stream, error)
	TopicAllowed       func(*Actor, string) bool
	TopicMisbehavior   func(*Actor, string)
}

// Config configures all actors in a Registry.
type Config struct {
	Host                  StreamOpener
	Hooks                 Hooks
	QueueSize             int
	MaxMessageSize        int
	MaxControlMessageSize int
}

// Registry owns at most one Actor for each peer.
type Registry struct {
	ctx    context.Context
	cancel context.CancelFunc
	config Config

	mu      sync.Mutex
	actors  map[peer.ID]*Actor
	stopped bool
}

func NewRegistry(ctx context.Context, config Config) (*Registry, error) {
	if ctx == nil {
		return nil, errors.New("peercomm: nil context")
	}
	if config.Host == nil {
		return nil, errors.New("peercomm: nil host")
	}
	if config.QueueSize <= 0 || config.MaxMessageSize <= 0 || config.MaxControlMessageSize <= 0 {
		return nil, errors.New("peercomm: queue and message limits must be positive")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Registry{ctx: ctx, cancel: cancel, config: config, actors: make(map[peer.ID]*Actor)}, nil
}

// GetOrCreate returns the current actor or atomically creates its successor.
func (r *Registry) GetOrCreate(p peer.ID) *Actor {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a := r.actors[p]; a != nil {
		return a
	}
	a := newActor(r, p)
	if !r.stopped {
		r.actors[p] = a
	}
	return a
}

// Lookup returns the current actor without creating it.
func (r *Registry) Lookup(p peer.ID) (*Actor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.actors[p]
	return a, ok
}

// IsCurrent reports whether actor is the authoritative generation for its peer.
func (r *Registry) IsCurrent(actor *Actor) bool {
	if actor == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.actors[actor.peer] == actor
}

// All returns an iterator over a stable snapshot of the current actors.
func (r *Registry) All() iter.Seq2[peer.ID, *Actor] {
	r.mu.Lock()
	actors := make(map[peer.ID]*Actor, len(r.actors))
	for id, actor := range r.actors {
		actors[id] = actor
	}
	r.mu.Unlock()
	return func(yield func(peer.ID, *Actor) bool) {
		for id, actor := range actors {
			if !yield(id, actor) {
				return
			}
		}
	}
}

// Retire removes and stops p's current actor. A later Actor call creates a fresh one.
func (r *Registry) Retire(p peer.ID) {
	r.mu.Lock()
	a := r.actors[p]
	if a != nil {
		delete(r.actors, p)
	}
	r.mu.Unlock()
	if a != nil {
		a.Retire()
	}
}

// Retirement describes logical transport teardown claimed while retiring an actor.
type Retirement struct {
	InboundProtocol protocol.ID
	HadInbound      bool
}

// RetireActor removes and stops actor only if it is still current. It claims
// logical inbound teardown before the actor can be replaced.
func (r *Registry) RetireActor(actor *Actor) (Retirement, bool) {
	if actor == nil {
		return Retirement{}, false
	}
	r.mu.Lock()
	if r.actors[actor.peer] != actor {
		r.mu.Unlock()
		return Retirement{}, false
	}
	retirement := actor.claimCurrentInboundClose()
	delete(r.actors, actor.peer)
	r.mu.Unlock()
	actor.Retire()
	return retirement, true
}

// Stop retires every actor and prevents ongoing communication.
func (r *Registry) Stop() {
	r.cancel()
	r.mu.Lock()
	r.stopped = true
	actors := r.actors
	r.actors = make(map[peer.ID]*Actor)
	r.mu.Unlock()
	for _, a := range actors {
		a.Retire()
	}
}

type commandKind uint8

const (
	commandStart commandKind = iota
	commandOpenResult
	commandOutboundClosed
	commandActivate
)

type command struct {
	kind       commandKind
	generation uint64
	backoff    time.Duration
	stream     network.Stream
	err        error
	hello      *pb.RPC
	protocols  []protocol.ID
}

// OpenRequest is an immutable outbound-open command prepared by PubSub.
type OpenRequest struct {
	Protocols []protocol.ID
	Backoff   time.Duration
}

// Actor owns a peer's outbound queue, stream generations, reconnect timer, and
// inbound replacement state.
type Actor struct {
	registry *Registry
	peer     peer.ID
	ctx      context.Context
	cancel   context.CancelFunc
	queue    *rpcQueue
	commands chan command
	done     chan struct{}

	inboundMu       sync.Mutex
	currentInbound  *inboundRun
	notifiedInbound *inboundRun
	retire          sync.Once

	topicMu             sync.Mutex
	topicEnabled        bool
	topicWriters        map[string]*topicWriter
	topicWritersWG      sync.WaitGroup
	topicInbound        map[string]*topicInboundState
	topicInboundStreams map[network.Stream]struct{}
	topicInboundTotal   int
}

type inboundRun struct {
	stream       network.Stream
	done         chan struct{}
	opened       bool
	closeClaimed bool
}

func newActor(r *Registry, p peer.ID) *Actor {
	ctx, cancel := context.WithCancel(r.ctx)
	a := &Actor{
		registry: r, peer: p, ctx: ctx, cancel: cancel,
		queue: newRPCQueue(r.config.QueueSize), commands: make(chan command, 16), done: make(chan struct{}),
		topicWriters: make(map[string]*topicWriter), topicInbound: make(map[string]*topicInboundState),
		topicInboundStreams: make(map[network.Stream]struct{}),
	}
	go a.run()
	return a
}

func (a *Actor) Peer() peer.ID         { return a.peer }
func (a *Actor) Done() <-chan struct{} { return a.done }

// Start submits a copied outbound-open request. Starting while a stream is live is a no-op.
func (a *Actor) Start(request OpenRequest) error {
	if len(request.Protocols) == 0 {
		return ErrNoProtocols
	}
	protocols := append([]protocol.ID(nil), request.Protocols...)
	return a.command(command{kind: commandStart, backoff: request.Backoff, protocols: protocols})
}

// Activate accepts a ready stream and supplies the prepared immutable hello.
func (a *Actor) Activate(s network.Stream, hello *pb.RPC) error {
	if hello != nil {
		hello = proto.Clone(hello).(*pb.RPC)
	}
	return a.command(command{kind: commandActivate, stream: s, hello: hello})
}

// Send enqueues an RPC without blocking. Urgent RPCs are drained before normal
// RPCs while preserving FIFO order within each class. A successful Send snapshots
// rpc, so callers may inspect or mutate the original afterward, and actors do not
// share protobuf runtime state. If Send fails, it imposes no asynchronous ownership
// or lifetime obligation on rpc.
func (a *Actor) Send(rpc *pb.RPC, urgent bool) error {
	if rpc == nil {
		return errors.New("peercomm: nil rpc")
	}
	select {
	case <-a.ctx.Done():
		return ErrActorRetired
	default:
	}
	err := a.splitAndSend(rpc, urgent)
	if errors.Is(err, ErrQueueClosed) {
		return ErrActorRetired
	}
	return err
}

// Retire permanently cancels this actor and closes its queue.
func (a *Actor) Retire() {
	a.retire.Do(func() {
		a.cancel()
		a.queue.close()
		a.DisableTopicStreams()
	})
}

func (a *Actor) command(c command) error {
	select {
	case <-a.ctx.Done():
		return ErrActorRetired
	default:
	}
	select {
	case a.commands <- c:
		return nil
	case <-a.ctx.Done():
		return ErrActorRetired
	case <-a.done:
		return ErrActorRetired
	}
}

// HandleInbound authenticates the remote peer, atomically replaces an older
// inbound stream, emits close before open for duplicates, and reads RPCs until
// terminal input. EOF closes politely; every other terminal condition resets.
func (a *Actor) HandleInbound(s network.Stream) {
	if s == nil || s.Conn().RemotePeer() != a.peer {
		if s != nil {
			_ = s.Reset()
		}
		return
	}

	run := &inboundRun{stream: s, done: make(chan struct{})}
	a.inboundMu.Lock()
	previous := a.currentInbound
	a.currentInbound = run
	a.inboundMu.Unlock()

	if previous != nil {
		_ = previous.stream.Reset()
		select {
		case <-previous.done:
		case <-a.ctx.Done():
			_ = s.Reset()
			a.finishInbound(run)
			return
		}
	}

	a.inboundMu.Lock()
	if a.currentInbound != run || a.ctx.Err() != nil {
		a.inboundMu.Unlock()
		_ = s.Reset()
		a.finishInbound(run)
		return
	}
	run.opened = true
	a.notifiedInbound = run
	a.inboundMu.Unlock()

	if h := a.registry.config.Hooks.InboundOpened; h != nil {
		h(a, s)
	}
	a.readInbound(run)
}

func (a *Actor) readInbound(run *inboundRun) {
	s := run.stream
	defer a.finishInbound(run)

	r := msgio.NewVarintReaderSize(s, a.registry.config.MaxMessageSize)
	for {
		_, _ = r.NextMsgLen()
		b, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			if errors.Is(err, io.EOF) {
				_ = s.Close()
			} else {
				_ = s.Reset()
			}
			return
		}
		if len(b) == 0 {
			r.ReleaseMsg(b)
			continue
		}
		if err = pb.ValidateRawRPCControlMessageSize(b, a.registry.config.MaxControlMessageSize); err != nil {
			r.ReleaseMsg(b)
			_ = s.Reset()
			return
		}
		rpc := new(pb.RPC)
		err = proto.Unmarshal(b, rpc)
		r.ReleaseMsg(b)
		if err != nil {
			_ = s.Reset()
			return
		}
		if h := a.registry.config.Hooks.InboundRPC; h != nil {
			h(a, s, TransportControl, rpc)
		}
		select {
		case <-a.ctx.Done():
			_ = s.Reset()
			return
		default:
		}
	}
}

func (a *Actor) finishInbound(run *inboundRun) {
	a.inboundMu.Lock()
	if a.currentInbound == run {
		a.currentInbound = nil
	}
	_, notify := a.claimInboundCloseLocked(run)
	a.inboundMu.Unlock()
	if notify {
		if h := a.registry.config.Hooks.InboundClosed; h != nil {
			h(a, run.stream)
		}
	}
	close(run.done)
}

func (a *Actor) claimCurrentInboundClose() Retirement {
	a.inboundMu.Lock()
	defer a.inboundMu.Unlock()
	proto, ok := a.claimInboundCloseLocked(a.notifiedInbound)
	return Retirement{InboundProtocol: proto, HadInbound: ok}
}

func (a *Actor) claimInboundCloseLocked(run *inboundRun) (protocol.ID, bool) {
	if run == nil || !run.opened || run.closeClaimed {
		return "", false
	}
	run.closeClaimed = true
	if a.notifiedInbound == run {
		a.notifiedInbound = nil
	}
	return run.stream.Protocol(), true
}

func (a *Actor) run() {
	defer func() {
		a.stopTopicWriters()
		a.topicWritersWG.Wait()
		close(a.done)
	}()
	var generation uint64
	var current network.Stream
	var pending network.Stream
	var streamCancel context.CancelFunc
	var timerC <-chan time.Time
	opening := false
	var protocols []protocol.ID

	terminate := func(notify bool, err error) {
		generation++
		opening = false
		timerC = nil
		if streamCancel != nil {
			streamCancel()
			streamCancel = nil
		}
		if pending != nil {
			_ = pending.Reset()
			pending = nil
		}
		if current != nil {
			s := current
			current = nil
			_ = s.Reset()
			if notify && a.registry.config.Hooks.OutboundDead != nil {
				a.registry.config.Hooks.OutboundDead(a, s, err)
			}
		}
	}
	schedule := func(backoff time.Duration) {
		if backoff < 0 {
			backoff = 0
		}
		timerC = time.After(backoff)
	}

	for {
		select {
		case <-a.ctx.Done():
			terminate(false, a.ctx.Err())
			a.closeInbound()
			return
		case <-timerC:
			timerC = nil
			if opening || current != nil {
				continue
			}
			generation++
			gen := generation
			opening = true
			go a.open(gen, protocols)
		case c := <-a.commands:
			switch c.kind {
			case commandStart:
				if current == nil && pending == nil && !opening {
					protocols = c.protocols
					schedule(c.backoff)
				}
			case commandOpenResult:
				if c.generation != generation || !opening {
					if c.stream != nil {
						_ = c.stream.Reset()
					}
					continue
				}
				opening = false
				if c.err != nil {
					if h := a.registry.config.Hooks.OutboundOpenFailed; h != nil {
						h(a, c.err)
					}
					continue
				}
				pending = c.stream
				if h := a.registry.config.Hooks.OutboundReady; h != nil {
					h(a, pending)
				}
			case commandActivate:
				if pending == nil || c.stream != pending {
					continue
				}
				current = pending
				pending = nil
				streamCtx, cancel := context.WithCancel(a.ctx)
				streamCancel = cancel
				go a.writeLoop(streamCtx, generation, current, c.hello)
				go a.watchDeath(streamCtx, generation, current)
			case commandOutboundClosed:
				if c.generation != generation || current != c.stream {
					continue
				}
				terminate(true, c.err)
			}
		}
	}
}

func (a *Actor) open(generation uint64, protocols []protocol.ID) {
	s, err := a.registry.config.Host.NewStream(a.ctx, a.peer, protocols...)
	if a.ctx.Err() != nil {
		if s != nil {
			_ = s.Reset()
		}
		return
	}
	select {
	case a.commands <- command{kind: commandOpenResult, generation: generation, stream: s, err: err}:
	case <-a.ctx.Done():
		if s != nil {
			_ = s.Reset()
		}
	}
}

func (a *Actor) writeLoop(ctx context.Context, generation uint64, s network.Stream, hello *pb.RPC) {
	if hello != nil && proto.Size(hello) > 0 {
		if err := a.writeRPC(s, hello); err != nil {
			_ = a.command(command{kind: commandOutboundClosed, generation: generation, stream: s, err: err})
			return
		}
	}
	for {
		rpc, err := a.queue.pop(ctx)
		if err != nil {
			return
		}
		if err = a.writeRPC(s, rpc); err != nil {
			_ = a.command(command{kind: commandOutboundClosed, generation: generation, stream: s, err: err})
			return
		}
	}
}

func (a *Actor) writeRPC(s network.Stream, rpc *pb.RPC) error {
	err := writeProto(s, rpc)
	if err != nil {
		if h := a.registry.config.Hooks.OutboundSendFailed; h != nil {
			h(a, s, rpc, err)
		}
		return err
	}
	if h := a.registry.config.Hooks.OutboundSent; h != nil {
		h(a, s, rpc)
	}
	return nil
}

func (a *Actor) watchDeath(ctx context.Context, generation uint64, s network.Stream) {
	one := []byte{0}
	_, err := s.Read(one)
	select {
	case <-ctx.Done():
		return
	default:
	}
	if err == nil {
		err = errors.New("peercomm: unexpected data on outbound stream")
	}
	_ = a.command(command{kind: commandOutboundClosed, generation: generation, stream: s, err: err})
}

func (a *Actor) closeInbound() {
	a.inboundMu.Lock()
	if a.currentInbound != nil {
		_ = a.currentInbound.stream.Reset()
	}
	a.inboundMu.Unlock()
}

func writeProto(s network.Stream, message proto.Message) error {
	size := uint64(proto.Size(message))
	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)
	n := binary.PutUvarint(buf, size)
	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], message)
	if err != nil {
		return err
	}
	if err = s.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return err
	}
	written, err := s.Write(out)
	if err == nil && written != len(out) {
		return io.ErrShortWrite
	}
	return err
}

type rpcQueue struct {
	mu        sync.Mutex
	available *sync.Cond
	urgent    []*pb.RPC
	normal    []*pb.RPC
	capacity  int
	closed    bool
}

func newRPCQueue(capacity int) *rpcQueue {
	q := &rpcQueue{capacity: capacity}
	q.available = sync.NewCond(&q.mu)
	return q
}

func (q *rpcQueue) push(rpc *pb.RPC, urgent bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if len(q.urgent)+len(q.normal) >= q.capacity {
		return ErrQueueFull
	}
	rpc = proto.Clone(rpc).(*pb.RPC)
	if urgent {
		q.urgent = append(q.urgent, rpc)
	} else {
		q.normal = append(q.normal, rpc)
	}
	q.available.Signal()
	return nil
}

func (q *rpcQueue) pop(ctx context.Context) (*pb.RPC, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stop := context.AfterFunc(ctx, q.available.Broadcast)
	defer stop()
	for len(q.urgent)+len(q.normal) == 0 && !q.closed && ctx.Err() == nil {
		q.available.Wait()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if q.closed {
		return nil, ErrQueueClosed
	}
	var rpc *pb.RPC
	if len(q.urgent) > 0 {
		rpc = q.urgent[0]
		q.urgent[0] = nil
		q.urgent = q.urgent[1:]
	} else {
		rpc = q.normal[0]
		q.normal[0] = nil
		q.normal = q.normal[1:]
	}
	return rpc, nil
}

func (q *rpcQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.available.Broadcast()
	q.mu.Unlock()
}
