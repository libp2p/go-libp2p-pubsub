package pubsub

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool)

	for t := range p.mySubs {
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s network.Stream) {
	peer := s.Conn().RemotePeer()

	// Create stream-level span for the entire stream lifecycle
	_, streamSpan := startSpan(context.Background(), "pubsub.handle_new_stream")
	streamSpan.SetAttributes(
		// attribute.String("pubsub.peer_id", peer.String()),
		attribute.String("pubsub.stream_direction", "inbound"),
	)
	defer streamSpan.End()

	streamStart := time.Now()
	totalMessages := 0
	totalBytes := 0
	duplicateStreamDetected := false

	p.inboundStreamsMx.Lock()
	other, dup := p.inboundStreams[peer]
	if dup {
		duplicateStreamDetected = true
		log.Debugf("duplicate inbound stream from %s; resetting other stream", peer)
		other.Reset()
	}
	p.inboundStreams[peer] = s
	p.inboundStreamsMx.Unlock()

	defer func() {
		streamDuration := time.Since(streamStart)

		// Set final stream metrics
		streamSpan.SetAttributes(
			attribute.Int("pubsub.total_messages_received", totalMessages),
			attribute.Int("pubsub.total_bytes_received", totalBytes),
			attribute.Int64("pubsub.stream_duration_ms", streamDuration.Milliseconds()),
			attribute.Bool("pubsub.duplicate_stream_detected", duplicateStreamDetected),
		)

		if streamDuration > 10*time.Second {
			streamSpan.SetAttributes(attribute.Bool("pubsub.long_lived_stream", true))
		}

		p.inboundStreamsMx.Lock()
		if p.inboundStreams[peer] == s {
			delete(p.inboundStreams, peer)
		}
		p.inboundStreamsMx.Unlock()
	}()

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize)
	var rpcSpan trace.Span
	defer func() {
		if rpcSpan != nil {
			rpcSpan.End()
		}
	}()
	for {
		// Create span for each message read operation
		_, msgSpan := startSpan(context.Background(), "pubsub.read_network_message")
		msgSpan.SetAttributes(
			// attribute.String("pubsub.peer_id", peer.String()),
			attribute.Int("pubsub.message_sequence", totalMessages+1),
		)

		// ignore the values. We only want to know when the first bytes came in.
		_, _ = r.NextMsgLen()
		readStart := time.Now()
		msgbytes, err := r.ReadMsg()
		readDuration := time.Since(readStart)

		if err != nil {
			r.ReleaseMsg(msgbytes)

			msgSpan.SetAttributes(
				attribute.String("pubsub.result", "read_error"),
				attribute.String("pubsub.error", err.Error()),
				attribute.Int64("pubsub.read_duration_us", readDuration.Microseconds()),
			)
			msgSpan.End()

			if err != io.EOF {
				s.Reset()
				log.Debugf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			} else {
				// Just be nice. They probably won't read this
				// but it doesn't hurt to send it.
				s.Close()
			}

			return
		}

		if len(msgbytes) == 0 {
			msgSpan.SetAttributes(
				attribute.String("pubsub.result", "empty_message"),
				attribute.Int64("pubsub.read_duration_us", readDuration.Microseconds()),
			)
			msgSpan.End()
			continue
		}

		messageSize := len(msgbytes)
		totalBytes += messageSize

		// Parse RPC and analyze content
		parseStart := time.Now()
		rpc := new(RPC)
		err = rpc.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		parseDuration := time.Since(parseStart)
		rpc.ctx, rpcSpan = startSpan(context.Background(), "pubsub.rpc.incoming")

		if err != nil {
			msgSpan.SetAttributes(
				attribute.String("pubsub.result", "parse_error"),
				attribute.String("pubsub.error", err.Error()),
				attribute.Int("pubsub.message_size_bytes", messageSize),
				attribute.Int64("pubsub.read_duration_us", readDuration.Microseconds()),
				attribute.Int64("pubsub.parse_duration_us", parseDuration.Microseconds()),
			)
			msgSpan.End()

			s.Reset()
			log.Warnf("bogus rpc from %s: %s", s.Conn().RemotePeer(), err)
			return
		}

		// Analyze RPC content for detailed metrics
		messageCount := len(rpc.GetPublish())
		subscriptionCount := len(rpc.GetSubscriptions())
		controlMessageCount := 0
		ihaveCount, iwantCount, graftCount, pruneCount, idontwantCount := 0, 0, 0, 0, 0

		if rpc.Control != nil {
			ihaveCount = len(rpc.Control.GetIhave())
			iwantCount = len(rpc.Control.GetIwant())
			graftCount = len(rpc.Control.GetGraft())
			pruneCount = len(rpc.Control.GetPrune())
			idontwantCount = len(rpc.Control.GetIdontwant())
			controlMessageCount = ihaveCount + iwantCount + graftCount + pruneCount + idontwantCount
		}

		// Queue to event loop
		queueStart := time.Now()
		rpc.from = peer
		rpc.receivedAt = time.Now() // Set timestamp when RPC was recvd from network

		var queueSpan trace.Span
		rpc.queuedCtx, queueSpan = otelTracer.Start(rpc.ctx, "pubsub.incoming.rpc.queued")

		var queueResult string
		select {
		case p.incoming <- rpc:
			queueResult = "queued"
			totalMessages++
		case <-p.ctx.Done():
			queueSpan.End()
			queueResult = "context_cancelled"
			msgSpan.SetAttributes(
				attribute.String("pubsub.result", queueResult),
				attribute.Int("pubsub.message_size_bytes", messageSize),
				attribute.Int("pubsub.data_message_count", messageCount),
				attribute.Int("pubsub.subscription_count", subscriptionCount),
				attribute.Int("pubsub.control_message_count", controlMessageCount),
				attribute.Int64("pubsub.read_duration_us", readDuration.Microseconds()),
				attribute.Int64("pubsub.parse_duration_us", parseDuration.Microseconds()),
			)
			msgSpan.End()

			// Close is useless because the other side isn't reading.
			s.Reset()
			return
		}
		queueDuration := time.Since(queueStart)
		totalProcessingDuration := time.Since(readStart)

		// Set comprehensive message attributes
		msgSpan.SetAttributes(
			attribute.String("pubsub.result", queueResult),
			attribute.Int("pubsub.message_size_bytes", messageSize),
			attribute.Int("pubsub.data_message_count", messageCount),
			attribute.Int("pubsub.subscription_count", subscriptionCount),
			attribute.Int("pubsub.control_message_count", controlMessageCount),
			attribute.Int("pubsub.ihave_count", ihaveCount),
			attribute.Int("pubsub.iwant_count", iwantCount),
			attribute.Int("pubsub.graft_count", graftCount),
			attribute.Int("pubsub.prune_count", pruneCount),
			attribute.Int("pubsub.idontwant_count", idontwantCount),

			// Timing breakdown
			attribute.Int64("pubsub.read_duration_us", readDuration.Microseconds()),
			attribute.Int64("pubsub.parse_duration_us", parseDuration.Microseconds()),
			attribute.Int64("pubsub.queue_duration_us", queueDuration.Microseconds()),
			attribute.Int64("pubsub.total_processing_duration_us", totalProcessingDuration.Microseconds()),

			// Network arrival timestamp (for correlation with handleIncomingRPC)
			attribute.String("pubsub.network_received_at", readStart.Format(time.RFC3339Nano)),
		)

		// Flag slow network operations
		if readDuration > 10*time.Millisecond {
			msgSpan.SetAttributes(attribute.Bool("pubsub.slow_network_read", true))
		}

		if parseDuration > 5*time.Millisecond {
			msgSpan.SetAttributes(attribute.Bool("pubsub.slow_parse", true))
		}

		if queueDuration > 1*time.Millisecond {
			msgSpan.SetAttributes(attribute.Bool("pubsub.slow_queue", true))
		}

		if totalProcessingDuration > 20*time.Millisecond {
			msgSpan.SetAttributes(attribute.Bool("pubsub.slow_message_processing", true))
		}

		// Flag large messages
		if messageSize > 100*1024 { // 100KB
			msgSpan.SetAttributes(attribute.Bool("pubsub.large_message", true))
		}

		// Flag control message heavy RPCs
		if controlMessageCount > 100 {
			msgSpan.SetAttributes(attribute.Bool("pubsub.control_heavy_rpc", true))
		}

		msgSpan.End()
	}
}

func (p *PubSub) notifyPeerDead(pid peer.ID) {
	p.peerDeadPrioLk.RLock()
	p.peerDeadMx.Lock()
	p.peerDeadPend[pid] = struct{}{}
	p.peerDeadMx.Unlock()
	p.peerDeadPrioLk.RUnlock()

	select {
	case p.peerDead <- struct{}{}:
	default:
	}
}

func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing *rpcQueue) {
	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		log.Debug("opening new stream to peer: ", err, pid)

		select {
		case p.newPeerError <- pid:
		case <-ctx.Done():
		}

		return
	}

	go p.handleSendingMessages(ctx, s, outgoing)
	go p.handlePeerDead(s)
	select {
	case p.newPeerStream <- s:
	case <-ctx.Done():
	}
}

func (p *PubSub) handleNewPeerWithBackoff(ctx context.Context, pid peer.ID, backoff time.Duration, outgoing *rpcQueue) {
	select {
	case <-time.After(backoff):
		p.handleNewPeer(ctx, pid, outgoing)
	case <-ctx.Done():
		return
	}
}

func (p *PubSub) handlePeerDead(s network.Stream) {
	pid := s.Conn().RemotePeer()

	_, err := s.Read([]byte{0})
	if err == nil {
		log.Debugf("unexpected message from %s", pid)
	}

	s.Reset()
	p.notifyPeerDead(pid)
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing *rpcQueue) {
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size())

		buf := pool.Get(varint.UvarintSize(size) + int(size))
		defer pool.Put(buf)

		n := binary.PutUvarint(buf, size)
		_, err := rpc.MarshalTo(buf[n:])
		if err != nil {
			return err
		}

		_, err = s.Write(buf)
		return err
	}

	defer s.Close()
	for ctx.Err() == nil {
		rpc, err := outgoing.Pop(ctx)
		if err != nil {
			log.Debugf("popping message from the queue to send to %s: %s", s.Conn().RemotePeer(), err)
			return
		}

		err = writeRpc(rpc)
		if err != nil {
			s.Reset()
			log.Debugf("writing message to %s: %s", s.Conn().RemotePeer(), err)
			return
		}
	}
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}

func rpcWithControl(msgs []*pb.Message,
	ihave []*pb.ControlIHave,
	iwant []*pb.ControlIWant,
	graft []*pb.ControlGraft,
	prune []*pb.ControlPrune,
	idontwant []*pb.ControlIDontWant) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Publish: msgs,
			Control: &pb.ControlMessage{
				Ihave:     ihave,
				Iwant:     iwant,
				Graft:     graft,
				Prune:     prune,
				Idontwant: idontwant,
			},
		},
	}
}

func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)
	*res = *rpc
	if rpc.Control != nil {
		res.Control = new(pb.ControlMessage)
		*res.Control = *rpc.Control
	}
	return res
}
