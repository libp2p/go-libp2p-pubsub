package pubsub

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Generic event tracer interface
type EventTracer interface {
	Trace(evt *pb.TraceEvent)
}

// pubsub tracer details
type pubsubTracer struct {
	tracer EventTracer
	pid    peer.ID
	msgID  MsgIdFunction
}

func (t *pubsubTracer) PublishMessage(msg *Message) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_PUBLISH_MESSAGE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		PublishMessage: &pb.TraceEvent_PublishMessage{
			MessageID: []byte(t.msgID(msg.Message)),
			Topics:    msg.Message.TopicIDs,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) RejectMessage(msg *Message, reason string) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_REJECT_MESSAGE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		RejectMessage: &pb.TraceEvent_RejectMessage{
			MessageID:    []byte(t.msgID(msg.Message)),
			ReceivedFrom: []byte(msg.ReceivedFrom),
			Reason:       &reason,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) DuplicateMessage(msg *Message) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DUPLICATE_MESSAGE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		DuplicateMessage: &pb.TraceEvent_DuplicateMessage{
			MessageID:    []byte(t.msgID(msg.Message)),
			ReceivedFrom: []byte(msg.ReceivedFrom),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) DeliverMessage(msg *Message) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DELIVER_MESSAGE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		DeliverMessage: &pb.TraceEvent_DeliverMessage{
			MessageID: []byte(t.msgID(msg.Message)),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) AddPeer(p peer.ID, proto protocol.ID) {
	if t == nil {
		return
	}

	protoStr := string(proto)
	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_ADD_PEER.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		AddPeer: &pb.TraceEvent_AddPeer{
			PeerID: []byte(p),
			Proto:  &protoStr,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) RemovePeer(p peer.ID) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_REMOVE_PEER.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		RemovePeer: &pb.TraceEvent_RemovePeer{
			PeerID: []byte(p),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) RecvRPC(rpc *RPC) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_RECV_RPC.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		RecvRPC: &pb.TraceEvent_RecvRPC{
			ReceivedFrom: []byte(rpc.from),
			Meta:         t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) SendRPC(rpc *RPC, p peer.ID) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_SEND_RPC.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		SendRPC: &pb.TraceEvent_SendRPC{
			SendTo: []byte(rpc.from),
			Meta:   t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) DropRPC(rpc *RPC, p peer.ID) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_DROP_RPC.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		DropRPC: &pb.TraceEvent_DropRPC{
			SendTo: []byte(rpc.from),
			Meta:   t.traceRPCMeta(rpc),
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) traceRPCMeta(rpc *RPC) *pb.TraceEvent_RPCMeta {
	rpcMeta := new(pb.TraceEvent_RPCMeta)

	var msgs []*pb.TraceEvent_MessageMeta
	for _, m := range rpc.Publish {
		msgs = append(msgs, &pb.TraceEvent_MessageMeta{
			MessageID: []byte(t.msgID(m)),
			Topics:    m.TopicIDs,
		})
	}
	rpcMeta.Messages = msgs

	var subs []*pb.TraceEvent_SubMeta
	for _, sub := range rpc.Subscriptions {
		subs = append(subs, &pb.TraceEvent_SubMeta{
			Subscribe: sub.Subscribe,
			Topic:     sub.Topicid,
		})
	}
	rpcMeta.Subscription = subs

	if rpc.Control != nil {
		var ihave []*pb.TraceEvent_ControlIHaveMeta
		for _, ctl := range rpc.Control.Ihave {
			var mids [][]byte
			for _, mid := range ctl.MessageIDs {
				mids = append(mids, []byte(mid))
			}
			ihave = append(ihave, &pb.TraceEvent_ControlIHaveMeta{
				Topic:      ctl.TopicID,
				MessageIDs: mids,
			})
		}

		var iwant []*pb.TraceEvent_ControlIWantMeta
		for _, ctl := range rpc.Control.Iwant {
			var mids [][]byte
			for _, mid := range ctl.MessageIDs {
				mids = append(mids, []byte(mid))
			}
			iwant = append(iwant, &pb.TraceEvent_ControlIWantMeta{
				MessageIDs: mids,
			})
		}

		var graft []*pb.TraceEvent_ControlGraftMeta
		for _, ctl := range rpc.Control.Graft {
			graft = append(graft, &pb.TraceEvent_ControlGraftMeta{
				Topic: ctl.TopicID,
			})
		}

		var prune []*pb.TraceEvent_ControlPruneMeta
		for _, ctl := range rpc.Control.Prune {
			prune = append(prune, &pb.TraceEvent_ControlPruneMeta{
				Topic: ctl.TopicID,
			})
		}

		rpcMeta.Control = &pb.TraceEvent_ControlMeta{
			Ihave: ihave,
			Iwant: iwant,
			Graft: graft,
			Prune: prune,
		}
	}

	return rpcMeta
}

func (t *pubsubTracer) Join(topic string) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_JOIN.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		Join: &pb.TraceEvent_Join{
			Topic: &topic,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) Leave(topic string) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_LEAVE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		Leave: &pb.TraceEvent_Leave{
			Topic: &topic,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) Graft(p peer.ID, topic string) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_GRAFT.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		Graft: &pb.TraceEvent_Graft{
			PeerID: []byte(p),
			Topic:  &topic,
		},
	}

	t.tracer.Trace(evt)
}

func (t *pubsubTracer) Prune(p peer.ID, topic string) {
	if t == nil {
		return
	}

	now := time.Now().UnixNano()
	evt := &pb.TraceEvent{
		Type:      pb.TraceEvent_PRUNE.Enum(),
		PeerID:    []byte(t.pid),
		Timestamp: &now,
		Prune: &pb.TraceEvent_Prune{
			PeerID: []byte(p),
			Topic:  &topic,
		},
	}

	t.tracer.Trace(evt)
}
