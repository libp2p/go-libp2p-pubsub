package pubsub

import (
	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool)

	for t := range p.mySubs {
		// don't announce fanout-only topics
		if topic := p.myTopics[t]; topic != nil && topic.fanoutOnly {
			continue
		}
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		var requestPartial, supportsPartialMessages bool
		if ts, ok := p.myTopics[t]; ok {
			requestPartial = ts.requestPartialMessages
			supportsPartialMessages = ts.supportsPartialMessages
		}
		as := &pb.RPC_SubOpts{
			Topicid:                proto.String(t),
			Subscribe:              proto.Bool(true),
			RequestsPartial:        &requestPartial,
			SupportsSendingPartial: &supportsPartialMessages,
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) enqueuePeerEvent(event incomingUnion) {
	select {
	case p.incoming <- event:
	case <-p.ctx.Done():
	}
}

func (p *PubSub) handleNewStream(s network.Stream) {
	response := make(chan *peercomm.Actor, 1)
	select {
	case p.eval <- func() {
		pid := s.Conn().RemotePeer()
		if p.blacklist.Contains(pid) {
			response <- nil
			return
		}
		a, ok := p.peerComm.Lookup(pid)
		if !ok {
			a = p.peerComm.GetOrCreate(pid)
			_ = a.Start(p.openRequest(0))
		}
		response <- a
	}:
	case <-p.ctx.Done():
		_ = s.Reset()
		return
	}
	select {
	case a := <-response:
		if a == nil {
			_ = s.Reset()
			return
		}
		a.HandleInbound(s)
	case <-p.ctx.Done():
		_ = s.Reset()
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
