package floodsub

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/whyrusleeping/go-floodsub/pb"

	ggio "github.com/gogo/protobuf/io"
	proto "github.com/gogo/protobuf/proto"
	peer "github.com/ipfs/go-libp2p-peer"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p/p2p/host"
	inet "github.com/libp2p/go-libp2p/p2p/net"
	protocol "github.com/libp2p/go-libp2p/p2p/protocol"
)

const ID = protocol.ID("/floodsub/1.0.0")

var (
	AddSubMessageType = "sub"
	UnsubMessageType  = "unsub"
	PubMessageType    = "pub"
)

var log = logging.Logger("floodsub")

type PubSub struct {
	host host.Host

	incoming chan *RPC
	outgoing chan *RPC
	newPeers chan inet.Stream
	peerDead chan peer.ID

	myTopics map[string]chan *Message
	pubsubLk sync.Mutex

	topics  map[string]map[peer.ID]struct{}
	peers   map[peer.ID]chan *RPC
	lastMsg map[peer.ID]uint64

	addSub chan *addSub

	ctx context.Context
}

type Message struct {
	*pb.Message
}

func (m *Message) GetFrom() peer.ID {
	return peer.ID(m.Message.GetFrom())
}

type RPC struct {
	pb.RPC

	// unexported on purpose, not sending this over the wire
	from peer.ID
}

func NewFloodSub(ctx context.Context, h host.Host) *PubSub {
	ps := &PubSub{
		host:     h,
		ctx:      ctx,
		incoming: make(chan *RPC, 32),
		outgoing: make(chan *RPC),
		newPeers: make(chan inet.Stream),
		peerDead: make(chan peer.ID),
		addSub:   make(chan *addSub),
		myTopics: make(map[string]chan *Message),
		topics:   make(map[string]map[peer.ID]struct{}),
		peers:    make(map[peer.ID]chan *RPC),
		lastMsg:  make(map[peer.ID]uint64),
	}

	h.SetStreamHandler(ID, ps.handleNewStream)
	h.Network().Notify(ps)

	go ps.processLoop(ctx)

	return ps
}

func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC
	for t, _ := range p.myTopics {
		rpc.Topics = append(rpc.Topics, t)
	}
	rpc.Type = &AddSubMessageType
	return &rpc
}

func (p *PubSub) handleNewStream(s inet.Stream) {
	defer s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	for {
		rpc := new(RPC)
		err := r.ReadMsg(&rpc.RPC)
		if err != nil {
			if err != io.EOF {
				log.Errorf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		rpc.from = s.Conn().RemotePeer()
		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s inet.Stream, in <-chan *RPC) {
	var dead bool
	bufw := bufio.NewWriter(s)
	wc := ggio.NewDelimitedWriter(bufw)

	writeMsg := func(msg proto.Message) error {
		err := wc.WriteMsg(msg)
		if err != nil {
			return err
		}

		return bufw.Flush()
	}

	defer wc.Close()
	for {
		select {
		case rpc, ok := <-in:
			if !ok {
				return
			}
			if dead {
				// continue in order to drain messages
				continue
			}

			err := writeMsg(&rpc.RPC)
			if err != nil {
				log.Errorf("writing message to %s: %s", s.Conn().RemotePeer(), err)
				dead = true
				go func() {
					p.peerDead <- s.Conn().RemotePeer()
				}()
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *PubSub) processLoop(ctx context.Context) {

	for {
		select {
		case s := <-p.newPeers:
			pid := s.Conn().RemotePeer()
			_, ok := p.peers[pid]
			if ok {
				log.Error("already have connection to peer: ", pid)
				s.Close()
				continue
			}

			messages := make(chan *RPC, 32)
			go p.handleSendingMessages(ctx, s, messages)
			messages <- p.getHelloPacket()

			p.peers[pid] = messages

		case pid := <-p.peerDead:
			delete(p.peers, pid)
		case sub := <-p.addSub:
			p.handleSubscriptionChange(sub)
		case rpc := <-p.incoming:
			err := p.handleIncomingRPC(rpc)
			if err != nil {
				log.Error("handling RPC: ", err)
			}
		case rpc := <-p.outgoing:
			switch rpc.GetType() {
			case AddSubMessageType, UnsubMessageType:
				for _, mch := range p.peers {
					mch <- rpc
				}
			case PubMessageType:
				//fmt.Println("publishing outgoing message")
				err := p.recvMessage(rpc)
				if err != nil {
					log.Error("error receiving message: ", err)
				}

				err = p.publishMessage(rpc)
				if err != nil {
					log.Error("publishing message: ", err)
				}
			}
		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}
func (p *PubSub) handleSubscriptionChange(sub *addSub) {
	ch, ok := p.myTopics[sub.topic]
	out := &RPC{
		RPC: pb.RPC{
			Topics: []string{sub.topic},
		},
	}

	if sub.cancel {
		if !ok {
			return
		}

		close(ch)
		delete(p.myTopics, sub.topic)
		out.Type = &UnsubMessageType
	} else {
		if ok {
			// we don't allow multiple subs per topic at this point
			sub.resp <- nil
			return
		}

		resp := make(chan *Message, 16)
		p.myTopics[sub.topic] = resp
		sub.resp <- resp
		out.Type = &AddSubMessageType
	}

	go func() {
		p.outgoing <- out
	}()
}

func (p *PubSub) recvMessage(rpc *RPC) error {
	subch, ok := p.myTopics[rpc.Msg.GetTopic()]
	if ok {
		//fmt.Println("writing out to subscriber!")
		subch <- &Message{rpc.Msg}
	}
	return nil
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) error {
	switch rpc.GetType() {
	case AddSubMessageType:
		for _, t := range rpc.Topics {
			tmap, ok := p.topics[t]
			if !ok {
				tmap = make(map[peer.ID]struct{})
				p.topics[t] = tmap
			}

			tmap[rpc.from] = struct{}{}
		}
	case UnsubMessageType:
		for _, t := range rpc.Topics {
			tmap, ok := p.topics[t]
			if !ok {
				return nil
			}
			delete(tmap, rpc.from)
		}
	case PubMessageType:
		if rpc.Msg == nil {
			return fmt.Errorf("nil pub message")
		}

		msg := &Message{rpc.Msg}

		// Note: Obviously this is an incredibly insecure way of
		// filtering out "messages we've already seen". But it works for a
		// cool demo, so i'm not gonna waste time thinking about it any more
		if p.lastMsg[msg.GetFrom()] >= msg.GetSeqno() {
			//log.Error("skipping 'old' message")
			return nil
		}

		if msg.GetFrom() == p.host.ID() {
			log.Error("skipping message from self")
			return nil
		}

		p.lastMsg[msg.GetFrom()] = msg.GetSeqno()

		if err := p.recvMessage(rpc); err != nil {
			log.Error("error receiving message: ", err)
		}

		err := p.publishMessage(rpc)
		if err != nil {
			log.Error("publish message: ", err)
		}
	}
	return nil
}

func (p *PubSub) publishMessage(rpc *RPC) error {
	tmap, ok := p.topics[rpc.Msg.GetTopic()]
	if !ok {
		return nil
	}

	for pid, _ := range tmap {
		if pid == rpc.from || pid == peer.ID(rpc.Msg.GetFrom()) {
			continue
		}

		mch, ok := p.peers[pid]
		if !ok {
			continue
		}

		go func() { mch <- rpc }()
	}

	return nil
}

type addSub struct {
	topic  string
	cancel bool
	resp   chan chan *Message
}

func (p *PubSub) Subscribe(topic string) (<-chan *Message, error) {
	resp := make(chan chan *Message)
	p.addSub <- &addSub{
		topic: topic,
		resp:  resp,
	}

	outch := <-resp
	if outch == nil {
		return nil, fmt.Errorf("error, duplicate subscription")
	}

	return outch, nil
}

func (p *PubSub) Unsub(topic string) {
	p.addSub <- &addSub{
		topic:  topic,
		cancel: true,
	}
}

func (p *PubSub) Publish(topic string, data []byte) error {
	seqno := uint64(time.Now().UnixNano())
	p.outgoing <- &RPC{
		RPC: pb.RPC{
			Msg: &pb.Message{
				Data:  data,
				Topic: &topic,
				From:  proto.String(string(p.host.ID())),
				Seqno: &seqno,
			},
			Type: &PubMessageType,
		},
	}
	return nil
}
