package floodsub

import (
	"bufio"
	"context"
	"encoding/binary"
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
	timecache "github.com/whyrusleeping/timecache"
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
	publish  chan *Message
	newPeers chan inet.Stream
	peerDead chan peer.ID

	myTopics map[string]chan *Message
	pubsubLk sync.Mutex

	topics       map[string]map[peer.ID]struct{}
	peers        map[peer.ID]chan *RPC
	seenMessages *timecache.TimeCache

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
		host:         h,
		ctx:          ctx,
		incoming:     make(chan *RPC, 32),
		publish:      make(chan *Message),
		newPeers:     make(chan inet.Stream),
		peerDead:     make(chan peer.ID),
		addSub:       make(chan *addSub),
		myTopics:     make(map[string]chan *Message),
		topics:       make(map[string]map[peer.ID]struct{}),
		peers:        make(map[peer.ID]chan *RPC),
		seenMessages: timecache.NewTimeCache(time.Second * 30),
	}

	h.SetStreamHandler(ID, ps.handleNewStream)
	h.Network().Notify(ps)

	go ps.processLoop(ctx)

	return ps
}

func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC
	for t, _ := range p.myTopics {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
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
		case msg := <-p.publish:
			err := p.recvMessage(msg.Message)
			if err != nil {
				log.Error("error receiving message: ", err)
			}

			err = p.publishMessage(p.host.ID(), msg.Message)
			if err != nil {
				log.Error("publishing message: ", err)
			}
		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}
func (p *PubSub) handleSubscriptionChange(sub *addSub) {

	subopt := pb.RPC_SubOpts{
		Topicid:   &sub.topic,
		Subscribe: &sub.sub,
	}

	ch, ok := p.myTopics[sub.topic]
	if sub.sub {
		if ok {
			// we don't allow multiple subs per topic at this point
			sub.resp <- nil
			return
		}

		resp := make(chan *Message, 16)
		p.myTopics[sub.topic] = resp
		sub.resp <- resp
	} else {
		if !ok {
			return
		}

		close(ch)
		delete(p.myTopics, sub.topic)
	}

	out := &RPC{
		RPC: pb.RPC{
			Subscriptions: []*pb.RPC_SubOpts{
				&subopt,
			},
		},
	}

	for _, peer := range p.peers {
		peer <- out
	}
}

func (p *PubSub) recvMessage(msg *pb.Message) error {
	if len(msg.GetTopicIDs()) > 1 {
		return fmt.Errorf("Dont yet handle multiple topics per message")
	}
	if len(msg.GetTopicIDs()) == 0 {
		return fmt.Errorf("no topic on received message")
	}

	topic := msg.GetTopicIDs()[0]
	subch, ok := p.myTopics[topic]
	if ok {
		subch <- &Message{msg}
	} else {
		log.Error("received message we we'rent subscribed to")
	}
	return nil
}

func (p *PubSub) seenMessage(id string) bool {
	return p.seenMessages.Has(id)
}

func (p *PubSub) markSeen(id string) {
	p.seenMessages.Add(id)
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) error {
	for _, subopt := range rpc.GetSubscriptions() {
		t := subopt.GetTopicid()
		if subopt.GetSubscribe() {
			tmap, ok := p.topics[t]
			if !ok {
				tmap = make(map[peer.ID]struct{})
				p.topics[t] = tmap
			}

			tmap[rpc.from] = struct{}{}
		} else {
			tmap, ok := p.topics[t]
			if !ok {
				continue
			}
			delete(tmap, rpc.from)
		}
	}

	for _, pmsg := range rpc.GetPublish() {
		msg := &Message{pmsg}

		id := msg.Message.GetFrom() + string(msg.GetSeqno())

		if p.seenMessage(id) {
			continue
		}

		if msg.GetFrom() == p.host.ID() {
			log.Error("skipping message from self")
			return nil
		}

		p.markSeen(id)

		if err := p.recvMessage(pmsg); err != nil {
			log.Error("error receiving message: ", err)
		}

		err := p.publishMessage(rpc.from, pmsg)
		if err != nil {
			log.Error("publish message: ", err)
		}
	}
	return nil
}

func (p *PubSub) publishMessage(from peer.ID, msg *pb.Message) error {
	if len(msg.GetTopicIDs()) != 1 {
		return fmt.Errorf("don't support publishing to multiple topics in a single message")
	}

	tmap, ok := p.topics[msg.GetTopicIDs()[0]]
	if !ok {
		return nil
	}

	out := &RPC{RPC: pb.RPC{Publish: []*pb.Message{msg}}}

	for pid, _ := range tmap {
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		mch, ok := p.peers[pid]
		if !ok {
			continue
		}

		go func() { mch <- out }()
	}

	return nil
}

type addSub struct {
	topic string
	sub   bool
	resp  chan chan *Message
}

func (p *PubSub) Subscribe(topic string) (<-chan *Message, error) {
	return p.SubscribeComplicated(&pb.TopicDescriptor{
		Name: proto.String(topic),
	})
}

func (p *PubSub) SubscribeComplicated(td *pb.TopicDescriptor) (<-chan *Message, error) {
	if td.GetAuth().GetMode() != pb.TopicDescriptor_AuthOpts_NONE {
		return nil, fmt.Errorf("Auth method not yet supported")
	}

	if td.GetEnc().GetMode() != pb.TopicDescriptor_EncOpts_NONE {
		return nil, fmt.Errorf("Encryption method not yet supported")
	}

	resp := make(chan chan *Message)
	p.addSub <- &addSub{
		topic: td.GetName(),
		resp:  resp,
		sub:   true,
	}

	outch := <-resp
	if outch == nil {
		return nil, fmt.Errorf("error, duplicate subscription")
	}

	return outch, nil
}

func (p *PubSub) Unsub(topic string) {
	p.addSub <- &addSub{
		topic: topic,
		sub:   false,
	}
}

func (p *PubSub) Publish(topic string, data []byte) error {
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(time.Now().UnixNano()))

	p.publish <- &Message{
		&pb.Message{
			Data:     data,
			TopicIDs: []string{topic},
			From:     proto.String(string(p.host.ID())),
			Seqno:    seqno,
		},
	}
	return nil
}
