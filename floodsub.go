package floodsub

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	pb "github.com/libp2p/go-floodsub/pb"

	proto "github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	timecache "github.com/whyrusleeping/timecache"
)

const ID = protocol.ID("/floodsub/1.0.0")

var log = logging.Logger("floodsub")

type PubSub struct {
	host host.Host

	// incoming messages from other peers
	incoming chan *RPC

	// messages we are publishing out to our peers
	publish chan *Message

	// addSub is a control channel for us to add and remove subscriptions
	addSub chan *addSub

	//
	getTopics chan *topicReq

	//
	getPeers chan *listPeerReq

	//
	addFeedHook chan *addFeedReq

	// a notification channel for incoming streams from other peers
	newPeers chan inet.Stream

	// a notification channel for when our peers die
	peerDead chan peer.ID

	// The set of topics we are subscribed to
	myTopics map[string][]*clientFeed

	// topics tracks which topics each of our peers are subscribed to
	topics map[string]map[peer.ID]struct{}

	peers        map[peer.ID]chan *RPC
	seenMessages *timecache.TimeCache

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
		getPeers:     make(chan *listPeerReq),
		addSub:       make(chan *addSub),
		getTopics:    make(chan *topicReq),
		addFeedHook:  make(chan *addFeedReq, 32),
		myTopics:     make(map[string][]*clientFeed),
		topics:       make(map[string]map[peer.ID]struct{}),
		peers:        make(map[peer.ID]chan *RPC),
		seenMessages: timecache.NewTimeCache(time.Second * 30),
	}

	h.SetStreamHandler(ID, ps.handleNewStream)
	h.Network().Notify((*PubSubNotif)(ps))

	go ps.processLoop(ctx)

	return ps
}

func (p *PubSub) processLoop(ctx context.Context) {
	for {
		select {
		case s := <-p.newPeers:
			pid := s.Conn().RemotePeer()
			ch, ok := p.peers[pid]
			if ok {
				log.Error("already have connection to peer: ", pid)
				close(ch)
			}

			messages := make(chan *RPC, 32)
			go p.handleSendingMessages(ctx, s, messages)
			messages <- p.getHelloPacket()

			p.peers[pid] = messages

		case req := <-p.addFeedHook:
			feeds, ok := p.myTopics[req.topic]

			var out chan *Message
			if ok {
				out = make(chan *Message, 32)
				nfeed := &clientFeed{
					out: out,
					ctx: req.ctx,
				}

				p.myTopics[req.topic] = append(feeds, nfeed)
			}

			req.resp <- out
		case pid := <-p.peerDead:
			ch, ok := p.peers[pid]
			if ok {
				close(ch)
			}

			delete(p.peers, pid)
			for _, t := range p.topics {
				delete(t, pid)
			}
		case treq := <-p.getTopics:
			var out []string
			for t := range p.myTopics {
				out = append(out, t)
			}
			treq.resp <- out
		case sub := <-p.addSub:
			p.handleSubscriptionChange(sub)
		case preq := <-p.getPeers:
			tmap, ok := p.topics[preq.topic]
			if preq.topic != "" && !ok {
				preq.resp <- nil
				continue
			}
			var peers []peer.ID
			for p := range p.peers {
				if preq.topic != "" {
					_, ok := tmap[p]
					if !ok {
						continue
					}
				}
				peers = append(peers, p)
			}
			preq.resp <- peers
		case rpc := <-p.incoming:
			err := p.handleIncomingRPC(rpc)
			if err != nil {
				log.Error("handling RPC: ", err)
				continue
			}
		case msg := <-p.publish:
			p.maybePublishMessage(p.host.ID(), msg.Message)
		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}

func (p *PubSub) handleSubscriptionChange(sub *addSub) {
	subopt := &pb.RPC_SubOpts{
		Topicid:   &sub.topic,
		Subscribe: &sub.sub,
	}

	feeds, ok := p.myTopics[sub.topic]
	if sub.sub {
		if ok {
			return
		}

		p.myTopics[sub.topic] = nil
	} else {
		if !ok {
			return
		}

		for _, f := range feeds {
			close(f.out)
		}
		delete(p.myTopics, sub.topic)
	}

	out := rpcWithSubs(subopt)
	for _, peer := range p.peers {
		peer <- out
	}
}

func (p *PubSub) notifySubs(msg *pb.Message) {
	for _, topic := range msg.GetTopicIDs() {
		var cleanup bool
		feeds := p.myTopics[topic]
		for _, f := range feeds {
			select {
			case f.out <- &Message{msg}:
			case <-f.ctx.Done():
				close(f.out)
				f.out = nil
				cleanup = true
			}
		}

		if cleanup {
			out := make([]*clientFeed, 0, len(feeds))
			for _, f := range feeds {
				if f.out != nil {
					out = append(out, f)
				}
			}
			p.myTopics[topic] = out
		}
	}
}

func (p *PubSub) seenMessage(id string) bool {
	return p.seenMessages.Has(id)
}

func (p *PubSub) markSeen(id string) {
	p.seenMessages.Add(id)
}

func (p *PubSub) subscribedToMsg(msg *pb.Message) bool {
	for _, t := range msg.GetTopicIDs() {
		if _, ok := p.myTopics[t]; ok {
			return true
		}
	}
	return false
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
		if !p.subscribedToMsg(pmsg) {
			log.Warning("received message we didn't subscribe to. Dropping.")
			continue
		}

		p.maybePublishMessage(rpc.from, pmsg)
	}
	return nil
}

func msgID(pmsg *pb.Message) string {
	return string(pmsg.GetFrom()) + string(pmsg.GetSeqno())
}

func (p *PubSub) maybePublishMessage(from peer.ID, pmsg *pb.Message) {
	id := msgID(pmsg)
	if p.seenMessage(id) {
		return
	}

	p.markSeen(id)

	p.notifySubs(pmsg)

	err := p.publishMessage(from, pmsg)
	if err != nil {
		log.Error("publish message: ", err)
	}
}

func (p *PubSub) publishMessage(from peer.ID, msg *pb.Message) error {
	tosend := make(map[peer.ID]struct{})
	for _, topic := range msg.GetTopicIDs() {
		tmap, ok := p.topics[topic]
		if !ok {
			continue
		}

		for p, _ := range tmap {
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg)
	for pid := range tosend {
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

func (p *PubSub) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
	err := p.AddTopicSubscription(&pb.TopicDescriptor{
		Name: proto.String(topic),
	})

	if err != nil {
		return nil, err
	}

	return p.GetFeed(ctx, topic)
}

type topicReq struct {
	resp chan []string
}

func (p *PubSub) GetTopics() []string {
	out := make(chan []string, 1)
	p.getTopics <- &topicReq{resp: out}
	return <-out
}

func (p *PubSub) AddTopicSubscription(td *pb.TopicDescriptor) error {
	if td.GetAuth().GetMode() != pb.TopicDescriptor_AuthOpts_NONE {
		return fmt.Errorf("Auth method not yet supported")
	}

	if td.GetEnc().GetMode() != pb.TopicDescriptor_EncOpts_NONE {
		return fmt.Errorf("Encryption method not yet supported")
	}

	p.addSub <- &addSub{
		topic: td.GetName(),
		sub:   true,
	}

	return nil
}

type addFeedReq struct {
	ctx   context.Context
	topic string
	resp  chan chan *Message
}

type clientFeed struct {
	out chan *Message
	ctx context.Context
}

func (p *PubSub) GetFeed(ctx context.Context, topic string) (<-chan *Message, error) {
	out := make(chan chan *Message, 1)
	p.addFeedHook <- &addFeedReq{
		ctx:   ctx,
		topic: topic,
		resp:  out,
	}

	resp := <-out
	if resp == nil {
		return nil, fmt.Errorf("not subscribed to topic %s", topic)
	}
	return resp, nil
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

type listPeerReq struct {
	resp  chan []peer.ID
	topic string
}

func (p *PubSub) ListPeers(topic string) []peer.ID {
	out := make(chan []peer.ID)
	p.getPeers <- &listPeerReq{
		resp:  out,
		topic: topic,
	}
	return <-out
}
