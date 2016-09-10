package floodsub

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	peer "github.com/ipfs/go-libp2p-peer"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p/p2p/host"
	inet "github.com/libp2p/go-libp2p/p2p/net"
	protocol "github.com/libp2p/go-libp2p/p2p/protocol"
)

const ID = protocol.ID("/floodsub/1.0.0")

const (
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
}

type Message struct {
	From      peer.ID
	Data      []byte
	Timestamp uint64
	Topic     string
}

func (m *Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"from":      base64.RawStdEncoding.EncodeToString([]byte(m.From)),
		"data":      m.Data,
		"timestamp": m.Timestamp,
		"topic":     m.Topic,
	})
}

func (m *Message) UnmarshalJSON(data []byte) error {
	mp := struct {
		Data      []byte
		Timestamp uint64
		Topic     string
		From      string
	}{}
	err := json.Unmarshal(data, &mp)
	if err != nil {
		return err
	}

	pid, err := base64.RawStdEncoding.DecodeString(mp.From)
	if err != nil {
		return err
	}

	m.Data = mp.Data
	m.Timestamp = mp.Timestamp
	m.Topic = mp.Topic
	m.From = peer.ID(pid)
	return nil
}

type RPC struct {
	Type   string
	Msg    *Message
	Topics []string

	// unexported on purpose, not sending this over the wire
	from peer.ID
}

func NewFloodSub(h host.Host) *PubSub {
	ps := &PubSub{
		host:     h,
		incoming: make(chan *RPC, 32),
		outgoing: make(chan *RPC),
		newPeers: make(chan inet.Stream),
		myTopics: make(map[string]chan *Message),
		topics:   make(map[string]map[peer.ID]struct{}),
		peers:    make(map[peer.ID]chan *RPC),
		lastMsg:  make(map[peer.ID]uint64),
		peerDead: make(chan peer.ID),
		addSub:   make(chan *addSub),
	}

	h.SetStreamHandler(ID, ps.handleNewStream)

	h.Network().Notify(ps)

	go ps.processLoop()

	return ps
}

func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC
	for t, _ := range p.myTopics {
		rpc.Topics = append(rpc.Topics, t)
	}
	rpc.Type = AddSubMessageType
	return &rpc
}

func (p *PubSub) handleNewStream(s inet.Stream) {
	defer s.Close()

	scan := bufio.NewScanner(s)
	for scan.Scan() {
		rpc := new(RPC)

		err := json.Unmarshal(scan.Bytes(), rpc)
		if err != nil {
			log.Errorf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			log.Error("data: ", scan.Text())
			// TODO: cleanup of some sort
			return
		}

		rpc.from = s.Conn().RemotePeer()
		p.incoming <- rpc
	}
}

func (p *PubSub) handleSendingMessages(s inet.Stream, in <-chan *RPC) {
	var dead bool
	for rpc := range in {
		if dead {
			continue
		}

		err := writeRPC(s, rpc)
		if err != nil {
			log.Errorf("writing message to %s: %s", s.Conn().RemotePeer(), err)
			dead = true
			go func() {
				p.peerDead <- s.Conn().RemotePeer()
			}()
		}
	}
}

func (p *PubSub) processLoop() {

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
			go p.handleSendingMessages(s, messages)
			messages <- p.getHelloPacket()

			p.peers[pid] = messages

			fmt.Println("added peer: ", pid)
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
			switch rpc.Type {
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
		}
	}
}
func (p *PubSub) handleSubscriptionChange(sub *addSub) {
	ch, ok := p.myTopics[sub.topic]
	out := &RPC{
		Topics: []string{sub.topic},
	}

	if sub.cancel {
		if !ok {
			return
		}

		close(ch)
		delete(p.myTopics, sub.topic)
		out.Type = UnsubMessageType
	} else {
		if ok {
			// we don't allow multiple subs per topic at this point
			sub.resp <- nil
			return
		}

		resp := make(chan *Message, 16)
		p.myTopics[sub.topic] = resp
		sub.resp <- resp
		out.Type = AddSubMessageType
	}

	go func() {
		p.outgoing <- out
	}()
}

func (p *PubSub) recvMessage(rpc *RPC) error {
	subch, ok := p.myTopics[rpc.Msg.Topic]
	if ok {
		//fmt.Println("writing out to subscriber!")
		subch <- rpc.Msg
	}
	return nil
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) error {
	switch rpc.Type {
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

		// Note: Obviously this is an incredibly insecure way of
		// filtering out "messages we've already seen". But it works for a
		// cool demo, so i'm not gonna waste time thinking about it any more
		if p.lastMsg[rpc.Msg.From] >= rpc.Msg.Timestamp {
			//log.Error("skipping 'old' message")
			return nil
		}

		if rpc.Msg.From == p.host.ID() {
			return nil
		}

		p.lastMsg[rpc.Msg.From] = rpc.Msg.Timestamp

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
	tmap, ok := p.topics[rpc.Msg.Topic]
	if !ok {
		return nil
	}

	for pid, _ := range tmap {
		if pid == rpc.from || pid == rpc.Msg.From {
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
	p.outgoing <- &RPC{
		Msg: &Message{
			Data:      data,
			Topic:     topic,
			From:      p.host.ID(),
			Timestamp: uint64(time.Now().UnixNano()),
		},
		Type: PubMessageType,
	}
	return nil
}

func writeRPC(s inet.Stream, rpc *RPC) error {
	data, err := json.Marshal(rpc)
	if err != nil {
		return err
	}

	data = append(data, '\n')
	_, err = s.Write(data)
	return err
}
