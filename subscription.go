package pubsub

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

type EventType int

const (
	PeerJoin EventType = iota
	PeerLeave
)

type Subscription struct {
	topic    string
	ch       chan *Message
	cancelCh chan<- *Subscription
	err      error

	peerEvtCh   chan PeerEvent
	backlogMx   sync.Mutex
	evtBacklog  map[peer.ID]EventType
	backlogCh   chan struct{}
	nextEventMx sync.Mutex
}

type PeerEvent struct {
	Type EventType
	Peer peer.ID
}

func (sub *Subscription) Topic() string {
	return sub.topic
}

// Next returns the next message in our subscription
func (sub *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case msg, ok := <-sub.ch:
		if !ok {
			return msg, sub.err
		}

		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sub *Subscription) Cancel() {
	sub.cancelCh <- sub
}

func (sub *Subscription) close() {
	close(sub.ch)
}

func (sub *Subscription) sendNotification(evt PeerEvent) {
	sub.backlogMx.Lock()
	defer sub.backlogMx.Unlock()

	sub.addToBacklog(evt)

	select {
	case sub.backlogCh <- struct{}{}:
	default:
	}
}

// addToBacklog assumes a lock has been taken to protect the backlog
func (sub *Subscription) addToBacklog(evt PeerEvent) {
	e, ok := sub.evtBacklog[evt.Peer]
	if !ok {
		sub.evtBacklog[evt.Peer] = evt.Type
	} else if e != evt.Type {
		delete(sub.evtBacklog, evt.Peer)
	}
}

// pullFromBacklog assumes a lock has been taken to protect the backlog
func (sub *Subscription) pullFromBacklog() (PeerEvent, bool) {
	for k, v := range sub.evtBacklog {
		evt := PeerEvent{Peer: k, Type: v}
		delete(sub.evtBacklog, k)
		return evt, true
	}
	return PeerEvent{}, false
}

// NextPeerEvent returns the next event regarding subscribed peers
// Guarantees: Peer Join and Peer Leave events for a given peer will fire in order.
// Unless a peer both Joins and Leaves before NextPeerEvent emits either event
// all events will eventually be received from NextPeerEvent.
func (sub *Subscription) NextPeerEvent(ctx context.Context) (PeerEvent, error) {
	sub.nextEventMx.Lock()
	defer sub.nextEventMx.Unlock()

	for {
		sub.backlogMx.Lock()
		evt, ok := sub.pullFromBacklog()
		sub.backlogMx.Unlock()

		if ok {
			return evt, nil
		}

		select {
		case <-sub.backlogCh:
			continue
		case <-ctx.Done():
			return PeerEvent{}, ctx.Err()
		}
	}
}
