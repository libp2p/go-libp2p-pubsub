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

	peerEvtCh chan PeerEvent
	evtLogMx  sync.Mutex
	evtLog    map[peer.ID]EventType
	evtLogCh  chan struct{}
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
	sub.evtLogMx.Lock()
	defer sub.evtLogMx.Unlock()

	sub.addToEventLog(evt)
}

// addToEventLog assumes a lock has been taken to protect the event log
func (sub *Subscription) addToEventLog(evt PeerEvent) {
	e, ok := sub.evtLog[evt.Peer]
	if !ok {
		sub.evtLog[evt.Peer] = evt.Type
		// send signal that an event has been added to the event log
		select {
		case sub.evtLogCh <- struct{}{}:
		default:
		}
	} else if e != evt.Type {
		delete(sub.evtLog, evt.Peer)
	}
}

// pullFromEventLog assumes a lock has been taken to protect the event log
func (sub *Subscription) pullFromEventLog() (PeerEvent, bool) {
	for k, v := range sub.evtLog {
		evt := PeerEvent{Peer: k, Type: v}
		delete(sub.evtLog, k)
		return evt, true
	}
	return PeerEvent{}, false
}

// NextPeerEvent returns the next event regarding subscribed peers
// Guarantees: Peer Join and Peer Leave events for a given peer will fire in order.
// Unless a peer both Joins and Leaves before NextPeerEvent emits either event
// all events will eventually be received from NextPeerEvent.
func (sub *Subscription) NextPeerEvent(ctx context.Context) (PeerEvent, error) {
	for {
		sub.evtLogMx.Lock()
		evt, ok := sub.pullFromEventLog()
		if ok {
			// make sure an event log signal is available if there are events in the event log
			if len(sub.evtLog) > 0 {
				select {
				case sub.evtLogCh <- struct{}{}:
				default:
				}
			}
			sub.evtLogMx.Unlock()
			return evt, nil
		}
		sub.evtLogMx.Unlock()

		select {
		case <-sub.evtLogCh:
			continue
		case <-ctx.Done():
			return PeerEvent{}, ctx.Err()
		}
	}
}
