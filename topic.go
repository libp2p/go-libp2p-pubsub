package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	logging "github.com/ipfs/go-log"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/peer"
	pkgerrors "github.com/pkg/errors"
)

// ErrTopicClosed is returned if a Topic is utilized after it has been closed
var ErrTopicClosed = errors.New("this Topic is closed, try opening a new one")

// ErrFailedToAddToPeerQueue is returned if we fail to achieve the desired target for WithWaitUntilQueued because of full outbound peer queues
var ErrFailedToAddToPeerQueue = errors.New("failed to achieve desired WithWaitUntilQueued target")

var topicHandleLog = logging.Logger("topicHandle")

// Topic is the handle for a pubsub topic
type Topic struct {
	p     *PubSub
	topic string

	evtHandlerMux sync.RWMutex
	evtHandlers   map[*TopicEventHandler]struct{}

	mux    sync.RWMutex
	closed bool
}

// EventHandler creates a handle for topic specific events
// Multiple event handlers may be created and will operate independently of each other
func (t *Topic) EventHandler(opts ...TopicEventHandlerOpt) (*TopicEventHandler, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return nil, ErrTopicClosed
	}

	h := &TopicEventHandler{
		topic: t,
		err:   nil,

		evtLog:   make(map[peer.ID]EventType),
		evtLogCh: make(chan struct{}, 1),
	}

	for _, opt := range opts {
		err := opt(h)
		if err != nil {
			return nil, err
		}
	}

	done := make(chan struct{}, 1)

	select {
	case t.p.eval <- func() {
		tmap := t.p.topics[t.topic]
		for p := range tmap {
			h.evtLog[p] = PeerJoin
		}

		t.evtHandlerMux.Lock()
		t.evtHandlers[h] = struct{}{}
		t.evtHandlerMux.Unlock()
		done <- struct{}{}
	}:
	case <-t.p.ctx.Done():
		return nil, t.p.ctx.Err()
	}

	<-done

	return h, nil
}

func (t *Topic) sendNotification(evt PeerEvent) {
	t.evtHandlerMux.RLock()
	defer t.evtHandlerMux.RUnlock()

	for h := range t.evtHandlers {
		h.sendNotification(evt)
	}
}

// Subscribe returns a new Subscription for the topic.
// Note that subscription is not an instanteneous operation. It may take some time
// before the subscription is processed by the pubsub main loop and propagated to our peers.
func (t *Topic) Subscribe(opts ...SubOpt) (*Subscription, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return nil, ErrTopicClosed
	}

	sub := &Subscription{
		topic: t.topic,
		ch:    make(chan *Message, 32),
		ctx:   t.p.ctx,
	}

	for _, opt := range opts {
		err := opt(sub)
		if err != nil {
			return nil, err
		}
	}

	out := make(chan *Subscription, 1)

	t.p.disc.Discover(sub.topic)

	select {
	case t.p.addSub <- &addSubReq{
		sub:  sub,
		resp: out,
	}:
	case <-t.p.ctx.Done():
		return nil, t.p.ctx.Err()
	}

	return <-out, nil
}

// RouterReady is a function that decides if a router is ready to publish
type RouterReady func(rt PubSubRouter, topic string) (bool, error)

type PublishOptions struct {
	ready         RouterReady
	nQueuedNotifs int
}

type PubOpt func(pub *PublishOptions) error

// Publish publishes data to topic.
func (t *Topic) Publish(ctx context.Context, data []byte, opts ...PubOpt) error {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return ErrTopicClosed
	}

	seqno := t.p.nextSeqno()
	id := t.p.host.ID()
	m := &pb.Message{
		Data:     data,
		TopicIDs: []string{t.topic},
		From:     []byte(id),
		Seqno:    seqno,
	}
	if t.p.signKey != nil {
		m.From = []byte(t.p.signID)
		err := signMessage(t.p.signID, t.p.signKey, m)
		if err != nil {
			return err
		}
	}

	pub := &PublishOptions{}
	for _, opt := range opts {
		err := opt(pub)
		if err != nil {
			return err
		}
	}

	if pub.ready != nil {
		t.p.disc.Bootstrap(ctx, t.topic, pub.ready)
	}

	var waitForMsgQueuedNotifications bool
	var msgQueuedTargetAchieved chan error
	// setup for receiving notifications when a message is added to a peer's outbound queue
	if pub.nQueuedNotifs != 0 {
		waitForMsgQueuedNotifications = true

		// create & register the listener
		listenerContext, cancel := context.WithCancel(ctx)
		notifChan := make(chan *msgQueuedNotification)
		listener := &msgQueuedEventListener{listenerContext, notifChan}

		done := make(chan struct{}, 1)
		select {
		case t.p.eval <- func() {
			t.p.msgQueuedEventListeners[msgID(m)] = listener
			done <- struct{}{}
		}:
		case <-t.p.ctx.Done():
			return t.p.ctx.Err()
		}
		<-done

		// remove the listener & cancel the listener context before we return
		defer func() {
			cancel()

			done := make(chan struct{}, 1)
			select {
			case t.p.eval <- func() {
				delete(t.p.msgQueuedEventListeners, msgID(m))
				done <- struct{}{}
			}:
			case <-t.p.ctx.Done():
				return
			}
			<-done
		}()

		// start listening to notifications
		msgQueuedTargetAchieved = make(chan error, 1)
		go func() {
			nSuccess := 0
			var failedPeers []peer.ID

			for {
				select {
				case <-listenerContext.Done():
					return
				case notif, ok := <-notifChan:
					if !ok {
						// notification channel is closed

						if pub.nQueuedNotifs == -1 {
							if len(failedPeers) == 0 {
								msgQueuedTargetAchieved <- nil
							} else {
								topicHandleLog.Warningf("Publish: failed on the -1/ALL option: failed to add messageID %s to queues for peers %v",
									msgID(m), failedPeers)

								msgQueuedTargetAchieved <- ErrFailedToAddToPeerQueue
							}
						} else {
							topicHandleLog.Warningf("Publish: did not achieve desired count: "+
								"failed to add messageID %s to queues for peers %v, success count is %d", msgID(m), failedPeers, nSuccess)

							msgQueuedTargetAchieved <- ErrFailedToAddToPeerQueue
						}
						return
					} else {
						if !notif.success {
							failedPeers = append(failedPeers, notif.peer)
						} else {
							nSuccess++
							if pub.nQueuedNotifs != -1 {
								if nSuccess == pub.nQueuedNotifs {
									msgQueuedTargetAchieved <- nil
									return
								}
							}
						}
					}
				}
			}
		}()
	}

	select {
	case t.p.publish <- &Message{m, id, nil}:
	case <-t.p.ctx.Done():
		return t.p.ctx.Err()
	}

	// wait for msg queued notifications
	if waitForMsgQueuedNotifications {
		select {
		case err := <-msgQueuedTargetAchieved:
			return err
		case <-ctx.Done():
			return pkgerrors.Wrap(ctx.Err(), "context expired while waiting for msg queued notifs")
		case <-t.p.ctx.Done():
			return pkgerrors.Wrap(t.p.ctx.Err(), "pubsub context expired while waiting for msg queued notifs")
		}
	}

	return nil
}

// WithReadiness returns a publishing option for only publishing when the router is ready.
// This option is not useful unless PubSub is also using WithDiscovery
func WithReadiness(ready RouterReady) PubOpt {
	return func(pub *PublishOptions) error {
		pub.ready = ready
		return nil
	}
}

// WithWaitUntilQueued blocks the publish until the message has been added to the outbound message queue for
// as many peers as the arg indicates
// A value of -1 means all peers in mesh/fanout for gossipsub & all subscribed peers in floodsub
// Please note that if nPeers is -1, the behavior is not fail fast
// If we fail to achieve the desired target because of full outbound peer queues, Publish will return ErrFailedToAddToPeerQueues
// However, the message could still have been added to the outbound queue for other peers
func WithWaitUntilQueued(nPeers int) PubOpt {
	return func(pub *PublishOptions) error {
		if nPeers < -1 {
			return errors.New("nPeers should be greater than or equal to -1, please refer to the docs for WithWaitUntilQueued")
		}

		pub.nQueuedNotifs = nPeers
		return nil
	}
}

// Close closes down the topic. Will return an error unless there are no active event handlers or subscriptions.
// Does not error if the topic is already closed.
func (t *Topic) Close() error {
	t.mux.Lock()
	defer t.mux.Unlock()
	if t.closed {
		return nil
	}

	req := &rmTopicReq{t, make(chan error, 1)}

	select {
	case t.p.rmTopic <- req:
	case <-t.p.ctx.Done():
		return t.p.ctx.Err()
	}

	err := <-req.resp

	if err == nil {
		t.closed = true
	}

	return err
}

// ListPeers returns a list of peers we are connected to in the given topic.
func (t *Topic) ListPeers() []peer.ID {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return []peer.ID{}
	}

	return t.p.ListPeers(t.topic)
}

type EventType int

const (
	PeerJoin EventType = iota
	PeerLeave
)

// TopicEventHandler is used to manage topic specific events. No Subscription is required to receive events.
type TopicEventHandler struct {
	topic *Topic
	err   error

	evtLogMx sync.Mutex
	evtLog   map[peer.ID]EventType
	evtLogCh chan struct{}
}

type TopicEventHandlerOpt func(t *TopicEventHandler) error

type PeerEvent struct {
	Type EventType
	Peer peer.ID
}

// Cancel closes the topic event handler
func (t *TopicEventHandler) Cancel() {
	topic := t.topic
	t.err = fmt.Errorf("topic event handler cancelled by calling handler.Cancel()")

	topic.evtHandlerMux.Lock()
	delete(topic.evtHandlers, t)
	t.topic.evtHandlerMux.Unlock()
}

func (t *TopicEventHandler) sendNotification(evt PeerEvent) {
	t.evtLogMx.Lock()
	t.addToEventLog(evt)
	t.evtLogMx.Unlock()
}

// addToEventLog assumes a lock has been taken to protect the event log
func (t *TopicEventHandler) addToEventLog(evt PeerEvent) {
	e, ok := t.evtLog[evt.Peer]
	if !ok {
		t.evtLog[evt.Peer] = evt.Type
		// send signal that an event has been added to the event log
		select {
		case t.evtLogCh <- struct{}{}:
		default:
		}
	} else if e != evt.Type {
		delete(t.evtLog, evt.Peer)
	}
}

// pullFromEventLog assumes a lock has been taken to protect the event log
func (t *TopicEventHandler) pullFromEventLog() (PeerEvent, bool) {
	for k, v := range t.evtLog {
		evt := PeerEvent{Peer: k, Type: v}
		delete(t.evtLog, k)
		return evt, true
	}
	return PeerEvent{}, false
}

// NextPeerEvent returns the next event regarding subscribed peers
// Guarantees: Peer Join and Peer Leave events for a given peer will fire in order.
// Unless a peer both Joins and Leaves before NextPeerEvent emits either event
// all events will eventually be received from NextPeerEvent.
func (t *TopicEventHandler) NextPeerEvent(ctx context.Context) (PeerEvent, error) {
	for {
		t.evtLogMx.Lock()
		evt, ok := t.pullFromEventLog()
		if ok {
			// make sure an event log signal is available if there are events in the event log
			if len(t.evtLog) > 0 {
				select {
				case t.evtLogCh <- struct{}{}:
				default:
				}
			}
			t.evtLogMx.Unlock()
			return evt, nil
		}
		t.evtLogMx.Unlock()

		select {
		case <-t.evtLogCh:
			continue
		case <-ctx.Done():
			return PeerEvent{}, ctx.Err()
		}
	}
}
