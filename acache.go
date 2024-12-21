package pubsub

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type IneedMeta struct {
	pid peer.ID
	mid string
}

type sendList struct {
	// Timeout
	t time.Duration
	// List of message ids
	l *list.List
	// Elements in l indexed by message ids.
	es map[string]*list.Element
}

type sendListEntry struct {
	meta *IneedMeta
	// Send time
	sendTime time.Time
	// Timeout time
	expiryTime time.Time
}

func newSendList(timeout time.Duration) *sendList {
	return &sendList{
		t:  timeout,
		l:  list.New(),
		es: make(map[string]*list.Element),
	}
}

// Front returns the first message id in the list.
func (sl *sendList) Front() *sendListEntry {
	e := sl.l.Front()
	if e != nil {
		entry := e.Value.(sendListEntry)
		return &entry
	} else {
		return nil
	}
}

// Push pushes the message id and the peer to the list with send time set to now and expiry time set to now plus timeout.
func (sl *sendList) Push(meta *IneedMeta) {
	// there shouldn't already be a message id in the list
	if _, ok := sl.es[meta.mid]; ok {
		panic(fmt.Errorf("there is already a message id in the list: %s", meta.mid))
	}
	// push to the back and remember the element
	sl.es[meta.mid] = sl.l.PushBack(sendListEntry{
		meta:       meta,
		sendTime:   time.Now(),
		expiryTime: time.Now().Add(sl.t),
	})
}

// Remove removes the message id from the list.
func (sl *sendList) Remove(mid string) {
	// there shouldn already be a message id in the list
	if _, ok := sl.es[mid]; !ok {
		panic(fmt.Errorf("there is no message id in the list to remove: %s", mid))
	}
	// remove it from both the list and the indexing map
	sl.l.Remove(sl.es[mid])
	delete(sl.es, mid)
}

// Has checks if the message id is in the list.
func (sl *sendList) Has(mid string) bool {
	_, ok := sl.es[mid]
	return ok
}

type AnnounceCache struct {
	lk sync.RWMutex
	// Queues indexed by messages ids containing the peers from whom we already receive IANNOUNCE, but not yet send INEED.
	m map[string][]peer.ID
	// List of pairs of peers and message ids that we already send INEED, but the timeout hasn't occured and the message is not received yet.
	// There is supposed to be at most one element per message id in the list at any time.
	sl *sendList
	// Channel to wake up the background routine and try to send INEED.
	c chan<- *IneedMeta
	// Channel used to notify a request to send INEED from the cache.
	R <-chan *IneedMeta
	// Channel used to notify a timeout of INEED from the cache.
	T <-chan *IneedMeta
	// Used to indicate that the cache is stopped
	stopped chan struct{}
}

func NewAnnounceCache(timeout time.Duration) *AnnounceCache {
	c := make(chan *IneedMeta)
	R := make(chan *IneedMeta)
	T := make(chan *IneedMeta)
	ac := &AnnounceCache{
		c:  c,
		R:  R,
		T:  T,
		m:  make(map[string][]peer.ID),
		sl: newSendList(timeout),

		stopped: make(chan struct{}),
	}
	go ac.background(c, R, T)

	return ac
}

func (ac *AnnounceCache) background(c <-chan *IneedMeta, R chan<- *IneedMeta, T chan<- *IneedMeta) {
	timer := time.NewTimer(0)
	for {
		select {
		case <-ac.stopped:
			return
		case meta := <-c:
			ac.lk.Lock()
			if !ac.sl.Has(meta.mid) {
				// If there is no INEED on flight, just send INEED right away by putting it in the list
				ac.sl.Push(meta)
				// Send the meta data to the cache user, so they can send INEED using that
				select {
				case R <- meta:
				case <-ac.stopped:
					ac.lk.Unlock()
					return
				}
			} else {
				ac.m[meta.mid] = append(ac.m[meta.mid], meta.pid)
			}
		case <-timer.C:
			ac.lk.Lock()
		}
		entry := ac.sl.Front()
		for entry != nil && entry.expiryTime.Before(time.Now()) {
			// If the ongoing INEED times out
			mid := entry.meta.mid

			// Remove it from the list
			ac.sl.Remove(mid)

			// Notify the cache user that the ongoing INEED times out
			select {
			case T <- entry.meta:
			case <-ac.stopped:
				ac.lk.Unlock()
				return
			}

			// If there is another peer waiting for INEED
			if len(ac.m[mid]) > 0 {
				meta := &IneedMeta{
					pid: ac.m[mid][0],
					mid: mid,
				}
				ac.m[mid] = ac.m[mid][1:]
				ac.sl.Push(meta)

				// Send the meta data to the cache user, so they can send INEED using that
				select {
				case R <- meta:
				case <-ac.stopped:
					ac.lk.Unlock()
					return
				}
			} else {
				delete(ac.m, mid)
			}

			// Look at the next entry
			entry = ac.sl.Front()
		}
		timer.Stop()
		if entry = ac.sl.Front(); entry != nil {
			// If there still the next entry, wake this background routine correspondingly
			timer.Reset(time.Until(entry.expiryTime))
		}
		ac.lk.Unlock()
	}
}

func (ac *AnnounceCache) Add(mid string, pid peer.ID) {
	meta := &IneedMeta{
		mid: mid,
		pid: pid,
	}
	select {
	case ac.c <- meta:
	case <-ac.stopped:
	}
}

// Clear clears all the pending IANNOUNCE and remove the ongoing INEED from the list so the the timeout
// will not be triggered
func (ac *AnnounceCache) Clear(mid string) {
	ac.lk.Lock()
	defer ac.lk.Unlock()

	// Clear the cache for the given message id
	ac.m[mid] = []peer.ID{}
	if ac.sl.Has(mid) {
		ac.sl.Remove(mid)
	}
}

func (ac *AnnounceCache) Stop() {
	close(ac.stopped)
}
