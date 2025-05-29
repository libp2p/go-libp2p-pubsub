package pubsub

import (
	"iter"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// chokeStrategy implements a very simple strategy:
// if we get a duplicate message, we choke the peer.
// if we see a IHAVE for a message we haven't received, we unchoke the peer.
type chokeStrategy struct {
	ChokeThreshold   time.Duration
	UnchokeThreshold time.Duration

	MCache  messageCache
	MsgIDFn func(msg *Message) string

	// TODO: make this a heap
	scheduledWork map[peer.ID][]scheduledUpdater

	// TODO: add logic to do grafting/pruning if a gossip peer tells us about a message we don't have
}

type messageCache interface {
	Get(id string) (*Message, bool)
}

type chokeAction struct {
	p     peer.ID
	choke bool
}

func (c *chokeStrategy) shouldChoke(msg *Message) bool {
	if cached, ok := c.MCache.Get(c.MsgIDFn(msg)); ok {
		// We already have this message. See if the time since we received is
		// over our choke threshold
		if time.Since(cached.ReceivedAt) > c.ChokeThreshold {
			return true
		}
	}

	return false
}

func (c *chokeStrategy) handleIHAVE(from peer.ID, ihave *pb.ControlIHave) {
	if c.scheduledWork == nil {
		c.scheduledWork = make(map[peer.ID][]scheduledUpdater)
	}

	// TODO: add limits
	c.scheduledWork[from] = append(c.scheduledWork[from], scheduledUpdater{
		notBefore: time.Now().Add(c.UnchokeThreshold),
		u: &unchokeFromIhaves{
			ihave:           ihave,
			ihaveReceivedAt: time.Now(),
			topic:           ihave.TopicID,
			mcache:          c.MCache,
		},
	})
}

type unchokeStrategy interface {
	shouldUnchoke(peer.ID, time.Duration) (*string, bool) // *string to avoid an extra allocation as pb types use *string
}

type scheduledUpdater struct {
	notBefore time.Time
	u         unchokeStrategy
}

type unchokeFromIhaves struct {
	ihave           *pb.ControlIHave
	ihaveReceivedAt time.Time
	topic           *string
	mcache          messageCache
}

func (u *unchokeFromIhaves) shouldUnchoke(peerThatSentIhave peer.ID, unchokeThreshold time.Duration) (*string, bool) {
	for _, mid := range u.ihave.MessageIDs {
		if cached, ok := u.mcache.Get(mid); ok {
			if cached.ReceivedFrom == peerThatSentIhave {
				// The peer gave us the message. Unchoke it
				return u.topic, true
			}

			// We got the message from another peer. If the difference is greater than our unchoke threshold, unchoke the peer
			timeToReceive := cached.ReceivedAt.Sub(u.ihaveReceivedAt)
			if timeToReceive > unchokeThreshold {
				return u.topic, true
			}
		} else {
			// We don't have this message. unchoke the peer
			// TODO: we should had this message by now. Should we actually prune the peer instead?
			return u.topic, true
		}
	}
	return nil, false
}

func (c *chokeStrategy) unchokeMessagesFor(p peer.ID) iter.Seq[*pb.ControlUnchoke] {
	return func(yield func(*pb.ControlUnchoke) bool) {
		for _, su := range c.scheduledWork[p] {
			if time.Now().After(su.notBefore) {
				if topic, unchoke := su.u.shouldUnchoke(p, c.UnchokeThreshold); unchoke {
					if !yield(&pb.ControlUnchoke{TopicID: topic}) {
						return
					}
				}
			}
		}
	}
}

// TODO clear scheduled work for peers when they leave the mesh
