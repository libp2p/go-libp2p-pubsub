package pubsub

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type historyEntry struct {
	mid   string
	topic string
}

type messageRef struct {
	*Message
	refs int
}

type MessageCache struct {
	msgID func(*Message) string

	// All messages unified storage, indexed by message ID
	// Messages can be in window, announcement wheel, or both
	msgs map[string]*messageRef

	// Sliding window for all messages
	history   [][]historyEntry
	gossipLen int

	// Time wheel for announcements with expiry-based cleanup
	// Behaves like a circular buffer of time buckets containing message IDs
	// Actual messages are stored in the unified storage `msgs`
	annWheel     [][]string
	annWheelPos  int
	annWheelTick time.Duration

	// Per-peer transmission counters
	peertx map[string]map[peer.ID]int
}

// NewMessageCache creates a sliding window cache that remembers messages for as
// long as `historyLen` slots.
//
// When queried for messages to advertise via gossip, the cache only returns messages
// in the last `gossipLen` slots.
//
// The `gossipLen` parameter must be smaller or equal to `historyLen`, or this
// function will panic.
//
// The slack between `gossipLen` and `historyLen` accounts for the reaction time
// between when a message is advertised via IHAVE gossip, and the peer pulls it
// via an IWANT command.
func NewMessageCache(gossipLen, historyLen int, heartbeatInterval, maxTTL time.Duration) *MessageCache {
	if gossipLen > historyLen {
		err := fmt.Errorf("invalid parameters for message cache; gossip slots (%d) cannot be larger than history slots (%d)",
			gossipLen, historyLen)
		panic(err)
	}

	wheelLen := ceilDivDuration(maxTTL, heartbeatInterval)
	wheel := make([][]string, wheelLen)

	return &MessageCache{
		msgs:         make(map[string]*messageRef),
		peertx:       make(map[string]map[peer.ID]int),
		history:      make([][]historyEntry, historyLen),
		gossipLen:    gossipLen,
		annWheel:     wheel,
		annWheelPos:  0,
		annWheelTick: heartbeatInterval,
		msgID: func(msg *Message) string {
			return DefaultMsgIdFn(msg.Message)
		},
	}
}

func (mc *MessageCache) SetMsgIdFn(msgID func(*Message) string) {
	mc.msgID = msgID
}

// AppendWindow adds a message to the sliding window cache.
// The message will be retained for the duration of the window.
// If the message already exists in the cache, its reference count is incremented.
func (mc *MessageCache) AppendWindow(msg *Message) {
	mid := mc.upsertMessage(msg)
	mc.history[0] = append(mc.history[0], historyEntry{mid: mid, topic: msg.GetTopic()})
}

// Get retrieves the message for the given message ID without modifying
// any transmission counts.
// It returns the message and a boolean indicating whether the message was found in the cache.
func (mc *MessageCache) Get(mid string) (*Message, bool) {
	ref, ok := mc.msgs[mid]
	if !ok {
		return nil, false
	}
	return ref.Message, true
}

// GetForPeer retrieves the message for the given message ID and increments
// the transmission count for the specified peer.
// It returns the message, the updated transmission count, and a boolean indicating
// whether the message was found in the cache.
func (mc *MessageCache) GetForPeer(mid string, p peer.ID) (*Message, int, bool) {
	ref, ok := mc.msgs[mid]
	if !ok {
		return nil, 0, false
	}

	tx, ok := mc.peertx[mid]
	if !ok {
		tx = make(map[peer.ID]int)
		mc.peertx[mid] = tx
	}
	tx[p]++

	return ref.Message, tx[p], true
}

// GossipForTopic returns the message IDs in the gossip window for the given topic.
func (mc *MessageCache) GossipForTopic(topic string) []string {
	var mids []string
	for _, entries := range mc.history[:mc.gossipLen] {
		for _, entry := range entries {
			if entry.topic == topic {
				mids = append(mids, entry.mid)
			}
		}
	}
	return mids
}

// ShiftWindow advances the sliding window by one slot.
// Messages that fall out of the window have their reference counts decremented
// and are removed from the cache if they are no longer referenced.
func (mc *MessageCache) ShiftWindow() {
	last := mc.history[len(mc.history)-1]
	for _, entry := range last {
		mc.tryDropMessage(entry.mid)
		delete(mc.peertx, entry.mid)
	}
	for i := len(mc.history) - 2; i >= 0; i-- {
		mc.history[i+1] = mc.history[i]
	}
	mc.history[0] = nil
}

// TrackAnn adds a message to the announcement cache with time-based expiry.
// Unlike AppendWindow, these messages are not part of the sliding window and expire at a specific time.
func (mc *MessageCache) TrackAnn(msg *Message, expiry time.Time) {
	ttl := time.Until(expiry)
	if ttl <= 0 {
		return
	}

	mid := mc.upsertMessage(msg)

	// Insert the message into the storage and the wheel
	offset := ceilDivDuration(ttl, mc.annWheelTick)
	bucket := (mc.annWheelPos + offset) % len(mc.annWheel)
	mc.annWheel[bucket] = append(mc.annWheel[bucket], mid)
}

// PruneAnns removes expired announcements from the cache.
// This should be called periodically (e.g., during heartbeat).
// Advances the time wheel by one tick and cleans up the current bucket.
func (mc *MessageCache) PruneAnns() {
	bucket := mc.annWheel[mc.annWheelPos]

	// Drop all messages in the current bucket
	for _, mid := range bucket {
		mc.tryDropMessage(mid)
		delete(mc.peertx, mid)
	}

	// Clear the current bucket and advance the wheel position
	mc.annWheel[mc.annWheelPos] = mc.annWheel[mc.annWheelPos][:0]
	mc.annWheelPos = (mc.annWheelPos + 1) % len(mc.annWheel)
}

// tryDropMessage decrements the reference count of the message with the given ID.
// If the reference count reaches zero, the message is removed from the cache.
// Returns true if the message was dropped, false otherwise.
func (mc *MessageCache) tryDropMessage(mid string) {
	ref, ok := mc.msgs[mid]
	if !ok {
		return
	}
	if ref.refs--; ref.refs == 0 {
		delete(mc.msgs, mid)
	}
}

func (mc *MessageCache) upsertMessage(msg *Message) string {
	mid := mc.msgID(msg)
	ref, exists := mc.msgs[mid]
	if !exists {
		ref = &messageRef{Message: msg}
		mc.msgs[mid] = ref
	}
	ref.refs++
	return mid
}

// ceilDivDuration performs ceiling division of two time.Duration values.
func ceilDivDuration(a, b time.Duration) int {
	switch {
	case b <= 0:
		panic("b must be > 0")
	case a <= 0:
		return 0
	default:
		return (int(a) + int(b) - 1) / int(b)
	}
}
