package partialmessages

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math/big"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/internal/merkle"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// testRouter implements the Router interface for testing
type testRouter struct {
	sendRPC   func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool)
	meshPeers func(topic string) iter.Seq[peer.ID]
}

// PeerRequestsPartial implements Router.
func (r *testRouter) PeerRequestsPartial(peer peer.ID, topic string) bool {
	return true
}

func (r *testRouter) SendRPC(p peer.ID, rpc *pubsub_pb.PartialMessagesExtension, urgent bool) {
	r.sendRPC(p, rpc, urgent)
}

func (r *testRouter) MeshPeers(topic string) iter.Seq[peer.ID] {
	return r.meshPeers(topic)
}

type rpcWithFrom struct {
	from peer.ID
	rpc  *pubsub_pb.PartialMessagesExtension
}

type peerState struct {
	sent  bitmap.Bitmap
	recvd bitmap.Bitmap
}

func (ps peerState) IsZero() bool {
	return ps.sent == nil && ps.recvd == nil
}

type mockNetworkPartialMessages struct {
	t           *testing.T
	pendingMsgs map[peer.ID][]rpcWithFrom

	allSentMsgs map[peer.ID][]rpcWithFrom

	handlers map[peer.ID]*PartialMessagesExtension[peerState]

	// deferredPublishes are executed after each RPC handling round.
	// This is needed because OnIncomingRPC may want to call PublishPartial,
	// but HandleRPC hasn't stored the updated PeerState yet at that point.
	deferredPublishes []func()
}

func (m *mockNetworkPartialMessages) removePeers() {
	for a := range m.handlers {
		for b := range m.handlers {
			if a == b {
				continue
			}
			m.handlers[a].RemovePeer(b)
			m.handlers[b].RemovePeer(a)
		}
	}

	// assert that there are no leaked peerInitiatedGroupCountPerTopics
	for _, h := range m.handlers {
		for topic, ctr := range h.peerInitiatedGroupCounter {
			if ctr.total != 0 {
				m.t.Errorf("unexpected peerInitiatedGroupCountPerTopic for topic %s: %d", topic, ctr.total)
			}
			for _, v := range ctr.perPeer {
				if v != 0 {
					m.t.Errorf("unexpected peerInitiatedGroupCountPerTopic for topic %s: %d", topic, v)
				}
			}
		}
	}
}

func (m *mockNetworkPartialMessages) handleRPCs() bool {
	for id, h := range m.handlers {
		if len(m.pendingMsgs[id]) > 0 {
			var rpc rpcWithFrom
			rpc, m.pendingMsgs[id] = m.pendingMsgs[id][0], m.pendingMsgs[id][1:]
			h.HandleRPC(rpc.from, rpc.rpc)
		}
	}
	// Process deferred publishes (queued by OnIncomingRPC callbacks).
	// These must run after HandleRPC has stored the updated PeerState.
	for len(m.deferredPublishes) > 0 {
		pubs := m.deferredPublishes
		m.deferredPublishes = nil
		for _, pub := range pubs {
			pub()
		}
	}
	moreLeft := false
	for id := range m.handlers {
		if len(m.pendingMsgs[id]) > 0 {
			moreLeft = true
			break
		}
	}
	return moreLeft
}

func (m *mockNetworkPartialMessages) sendRPC(from, to peer.ID, rpc *pubsub_pb.PartialMessagesExtension, _ bool) {
	if to == "" {
		panic("empty peer ID")
	}
	// fmt.Printf("Sending RPC from %s to %s: %+v\n", from, to, rpc)
	m.pendingMsgs[to] = append(m.pendingMsgs[to], rpcWithFrom{from, rpc})
	m.allSentMsgs[to] = append(m.allSentMsgs[to], rpcWithFrom{from, rpc})
}

const testPartialMessageLeaves = 8

// testPartialMessage represents a partial message where parts can be verified
// by a merkle tree commitment. By convention, there are
// `testPartialMessageLeaves` parts.
type testPartialMessage struct {
	Commitment []byte
	Parts      [testPartialMessageLeaves][]byte
	Proofs     [testPartialMessageLeaves][]merkle.ProofStep

	// EagerPushHeader is extra data included only when eager pushing.
	// This demonstrates how applications can include additional metadata
	// on the first push to a peer.
	EagerPushHeader []byte
	// shouldEagerPush indicates if this message should perform eager push.
	// Only true for messages created by the sender, not for messages extended
	// from received data.
	shouldEagerPush bool

	republish func(partial *testPartialMessage)
	onErr     func(error)
}

func (pm *testPartialMessage) complete() bool {
	for _, part := range pm.Parts {
		if len(part) == 0 {
			return false
		}
	}
	return true
}

// AvailableParts returns a bitmap of available parts
func (pm *testPartialMessage) PartsMetadata() bitmap.Bitmap {
	out := bitmap.NewBitmapWithOnesCount(testPartialMessageLeaves)
	for i, part := range pm.Parts {
		if len(part) == 0 {
			out.Clear(i)
		}
	}
	return out
}

func (pm *testPartialMessage) extendFromEncodedPartialMessage(_ peer.ID, data []byte) (extended bool) {
	if len(data) == 0 {
		return
	}
	var decoded testPartialMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		pm.onErr(err)
		return
	}

	// Verify
	if !bytes.Equal(pm.Commitment, decoded.Commitment) {
		pm.onErr(errors.New("commitment mismatch"))
		return
	}

	// Copy eager push header if present (only sent on eager push)
	if len(decoded.EagerPushHeader) > 0 && len(pm.EagerPushHeader) == 0 {
		pm.EagerPushHeader = decoded.EagerPushHeader
	}

	for i, part := range decoded.Parts {
		if len(pm.Parts[i]) > 0 {
			continue
		}
		if len(part) == 0 {
			continue
		}
		proof := decoded.Proofs[i]
		if len(proof) == 0 {
			continue
		}
		if !merkle.VerifyProof(part, pm.Commitment, proof) {
			pm.onErr(errors.New("proof verification failed"))
			return
		}

		pm.Parts[i] = part
		pm.Proofs[i] = proof
		extended = true
	}

	nonEmptyParts := 0
	for i := range pm.Parts {
		if len(pm.Parts[i]) > 0 {
			nonEmptyParts++
		}
	}

	return
}

// GroupID implements PartialMessage.
func (pm *testPartialMessage) GroupID() []byte {
	return pm.Commitment
}

func (pm *testPartialMessage) shouldRequest(partsMetadata []byte) bool {
	var myParts big.Int
	myParts.SetBytes(pm.PartsMetadata())
	var zero big.Int

	var peerHas big.Int
	peerHas.SetBytes(partsMetadata)

	var iWant big.Int
	iWant.Xor(&myParts, &peerHas)
	iWant.And(&iWant, &peerHas)

	return iWant.Cmp(&zero) != 0
}

// ForPeer implements Message.
func (pm *testPartialMessage) ForPeer(remote peer.ID, requestedMessage bool, peerState peerState) (peerState, []byte, PartsMetadata, error) {
	myPartsMeta := pm.PartsMetadata()

	var encodedMsg []byte
	if requestedMessage {
		if peerState.recvd != nil {
			// Peer has told us what they have — send parts they're missing
			peerHas := peerState.recvd
			var added bool
			var tempMessage testPartialMessage
			tempMessage.Commitment = pm.Commitment
			for i := range pm.Parts {
				if peerHas.Get(i) {
					continue
				}
				if len(pm.Parts[i]) == 0 {
					continue
				}
				tempMessage.Parts[i] = pm.Parts[i]
				tempMessage.Proofs[i] = pm.Proofs[i]
				added = true
			}
			if added {
				b, err := json.Marshal(tempMessage)
				if err != nil {
					return peerState, nil, nil, err
				}
				encodedMsg = b
			}
			peerState.recvd = bitmap.Merge(peerHas, myPartsMeta)
		} else if pm.shouldEagerPush {
			// No peer metadata yet, eager push all parts + header
			var tempMessage testPartialMessage
			tempMessage.Commitment = pm.Commitment
			tempMessage.EagerPushHeader = pm.EagerPushHeader
			for i := range pm.Parts {
				if len(pm.Parts[i]) == 0 {
					continue
				}
				tempMessage.Parts[i] = pm.Parts[i]
				tempMessage.Proofs[i] = pm.Proofs[i]
			}
			b, err := json.Marshal(tempMessage)
			if err != nil {
				return peerState, nil, nil, err
			}
			encodedMsg = b
			peerState.recvd = slices.Clone(myPartsMeta)
		}
	}

	// Send partsMetadata if different from what we last sent
	var partsMetadataToSend PartsMetadata
	if !bytes.Equal(myPartsMeta, peerState.sent) {
		partsMetadataToSend = PartsMetadata(myPartsMeta)
		peerState.sent = slices.Clone(myPartsMeta)
	}

	return peerState, encodedMsg, partsMetadataToSend, nil
}

var _ Message[peerState] = (*testPartialMessage)(nil)

type testPeers struct {
	peers    []peer.ID
	handlers []*PartialMessagesExtension[peerState]
	network  *mockNetworkPartialMessages
	// Track partial messages per peer per topic per group
	partialMessages map[peer.ID]map[string]map[string]*testPartialMessage
}

func createPeers(t *testing.T, topic string, n int, nonMesh bool) *testPeers {
	nw := &mockNetworkPartialMessages{
		t:           t,
		pendingMsgs: make(map[peer.ID][]rpcWithFrom),
		allSentMsgs: make(map[peer.ID][]rpcWithFrom),
		handlers:    make(map[peer.ID]*PartialMessagesExtension[peerState]),
	}

	peers := make([]peer.ID, n)
	handlers := make([]*PartialMessagesExtension[peerState], n)

	// Create peer IDs
	for i := range n {
		peers[i] = peer.ID(fmt.Sprintf("%d", i+1))
	}

	// Create testPeers structure first
	testPeers := &testPeers{
		peers:           peers,
		handlers:        handlers,
		network:         nw,
		partialMessages: make(map[peer.ID]map[string]map[string]*testPartialMessage),
	}

	// Initialize partial message tracking for each peer
	for _, peerID := range peers {
		testPeers.partialMessages[peerID] = make(map[string]map[string]*testPartialMessage)
	}

	// Create handlers for each peer
	for i := range n {
		currentPeer := peers[i]

		// Create router for this peer
		router := &testRouter{
			sendRPC: func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool) {
				nw.sendRPC(currentPeer, p, r, urgent)
			},
			meshPeers: func(topic string) iter.Seq[peer.ID] {
				return func(yield func(peer.ID) bool) {
					if nonMesh {
						// No peers are in the mesh
						return
					}

					// Yield all other peers
					for j, otherPeer := range peers {
						if j != i {
							if !yield(otherPeer) {
								return
							}
						}
					}
				}
			},
		}

		var handler *PartialMessagesExtension[peerState]
		// Create handler
		handler = &PartialMessagesExtension[peerState]{
			Logger: slog.Default().With("id", i),
			GossipForPeer: func(topic string, groupID string, remote peer.ID, peerState peerState) (peerState, []byte, PartsMetadata, error) {
				pm := testPeers.partialMessages[currentPeer][topic][groupID]
				if pm == nil {
					return peerState, nil, nil, nil
				}
				return pm.ForPeer(remote, false, peerState)
			},
			OnIncomingRPC: func(from peer.ID, peerState peerState, rpc *pubsub_pb.PartialMessagesExtension) (peerState, error) {
				// Handle incoming partial message data - use testPeers to track state
				// Get or create the partial message for this topic/group
				if testPeers.partialMessages[currentPeer][topic] == nil {
					testPeers.partialMessages[currentPeer][topic] = make(map[string]*testPartialMessage)
				}

				groupID := rpc.GroupID
				groupKey := string(groupID)
				pm := testPeers.partialMessages[currentPeer][topic][groupKey]
				if pm == nil {
					pm = &testPartialMessage{
						Commitment: groupID,
						republish: func(pm *testPartialMessage) {
							nw.deferredPublishes = append(nw.deferredPublishes, func() {
								handlers[i].PublishPartial(topic, pm, PublishOptions{})
							})
						},
					}
					testPeers.partialMessages[currentPeer][topic][groupKey] = pm
				}

				// Merge peer's partsMetadata
				if rpc.PartsMetadata != nil {
					peerState.recvd = bitmap.Merge(peerState.recvd, bitmap.Bitmap(rpc.PartsMetadata))
				}

				// Extend the partial message with the incoming data
				recvdNewData := pm.extendFromEncodedPartialMessage(from, rpc.PartialMessage)

				if recvdNewData {
					// Peer can infer we now have these parts
					peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					nw.deferredPublishes = append(nw.deferredPublishes, func() {
						pm.republish(pm)
					})
					return peerState, nil
				}

				peerHasUsefulData := pm.shouldRequest(rpc.PartsMetadata)

				myParts := pm.PartsMetadata()
				peerHasBM := peerState.recvd
				wantBM := bitmap.Bitmap(make([]byte, len(myParts)))
				copy(wantBM, myParts)
				wantBM.Xor(peerHasBM)
				wantBM.And(myParts)
				weHaveUsefulData := false
				for i := range testPartialMessageLeaves {
					if wantBM.Get(i) {
						weHaveUsefulData = true
						break
					}
				}

				if weHaveUsefulData || peerHasUsefulData {
					nw.deferredPublishes = append(nw.deferredPublishes, func() {
						handler.PublishPartial(topic, pm, PublishOptions{})
					})
				}
				return peerState, nil
			},
			GroupTTLByHeatbeat: 5,
		}
		handler.Init(router)

		handlers[i] = handler
		nw.handlers[currentPeer] = handler
	}

	t.Cleanup(func() {
		testPeers.cleanup(t)
	})

	return testPeers
}

// Helper method to get or create a partial message for a peer
func (tp *testPeers) getOrCreatePartialMessage(peerIndex int, topic string, groupID []byte) *testPartialMessage {
	peerID := tp.peers[peerIndex]
	if tp.partialMessages[peerID][topic] == nil {
		tp.partialMessages[peerID][topic] = make(map[string]*testPartialMessage)
	}

	groupKey := string(groupID)
	pm := tp.partialMessages[peerID][topic][groupKey]
	if pm == nil {
		handler := tp.handlers[peerIndex]
		pm = &testPartialMessage{
			Commitment: groupID,
			republish: func(pm *testPartialMessage) {
				handler.PublishPartial(topic, pm, PublishOptions{})
			},
		}
		tp.partialMessages[peerID][topic][groupKey] = pm
	}
	return pm
}

func (tp *testPeers) cleanup(t *testing.T) {
	// Assert no more state is left
	for range 10 {
		for _, h := range tp.handlers {
			h.Heartbeat()
		}
	}
	for _, h := range tp.handlers {
		if len(h.statePerTopicPerGroup) != 0 {
			t.Fatal("handlers should have cleaned up all their state")
		}
	}

	// Assert no empty RPCs
	for _, msgs := range tp.network.allSentMsgs {
		for _, msg := range msgs {
			if msg.rpc.Size() == 0 {
				t.Fatal("empty message")
			}
		}
	}
}

// Helper function to register a message with the test framework
func (tp *testPeers) registerMessage(peerIndex int, topic string, msg *testPartialMessage) {
	peerID := tp.peers[peerIndex]
	if tp.partialMessages[peerID][topic] == nil {
		tp.partialMessages[peerID][topic] = make(map[string]*testPartialMessage)
	}
	tp.partialMessages[peerID][topic][string(msg.GroupID())] = msg
}

func newFullTestMessage(r io.Reader, ext *PartialMessagesExtension[peerState], topic string) (*testPartialMessage, error) {
	out := &testPartialMessage{}
	for i := range out.Parts {
		out.Parts[i] = make([]byte, 8)
		if _, err := io.ReadFull(r, out.Parts[i]); err != nil {
			return nil, err
		}
	}
	out.Commitment = merkle.MerkleRoot(out.Parts[:])
	for i := range out.Parts {
		out.Proofs[i] = merkle.MerkleProof(out.Parts[:], i)
	}
	out.republish = func(pm *testPartialMessage) {
		ext.PublishPartial(topic, pm, PublishOptions{})
	}
	return out, nil
}

func newEmptyTestMessage(commitment []byte, ext *PartialMessagesExtension[peerState], topic string) *testPartialMessage {
	return &testPartialMessage{
		Commitment: commitment,
		republish: func(pm *testPartialMessage) {
			ext.PublishPartial(topic, pm, PublishOptions{})
		},
	}
}

func TestPartialMessages(t *testing.T) {
	topic := "test-topic"
	rand := rand.New(rand.NewSource(0))
	// For debugging:
	// slog.SetLogLoggerLevel(slog.LevelDebug)

	t.Run("h1 has all the data. h2 requests it", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		h1Msg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		peers.registerMessage(0, topic, h1Msg)

		// h1 knows the full message
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		peers.registerMessage(0, topic, h1Msg)

		// h2 only knows the group id
		h2Msg := newEmptyTestMessage(h1Msg.Commitment, peers.handlers[1], topic)
		peers.registerMessage(1, topic, h2Msg)
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert h2 has the full message
		if !h2Msg.complete() {
			t.Fatal("h2 should have the full message", h2Msg.PartsMetadata())
		}
	})

	t.Run("h1 has all the data and eager pushes. h2 has the next message also eager pushes", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		h1Msg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		// Set eager push header - this should only be sent on eager push
		h1Msg.EagerPushHeader = []byte("h1-eager-header")
		h1Msg.shouldEagerPush = true

		// h1 knows the full message and eager pushes
		err = peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// h2 will receive partial message data through OnIncomingRPC
		// We can access it through our tracking system
		lastPartialMessageh2 := peers.getOrCreatePartialMessage(1, topic, h1Msg.Commitment)

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert h2 has the full message
		if !lastPartialMessageh2.complete() {
			t.Fatal("h2 should have the full message")
		}

		// Assert h2 received the eager push header from h1
		if !bytes.Equal(lastPartialMessageh2.EagerPushHeader, h1Msg.EagerPushHeader) {
			t.Fatalf("h2 should have received eager push header from h1, got %q, want %q",
				lastPartialMessageh2.EagerPushHeader, h1Msg.EagerPushHeader)
		}

		h2Msg, err := newFullTestMessage(rand, peers.handlers[1], topic)
		if err != nil {
			t.Fatal(err)
		}
		// Set eager push header for h2's message
		h2Msg.EagerPushHeader = []byte("h2-eager-header")
		h2Msg.shouldEagerPush = true

		// h2 knows the full message and eager pushes
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})

		// h1 will receive partial message data through OnIncomingRPC
		// We can access it through our tracking system
		lastPartialMessageh1 := peers.getOrCreatePartialMessage(0, topic, h2Msg.Commitment)

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert h1 has the full message
		if !lastPartialMessageh1.complete() {
			t.Fatal("h1 should have the full message")
		}

		// Assert h1 received the eager push header from h2
		if !bytes.Equal(lastPartialMessageh1.EagerPushHeader, h2Msg.EagerPushHeader) {
			t.Fatalf("h1 should have received eager push header from h2, got %q, want %q",
				lastPartialMessageh1.EagerPushHeader, h2Msg.EagerPushHeader)
		}
	})

	t.Run("h1 has all the data. h2 doesn't know anything", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		h1Msg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		peers.registerMessage(0, topic, h1Msg)

		// h1 knows the full message
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// h2 should now have the partial message after receiving data
		h2Msg := peers.getOrCreatePartialMessage(1, topic, h1Msg.Commitment)

		// Assert h2 has the full message
		if !h2Msg.complete() {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 is not in h2's mesh, but sends h2 a partial message", func(t *testing.T) {
		peers := createPeers(t, topic, 2, true)
		defer peers.network.removePeers()

		h1Msg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		peers.registerMessage(0, topic, h1Msg)

		// h1 knows the full message and explicitly publishes to peer 1 (e.g. fanout or gossip (like an IHAVE))
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{PublishToPeers: []peer.ID{peers.peers[1]}})

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// h2 should now have the partial message after receiving data
		h2Msg := peers.getOrCreatePartialMessage(1, topic, h1Msg.Commitment)

		// Assert h2 has the full message
		if !h2Msg.complete() {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 has all the data. h2 has some of it", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		h1Msg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}

		// h1 knows the full message
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		peers.registerMessage(0, topic, h1Msg)

		// h2 only knows part of it
		h2Msg := newEmptyTestMessage(h1Msg.Commitment, peers.handlers[1], topic)
		for i := range h2Msg.Parts {
			if i%2 == 0 {
				h2Msg.Parts[i] = h1Msg.Parts[i]
				h2Msg.Proofs[i] = h1Msg.Proofs[i]
			}
		}
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})
		peers.registerMessage(1, topic, h2Msg)

		emptyMsg := &testPartialMessage{}
		emptyMetadata := emptyMsg.PartsMetadata()
		if bytes.Equal(peers.network.pendingMsgs[peers.peers[0]][0].rpc.PartsMetadata, emptyMetadata) {
			t.Fatal("h2 request should not be the same as an empty message")
		}

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert that h2 only sent a single Partial IWANT
		count := 0
		for _, rpc := range peers.network.allSentMsgs[peers.peers[0]] {
			if rpc.rpc.PartsMetadata != nil {
				count++
			}
		}
		if count != 1 {
			t.Fatal("h2 should only have sent one part updates")
		}

		// Assert h2 has the full message
		if !h2Msg.complete() {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 has half the data. h2 has the other half of it", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		fullMsg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		h1Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[0], topic)
		for i := range fullMsg.Parts {
			if i%2 == 0 {
				h1Msg.Parts[i] = fullMsg.Parts[i]
				h1Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h2 only knows part of it
		h2Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[1], topic)
		for i := range h2Msg.Parts {
			if i%2 == 1 {
				h2Msg.Parts[i] = fullMsg.Parts[i]
				h2Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h1 knows half
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		peers.registerMessage(0, topic, h1Msg)
		// h2 knows the other half
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})
		peers.registerMessage(1, topic, h2Msg)

		emptyMsg := &testPartialMessage{}
		emptyMetadata := emptyMsg.PartsMetadata()
		if bytes.Equal(peers.network.pendingMsgs[peers.peers[0]][0].rpc.PartsMetadata, emptyMetadata) {
			t.Fatal("h2 metadata should not be the same as an empty message's metadata")
		}

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert that h2 only sent a single Partial IWANT
		count := 0
		for _, rpc := range peers.network.allSentMsgs[peers.peers[0]] {
			if len(rpc.rpc.PartsMetadata) > 0 {
				count++
			}
		}
		if count != 1 {
			t.Fatal("h2 should only have sent one part update")
		}

		// Assert h2 has the full message
		if !h2Msg.complete() {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 and h2 have the the same half of data. No partial messages should be sent", func(t *testing.T) {
		peers := createPeers(t, topic, 2, false)
		defer peers.network.removePeers()

		fullMsg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}
		h1Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[0], topic)
		for i := range fullMsg.Parts {
			if i%2 == 0 {
				h1Msg.Parts[i] = fullMsg.Parts[i]
				h1Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h2 only knows part of it (same as h1)
		h2Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[1], topic)
		for i := range h2Msg.Parts {
			if i%2 == 0 {
				h2Msg.Parts[i] = fullMsg.Parts[i]
				h2Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h1 knows half
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		// h2 knows the same half
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})

		// Handle all RPCs
		for peers.network.handleRPCs() {
		}

		// Assert that no peer sent a partial message
		count := 0
		for _, rpcs := range peers.network.allSentMsgs {
			for _, rpc := range rpcs {
				if len(rpc.rpc.PartialMessage) > 0 {
					count++
				}
			}
		}
		if count > 0 {
			t.Fatal("No partial messages should have been sent")
		}
	})

	t.Run("three peers with distributed partial data", func(t *testing.T) {
		peers := createPeers(t, topic, 3, false)
		defer peers.network.removePeers()

		fullMsg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}

		// Peer 1 has parts 0, 3, 6
		h1Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[0], topic)
		for i := range fullMsg.Parts {
			if i%3 == 0 {
				h1Msg.Parts[i] = fullMsg.Parts[i]
				h1Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// Peer 2 has parts 1, 4, 7
		h2Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[1], topic)
		for i := range fullMsg.Parts {
			if i%3 == 1 {
				h2Msg.Parts[i] = fullMsg.Parts[i]
				h2Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// Peer 3 has parts 2, 5
		h3Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[2], topic)
		for i := range fullMsg.Parts {
			if i%3 == 2 {
				h3Msg.Parts[i] = fullMsg.Parts[i]
				h3Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// All peers publish their partial messages
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		peers.registerMessage(0, topic, h1Msg)
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})
		peers.registerMessage(1, topic, h2Msg)
		peers.handlers[2].PublishPartial(topic, h3Msg, PublishOptions{})
		peers.registerMessage(2, topic, h3Msg)

		// Handle all RPCs until convergence
		for peers.network.handleRPCs() {
		}

		// Assert all peers have the full message
		for i := range peers.handlers {
			var msg *testPartialMessage
			switch i {
			case 0:
				msg = h1Msg
			case 1:
				msg = h2Msg
			case 2:
				msg = h3Msg
			}

			if !msg.complete() {
				t.Fatalf("peer %d should have the full message", i+1)
			}
		}
	})
	t.Run("three peers. peer 1 has all data and eager pushes. Receivers eager push new content", func(t *testing.T) {
		peers := createPeers(t, topic, 3, false)
		defer peers.network.removePeers()

		fullMsg, err := newFullTestMessage(rand, peers.handlers[0], topic)
		if err != nil {
			t.Fatal(err)
		}

		// Peer 1 has all the data
		h1Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[0], topic)
		for i := range fullMsg.Parts {
			h1Msg.Parts[i] = fullMsg.Parts[i]
			h1Msg.Proofs[i] = fullMsg.Proofs[i]
		}

		// Peer 2 has no parts
		h2Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[1], topic)

		// Eagerly push new data to peers
		h2Msg.republish = func(pm *testPartialMessage) {
			peers.handlers[1].PublishPartial(topic, pm, PublishOptions{})
		}
		// Peer 3 has no parts
		h3Msg := newEmptyTestMessage(fullMsg.Commitment, peers.handlers[2], topic)
		// Eagerly push new data to peers
		h3Msg.republish = func(pm *testPartialMessage) {
			peers.handlers[2].PublishPartial(topic, pm, PublishOptions{})
		}
		// All peers publish their partial messages
		peers.handlers[0].PublishPartial(topic, h1Msg, PublishOptions{})
		peers.registerMessage(0, topic, h1Msg)
		peers.handlers[1].PublishPartial(topic, h2Msg, PublishOptions{})
		peers.registerMessage(1, topic, h2Msg)
		peers.handlers[2].PublishPartial(topic, h3Msg, PublishOptions{})
		peers.registerMessage(2, topic, h3Msg)

		// Handle all RPCs until convergence
		for peers.network.handleRPCs() {
		}

		// Assert all peers have the full message
		for i := range peers.handlers {
			var msg *testPartialMessage
			switch i {
			case 0:
				msg = h1Msg
			case 1:
				msg = h2Msg
			case 2:
				msg = h3Msg
			}

			if !msg.complete() {
				t.Fatalf("peer %d should have the full message", i+1)
			}
		}
	})
}

func TestGossipDelivery(t *testing.T) {
	topic := "test-topic"
	rand := rand.New(rand.NewSource(42))

	// Create peers where h2 is NOT in h1's mesh but IS in h1's gossipPeers
	// This tests the gossip path
	nw := &mockNetworkPartialMessages{
		t:           t,
		pendingMsgs: make(map[peer.ID][]rpcWithFrom),
		allSentMsgs: make(map[peer.ID][]rpcWithFrom),
		handlers:    make(map[peer.ID]*PartialMessagesExtension[peerState]),
	}

	h1ID := peer.ID("h1")
	h2ID := peer.ID("h2")

	partialMessages := map[peer.ID]map[string]map[string]*testPartialMessage{
		h1ID: make(map[string]map[string]*testPartialMessage),
		h2ID: make(map[string]map[string]*testPartialMessage),
	}

	var h1Handler, h2Handler *PartialMessagesExtension[peerState]

	// Create h1 router - h2 is NOT in mesh but IS in gossipPeers
	h1Router := &testRouter{
		sendRPC: func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool) {
			nw.sendRPC(h1ID, p, r, urgent)
		},
		meshPeers: func(topic string) iter.Seq[peer.ID] {
			// h2 is NOT in mesh
			return func(yield func(peer.ID) bool) {}
		},
	}

	// Create h2 router - h1 is NOT in mesh
	h2Router := &testRouter{
		sendRPC: func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool) {
			nw.sendRPC(h2ID, p, r, urgent)
		},
		meshPeers: func(topic string) iter.Seq[peer.ID] {
			// h1 is NOT in mesh
			return func(yield func(peer.ID) bool) {}
		},
	}

	gossipOnIncomingRPC := func(selfID peer.ID, selfHandler **PartialMessagesExtension[peerState]) func(peer.ID, peerState, *pubsub_pb.PartialMessagesExtension) (peerState, error) {
		return func(from peer.ID, peerState peerState, rpc *pubsub_pb.PartialMessagesExtension) (peerState, error) {
			if partialMessages[selfID][topic] == nil {
				partialMessages[selfID][topic] = make(map[string]*testPartialMessage)
			}
			groupKey := string(rpc.GroupID)
			pm := partialMessages[selfID][topic][groupKey]
			if pm == nil {
				pm = &testPartialMessage{
					Commitment: rpc.GroupID,
					republish: func(pm *testPartialMessage) {
						nw.deferredPublishes = append(nw.deferredPublishes, func() {
							(*selfHandler).PublishPartial(topic, pm, PublishOptions{})
						})
					},
				}
				partialMessages[selfID][topic][groupKey] = pm
			}
			if rpc.PartsMetadata != nil {
				peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
			}
			recvdNewData := pm.extendFromEncodedPartialMessage(from, rpc.PartialMessage)
			if recvdNewData {
				// Peer can infer our updated parts since they sent us data
				peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
			}
			nw.deferredPublishes = append(nw.deferredPublishes, func() {
				(*selfHandler).PublishPartial(topic, pm, PublishOptions{})
			})
			return peerState, nil
		}
	}

	gossipForPeer := func(selfID peer.ID) func(string, string, peer.ID, peerState) (peerState, []byte, PartsMetadata, error) {
		return func(topic string, groupID string, remote peer.ID, peerState peerState) (peerState, []byte, PartsMetadata, error) {
			pm := partialMessages[selfID][topic][groupID]
			if pm == nil {
				return peerState, nil, nil, nil
			}
			return pm.ForPeer(remote, false, peerState)
		}
	}

	// Create h1 handler
	h1Handler = &PartialMessagesExtension[peerState]{
		Logger:             slog.Default().With("id", "h1"),
		GossipForPeer:      gossipForPeer(h1ID),
		OnIncomingRPC:      gossipOnIncomingRPC(h1ID, &h1Handler),
		GroupTTLByHeatbeat: 5,
	}
	h1Handler.Init(h1Router)

	// Create h2 handler
	h2Handler = &PartialMessagesExtension[peerState]{
		Logger:             slog.Default().With("id", "h2"),
		GossipForPeer:      gossipForPeer(h2ID),
		OnIncomingRPC:      gossipOnIncomingRPC(h2ID, &h2Handler),
		GroupTTLByHeatbeat: 5,
	}
	h2Handler.Init(h2Router)

	nw.handlers[h1ID] = h1Handler
	nw.handlers[h2ID] = h2Handler

	// Create full message for h1
	h1Msg, err := newFullTestMessage(rand, h1Handler, topic)
	if err != nil {
		t.Fatal(err)
	}
	partialMessages[h1ID][topic] = make(map[string]*testPartialMessage)
	partialMessages[h1ID][topic][string(h1Msg.GroupID())] = h1Msg

	// h1 publishes - since h2 is not in mesh, nothing should be sent to h2 yet
	h1Handler.PublishPartial(topic, h1Msg, PublishOptions{})

	// Handle any pending RPCs (should be none to h2 since not in mesh)
	for nw.handleRPCs() {
	}

	// Assert h2 does NOT have the message yet
	h2Msg := partialMessages[h2ID][topic][string(h1Msg.GroupID())]
	if h2Msg != nil && h2Msg.complete() {
		t.Fatal("h2 should NOT have the message before gossip")
	}

	h1Handler.EmitGossip(topic, []peer.ID{h2ID})

	// Handle RPCs - h2 should receive partsMetadata via gossip and request data
	for nw.handleRPCs() {
	}

	// Assert h2 now has the full message
	h2Msg = partialMessages[h2ID][topic][string(h1Msg.GroupID())]
	if h2Msg == nil || !h2Msg.complete() {
		t.Fatal("h2 should have the full message after gossip")
	}

	// Count partsMetadata RPCs sent to h2 so far
	partsMetadataCountBefore := 0
	for _, rpc := range nw.allSentMsgs[h2ID] {
		if rpc.rpc.PartsMetadata != nil && rpc.rpc.PartialMessage == nil {
			partsMetadataCountBefore++
		}
	}

	// EmitGossip - deduplication should prevent sending partsMetadata again
	h1Handler.EmitGossip(topic, []peer.ID{h2ID})
	for nw.handleRPCs() {
	}

	// Count partsMetadata RPCs sent to h2 after second heartbeat
	partsMetadataCountAfter := 0
	for _, rpc := range nw.allSentMsgs[h2ID] {
		if rpc.rpc.PartsMetadata != nil && rpc.rpc.PartialMessage == nil {
			partsMetadataCountAfter++
		}
	}

	// Assert deduplication works - no additional partsMetadata RPCs should have been sent
	if partsMetadataCountAfter != partsMetadataCountBefore {
		t.Fatalf("deduplication failed: expected %d partsMetadata RPCs, got %d", partsMetadataCountBefore, partsMetadataCountAfter)
	}

	// Cleanup
	h1Handler.RemovePeer(h2ID)
	h2Handler.RemovePeer(h1ID)
	for range 10 {
		h1Handler.Heartbeat()
		h2Handler.Heartbeat()
	}
}

func TestPeerInitiatedCounter(t *testing.T) {
	// slog.SetLogLoggerLevel(slog.LevelDebug)
	topic := "test-topic"
	randParts := func() []byte {
		buf := make([]byte, 8)
		_, _ = cryptorand.Read(buf)
		return buf
	}
	handler := PartialMessagesExtension[peerState]{
		Logger: slog.Default(),
		GossipForPeer: func(topic string, groupID string, remote peer.ID, peerState peerState) (peerState, []byte, PartsMetadata, error) {
			return peerState, nil, nil, nil
		},
		OnIncomingRPC: func(from peer.ID, peerState peerState, rpc *pubsub_pb.PartialMessagesExtension) (peerState, error) {
			if rpc.PartsMetadata != nil {
				peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
			}
			return peerState, nil
		},
		PeerInitiatedGroupLimitPerTopic:        4,
		PeerInitiatedGroupLimitPerTopicPerPeer: 2,
	}
	router := testRouter{
		sendRPC:   func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool) {},
		meshPeers: func(topic string) iter.Seq[peer.ID] { return func(yield func(peer.ID) bool) {} },
	}
	handler.Init(&router)

	err := handler.HandleRPC("1", &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       []byte("1"),
		PartsMetadata: randParts(),
	})
	if err != nil {
		t.Fatal(err)
	}

	assertCounts := func(expectedTotal int, expectedMap map[peer.ID]int) {
		t.Helper()
		if handler.peerInitiatedGroupCounter[topic].total != expectedTotal {
			t.Fatal()
		}
		if !reflect.DeepEqual(handler.peerInitiatedGroupCounter[topic].perPeer, expectedMap) {
			t.Fatal()
		}
	}

	assertCounts(1, map[peer.ID]int{"1": 1})

	err = handler.HandleRPC("1", &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       []byte("2"),
		PartsMetadata: randParts(),
	})
	if err != nil {
		t.Fatal(err)
	}

	assertCounts(2, map[peer.ID]int{"1": 2})

	err = handler.HandleRPC("1", &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       []byte("3"),
		PartsMetadata: randParts(),
	})
	if err != errPeerInitiatedGroupLimitReached {
		t.Fatal(err)
	}

	assertCounts(2, map[peer.ID]int{"1": 2})

	// Two peers publish new groups
	for id := range 2 {
		otherPeer := fmt.Sprintf("peer%d", id)
		err = handler.HandleRPC(peer.ID(otherPeer), &pubsub_pb.PartialMessagesExtension{
			TopicID:       &topic,
			GroupID:       []byte(otherPeer),
			PartsMetadata: randParts(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// The third one goves over the limit
	otherPeer := fmt.Sprintf("peer%d", 3)
	err = handler.HandleRPC(peer.ID(otherPeer), &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       []byte(otherPeer),
		PartsMetadata: randParts(),
	})
	if err != errPeerInitiatedGroupTotalLimitReached {
		t.Fatal(err)
	}

	// All peers go away, and the counts should be back to 0
	handler.RemovePeer("1")
	for id := range 2 {
		otherPeer := fmt.Sprintf("peer%d", id)
		handler.RemovePeer(peer.ID(otherPeer))
	}

	assertCounts(0, map[peer.ID]int{})

	// Test heartbeat cleanup
	err = handler.HandleRPC("1", &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       []byte("4"),
		PartsMetadata: randParts(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCounts(1, map[peer.ID]int{"1": 1})

	for range minGroupTTL + 1 {
		handler.Heartbeat()
	}
	assertCounts(0, map[peer.ID]int{})
}

func FuzzPeerInitiatedCounter(f *testing.F) {
	topic := "test-topic"
	f.Fuzz(func(t *testing.T, script []byte, totalLimit uint8, peerLimit uint8) {
		// This fuzzer works like a simple interpreter. It interprets the script as bytecode.
		if len(script) == 0 {
			return
		}
		if totalLimit == 0 {
			totalLimit = uint8(defaultPeerInitiatedGroupLimitPerTopic)
		}
		if peerLimit == 0 {
			peerLimit = uint8(defaultPeerInitiatedGroupLimitPerTopicPerPeer)
		}

		handler := PartialMessagesExtension[peerState]{
			Logger: slog.Default(),
			GossipForPeer: func(topic string, groupID string, remote peer.ID, peerState peerState) (peerState, []byte, PartsMetadata, error) {
				return peerState, nil, nil, nil
			},
			OnIncomingRPC: func(from peer.ID, peerState peerState, rpc *pubsub_pb.PartialMessagesExtension) (peerState, error) {
				if rpc.PartsMetadata != nil {
					peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
				}
				return peerState, nil
			},
			GroupTTLByHeatbeat:                     minGroupTTL,
			PeerInitiatedGroupLimitPerTopic:        int(totalLimit),
			PeerInitiatedGroupLimitPerTopicPerPeer: int(peerLimit),
		}
		router := testRouter{
			sendRPC:   func(p peer.ID, r *pubsub_pb.PartialMessagesExtension, urgent bool) {},
			meshPeers: func(topic string) iter.Seq[peer.ID] { return func(yield func(peer.ID) bool) {} },
		}
		handler.Init(&router)
		partsMetadata := []byte{0, 0, 0, 0}

		expectedTotal := 0
		expectedPeercounts := map[peer.ID]int{}

		for i := 0; len(script) > 0; i++ {
			switch script[0] % 3 {
			case 0: // handle new rpc
				script = script[1:]
				if len(script) < 1 {
					return
				}
				otherPeer := peer.ID(fmt.Sprintf("%d", script[0]))
				script = script[1:]

				var expectNoError bool
				if expectedTotal < int(totalLimit) && expectedPeercounts[peer.ID(otherPeer)] < int(peerLimit) {
					expectedPeercounts[peer.ID(otherPeer)]++
					expectedTotal++
					expectNoError = true
				}

				err := handler.HandleRPC(peer.ID(otherPeer), &pubsub_pb.PartialMessagesExtension{
					TopicID:       &topic,
					GroupID:       fmt.Appendf(nil, "%d", i),
					PartsMetadata: partsMetadata,
				})
				if expectNoError && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case 1: // remove peer
				script = script[1:]
				if len(script) < 1 {
					return
				}
				otherPeer := peer.ID(fmt.Sprintf("%d", script[0]))
				script = script[1:]

				if expectedPeercounts[peer.ID(otherPeer)] > 0 {
					expectedTotal -= expectedPeercounts[peer.ID(otherPeer)]
					delete(expectedPeercounts, peer.ID(otherPeer))
				}

				handler.RemovePeer(peer.ID(otherPeer))
			case 2: // heartbeat until everything is cleared
				script = script[1:]
				for range handler.GroupTTLByHeatbeat + 1 {
					handler.Heartbeat()
				}
				expectedTotal = 0
				expectedPeercounts = map[peer.ID]int{}
			default:
				// no-op
				script = script[1:]
			}

			ctr, ok := handler.peerInitiatedGroupCounter[topic]
			if ok {

				if expectedTotal != ctr.total {
					t.Fatalf("expected total %d, got %d", expectedTotal, ctr.total)
				}
				if !reflect.DeepEqual(expectedPeercounts, ctr.perPeer) {
					t.Fatalf("expected peer counts %v, got %v", expectedPeercounts, ctr.perPeer)
				}
			} else {
				if expectedTotal != 0 {
					t.Fatalf("expected total %d, got %d", expectedTotal, 0)
				}
				if !reflect.DeepEqual(expectedPeercounts, map[peer.ID]int{}) {
					t.Fatalf("expected peer counts %v, got %v", expectedPeercounts, map[peer.ID]int{})
				}
			}
		}
	})
}
