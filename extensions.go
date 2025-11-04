package pubsub

import (
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerExtensions struct {
	TestExtension       bool
	TopicTableExtension *pubsub_pb.ExtTopicTable
}

type TestExtensionConfig struct {
	OnReceiveTestExtension func(from peer.ID)
}

type TopicTableExtensionConfig struct {
	TopicBundles [][]string
}

func WithTestExtension(c TestExtensionConfig) Option {
	return func(ps *PubSub) error {
		if rt, ok := ps.rt.(*GossipSubRouter); ok {
			rt.extensions.testExtension = &testExtension{
				sendRPC:                rt.extensions.sendRPC,
				onReceiveTestExtension: c.OnReceiveTestExtension,
			}
			rt.extensions.myExtensions.TestExtension = true
		}
		return nil
	}
}

func WithTopicTableExtension(c TopicTableExtensionConfig) Option {
	return func(ps *PubSub) error {
		if rt, ok := ps.rt.(*GossipSubRouter); ok {
			e, err := newTopicTableExtension(c.TopicBundles)
			if err != nil {
				return err
			}

			rt.extensions.myExtensions.TopicTableExtension = e.GetControlExtension()
			rt.extensions.topicTableExtension = e
		}
		return nil
	}
}

func hasPeerExtensions(rpc *RPC) bool {
	if rpc != nil && rpc.Control != nil && rpc.Control.Extensions != nil {
		return true
	}
	return false
}

func peerExtensionsFromRPC(rpc *RPC) PeerExtensions {
	out := PeerExtensions{}
	if hasPeerExtensions(rpc) {
		out.TestExtension = rpc.Control.Extensions.GetTestExtension()
		out.TopicTableExtension = rpc.Control.Extensions.GetTopicTableExtension()
	}
	return out
}

func (pe *PeerExtensions) ExtendHelloRPC(rpc *RPC) *RPC {
	if pe.TestExtension {
		if rpc.Control == nil {
			rpc.Control = &pubsub_pb.ControlMessage{}
		}
		if rpc.Control.Extensions == nil {
			rpc.Control.Extensions = &pubsub_pb.ControlExtensions{}
		}
		rpc.Control.Extensions.TestExtension = &pe.TestExtension
	}
	if pe.TopicTableExtension != nil {
		if rpc.Control == nil {
			rpc.Control = &pubsub_pb.ControlMessage{}
		}
		if rpc.Control.Extensions == nil {
			rpc.Control.Extensions = &pubsub_pb.ControlExtensions{}
		}
		rpc.Control.Extensions.TopicTableExtension = pe.TopicTableExtension
	}
	return rpc
}

type extensionsState struct {
	myExtensions      PeerExtensions
	peerExtensions    map[peer.ID]PeerExtensions // peer's extensions
	sentExtensions    map[peer.ID]struct{}
	reportMisbehavior func(peer.ID)
	sendRPC           func(p peer.ID, r *RPC, urgent bool)

	testExtension       *testExtension
	topicTableExtension *topicTableExtension
}

func newExtensionsState(myExtensions PeerExtensions, reportMisbehavior func(peer.ID), sendRPC func(peer.ID, *RPC, bool)) *extensionsState {
	return &extensionsState{
		myExtensions:      myExtensions,
		peerExtensions:    make(map[peer.ID]PeerExtensions),
		sentExtensions:    make(map[peer.ID]struct{}),
		reportMisbehavior: reportMisbehavior,
		sendRPC:           sendRPC,

		testExtension:       nil,
		topicTableExtension: nil,
	}
}

func (es *extensionsState) HandleRPC(rpc *RPC) {
	if _, ok := es.peerExtensions[rpc.from]; !ok {
		// We know this is the first message because we didn't have extensions
		// for this peer, and we always set extensions on the first rpc.
		es.peerExtensions[rpc.from] = peerExtensionsFromRPC(rpc)
		if _, ok := es.sentExtensions[rpc.from]; ok {
			// We just finished both sending and receiving the extensions
			// control message.
			es.extensionsAddPeer(rpc.from)
		}
	} else {
		// We already have an extension for this peer. If they send us another
		// extensions control message, that is a protocol error. We should
		// down score them because they are misbehaving.
		if hasPeerExtensions(rpc) {
			es.reportMisbehavior(rpc.from)
		}
	}

	es.extensionsHandleRPC(rpc)
}

func (es *extensionsState) InterceptRPC(rpc *RPC) *RPC {
	if es.myExtensions.TopicTableExtension != nil && es.peerExtensions[rpc.from].TopicTableExtension != nil {
		rpc = es.topicTableExtension.InterceptRPC(rpc)
	}
	return rpc
}

func (es *extensionsState) AddPeer(id peer.ID, helloPacket *RPC) *RPC {
	// Send our extensions as the first message.
	helloPacket = es.myExtensions.ExtendHelloRPC(helloPacket)

	es.sentExtensions[id] = struct{}{}
	if _, ok := es.peerExtensions[id]; ok {
		// We've just finished sending and receiving the extensions control
		// message.
		es.extensionsAddPeer(id)
	}
	return helloPacket
}

func (es *extensionsState) RemovePeer(id peer.ID) {
	_, recvdExt := es.peerExtensions[id]
	_, sentExt := es.sentExtensions[id]
	if recvdExt && sentExt {
		// Add peer was previously called, so we need to call remove peer
		es.extensionsRemovePeer(id)
	}
	delete(es.peerExtensions, id)
	if len(es.peerExtensions) == 0 {
		es.peerExtensions = make(map[peer.ID]PeerExtensions)
	}
	delete(es.sentExtensions, id)
	if len(es.sentExtensions) == 0 {
		es.sentExtensions = make(map[peer.ID]struct{})
	}
}

func (es *extensionsState) ExtendRPC(id peer.ID, rpc *RPC) *RPC {
	if es.myExtensions.TopicTableExtension != nil && es.peerExtensions[id].TopicTableExtension != nil {
		rpc = es.topicTableExtension.ExtendRPC(id, rpc)
	}
	return rpc
}

// extensionsAddPeer is only called once we've both sent and received the
// extensions control message.
func (es *extensionsState) extensionsAddPeer(id peer.ID) {
	if es.myExtensions.TestExtension && es.peerExtensions[id].TestExtension {
		es.testExtension.AddPeer(id)
	}
	if es.myExtensions.TopicTableExtension != nil && es.peerExtensions[id].TopicTableExtension != nil {
		hashSlices := es.peerExtensions[id].TopicTableExtension.GetTopicBundleHashes()
		// Parsing the slices of bytes, to get a slice of TopicBundleHash
		bundleHashes := make([]TopicBundleHash, len(hashSlices))
		for _, buf := range hashSlices {
			hash, err := newTopicBundleHash(buf)
			if err != nil {
				// If there is an error parsing the hash, just quietly return
				return
			}
			bundleHashes = append(bundleHashes, *hash)
		}
		// If there is an error adding a peer, just quietly skip it
		_ = es.topicTableExtension.AddPeer(id, bundleHashes)
	}
}

// extensionsRemovePeer is always called after extensionsAddPeer.
func (es *extensionsState) extensionsRemovePeer(id peer.ID) {
}

func (es *extensionsState) extensionsHandleRPC(rpc *RPC) {
	if es.myExtensions.TestExtension && es.peerExtensions[rpc.from].TestExtension {
		es.testExtension.HandleRPC(rpc.from, rpc.TestExtension)
	}
}
