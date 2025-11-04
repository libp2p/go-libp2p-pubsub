package pubsub

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"sort"
	"strings"
)

type TopicBundleHash [4]byte

func newTopicBundleHash(bytes []byte) (*TopicBundleHash, error) {
	if len(bytes) != 4 {
		return nil, fmt.Errorf("expected 4 bytes for TopicBundleHash found: %d", len(bytes))
	}
	var hash TopicBundleHash
	copy(hash[:], bytes)

	return &hash, nil
}

type topicTableExtension struct {
	bundleHashes      []TopicBundleHash
	intersectedHashes map[peer.ID][]TopicBundleHash

	indexToName map[TopicBundleHash][]string       // bundle hash -> list of topics
	nameToIndex map[TopicBundleHash]map[string]int // bundle hash -> topic -> 0-based index in bundle
}

func newTopicTableExtension(myBundles [][]string) (*topicTableExtension, error) {
	bundleHashes := make([]TopicBundleHash, 0, len(myBundles))

	indexToName := make(map[TopicBundleHash][]string)
	nameToIndex := make(map[TopicBundleHash]map[string]int)

	for _, topics := range myBundles {
		sort.Strings(topics)

		hash := computeTopicBundleHash(topics)
		bundleHashes = append(bundleHashes, hash)

		indexToName[hash] = topics
		nameToIndex[hash] = make(map[string]int)
		for idx, topic := range topics {
			nameToIndex[hash][topic] = idx
		}
	}
	if err := validateBundles(bundleHashes); err != nil {
		return nil, err
	}
	e := &topicTableExtension{
		bundleHashes:      bundleHashes,
		intersectedHashes: make(map[peer.ID][]TopicBundleHash),
		indexToName:       indexToName,
		nameToIndex:       nameToIndex,
	}
	return e, nil
}

func (e *topicTableExtension) GetControlExtension() *pubsub_pb.ExtTopicTable {
	hashSlices := make([][]byte, 0, len(e.bundleHashes))

	for _, hash := range e.bundleHashes {
		hashSlices = append(hashSlices, hash[:])
	}
	return &pubsub_pb.ExtTopicTable{
		TopicBundleHashes: hashSlices,
	}
}

func (e *topicTableExtension) InterceptRPC(rpc *RPC) *RPC {
	// Replace all topic indices with topic names for this peer
	id := rpc.from

	// Replace in IHAVE messages
	for _, ihave := range rpc.GetControl().GetIhave() {
		if topicIndex, ok := ihave.GetTopicRef().(*pubsub_pb.ControlIHave_TopicIndex); ok {
			if topicName, err := e.GetTopicName(id, int(topicIndex.TopicIndex)); err == nil {
				ihave.TopicRef = &pubsub_pb.ControlIHave_TopicID{TopicID: topicName}
			}
		}
	}
	// Replace in GRAFT messages
	for _, graft := range rpc.GetControl().GetGraft() {
		if topicIndex, ok := graft.GetTopicRef().(*pubsub_pb.ControlGraft_TopicIndex); ok {
			if topicName, err := e.GetTopicName(id, int(topicIndex.TopicIndex)); err == nil {
				graft.TopicRef = &pubsub_pb.ControlGraft_TopicID{TopicID: topicName}
			}
		}
	}
	// Replace in PRUNE messages
	for _, prune := range rpc.GetControl().GetPrune() {
		if topicIndex, ok := prune.GetTopicRef().(*pubsub_pb.ControlPrune_TopicIndex); ok {
			if topicName, err := e.GetTopicName(id, int(topicIndex.TopicIndex)); err == nil {
				prune.TopicRef = &pubsub_pb.ControlPrune_TopicID{TopicID: topicName}
			}
		}
	}
	// Replace in published messages
	for _, msg := range rpc.GetPublish() {
		if topicIndex, ok := msg.GetTopicRef().(*pubsub_pb.Message_TopicIndex); ok {
			if topicName, err := e.GetTopicName(id, int(topicIndex.TopicIndex)); err == nil {
				msg.TopicRef = &pubsub_pb.Message_Topic{Topic: topicName}
			}
		}
	}
	// Replace in subscriptions
	for _, sub := range rpc.GetSubscriptions() {
		if topicIndex, ok := sub.GetTopicRef().(*pubsub_pb.RPC_SubOpts_TopicIndex); ok {
			if topicName, err := e.GetTopicName(id, int(topicIndex.TopicIndex)); err == nil {
				sub.TopicRef = &pubsub_pb.RPC_SubOpts_Topicid{Topicid: topicName}
			}
		}
	}
	return rpc
}

func (e *topicTableExtension) AddPeer(id peer.ID, bundles []TopicBundleHash) error {
	if err := validateBundles(bundles); err != nil {
		return err
	}
	e.intersectedHashes[id] = computeBundleIntersection(e.bundleHashes, bundles)
	return nil
}

func (e *topicTableExtension) ExtendRPC(id peer.ID, rpc *RPC) *RPC {
	// If it's a hello packet, don't replace the topic names with topic indices
	if rpc.GetControl().GetExtensions().GetTopicTableExtension() != nil {
		return rpc
	}

	// Replace all topic names with topic indices for this peer

	// Replace in IHAVE messages
	for _, ihave := range rpc.GetControl().GetIhave() {
		if topicID, ok := ihave.GetTopicRef().(*pubsub_pb.ControlIHave_TopicID); ok {
			if idx, err := e.GetTopicIndex(id, topicID.TopicID); err == nil {
				ihave.TopicRef = &pubsub_pb.ControlIHave_TopicIndex{TopicIndex: uint32(idx)}
			}
		}
	}
	// Replace in GRAFT messages
	for _, graft := range rpc.GetControl().GetGraft() {
		if topicID, ok := graft.GetTopicRef().(*pubsub_pb.ControlGraft_TopicID); ok {
			if idx, err := e.GetTopicIndex(id, topicID.TopicID); err == nil {
				graft.TopicRef = &pubsub_pb.ControlGraft_TopicIndex{TopicIndex: uint32(idx)}
			}
		}
	}
	// Replace in PRUNE messages
	for _, prune := range rpc.GetControl().GetPrune() {
		if topicID, ok := prune.GetTopicRef().(*pubsub_pb.ControlPrune_TopicID); ok {
			if idx, err := e.GetTopicIndex(id, topicID.TopicID); err == nil {
				prune.TopicRef = &pubsub_pb.ControlPrune_TopicIndex{TopicIndex: uint32(idx)}
			}
		}
	}
	// Replace in published messages
	for _, msg := range rpc.GetPublish() {
		if topic, ok := msg.GetTopicRef().(*pubsub_pb.Message_Topic); ok {
			if idx, err := e.GetTopicIndex(id, topic.Topic); err == nil {
				msg.TopicRef = &pubsub_pb.Message_TopicIndex{TopicIndex: uint32(idx)}
			}
		}
	}
	// Replace in subscriptions
	for _, sub := range rpc.GetSubscriptions() {
		if topicid, ok := sub.GetTopicRef().(*pubsub_pb.RPC_SubOpts_Topicid); ok {
			if idx, err := e.GetTopicIndex(id, topicid.Topicid); err == nil {
				sub.TopicRef = &pubsub_pb.RPC_SubOpts_TopicIndex{TopicIndex: uint32(idx)}
			}
		}
	}
	return rpc
}

// Note that topicIndex is 1-based
func (e *topicTableExtension) GetTopicName(id peer.ID, topicIndex int) (string, error) {
	if topicIndex < 1 {
		return "", fmt.Errorf("invalid topic index: %d", topicIndex)
	}

	// Turn the index to 0-based
	idx := topicIndex - 1

	for _, hash := range e.intersectedHashes[id] {
		if idx < len(e.indexToName[hash]) {
			return e.indexToName[hash][idx], nil
		} else {
			idx -= len(e.indexToName[hash])
		}
	}
	return "", fmt.Errorf("invalid topic index: %d", topicIndex)
}

// It returns a 1-based index
func (e *topicTableExtension) GetTopicIndex(id peer.ID, topicName string) (int, error) {
	topicIndex := 0

	for _, hash := range e.intersectedHashes[id] {
		if idx, ok := e.nameToIndex[hash][topicName]; ok {
			topicIndex += idx
			// Turn the index to 1-based
			topicIndex += 1
			return topicIndex, nil
		} else {
			topicIndex += len(e.nameToIndex[hash])
		}
	}
	return 0, fmt.Errorf("the topic not found: %s", topicName)
}

func validateBundles(bundles []TopicBundleHash) error {
	seen := make(map[TopicBundleHash]struct{}, len(bundles))
	for _, bundle := range bundles {
		if _, ok := seen[bundle]; ok {
			return fmt.Errorf("duplicates found")
		}
		seen[bundle] = struct{}{}
	}
	return nil
}

// Assume that the topics have been sorted
func computeTopicBundleHash(sortedTopics []string) TopicBundleHash {
	concatenated := strings.Join(sortedTopics, "")
	hash := sha256.Sum256([]byte(concatenated))

	var result TopicBundleHash
	copy(result[:], hash[len(hash)-4:])
	return result
}

func computeBundleIntersection(first, second []TopicBundleHash) []TopicBundleHash {
	var result []TopicBundleHash

	// Find common prefix where elements at each index are equal in both slices.
	for i := 0; i < min(len(first), len(second)) && bytes.Equal(first[i][:], second[i][:]); i++ {
		result = append(result, first[i])
	}

	// Store the length of the matching prefix. This is our marker.
	prefixLen := len(result)

	// Build a set of the remaining elements in the first slice after the prefix.
	// For each remaining element in the second slice, if it exists in the set,
	// add it to the result. (Duplicates possible if not validated up front.)
	seen := make(map[TopicBundleHash]struct{})
	for _, v := range first[prefixLen:] {
		seen[v] = struct{}{}
	}
	for _, v := range second[prefixLen:] {
		if _, ok := seen[v]; ok {
			result = append(result, v)
		}
	}

	// Sort the unordered tail lexicographically.
	unordered := result[prefixLen:]
	sort.Slice(unordered, func(i, j int) bool {
		return bytes.Compare(unordered[i][:], unordered[j][:]) < 0
	})

	return result
}
