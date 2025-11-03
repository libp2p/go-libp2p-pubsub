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

func (e *topicTableExtension) AddPeer(id peer.ID, bundles []TopicBundleHash) error {
	if err := validateBundles(bundles); err != nil {
		return err
	}
	e.intersectedHashes[id] = computeBundleIntersection(e.bundleHashes, bundles)
	return nil
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
