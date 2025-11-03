package pubsub

import (
	"crypto/sha256"
	"sort"
	"strings"
)

type topicTableExtension struct {
}

type TopicBundleHash [4]byte

func computeTopicBundleHash(topics []string) TopicBundleHash {
	sortedTopics := make([]string, len(topics))
	copy(sortedTopics, topics)

	sort.Strings(sortedTopics)

	concatenated := strings.Join(sortedTopics, "")

	hash := sha256.Sum256([]byte(concatenated))

	var result TopicBundleHash
	copy(result[:], hash[len(hash)-4:])
	return result
}
