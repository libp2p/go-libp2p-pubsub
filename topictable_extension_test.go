package pubsub

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestGetTopicName(t *testing.T) {
	// Create a topic table extension with some bundles
	myBundles := [][]string{
		{"topic-c", "topic-a", "topic-b"}, // Will be sorted to: topic-a, topic-b, topic-c
		{"topic-z", "topic-x", "topic-y"}, // Will be sorted to: topic-x, topic-y, topic-z
	}

	ext, err := newTopicTableExtension(myBundles)
	if err != nil {
		t.Fatalf("failed to create topic table extension: %v", err)
	}

	// Create peer bundles that intersect with our bundles
	peerID := peer.ID("test-peer")
	peerBundles := []TopicBundleHash{
		computeTopicBundleHash([]string{"topic-a", "topic-b", "topic-c"}),
		computeTopicBundleHash([]string{"topic-x", "topic-y", "topic-z"}),
	}

	err = ext.AddPeer(peerID, peerBundles)
	if err != nil {
		t.Fatalf("failed to add peer: %v", err)
	}

	tests := []struct {
		name       string
		topicIndex int
		want       string
		wantErr    bool
	}{
		{
			name:       "first topic in first bundle",
			topicIndex: 1,
			want:       "topic-a",
			wantErr:    false,
		},
		{
			name:       "second topic in first bundle",
			topicIndex: 2,
			want:       "topic-b",
			wantErr:    false,
		},
		{
			name:       "third topic in first bundle",
			topicIndex: 3,
			want:       "topic-c",
			wantErr:    false,
		},
		{
			name:       "first topic in second bundle",
			topicIndex: 4,
			want:       "topic-x",
			wantErr:    false,
		},
		{
			name:       "second topic in second bundle",
			topicIndex: 5,
			want:       "topic-y",
			wantErr:    false,
		},
		{
			name:       "third topic in second bundle",
			topicIndex: 6,
			want:       "topic-z",
			wantErr:    false,
		},
		{
			name:       "invalid index: zero",
			topicIndex: 0,
			want:       "",
			wantErr:    true,
		},
		{
			name:       "invalid index: negative",
			topicIndex: -1,
			want:       "",
			wantErr:    true,
		},
		{
			name:       "invalid index: out of bounds",
			topicIndex: 7,
			want:       "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ext.GetTopicName(peerID, tt.topicIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTopicName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetTopicName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTopicIndex(t *testing.T) {
	// Create a topic table extension with some bundles
	myBundles := [][]string{
		{"topic-c", "topic-a", "topic-b"}, // Will be sorted to: topic-a, topic-b, topic-c
		{"topic-z", "topic-x", "topic-y"}, // Will be sorted to: topic-x, topic-y, topic-z
	}

	ext, err := newTopicTableExtension(myBundles)
	if err != nil {
		t.Fatalf("failed to create topic table extension: %v", err)
	}

	// Create peer bundles that intersect with our bundles
	peerID := peer.ID("test-peer")
	peerBundles := []TopicBundleHash{
		computeTopicBundleHash([]string{"topic-a", "topic-b", "topic-c"}),
		computeTopicBundleHash([]string{"topic-x", "topic-y", "topic-z"}),
	}

	err = ext.AddPeer(peerID, peerBundles)
	if err != nil {
		t.Fatalf("failed to add peer: %v", err)
	}

	tests := []struct {
		name      string
		topicName string
		want      int
		wantErr   bool
	}{
		{
			name:      "first topic in first bundle",
			topicName: "topic-a",
			want:      1,
			wantErr:   false,
		},
		{
			name:      "second topic in first bundle",
			topicName: "topic-b",
			want:      2,
			wantErr:   false,
		},
		{
			name:      "third topic in first bundle",
			topicName: "topic-c",
			want:      3,
			wantErr:   false,
		},
		{
			name:      "first topic in second bundle",
			topicName: "topic-x",
			want:      4,
			wantErr:   false,
		},
		{
			name:      "second topic in second bundle",
			topicName: "topic-y",
			want:      5,
			wantErr:   false,
		},
		{
			name:      "third topic in second bundle",
			topicName: "topic-z",
			want:      6,
			wantErr:   false,
		},
		{
			name:      "topic not found",
			topicName: "topic-nonexistent",
			want:      0,
			wantErr:   true,
		},
		{
			name:      "empty topic name",
			topicName: "",
			want:      0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ext.GetTopicIndex(peerID, tt.topicName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTopicIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetTopicIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTopicNameAndIndexRoundTrip(t *testing.T) {
	// Test that GetTopicName and GetTopicIndex are inverse operations
	myBundles := [][]string{
		{"alpha", "beta", "gamma"},
		{"delta", "epsilon", "zeta"},
		{"eta", "theta"},
	}

	ext, err := newTopicTableExtension(myBundles)
	if err != nil {
		t.Fatalf("failed to create topic table extension: %v", err)
	}

	peerID := peer.ID("test-peer")
	peerBundles := []TopicBundleHash{
		computeTopicBundleHash([]string{"alpha", "beta", "gamma"}),
		computeTopicBundleHash([]string{"delta", "epsilon", "zeta"}),
		computeTopicBundleHash([]string{"eta", "theta"}),
	}

	err = ext.AddPeer(peerID, peerBundles)
	if err != nil {
		t.Fatalf("failed to add peer: %v", err)
	}

	// Test round trip: index -> name -> index
	for i := 1; i <= 8; i++ {
		name, err := ext.GetTopicName(peerID, i)
		if err != nil {
			t.Fatalf("GetTopicName(%d) failed: %v", i, err)
		}

		index, err := ext.GetTopicIndex(peerID, name)
		if err != nil {
			t.Fatalf("GetTopicIndex(%s) failed: %v", name, err)
		}

		if index != i {
			t.Errorf("Round trip failed: started with index %d, got back %d (via name %s)", i, index, name)
		}
	}
}

func TestGetTopicWithPartialIntersection(t *testing.T) {
	// Test case where peer has only partial intersection with our bundles
	myBundles := [][]string{
		{"topic-a", "topic-b", "topic-c"},
		{"topic-x", "topic-y", "topic-z"},
		{"topic-1", "topic-2", "topic-3"},
	}

	ext, err := newTopicTableExtension(myBundles)
	if err != nil {
		t.Fatalf("failed to create topic table extension: %v", err)
	}

	peerID := peer.ID("test-peer")
	// Peer only has the second bundle in common
	peerBundles := []TopicBundleHash{
		computeTopicBundleHash([]string{"topic-x", "topic-y", "topic-z"}),
		computeTopicBundleHash([]string{"topic-k", "topic-l", "topic-m"}),
	}

	err = ext.AddPeer(peerID, peerBundles)
	if err != nil {
		t.Fatalf("failed to add peer: %v", err)
	}

	// Should be able to access topics in the intersected bundle
	name, err := ext.GetTopicName(peerID, 1)
	if err != nil {
		t.Fatalf("GetTopicName(1) failed: %v", err)
	}
	if name != "topic-x" {
		t.Errorf("GetTopicName(1) = %v, want topic-x", name)
	}

	// Should be able to get index for topic in intersected bundle
	index, err := ext.GetTopicIndex(peerID, "topic-y")
	if err != nil {
		t.Fatalf("GetTopicIndex(topic-y) failed: %v", err)
	}
	if index != 2 {
		t.Errorf("GetTopicIndex(topic-y) = %v, want 2", index)
	}

	// Should not be able to access topics outside intersection
	_, err = ext.GetTopicIndex(peerID, "topic-a")
	if err == nil {
		t.Error("GetTopicIndex(topic-a) should have failed for topic outside intersection")
	}
	_, err = ext.GetTopicIndex(peerID, "topic-k")
	if err == nil {
		t.Error("GetTopicIndex(topic-k) should have failed for topic outside intersection")
	}

	// Should not be able to access index beyond intersection
	_, err = ext.GetTopicName(peerID, 4)
	if err == nil {
		t.Error("GetTopicName(4) should have failed for index beyond intersection")
	}
}
