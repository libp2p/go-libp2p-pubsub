package pubsub

import (
	"testing"

	compat_pb "github.com/libp2p/go-libp2p-pubsub/compat"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestMultitopicMessageCompatibility(t *testing.T) {
	topic1 := "topic1"
	topic2 := "topic2"

	newMessage1 := &pb.Message{
		From:      []byte("A"),
		Data:      []byte("blah"),
		Seqno:     []byte("123"),
		Topic:     &topic1,
		Signature: []byte("a-signature"),
		Key:       []byte("a-key"),
	}
	oldMessage1 := &compat_pb.Message{
		From:      []byte("A"),
		Data:      []byte("blah"),
		Seqno:     []byte("123"),
		TopicIDs:  []string{topic1},
		Signature: []byte("a-signature"),
		Key:       []byte("a-key"),
	}
	oldMessage2 := &compat_pb.Message{
		From:      []byte("A"),
		Data:      []byte("blah"),
		Seqno:     []byte("123"),
		TopicIDs:  []string{topic1, topic2},
		Signature: []byte("a-signature"),
		Key:       []byte("a-key"),
	}

	newMessage1b, err := newMessage1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	oldMessage1b, err := oldMessage1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	oldMessage2b, err := oldMessage2.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	newMessage := new(pb.Message)
	oldMessage := new(compat_pb.Message)

	err = newMessage.Unmarshal(oldMessage1b)
	if err != nil {
		t.Fatal(err)
	}
	if newMessage.GetTopic() != topic1 {
		t.Fatalf("bad topic: expected %s, got %s", topic1, newMessage.GetTopic())
	}

	newMessage.Reset()
	err = newMessage.Unmarshal(oldMessage2b)
	if err != nil {
		t.Fatal(err)
	}
	if newMessage.GetTopic() != topic2 {
		t.Fatalf("bad topic: expected %s, got %s", topic2, newMessage.GetTopic())
	}

	err = oldMessage.Unmarshal(newMessage1b)
	if err != nil {
		t.Fatal(err)
	}
	topics := oldMessage.GetTopicIDs()
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0] != topic1 {
		t.Fatalf("bad topic: expected %s, got %s", topic1, topics[0])
	}
}
