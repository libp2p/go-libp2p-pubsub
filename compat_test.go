package pubsub

import (
	"testing"

	compat_pb "github.com/libp2p/go-libp2p-pubsub/compat"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/proto"
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

	newMessage1b, err := proto.Marshal(newMessage1)
	if err != nil {
		t.Fatal(err)
	}
	oldMessage1b, err := proto.Marshal(oldMessage1)
	if err != nil {
		t.Fatal(err)
	}
	oldMessage2b, err := proto.Marshal(oldMessage2)
	if err != nil {
		t.Fatal(err)
	}

	newMessage := new(pb.Message)
	oldMessage := new(compat_pb.Message)

	err = proto.Unmarshal(oldMessage1b, newMessage)
	if err != nil {
		t.Fatal(err)
	}
	if newMessage.GetTopic() != topic1 {
		t.Fatalf("bad topic: expected %s, got %s", topic1, newMessage.GetTopic())
	}

	newMessage.Reset()
	err = proto.Unmarshal(oldMessage2b, newMessage)
	if err != nil {
		t.Fatal(err)
	}
	if newMessage.GetTopic() != topic2 {
		t.Fatalf("bad topic: expected %s, got %s", topic2, newMessage.GetTopic())
	}

	err = proto.Unmarshal(newMessage1b, oldMessage)
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
