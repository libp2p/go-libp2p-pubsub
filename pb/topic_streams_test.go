package pubsub_pb

import (
	"testing"
)

func TestTopicRPCPayloadOneof(t *testing.T) {
	publish := &Message{Data: []byte("payload")}
	rpc := &TopicRPC{Payload: &TopicRPC_Publish{Publish: publish}}
	if rpc.GetPublish() != publish || rpc.GetPartial() != nil {
		t.Fatal("expected publish payload")
	}

	partial := &PartialMessagesExtension{PartialMessage: []byte("partial")}
	rpc.Payload = &TopicRPC_Partial{Partial: partial}
	if rpc.GetPartial() != partial || rpc.GetPublish() != nil {
		t.Fatal("expected partial payload")
	}
}
