package pubsub_pb

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestValidateRawRPCControlMessageSizeRejectsOversizedControlField(t *testing.T) {
	topic := strings.Repeat("x", 32)
	msg := marshalTestMessage(t, &RPC{
		Control: &ControlMessage{
			Graft: []*ControlGraft{{TopicID: proto.String(topic)}},
		},
	})

	if err := ValidateRawRPCControlMessageSize(msg, 8); err == nil {
		t.Fatal("expected oversized control field error")
	}
}

func TestValidateRawRPCControlMessageSizeRejectsCumulativeControlFields(t *testing.T) {
	topic := strings.Repeat("x", 32)
	control := marshalTestMessage(t, &ControlMessage{
		Graft: []*ControlGraft{{TopicID: proto.String(topic)}},
	})
	msg := appendRawRPCControlField(nil, control)
	msg = appendRawRPCControlField(msg, control)

	if err := ValidateRawRPCControlMessageSize(msg, encodedRawRPCFieldSize(3, control)+1); err == nil {
		t.Fatal("expected cumulative oversized control field error")
	}
}

func TestValidateRawRPCControlMessageSizeRejectsCumulativeNonExemptFields(t *testing.T) {
	topic := strings.Repeat("x", 32)
	subscription := marshalTestMessage(t, &RPC_SubOpts{Topicid: proto.String(topic)})
	control := marshalTestMessage(t, &ControlMessage{
		Graft: []*ControlGraft{{TopicID: proto.String(topic)}},
	})
	msg := appendRawRPCSubscriptionField(nil, subscription)
	msg = appendRawRPCControlField(msg, control)

	limit := encodedRawRPCFieldSize(1, subscription) + encodedRawRPCFieldSize(3, control) - 1
	if err := ValidateRawRPCControlMessageSize(msg, limit); err == nil {
		t.Fatal("expected cumulative oversized non-exempt field error")
	}
}

func TestValidateRawRPCControlMessageSizeCountsFieldFraming(t *testing.T) {
	var msg []byte
	for range 3 {
		msg = appendRawRPCSubscriptionField(msg, nil)
	}

	if err := ValidateRawRPCControlMessageSize(msg, 5); err == nil {
		t.Fatal("expected field framing to exceed control size limit")
	}
}

func TestValidateRawRPCControlMessageSizeCountsUnknownTopLevelFields(t *testing.T) {
	msg := protowire.AppendTag(nil, 100, protowire.VarintType)
	msg = protowire.AppendVarint(msg, 1234)
	msg = protowire.AppendTag(msg, 101, protowire.BytesType)
	msg = protowire.AppendBytes(msg, []byte("not a protobuf message"))

	if err := ValidateRawRPCControlMessageSize(msg, len(msg)-1); err == nil {
		t.Fatal("expected unknown top-level fields to exceed control size limit")
	}
}

func TestValidateRawRPCControlMessageSizeAllowsOversizedPublishAndPartialFields(t *testing.T) {
	topic := strings.Repeat("x", 32)

	msg := marshalTestMessage(t, &RPC{
		Publish: []*Message{{Topic: proto.String(topic), Data: []byte(strings.Repeat("x", 32))}},
		Partial: &PartialMessagesExtension{
			TopicID:        proto.String(topic),
			PartialMessage: []byte(strings.Repeat("x", 32)),
		},
	})

	if err := ValidateRawRPCControlMessageSize(msg, 8); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRawRPCControlMessageSizeAllowsControlFieldWithinLimit(t *testing.T) {
	topic := strings.Repeat("x", 32)
	msg := marshalTestMessage(t, &RPC{
		Control: &ControlMessage{
			Graft: []*ControlGraft{{TopicID: proto.String(topic)}},
		},
	})

	if err := ValidateRawRPCControlMessageSize(msg, 128); err != nil {
		t.Fatal(err)
	}
}

func marshalTestMessage(t *testing.T, msg proto.Message) []byte {
	t.Helper()

	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func appendRawRPCControlField(msg []byte, control []byte) []byte {
	msg = protowire.AppendTag(msg, 3, protowire.BytesType)
	return protowire.AppendBytes(msg, control)
}

func appendRawRPCSubscriptionField(msg []byte, subscription []byte) []byte {
	msg = protowire.AppendTag(msg, 1, protowire.BytesType)
	return protowire.AppendBytes(msg, subscription)
}

func encodedRawRPCFieldSize(field protowire.Number, msg []byte) int {
	return protowire.SizeTag(field) + protowire.SizeBytes(len(msg))
}
