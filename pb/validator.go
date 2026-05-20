package pubsub_pb

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	rpcPublishFieldNumber protowire.Number = 2
	rpcPartialFieldNumber protowire.Number = 10
)

// ValidateRawRPCControlMessageSize validates that all non-publish,
// non-PartialMessagesExtension fields in a raw RPC are no larger than
// maxControlMessageSize.
func ValidateRawRPCControlMessageSize(msg []byte, maxControlMessageSize int) error {
	controlSize := 0
	for len(msg) > 0 {
		field, typ, tagLen := protowire.ConsumeTag(msg)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}

		fieldValue := msg[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(field, typ, fieldValue)
		if fieldLen < 0 {
			return protowire.ParseError(fieldLen)
		}

		switch field {
		case rpcPublishFieldNumber, rpcPartialFieldNumber:
		default:
			controlSize += tagLen + fieldLen
			if controlSize > maxControlMessageSize {
				return fmt.Errorf("rpc control size exceeds max control message size: %d > %d", controlSize, maxControlMessageSize)
			}
		}

		msg = fieldValue[fieldLen:]
	}
	return nil
}
