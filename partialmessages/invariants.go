package partialmessages

import (
	"bytes"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

// InvariantChecker is a test tool to test an implementation of PartialMessage
// upholds its invariants. Use this in your application's tests to validate your
// PartialMessage implementation.
type InvariantChecker[P Message] interface {
	// SplitIntoParts returns a list of partial messages where there are no
	// overlaps between messages, and the sum of all messages is the original
	// partial message.
	SplitIntoParts(in P) ([]P, error)

	// FullMessage should return a complete partial message.
	FullMessage() (P, error)

	// EmptyMessage should return a empty partial message.
	EmptyMessage() P

	// ExtendFromBytes extends a from data and returns the extended partial
	// message or an error. An implementation may mutate a and return a.
	ExtendFromBytes(a P, data []byte) (P, error)

	// ShouldRequest should return true if peer has something we're interested
	// in (as determined from the parts metadata)
	ShouldRequest(a P, from peer.ID, partsMetadata []byte) bool

	MergePartsMetadata(left, right PartsMetadata) PartsMetadata

	Equal(a, b P) bool
}

func TestPartialMessageInvariants[P Message](t *testing.T, checker InvariantChecker[P]) {
	extend := func(a, b P) (P, error) {
		emptyParts := checker.EmptyMessage().PartsMetadata()
		encodedB, err := b.PartialMessageBytes(emptyParts)
		if err != nil {
			var out P
			return out, err
		}
		return checker.ExtendFromBytes(a, encodedB)
	}

	t.Run("A empty message should not return a nil slice for parts metadata (should encode a request)", func(t *testing.T) {
		empty := checker.EmptyMessage()
		b := empty.PartsMetadata()
		if b == nil {
			t.Errorf("did not expect empty slice")
		}
	})
	t.Run("Splitting a full message, and then recombining it yields the original message", func(t *testing.T) {
		fullMessage, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		parts, err := checker.SplitIntoParts(fullMessage)
		if err != nil {
			t.Fatal(err)
		}

		recombined := checker.EmptyMessage()
		for _, part := range parts {
			b, err := part.PartialMessageBytes(recombined.PartsMetadata())
			if err != nil {
				t.Fatal(err)
			}
			recombined, err = checker.ExtendFromBytes(recombined, b)
			if err != nil {
				t.Fatal(err)
			}
		}

		if !checker.Equal(fullMessage, recombined) {
			t.Errorf("Expected %v, got %v", fullMessage, recombined)
		}
	})

	t.Run("Empty message requesting parts it doesn't have returns nil response", func(t *testing.T) {
		emptyMessage := checker.EmptyMessage()

		// Get metadata representing all parts from the empty message
		emptyMsgPartsMeta := emptyMessage.PartsMetadata()

		// Empty message should not be able to fulfill any request
		response, err := emptyMessage.PartialMessageBytes(emptyMsgPartsMeta)
		if err != nil {
			t.Fatal(err)
		}
		rest := checker.MergePartsMetadata(emptyMsgPartsMeta, emptyMessage.PartsMetadata())

		if len(response) != 0 {
			t.Error("Empty message should return nil response when requesting parts it doesn't have")
		}

		// The rest should be the same as the original request since nothing was fulfilled
		if len(rest) == 0 && len(emptyMsgPartsMeta) > 0 {
			t.Error("Empty message should return the full request as 'rest' when it cannot fulfill anything")
		}
	})

	t.Run("Partial fulfillment returns correct rest and can be completed by another message", func(t *testing.T) {
		fullMessage, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		parts, err := checker.SplitIntoParts(fullMessage)
		if err != nil {
			t.Fatal(err)
		}

		// Skip this test if we can't split into at least 2 parts
		if len(parts) < 2 {
			t.Skip("Cannot test partial fulfillment with less than 2 parts")
		}

		// Get metadata representing all parts needed
		emptyMessage := checker.EmptyMessage()
		emptyMsgPartsMeta := emptyMessage.PartsMetadata()

		// Request all parts from the partial message
		response1, err := parts[0].PartialMessageBytes(emptyMsgPartsMeta)
		if err != nil {
			t.Fatal(err)
		}
		rest1 := checker.MergePartsMetadata(emptyMsgPartsMeta, parts[0].PartsMetadata())

		// Should get some response since partial message has at least one part
		if len(response1) == 0 {
			t.Error("Partial message should return some data when it has parts to fulfill")
		}

		// Rest should be non-zero and different from original request since something was fulfilled
		if len(rest1) == 0 {
			t.Fatal("Rest should be non-zero when partial fulfillment occurred")
		}
		if bytes.Equal(rest1, emptyMsgPartsMeta) {
			t.Fatalf("Rest should be different from original request since partial fulfillment occurred")
		}

		// Create another partial message with the remaining parts
		remainingPartial := checker.EmptyMessage()
		for i := 1; i < len(parts); i++ {
			remainingPartial, err = extend(remainingPartial, parts[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		// The remaining partial message should be able to fulfill the "rest" request
		response2, err := remainingPartial.PartialMessageBytes(rest1)
		if err != nil {
			t.Fatal(err)
		}
		rest2 := checker.MergePartsMetadata(rest1, remainingPartial.PartsMetadata())

		// response2 should be non-empty since we have remaining parts to fulfill
		if len(response2) == 0 {
			t.Error("Response2 should be non-empty when fulfilling remaining parts")
		}

		// After fulfilling the rest, the metadata should be the same as full
		if !bytes.Equal(rest2, fullMessage.PartsMetadata()) {
			t.Errorf("After fulfilling all parts, the parts metadata should be the same as the full message, saw %v", rest2)
		}

		// Combine both responses and verify we can reconstruct the full message
		reconstructed := checker.EmptyMessage()
		reconstructed, err = checker.ExtendFromBytes(reconstructed, response1)
		if err != nil {
			t.Fatal(err)
		}
		if len(response2) > 0 {
			reconstructed, err = checker.ExtendFromBytes(reconstructed, response2)
			if err != nil {
				t.Fatal(err)
			}
		}

		// The reconstructed message should be equivalent to the full message
		if !checker.Equal(fullMessage, reconstructed) {
			t.Errorf("Reconstructed message from partial responses should equal full message")
		}
	})

	t.Run("PartialMessageBytesFromMetadata with empty metadata requests all parts", func(t *testing.T) {
		fullMessage, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		// Request with empty metadata should return all available parts
		emptyMeta := checker.EmptyMessage().PartsMetadata()
		response, err := fullMessage.PartialMessageBytes(emptyMeta)
		rest := checker.MergePartsMetadata(emptyMeta, fullMessage.PartsMetadata())
		if err != nil {
			t.Fatal(err)
		}

		// Should get some response from a full message
		if len(response) == 0 {
			t.Error("Full message should return data when requested with empty metadata")
		}

		// Should have no remaining parts since full message can fulfill everything
		if !bytes.Equal(rest, fullMessage.PartsMetadata()) {
			t.Error("Full message should have no remaining parts when fulfilling empty metadata request")
		}
	})

	t.Run("Available parts, missing parts, and partial message bytes consistency", func(t *testing.T) {
		fullMessage, err := checker.FullMessage()
		if err != nil {
			t.Fatal(err)
		}

		// Get the available parts
		fullMsgPartsMeta := fullMessage.PartsMetadata()

		// Assert available parts is non-zero length
		if len(fullMsgPartsMeta) == 0 {
			t.Error("Full message should have non-zero available parts")
		}

		// Split the full message into parts
		parts, err := checker.SplitIntoParts(fullMessage)
		if err != nil {
			t.Fatal(err)
		}

		var partialMessageResponses [][]byte

		// Test each part and empty message
		testMessages := make([]P, len(parts)+1)
		copy(testMessages, parts)
		testMessages[len(parts)] = checker.EmptyMessage()

		for i, testMsg := range testMessages {
			// Assert that ShouldRequest returns true for the available parts
			if !checker.ShouldRequest(testMsg, "", fullMsgPartsMeta) {
				t.Errorf("Message %d should request the available parts", i)
			}

			// Get the MissingParts() and have the full message fulfill the request
			msgPartsMeta := testMsg.PartsMetadata()

			response, err := fullMessage.PartialMessageBytes(msgPartsMeta)
			rest := checker.MergePartsMetadata(msgPartsMeta, fullMessage.PartsMetadata())
			if err != nil {
				t.Fatal(err)
			}

			// Assert that the rest is nil
			if !bytes.Equal(rest, fullMessage.PartsMetadata()) {
				t.Errorf("rest should be equal to fullMessage.PartsMetadata() for message %d", i)
			}

			// Store each partial message bytes
			if len(response) > 0 {
				partialMessageResponses = append(partialMessageResponses, response)
			}

			// Call ExtendFromEncodedPartialMessage
			testMsg, err = checker.ExtendFromBytes(testMsg, response)
			if err != nil {
				t.Fatal(err)
			}

			// Assert the extended form is now equal to the full message
			if !checker.Equal(fullMessage, testMsg) {
				t.Errorf("Extended message %d should equal full message", i)
			}
		}

		// Assert that none of the partial message bytes are equal to each other.
		for i := range partialMessageResponses {
			for j := i + 1; j < len(partialMessageResponses); j++ {
				if bytes.Equal(partialMessageResponses[i], partialMessageResponses[j]) {
					t.Errorf("Partial message bytes %d and %d should not be equal", i, j)
				}
			}
		}
	})
}
