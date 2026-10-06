package pubsub

import (
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/proto"
)

// The SPREAD marker names individual publish entries, so an RPC carrying both
// SPREAD and non-SPREAD messages must mark only the ones it named.
func TestSpreadIndexSet(t *testing.T) {
	cases := []struct {
		name         string
		indices      []uint32
		publishCount int
		want         []int
	}{
		{"nil ext", nil, 3, nil},
		{"empty", []uint32{}, 3, nil},
		{"subset", []uint32{0, 2}, 3, []int{0, 2}},
		{"out of range dropped", []uint32{1, 7}, 3, []int{1}},
		{"all out of range", []uint32{7, 8}, 3, nil},
		{"duplicates collapse", []uint32{1, 1}, 3, []int{1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ext *pb.SpreadExtension
			if tc.indices != nil {
				ext = &pb.SpreadExtension{PublishIndices: tc.indices}
			}
			got := spreadIndexSet(ext, tc.publishCount)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d indices, want %d: %v", len(got), len(tc.want), got)
			}
			for _, i := range tc.want {
				if _, ok := got[i]; !ok {
					t.Fatalf("index %d missing from %v", i, got)
				}
			}
		})
	}
}

func TestSpreadForPublishRange(t *testing.T) {
	ext := &pb.SpreadExtension{PublishIndices: []uint32{0, 3, 4}}

	// A piece covering the first two messages keeps only index 0.
	if got := spreadForPublishRange(ext, 0, 2); got == nil ||
		len(got.GetPublishIndices()) != 1 || got.GetPublishIndices()[0] != 0 {
		t.Fatalf("first piece: got %v", got)
	}

	// A piece covering messages 3..4 rebases them to 0..1.
	got := spreadForPublishRange(ext, 3, 2)
	if got == nil || len(got.GetPublishIndices()) != 2 ||
		got.GetPublishIndices()[0] != 0 || got.GetPublishIndices()[1] != 1 {
		t.Fatalf("second piece: got %v", got)
	}

	// A piece with no marked message carries no marker at all.
	if got := spreadForPublishRange(ext, 1, 2); got != nil {
		t.Fatalf("unmarked piece: got %v", got)
	}
	if got := spreadForPublishRange(nil, 0, 2); got != nil {
		t.Fatalf("nil ext: got %v", got)
	}
}

// An oversized SPREAD RPC used to lose its marker when split, so the receiving
// peer disseminated the message as plain GossipSub with no error anywhere.
func TestRPCSplitPreservesSpreadMarker(t *testing.T) {
	const msgCount = 6
	rpc := &RPC{}
	for i := 0; i < msgCount; i++ {
		rpc.Publish = append(rpc.Publish, &pb.Message{
			Data:  make([]byte, 200),
			Topic: proto.String("test"),
		})
	}
	// Mark every message except the second one.
	for i := 0; i < msgCount; i++ {
		if i == 1 {
			continue
		}
		if rpc.Spread == nil {
			rpc.Spread = &pb.SpreadExtension{}
		}
		rpc.Spread.PublishIndices = append(rpc.Spread.PublishIndices, uint32(i))
	}

	// A limit well below the whole RPC forces several pieces.
	limit := proto.Size(&rpc.RPC) / 3

	seen := map[int]bool{}
	pieces := 0
	global := 0
	for piece := range rpc.split(limit, limit) {
		pieces++
		marked := spreadIndexSet(piece.Spread, len(piece.Publish))
		for i := range piece.Publish {
			if _, ok := marked[i]; ok {
				seen[global] = true
			}
			global++
		}
	}

	if pieces < 2 {
		t.Fatalf("expected the RPC to split, got %d piece(s)", pieces)
	}
	if global != msgCount {
		t.Fatalf("pieces carried %d messages, want %d", global, msgCount)
	}
	for i := 0; i < msgCount; i++ {
		want := i != 1
		if seen[i] != want {
			t.Errorf("message %d: marked=%v, want %v", i, seen[i], want)
		}
	}
}

// A piece that carries no publish entries must not carry a marker that indexes
// into a publish list it does not have.
func TestRPCSplitDropsSpreadMarkerOnControlOnlyPiece(t *testing.T) {
	rpc := &RPC{}
	rpc.Publish = []*pb.Message{{Data: make([]byte, 200), Topic: proto.String("test")}}
	rpc.Spread = &pb.SpreadExtension{PublishIndices: []uint32{0}}
	rpc.Control = &pb.ControlMessage{
		Graft: []*pb.ControlGraft{{TopicID: proto.String("test")}},
	}

	limit := proto.Size(&rpc.RPC) / 2
	for piece := range rpc.split(limit, limit) {
		if len(piece.Publish) == 0 && piece.Spread != nil {
			t.Fatalf("control-only piece carries a SPREAD marker: %v", piece.Spread)
		}
	}

	// split() must leave the RPC it was given untouched.
	if rpc.Spread == nil || len(rpc.Spread.GetPublishIndices()) != 1 {
		t.Fatalf("split mutated the original RPC's marker: %v", rpc.Spread)
	}
}
