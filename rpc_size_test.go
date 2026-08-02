package pubsub

import (
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/proto"
)

// controlRPCSizeViaTempStruct is the straightforward implementation that
// allocates a temporary pb.RPC. It's kept here only as a reference oracle to
// verify that the allocation-free controlRPCSize produces identical results.
func controlRPCSizeViaTempStruct(rpc *RPC) int {
	if rpc == nil {
		return 0
	}
	return proto.Size(&pb.RPC{
		Subscriptions: rpc.Subscriptions,
		Control:       rpc.Control,
	})
}

func TestControlRPCSize(t *testing.T) {
	topic := "some-topic"
	subscribe := true
	unsubscribe := false

	// Typical gossipsub message IDs are from+seqno (roughly 40 bytes).
	msgIDs := func(n, size int) []string {
		ids := make([]string, n)
		b := make([]byte, size)
		for i := range ids {
			b[0] = byte(i)
			ids[i] = string(b)
		}
		return ids
	}

	typicalMsgIDs := msgIDs(10, 40)
	publishMsg := &pb.Message{
		Data:  make([]byte, 256),
		Topic: &topic,
	}

	cases := []struct {
		name string
		rpc  *RPC
	}{
		{
			name: "empty",
			rpc:  &RPC{},
		},
		{
			name: "subscription",
			rpc: &RPC{
				RPC: pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{
						{Subscribe: &subscribe, Topicid: &topic},
					},
				},
			},
		},
		{
			name: "unsubscribe",
			rpc: &RPC{
				RPC: pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{
						{Subscribe: &unsubscribe, Topicid: &topic},
					},
				},
			},
		},
		{
			name: "multipleSubscriptions",
			rpc: &RPC{
				RPC: pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{
						{Subscribe: &subscribe, Topicid: &topic},
						{Subscribe: &subscribe, Topicid: strPtr("other-topic")},
						{Subscribe: &unsubscribe, Topicid: strPtr("old-topic")},
					},
				},
			},
		},
		{
			name: "graftPrune",
			rpc: rpcWithControl(nil, nil, nil,
				[]*pb.ControlGraft{{TopicID: &topic}},
				[]*pb.ControlPrune{{TopicID: strPtr("other-topic")}},
				nil,
			),
		},
		{
			name: "ihaveSmall",
			rpc: rpcWithControl(nil,
				[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: typicalMsgIDs[:3]}},
				nil, nil, nil, nil,
			),
		},
		{
			name: "ihaveTypical",
			rpc: rpcWithControl(nil,
				[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: typicalMsgIDs}},
				nil, nil, nil, nil,
			),
		},
		{
			name: "iwantSmall",
			rpc: rpcWithControl(nil, nil,
				[]*pb.ControlIWant{{MessageIDs: typicalMsgIDs[:3]}},
				nil, nil, nil,
			),
		},
		{
			name: "ihaveIwantResponse",
			rpc: rpcWithControl(nil,
				[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: typicalMsgIDs[:5]}},
				[]*pb.ControlIWant{{MessageIDs: typicalMsgIDs[:2]}},
				nil, nil, nil,
			),
		},
		{
			name: "idontwant",
			rpc: rpcWithControl(nil, nil, nil, nil, nil,
				[]*pb.ControlIDontWant{{MessageIDs: typicalMsgIDs[:5]}},
			),
		},
		{
			name: "publishOnly",
			rpc:  rpcWithMessages(publishMsg),
		},
		{
			name: "publishWithControl",
			rpc: rpcWithControl(
				[]*pb.Message{publishMsg},
				[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: typicalMsgIDs[:3]}},
				nil, nil, nil, nil,
			),
		},
		{
			name: "heartbeatControl",
			rpc: rpcWithControl(nil,
				[]*pb.ControlIHave{
					{TopicID: &topic, MessageIDs: typicalMsgIDs},
					{TopicID: strPtr("other-topic"), MessageIDs: typicalMsgIDs[:5]},
				},
				[]*pb.ControlIWant{{MessageIDs: typicalMsgIDs[:3]}},
				[]*pb.ControlGraft{{TopicID: strPtr("new-topic")}},
				[]*pb.ControlPrune{{TopicID: strPtr("old-topic")}},
				nil,
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			size := controlRPCSize(tc.rpc)
			if want := controlRPCSizeViaTempStruct(tc.rpc); size != want {
				t.Fatalf("controlRPCSize=%d, want %d (temp-struct oracle)", size, want)
			}
			if allocs := testing.AllocsPerRun(100, func() {
				if got := controlRPCSize(tc.rpc); got != size {
					t.Fatalf("controlRPCSize changed from %d to %d", size, got)
				}
			}); allocs != 0 {
				t.Errorf("controlRPCSize allocated %v times per run, want 0", allocs)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}
