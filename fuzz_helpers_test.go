package pubsub

import (
	"encoding/binary"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func generateU16(data *[]byte) uint16 {
	if len(*data) < 2 {
		return 0
	}

	out := binary.LittleEndian.Uint16((*data)[:2])
	*data = (*data)[2:]
	return out
}

func generateBool(data *[]byte) bool {
	if len(*data) < 1 {
		return false
	}

	out := (*data)[0]&1 == 1
	*data = (*data)[1:]
	return out
}

func generateMessage(data []byte, limit int) *pb.Message {
	msgSize := int(generateU16(&data)) % limit
	return &pb.Message{Data: make([]byte, msgSize)}
}

func generateSub(data []byte, limit int) *pb.RPC_SubOpts {
	topicIDSize := int(generateU16(&data)) % limit
	subscribe := generateBool(&data)

	str := string(make([]byte, topicIDSize))
	return &pb.RPC_SubOpts{Subscribe: &subscribe, Topicid: &str}
}

func generateControl(data []byte, limit int) *pb.ControlMessage {
	numIWANTMsgs := int(generateU16(&data)) % (limit / 2)
	numIHAVEMsgs := int(generateU16(&data)) % (limit / 2)

	ctl := &pb.ControlMessage{}

	ctl.Iwant = make([]*pb.ControlIWant, 0, numIWANTMsgs)
	for i := 0; i < numIWANTMsgs; i++ {
		msgSize := int(generateU16(&data)) % limit
		msgCount := int(generateU16(&data)) % limit
		ctl.Iwant = append(ctl.Iwant, &pb.ControlIWant{})
		ctl.Iwant[i].MessageIDs = make([]string, 0, msgCount)
		for j := 0; j < msgCount; j++ {
			ctl.Iwant[i].MessageIDs = append(ctl.Iwant[i].MessageIDs, string(make([]byte, msgSize)))
		}
	}
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	ctl.Ihave = make([]*pb.ControlIHave, 0, numIHAVEMsgs)
	for i := 0; i < numIHAVEMsgs; i++ {
		msgSize := int(generateU16(&data)) % limit
		msgCount := int(generateU16(&data)) % limit
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Ihave = append(ctl.Ihave, &pb.ControlIHave{TopicID: &topic})

		ctl.Ihave[i].MessageIDs = make([]string, 0, msgCount)
		for j := 0; j < msgCount; j++ {
			ctl.Ihave[i].MessageIDs = append(ctl.Ihave[i].MessageIDs, string(make([]byte, msgSize)))
		}
	}
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	numGraft := int(generateU16(&data)) % limit
	ctl.Graft = make([]*pb.ControlGraft, 0, numGraft)
	for i := 0; i < numGraft; i++ {
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Graft = append(ctl.Graft, &pb.ControlGraft{TopicID: &topic})
	}
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	numPrune := int(generateU16(&data)) % limit
	ctl.Prune = make([]*pb.ControlPrune, 0, numPrune)
	for i := 0; i < numPrune; i++ {
		topicSize := int(generateU16(&data)) % limit
		topic := string(make([]byte, topicSize))
		ctl.Prune = append(ctl.Prune, &pb.ControlPrune{TopicID: &topic})
	}
	if ctl.Size() > limit {
		return &pb.ControlMessage{}
	}

	return ctl
}

func generateRPC(data []byte, limit int) *RPC {
	rpc := &RPC{RPC: pb.RPC{}}
	sizeTester := RPC{RPC: pb.RPC{}}

	msgCount := int(generateU16(&data)) % (limit / 2)
	rpc.Publish = make([]*pb.Message, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		msg := generateMessage(data, limit)

		sizeTester.Publish = []*pb.Message{msg}
		size := sizeTester.Size()
		sizeTester.Publish = nil
		if size > limit {
			continue
		}

		rpc.Publish = append(rpc.Publish, msg)
	}

	subCount := int(generateU16(&data)) % (limit / 2)
	rpc.Subscriptions = make([]*pb.RPC_SubOpts, 0, subCount)
	for i := 0; i < subCount; i++ {
		sub := generateSub(data, limit)

		sizeTester.Subscriptions = []*pb.RPC_SubOpts{sub}
		size := sizeTester.Size()
		sizeTester.Subscriptions = nil
		if size > limit {
			continue
		}

		rpc.Subscriptions = append(rpc.Subscriptions, sub)
	}

	ctl := generateControl(data, limit)

	sizeTester.Control = ctl
	size := sizeTester.Size()
	sizeTester.Control = nil
	if size <= limit {
		rpc.Control = ctl

	}

	return rpc
}
