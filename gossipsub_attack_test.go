package pubsub

import (
	"context"
	"math/rand"
	"testing"
	"time"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Test that when Gossipsub receives too many IWANT messages from a peer
// for the same message ID, it cuts off the peer
func TestGossipsubAttackSpamIWANT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// Used to publish a message with random data
	publishMsg := func() {
		data := make([]byte, 16)
		rand.Read(data)

		if err = ps.Publish(mytopic, data); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for 200ms after the last message before checking we got the
	// right number of messages
	msgWaitMax := 200 * time.Millisecond
	msgCount := 0
	msgTimer := time.NewTimer(msgWaitMax)

	// Checks we received the right number of messages
	checkMsgCount := func() {
		// After the original message from the legit host, we keep sending
		// IWANT until it stops replying. So the number of messages is
		// <original message> + GossipSubGossipRetransmission
		exp := 1 + GossipSubGossipRetransmission
		if msgCount != exp {
			t.Fatalf("Expected %d messages, got %d", exp, msgCount)
		}
	}

	// Wait for the timer to expire
	go func() {
		select {
		case <-msgTimer.C:
			checkMsgCount()
			cancel()
			return
		case <-ctx.Done():
			checkMsgCount()
		}
	}()

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				subs := []*pb.RPC_SubOpts{
					&pb.RPC_SubOpts{Subscribe: sub.Subscribe, Topicid: sub.Topicid},
				}
				graft := []*pb.ControlGraft{
					&pb.ControlGraft{TopicID: sub.Topicid},
				}

				writeMsg(&pb.RPC{
					Subscriptions: subs,
					Control:       &pb.ControlMessage{Graft: graft},
				})

				go func() {
					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(100 * time.Millisecond)

					// Publish a message from the legit host
					publishMsg()
				}()
			}
		}

		// Each time the legit host sends a message
		for _, msg := range irpc.GetPublish() {
			// Increment the number of messages and reset the timer
			msgCount++
			msgTimer.Reset(msgWaitMax)

			// Shouldn't get more than the expected number of messages
			exp := 1 + GossipSubGossipRetransmission
			if msgCount > exp {
				cancel()
				t.Fatal("Received too many responses")
			}

			// Send an IWANT with the message ID, causing the legit host
			// to send another message (until it cuts off the attacker for
			// being spammy)
			iwantlst := []string{DefaultMsgIdFn(msg)}
			iwant := []*pb.ControlIWant{&pb.ControlIWant{MessageIDs: iwantlst}}
			orpc := rpcWithControl(nil, nil, iwant, nil, nil)
			writeMsg(&orpc.RPC)
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// Test that when Gossipsub receives GRAFT for an unknown topic, it ignores
// the request
func TestGossipsubAttackGRAFTNonExistentTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// Checks that we haven't received any PRUNE message
	pruneCount := 0
	checkForPrune := func() {
		// We send a GRAFT for a non-existent topic so we shouldn't
		// receive a PRUNE in response
		if pruneCount != 0 {
			t.Fatalf("Got %d unexpected PRUNE messages", pruneCount)
		}
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				var subs []*pb.RPC_SubOpts
				var graft []*pb.ControlGraft
				subs = append(subs, &pb.RPC_SubOpts{Subscribe: sub.Subscribe, Topicid: sub.Topicid})
				graft = append(graft, &pb.ControlGraft{TopicID: sub.Topicid})

				// Graft to the peer on a non-existent topic
				nonExistentTopic := "non-existent"
				graft = append(graft, &pb.ControlGraft{TopicID: &nonExistentTopic})

				writeMsg(&pb.RPC{
					Control: &pb.ControlMessage{Graft: graft},
				})

				go func() {
					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(100 * time.Millisecond)

					// We shouldn't get any prune messages becaue the topic
					// doesn't exist
					checkForPrune()
					cancel()
				}()
			}
		}

		// Record the count of received PRUNE messages
		if ctl := irpc.GetControl(); ctl != nil {
			pruneCount += len(ctl.GetPrune())
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// Test that when Gossipsub receives GRAFT for a peer that has been PRUNED,
// it ignores the request until the backoff period has expired
func TestGossipsubAttackGRAFTDuringBackoff(t *testing.T) {
	originalGossipSubPruneBackoff := GossipSubPruneBackoff
	GossipSubPruneBackoff = 50 * time.Millisecond
	defer func() {
		GossipSubPruneBackoff = originalGossipSubPruneBackoff
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	pruneCount := 0
	checkPruneCount := func(exp int) {
		if pruneCount != exp {
			go cancel()
			t.Fatalf("Expected %d PRUNE messages but got %d", exp, pruneCount)
		}
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				var subs []*pb.RPC_SubOpts
				var graft []*pb.ControlGraft
				subs = append(subs, &pb.RPC_SubOpts{Subscribe: sub.Subscribe, Topicid: sub.Topicid})
				graft = append(graft, &pb.ControlGraft{TopicID: sub.Topicid})

				writeMsg(&pb.RPC{
					Subscriptions: subs,
					Control:       &pb.ControlMessage{Graft: graft},
				})

				go func() {
					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(20 * time.Millisecond)

					// No PRUNE should have been sent at this stage
					expectedPruneCount := 0
					checkPruneCount(expectedPruneCount)

					// Send a PRUNE to remove the attacker node from the legit
					// host's mesh
					var prune []*pb.ControlPrune
					prune = append(prune, &pb.ControlPrune{TopicID: sub.Topicid})
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Prune: prune},
					})

					time.Sleep(20 * time.Millisecond)

					// No PRUNE should have been sent at this stage
					checkPruneCount(expectedPruneCount)

					// Send a GRAFT to attempt to rejoin the mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					// It's been less than the backoff time since the last
					// PRUNE, so expect to get a PRUNE in response to the GRAFT
					expectedPruneCount++
					checkPruneCount(expectedPruneCount)

					// Wait until after the prune backoff period
					time.Sleep(GossipSubPruneBackoff * 2)

					// Send a GRAFT again to attempt to rejoin the mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					// The prune backoff period has passed so the GRAFT should
					// be accepted and this node should not receive a PRUNE
					checkPruneCount(expectedPruneCount)

					cancel()
				}()
			}
		}

		if ctl := irpc.GetControl(); ctl != nil {
			pruneCount += len(ctl.GetPrune())
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

func turnOnPubsubDebug() {
	logging.SetLogLevel("pubsub", "debug")
}

type mockGSOnRead func(writeMsg func(*pb.RPC), irpc *pb.RPC)

func newMockGS(ctx context.Context, t *testing.T, attacker host.Host, onReadMsg mockGSOnRead) {
	// Listen on the gossipsub protocol
	const gossipSubID = protocol.ID("/meshsub/1.0.0")
	const maxMessageSize = 1024 * 1024
	attacker.SetStreamHandler(gossipSubID, func(stream network.Stream) {
		// When an incoming stream is opened, set up an outgoing stream
		p := stream.Conn().RemotePeer()
		ostream, err := attacker.NewStream(ctx, p, gossipSubID)
		if err != nil {
			t.Fatal(err)
		}

		r := ggio.NewDelimitedReader(stream, maxMessageSize)
		w := ggio.NewDelimitedWriter(ostream)

		var irpc pb.RPC

		writeMsg := func(rpc *pb.RPC) {
			if err = w.WriteMsg(rpc); err != nil {
				t.Fatalf("error writing RPC: %s", err)
			}
		}

		// Keep reading messages and responding
		for {
			// Bail out when the test finishes
			if ctx.Err() != nil {
				return
			}

			irpc.Reset()

			err := r.ReadMsg(&irpc)

			// Bail out when the test finishes
			if ctx.Err() != nil {
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			onReadMsg(writeMsg, &irpc)
		}
	})
}
