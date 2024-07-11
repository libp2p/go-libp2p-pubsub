package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestGossipSubMatchingFn(t *testing.T) {
	customsubA100 := protocol.ID("/customsub_a/1.0.0")
	customsubA101Beta := protocol.ID("/customsub_a/1.0.1-beta")
	customsubB100 := protocol.ID("/customsub_b/1.0.0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := getDefaultHosts(t, 4)
	psubs := []*PubSub{
		getGossipsub(ctx, h[0], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubA100, GossipSubID_v11}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[1], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubA101Beta}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[2], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{GossipSubID_v11}, GossipSubDefaultFeatures)),
		getGossipsub(ctx, h[3], WithProtocolMatchFn(protocolNameMatch), WithGossipSubProtocols([]protocol.ID{customsubB100}, GossipSubDefaultFeatures)),
	}

	connect(t, h[0], h[1])
	connect(t, h[0], h[2])
	connect(t, h[0], h[3])

	// verify that the peers are connected
	time.Sleep(2 * time.Second)
	for i := 1; i < len(h); i++ {
		if len(h[0].Network().ConnsToPeer(h[i].ID())) == 0 {
			t.Fatal("expected a connection between peers")
		}
	}

	// build the mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	// publish a message
	msg := []byte("message")
	psubs[0].Publish("test", msg)

	assertReceive(t, subs[0], msg)
	assertReceive(t, subs[1], msg) // Should match via semver over CustomSub name, ignoring the version
	assertReceive(t, subs[2], msg) // Should match via GossipSubID_v11

	// No message should be received because customsubA and customsubB have different names
	ctxTimeout, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer timeoutCancel()
	received := false
	for {
		msg, err := subs[3].Next(ctxTimeout)
		if err != nil {
			break
		}
		if msg != nil {
			received = true
		}
	}
	if received {
		t.Fatal("Should not have received a message")
	}
}

func protocolNameMatch(base protocol.ID) func(protocol.ID) bool {
	return func(check protocol.ID) bool {
		baseName := strings.Split(string(base), "/")[1]
		checkName := strings.Split(string(check), "/")[1]
		return baseName == checkName
	}
}
