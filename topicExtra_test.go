package pubsub

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	tnet "github.com/libp2p/go-libp2p-testing/net"
)

func TestTopicRelay_PublishWithSk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const topic = "foobar"
	const numHosts = 5

	virtualPeer := tnet.RandPeerNetParamsOrFatal(t)
	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	// [0.Rel] - [1.Rel] - [2.Sub]
	//             |
	//           [3.Rel] - [4.Sub]

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[3])
	connect(t, hosts[3], hosts[4])

	time.Sleep(time.Millisecond * 100)

	var subs []*Subscription

	for i, topic := range topics {
		if i == 2 || i == 4 {
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, sub)
		} else {
			_, err := topic.Relay()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 100; i++ {
		msg := []byte("message")

		owner := rand.Intn(len(topics))

		err := topics[owner].PublishWithSk(ctx, msg, virtualPeer.PrivKey, virtualPeer.ID)
		if err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs {
			received, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(msg, received.Data) {
				t.Fatal("received message is other than expected")
			}
		}
	}
}
