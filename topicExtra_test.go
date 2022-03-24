package pubsub

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	tnet "github.com/libp2p/go-libp2p-testing/net"
)

func TestTopic_PublishWithSk(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const topic = "foobar"
	const numHosts = 5

	virtualPeer := tnet.RandPeerNetParamsOrFatal(t)
	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	t.Run("nil sign private key should error", func(t *testing.T) {
		err := topics[0].PublishWithSk(ctx, []byte("buff"), nil, virtualPeer.ID)
		if err != errNilSignKey {
			t.Fatal("error should have been of type errNilSignKey")
		}
	})
	t.Run("empty peer ID should error", func(t *testing.T) {
		err := topics[0].PublishWithSk(ctx, []byte("buff"), virtualPeer.PrivKey, "")
		if err != errEmptyPeerID {
			t.Fatal("error should have been of type errEmptyPeerID")
		}
	})
}

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

	for i, topicValue := range topics {
		if i == 2 || i == 4 {
			sub, err := topicValue.Subscribe()
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, sub)
		} else {
			_, err := topicValue.Relay()
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
			received, errSub := sub.Next(ctx)
			if errSub != nil {
				t.Fatal(errSub)
			}

			if !bytes.Equal(msg, received.Data) {
				t.Fatal("received message is other than expected")
			}
		}
	}
}
