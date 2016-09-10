package floodsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	host "gx/ipfs/Qmf4ETeAWXuThBfWwonVyFqGFSgTWepUDEr1txcctvpTXS/go-libp2p/p2p/host"
	netutil "gx/ipfs/Qmf4ETeAWXuThBfWwonVyFqGFSgTWepUDEr1txcctvpTXS/go-libp2p/p2p/test/util"
)

func getNetHosts(t *testing.T, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h := netutil.GenHostSwarm(t, context.Background())
		out = append(out, h)
	}

	return out
}

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func connectAll(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(t, a, b)
		}
	}
}

func TestBasicFloodsub(t *testing.T) {
	hosts := getNetHosts(t, 20)

	var psubs []*PubSub
	for _, h := range hosts {
		psubs = append(psubs, NewFloodSub(h))
	}

	var msgs []<-chan *Message
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	connectAll(t, hosts)

	time.Sleep(time.Millisecond * 100)
	psubs[0].Publish("foobar", []byte("ipfs rocks"))

	for i, resp := range msgs {
		fmt.Printf("reading message from peer %d\n", i)
		msg := <-resp
		fmt.Printf("%s - %d: topic %s, from %s: %s\n", time.Now(), i, msg.Topic, msg.From, string(msg.Data))
	}

	psubs[2].Publish("foobar", []byte("libp2p is cool too"))
	for i, resp := range msgs {
		fmt.Printf("reading message from peer %d\n", i)
		msg := <-resp
		fmt.Printf("%s - %d: topic %s, from %s: %s\n", time.Now(), i, msg.Topic, msg.From, string(msg.Data))
	}

	for i := 0; i < 100; i++ {
		fmt.Println("loop: ", i)
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, resp := range msgs {
			got := <-resp
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}

	fmt.Println("Total Sent Messages: ", messageCount)
}
