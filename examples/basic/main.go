package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-host"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func main() {

	// IPFS uses golog for the logging - Change to DEBUG for extra info
	// golog.SetAllLoggers(gologging.DEBUG)

	// 1. Creation of three hosts
	h1 := newHost(2001)
	h2 := newHost(2002)
	h3 := newHost(2003)
	fmt.Printf("host 1: \n\t-Addr:%s\n\t-ID: %s\n", h1.Addrs()[0], h1.ID().Pretty())
	fmt.Printf("host 2: \n\t-Addr:%s\n\t-ID: %s\n", h2.Addrs()[0], h2.ID().Pretty())
	fmt.Printf("host 3: \n\t-Addr:%s\n\t-ID: %s\n", h3.Addrs()[0], h3.ID().Pretty())

	// 2. Fill peerstore - make a transitive connection from 1 to 3 by 2
	// add h1 to h2's store
	h2.Peerstore().AddAddr(h1.ID(), h1.Addrs()[0], pstore.PermanentAddrTTL)
	// add h2 to h1's store
	h1.Peerstore().AddAddr(h2.ID(), h2.Addrs()[0], pstore.PermanentAddrTTL)
	// add h3 to h2's store
	h2.Peerstore().AddAddr(h3.ID(), h3.Addrs()[0], pstore.PermanentAddrTTL)
	// add h2 to h3's store
	h3.Peerstore().AddAddr(h3.ID(), h3.Addrs()[0], pstore.PermanentAddrTTL)

	// 3. Create the PubSub structures for each of the hosts
	topic := "random"
	opts := pubsub.WithMessageSigning(false)
	g1, err := pubsub.NewGossipSub(context.Background(), h1, opts)
	requireNil(err)
	g2, err := pubsub.NewGossipSub(context.Background(), h2, opts)
	requireNil(err)
	g3, err := pubsub.NewGossipSub(context.Background(), h3, opts)
	requireNil(err)

	// 4. Subscribe to the relevant topic
	s2, err := g2.Subscribe(topic)
	requireNil(err)
	s3, err := g3.Subscribe(topic)
	requireNil(err)

	// 5. Make the connections to the peers
	err = h1.Connect(context.Background(), h2.Peerstore().PeerInfo(h2.ID()))
	requireNil(err)
	err = h2.Connect(context.Background(), h3.Peerstore().PeerInfo(h3.ID()))
	requireNil(err)

	// 6. Wait some time for the heartbeat to start, subscriptions pass around
	// etc
	time.Sleep(2 * time.Second)

	// 7. Node 1 publish
	msg := []byte("Hello Word")
	requireNil(g1.Publish(topic, msg))

	// 8. Node 2 and 3 reads - node 3 receives the message as well because 2 is
	// relaying it.
	pbMsg, err := s2.Next(context.Background())
	requireNil(err)
	checkEqual(msg, pbMsg.Data)
	fmt.Printf("Host 2 received %s\n", string(msg))

	pbMsg, err = s3.Next(context.Background())
	requireNil(err)
	checkEqual(msg, pbMsg.Data)
	fmt.Printf("Host 3 received %s\n", string(msg))
}

func checkEqual(exp, rcvd []byte) {
	if !bytes.Equal(exp, rcvd) {
		panic("not equal")
	}
}

func requireNil(err error) {
	if err != nil {
		panic(err)
	}
}

func newHost(port int) host.Host {
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)),
		libp2p.DisableRelay(),
	}
	basicHost, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		panic(err)
	}
	return basicHost
}
