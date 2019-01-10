# Basic PubSub Example 

The `main.go` exposes the basic methods to play with the PubSub api. 
It creates a simple scenario with three peers (1, 2 & 3) with the following
network topology:
```
1 <---> 2 <---> 3
```

**Goal**: Node 1 sends a message to the *"random"* topic and node 2 and 3 must
receive the message.

# Code explanation

1. **Host Creation**: We first need to create the three hosts using [go-libp2p]("https://github.com/libp2p/go-libp2p"). A `Host` is the central interface in libp2p that is able to connect to other `Host` (w/o encryption, signing, etc), relay messages, etc.
```go
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)),
		libp2p.DisableRelay(),
	}
	basicHost, err := libp2p.New(context.Background(), opts...)
```
This create three hosts using TCP as their network backend. See issue [#21](https://github.com/libp2p/go-libp2p-examples/issues/21) for more information about the `DisableRelay()` option.

2. **Peerstore filling**: Our hosts needs to know how to contact each other. We
give them the information through their peerstore as the following:
```go
h2.Peerstore().AddAddr(h1.ID(), h1.Addrs()[0], pstore.PermanentAddrTTL)
```

3. **PubSub Creation**: Now we start using the pubsub library. We create a
PubSub interface responsible for managing the overlay network used to gossip
around the messages. Each PubSub wraps around the Host we created previously. We do the following for the three hosts.
```go
opts := pubsub.WithMessageSigning(false)
g1, err := pubsub.NewGossipSub(context.Background(), h1, opts)
```

4. **Topic Subscription**: Node 2 and 3 both registers to the *random* topic,
   and let the overlay network knows about it. Node 1 will know about node 2's
   interest and node 2 will know about node's 3 interest.
```go
s2, err := g2.Subscribe(topic)
```

5. **Wait**: We need to wait a little before the interests propagate correctly
   to the other peers, the heartbeats are setup, etc. 

6. **Publishing**: Node 1 publishes a slice of byte to the network under the
   *random* topic.
```go	
msg := []byte("Hello Word")
err := g1.Publish(topic, msg)
```

7. **Reading Next Message**: Node 2 and node 3 are notified of a message for the
   topic they're registered. Underneath the basic API, the pubsub library made
   node 2 forward the message it received from node 1, to node 3. Gossiping
   happened !
```go
pbMsg, err = s2.Next(context.Background())
pbMsg, err = s3.Next(context.Background())
```



