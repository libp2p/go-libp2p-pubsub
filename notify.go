package dumbsub

import (
	"context"
	"fmt"

	ma "github.com/jbenet/go-multiaddr"
	inet "github.com/libp2p/go-libp2p/p2p/net"
)

var _ inet.Notifiee = (*PubSub)(nil)

func (p *PubSub) OpenedStream(n inet.Network, s inet.Stream) {

}

func (p *PubSub) ClosedStream(n inet.Network, s inet.Stream) {

}

func (p *PubSub) Connected(n inet.Network, c inet.Conn) {
	fmt.Printf("got connection! %s -> %s\n", c.LocalPeer(), c.RemotePeer())
	s, err := p.host.NewStream(context.Background(), c.RemotePeer(), ID)
	if err != nil {
		log.Error("opening new stream to peer: ", err)
		return
	}

	p.newPeers <- s
}

func (p *PubSub) Disconnected(n inet.Network, c inet.Conn) {

}

func (p *PubSub) Listen(n inet.Network, _ ma.Multiaddr) {

}

func (p *PubSub) ListenClose(n inet.Network, _ ma.Multiaddr) {

}
