package floodsub

import (
	"context"

	ma "gx/ipfs/QmYzDkkgAEmrcNzFCiYo6L1dTX4EAG1gZkbtdbd9trL4vd/go-multiaddr"
	inet "gx/ipfs/Qmf4ETeAWXuThBfWwonVyFqGFSgTWepUDEr1txcctvpTXS/go-libp2p/p2p/net"
)

var _ inet.Notifiee = (*PubSub)(nil)

func (p *PubSub) OpenedStream(n inet.Network, s inet.Stream) {

}

func (p *PubSub) ClosedStream(n inet.Network, s inet.Stream) {

}

func (p *PubSub) Connected(n inet.Network, c inet.Conn) {
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
