package pubsub

import (
	"github.com/libp2p/go-libp2p-core/network"
	ma "github.com/multiformats/go-multiaddr"
)

var _ network.Notifiee = (*PubSubNotif)(nil)

type PubSubNotif PubSub

func (p *PubSubNotif) OpenedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) ClosedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) Connected(n network.Network, c network.Conn) {

	go func() {
		p.newPeersPrioLk.RLock()
		p.newPeersMx.Lock()
		p.newPeersPend[c.RemotePeer()] = struct{}{}
		p.newPeersMx.Unlock()
		p.newPeersPrioLk.RUnlock()

		select {
		case p.newPeers <- struct{}{}:
		default:
		}
	}()
}

func (p *PubSubNotif) Disconnected(n network.Network, c network.Conn) {
}

func (p *PubSubNotif) Listen(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) ListenClose(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) Initialize() {
	p.newPeersPrioLk.RLock()
	p.newPeersMx.Lock()
	for _, pid := range p.host.Network().Peers() {
		p.newPeersPend[pid] = struct{}{}
	}
	p.newPeersMx.Unlock()
	p.newPeersPrioLk.RUnlock()

	select {
	case p.newPeers <- struct{}{}:
	default:
	}
}
