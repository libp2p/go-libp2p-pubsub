package pubsub

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

type PubSubNotif PubSub

func (p *PubSubNotif) startMonitoring() error {
	sub, err := p.host.EventBus().Subscribe([]interface{}{
		new(event.EvtPeerConnectednessChanged),
		new(event.EvtPeerProtocolsUpdated),
	}, eventbus.Name("libp2p/pubsub/notify"))
	if err != nil {
		return fmt.Errorf("unable to subscribe to EventBus: %w", err)
	}

	go func() {
		defer sub.Close()

		for {
			var e interface{}
			select {
			case <-p.ctx.Done():
				return
			case e = <-sub.Out():
			}

			switch evt := e.(type) {
			case event.EvtPeerConnectednessChanged:
				switch evt.Connectedness {
				case network.Connected:
					go p.AddPeers(evt.Peer)
				case network.NotConnected:
					go p.RemovePeers(evt.Peer)
				}
			case event.EvtPeerProtocolsUpdated:
				supportedProtocols := p.rt.Protocols()

			protocol_loop:
				for _, addedProtocol := range evt.Added {
					for _, wantedProtocol := range supportedProtocols {
						if wantedProtocol == addedProtocol {
							go p.AddPeers(evt.Peer)
							break protocol_loop
						}
					}
				}
			}
		}
	}()

	return nil
}

func (p *PubSubNotif) isTransient(pid peer.ID) bool {
	for _, c := range p.host.Network().ConnsToPeer(pid) {
		if !c.Stat().Transient {
			return false
		}
	}

	return true
}

func (p *PubSubNotif) AddPeers(peers ...peer.ID) {
	p.newPeersPrioLk.RLock()
	p.newPeersMx.Lock()

	for _, pid := range peers {
		if !p.isTransient(pid) && p.host.Network().Connectedness(pid) == network.Connected {
			p.newPeersPend[pid] = struct{}{}
		}
	}

	// do we need to update ?
	haveNewPeer := len(p.newPeersPend) > 0

	p.newPeersMx.Unlock()
	p.newPeersPrioLk.RUnlock()

	if haveNewPeer {
		select {
		case p.newPeers <- struct{}{}:
		default:
		}
	}
}

func (p *PubSubNotif) RemovePeers(peers ...peer.ID) {
	p.peerDeadPrioLk.RLock()
	p.peerDeadMx.Lock()

	for _, pid := range peers {
		if !p.isTransient(pid) && p.host.Network().Connectedness(pid) == network.NotConnected {
			p.peerDeadPend[pid] = struct{}{}
		}
	}

	// do we need to update ?
	haveDeadPeer := len(p.peerDeadPend) > 0

	p.peerDeadMx.Unlock()
	p.peerDeadPrioLk.RUnlock()

	if haveDeadPeer {
		select {
		case p.peerDead <- struct{}{}:
		default:
		}
	}
}
