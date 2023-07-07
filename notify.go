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
	}, eventbus.Name("pubsub/peers-notify")) // @NOTE(gfanton): is the naming is correct ?
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
				// send record to connected peer only
				if evt.Connectedness == network.Connected {
					go p.AddPeers(evt.Peer)
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
		if p.host.Network().Connectedness(pid) != network.Connected || p.isTransient(pid) {
			continue
		}

		p.newPeersPend[pid] = struct{}{}
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
