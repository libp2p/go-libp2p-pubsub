package pubsub

import (
	"iter"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageBatch allows a user to batch related messages and then publish them at
// once. This allows the Scheduler to define an order for the outgoing RPCs.
// This helps bandwidth constrained peers.
type MessageBatch struct {
	Scheduler RPCScheduler
	messages  []*Message
}

// RPCScheduler is the publishing strategy publishing a set of RPCs.
type RPCScheduler interface {
	// AddRPC adds an RPC to the strategy.
	AddRPC(peer peer.ID, msgID string, rpc *RPC)
	// All returns an ordered iterator of RPCs to publish.
	All() iter.Seq2[peer.ID, *RPC]
}

type pendingRPC struct {
	peer peer.ID
	rpc  *RPC
}

type RarestFirstStrategy struct {
	sync.Mutex
	rpcs map[string][]pendingRPC
}

func (s *RarestFirstStrategy) AddRPC(peer peer.ID, msgID string, rpc *RPC) {
	s.Lock()
	defer s.Unlock()
	if s.rpcs == nil {
		s.rpcs = make(map[string][]pendingRPC)
	}
	s.rpcs[msgID] = append(s.rpcs[msgID], pendingRPC{peer: peer, rpc: rpc})
}

func (s *RarestFirstStrategy) All() iter.Seq2[peer.ID, *RPC] {
	return func(yield func(peer.ID, *RPC) bool) {
		s.Lock()
		defer s.Unlock()

		for len(s.rpcs) > 0 {
			for msgID, rpcs := range s.rpcs {
				if len(rpcs) == 0 {
					delete(s.rpcs, msgID)
					continue
				}
				if !yield(rpcs[0].peer, rpcs[0].rpc) {
					return
				}

				s.rpcs[msgID] = rpcs[1:]
			}
		}
	}
}
