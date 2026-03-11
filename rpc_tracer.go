package pubsub

import (
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RPCTracer collects gossipsub metrics for attestation topic bandwidth
// and per-peer QUIC connection RTT.
type RPCTracer interface {
	// OnRPCSent is called after sending an RPC
	OnRPCSent(peerID peer.ID, duration time.Duration, rpc *pb.RPC)

	// OnRPCReceived is called after receiving an RPC
	OnRPCReceived(peerID peer.ID, duration time.Duration, rpc *pb.RPC)

	// OnPeerRTT is called once per heartbeat per mesh peer with QUIC SmoothedRTT.
	// transport indicates the connection type (e.g. "quic", "yamux", "mplex").
	OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string, remoteAddr string)
	// OnMeshSize is the size of the topic mesh.
	OnMeshSize(topic string, count int)
	// Close shuts down the tracer.
	Close() error
}

// WithRPCTracer enables prometheus metrics and slog file logging
// for gossipsub attestation bandwidth and per-peer RTT.
func WithRPCTracer(mt RPCTracer) Option {
	return func(ps *PubSub) error {
		ps.rpcTracer = mt
		gs, ok := ps.rt.(*GossipSubRouter)
		if ok {
			gs.enableRTTReporter = true
		}
		return nil
	}
}
