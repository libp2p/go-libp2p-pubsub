package pubsub

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
)

// MetricsTracer collects gossipsub metrics for attestation topic bandwidth
// and per-peer QUIC connection RTT.
type MetricsTracer interface {
	// OnRPCSent is called after sending an RPC
	OnRPCSent(peerID peer.ID, duration time.Duration, rpc *pb.RPC, idGen *msgIDGenerator)

	// OnRPCReceived is called after receiving an RPC
	OnRPCReceived(peerID peer.ID, duration time.Duration, rpc *pb.RPC, idGen *msgIDGenerator)

	// OnPeerRTT is called once per heartbeat per mesh peer with QUIC SmoothedRTT.
	// transport indicates the connection type (e.g. "quic", "yamux", "mplex").
	OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string)
	// OnMeshSize is the size of the topic mesh.
	OnMeshSize(topic string, count int)
	// Close shuts down the tracer.
	Close() error
}

var _ MetricsTracer = (*prometheusTracer)(nil)

var (
	attestationBytesSentMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Subsystem: "attestation",
		Name:      "bytes_sent_total",
		Help:      "Total bytes sent for attestation topics",
	}, []string{"topic"})

	attestationBytesReceivedMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Subsystem: "attestation",
		Name:      "bytes_received_total",
		Help:      "Total bytes received for attestation topics",
	}, []string{"topic"})

	peerSmoothedRTTMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gossipsub",
		Subsystem: "quic",
		Name:      "smoothed_rtt_milliseconds",
		Help:      "QUIC SmoothedRTT per topic in milliseconds",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(attestationBytesSentMetric, attestationBytesReceivedMetric, peerSmoothedRTTMetric)
}

type prometheusTracer struct {
	logger  *slog.Logger
	logFile *os.File

	attestationBytesSent     *prometheus.CounterVec
	attestationBytesReceived *prometheus.CounterVec
	peerSmoothedRTT          *prometheus.HistogramVec

	mx            sync.Mutex
	nextMessageID int
	msgIDCount    map[string]int
}

func newPrometheusTracer(filepath string) (*prometheusTracer, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(f, nil))

	return &prometheusTracer{
		logger:                   logger,
		logFile:                  f,
		attestationBytesSent:     attestationBytesSentMetric,
		attestationBytesReceived: attestationBytesReceivedMetric,
		peerSmoothedRTT:          peerSmoothedRTTMetric,
		msgIDCount:               make(map[string]int),
	}, nil
}

func (t *prometheusTracer) getMessageID(msgID string) int {
	t.mx.Lock()
	defer t.mx.Unlock()

	c, ok := t.msgIDCount[msgID]
	if !ok {
		c = t.nextMessageID
		t.nextMessageID++
		t.msgIDCount[msgID] = c
	}
	return c
}

func (t *prometheusTracer) OnRPCSent(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
	idGen *msgIDGenerator,
) {
	topicIHaves := make(map[string]int)
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				topicIHaves[hv.GetTopicID()] += len(hv.MessageIDs) * msgIDSize
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_sent",
		"peerID", peerID,
		"total_size", proto.Size(rpc),
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, sz := range topicIHaves {
		t.logger.Info("topic_ihave_sent",
			"peerID", peerID,
			"topic", topic,
			"ihave_size", sz,
			"took", duration.Milliseconds(),
		)
	}

	for _, msg := range rpc.GetPublish() {
		msgID := idGen.RawID(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.attestationBytesSent.WithLabelValues(msg.GetTopic()).Add(float64(sz))
		t.logger.Info("topic_message_sent",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID,
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}
}

func (t *prometheusTracer) OnRPCReceived(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
	idGen *msgIDGenerator,
) {
	topicIHaves := make(map[string]int)
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				topicIHaves[hv.GetTopicID()] += len(hv.MessageIDs) * msgIDSize
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_received",
		"peerID", peerID,
		"total_size", proto.Size(rpc),
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, sz := range topicIHaves {
		t.logger.Info("topic_ihave_received",
			"peerID", peerID,
			"topic", topic,
			"ihave_size", sz,
			"took", duration.Milliseconds(),
		)
	}

	for _, msg := range rpc.GetPublish() {
		msgID := idGen.RawID(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.attestationBytesReceived.WithLabelValues(msg.GetTopic()).Add(float64(sz))
		t.logger.Info("topic_message_received",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID,
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}
}

func (t *prometheusTracer) OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string) {
	t.peerSmoothedRTT.WithLabelValues(topic).Observe(float64(rtt.Milliseconds()))
	t.logger.Info("peer_rtt",
		"peerID", peerID,
		"topic", topic,
		"smoothedRTT_ms", rtt.Milliseconds(),
		"transport", transport,
	)
}

func (t *prometheusTracer) OnMeshSize(topic string, size int) {
	t.logger.Info("mesh_size", "topic", topic, "size", size)
}

func (t *prometheusTracer) Close() error {
	return t.logFile.Close()
}

// WithPrometheusTracer enables prometheus metrics and slog file logging
// for gossipsub attestation bandwidth and per-peer RTT.
func WithPrometheusTracer(filepath string) Option {
	return func(ps *PubSub) error {
		mt, err := newPrometheusTracer(filepath)
		if err != nil {
			return fmt.Errorf("failed to create prometheus tracer: %w", err)
		}
		ps.metricsTracer = mt
		gs, ok := ps.rt.(*GossipSubRouter)
		if ok {
			gs.enableRTTReporter = true
		}
		return nil
	}
}
