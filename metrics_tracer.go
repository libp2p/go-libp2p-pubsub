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
	OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string, remoteAddr string)
	// OnMeshSize is the size of the topic mesh.
	OnMeshSize(topic string, count int)
	// Close shuts down the tracer.
	Close() error
}

var _ MetricsTracer = (*prometheusTracer)(nil)

var (
	topicMessageBytesSentMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "topic_message_bytes_sent_total",
		Help:      "Total bytes sent per topic",
	}, []string{"node_id", "topic"})

	topicMessageBytesReceivedMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "topic_message_bytes_received_total",
		Help:      "Total bytes received per topic",
	}, []string{"node_id", "topic"})

	topicMessageCountSentMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "topic_message_count_sent_total",
		Help:      "Total messages sent per topic",
	}, []string{"node_id", "topic"})

	topicMessageCountReceivedMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "topic_message_count_received_total",
		Help:      "Total messages received per topic",
	}, []string{"node_id", "topic"})

	peerSmoothedRTTMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gossipsub",
		Subsystem: "quic",
		Name:      "smoothed_rtt_seconds",
		Help:      "QUIC SmoothedRTT per topic in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"node_id", "topic"})

	ihaveBytesMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "ihave_bytes_total",
		Help:      "Total bytes of IHAVE message IDs",
	}, []string{"node_id", "direction", "topic"})

	ihaveCountMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "ihave_count_total",
		Help:      "Total count of IHAVE message IDs",
	}, []string{"node_id", "direction", "topic"})

	iwantBytesMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "iwant_bytes_total",
		Help:      "Total bytes of IWANT message IDs",
	}, []string{"node_id", "direction"})

	iwantCountMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gossipsub",
		Name:      "iwant_count_total",
		Help:      "Total count of IWANT message IDs",
	}, []string{"node_id", "direction"})

	meshSizeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "mesh_size",
		Help:      "Current mesh size per topic",
	}, []string{"node_id", "topic"})
)

func init() {
	prometheus.MustRegister(
		topicMessageBytesSentMetric, topicMessageBytesReceivedMetric,
		topicMessageCountSentMetric, topicMessageCountReceivedMetric,
		peerSmoothedRTTMetric,
		ihaveBytesMetric, ihaveCountMetric, iwantBytesMetric, iwantCountMetric,
		meshSizeMetric,
	)
}

type prometheusTracer struct {
	nodeID  string
	logger  *slog.Logger
	logFile *os.File

	topicMessageBytesSent     *prometheus.CounterVec
	topicMessageBytesReceived *prometheus.CounterVec
	topicMessageCountSent     *prometheus.CounterVec
	topicMessageCountReceived *prometheus.CounterVec
	peerSmoothedRTT           *prometheus.HistogramVec
	ihaveBytes                *prometheus.CounterVec
	ihaveCount                *prometheus.CounterVec
	iwantBytes                *prometheus.CounterVec
	iwantCount                *prometheus.CounterVec
	meshSize                  *prometheus.GaugeVec

	mx            sync.Mutex
	nextMessageID int
	msgIDCount    map[string]int
}

func newPrometheusTracer(filepath string, nodeID string) (*prometheusTracer, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(f, nil))

	return &prometheusTracer{
		nodeID:                    nodeID,
		logger:                    logger,
		logFile:                   f,
		topicMessageBytesSent:     topicMessageBytesSentMetric,
		topicMessageBytesReceived: topicMessageBytesReceivedMetric,
		topicMessageCountSent:     topicMessageCountSentMetric,
		topicMessageCountReceived: topicMessageCountReceivedMetric,
		peerSmoothedRTT:           peerSmoothedRTTMetric,
		ihaveBytes:                ihaveBytesMetric,
		ihaveCount:                ihaveCountMetric,
		iwantBytes:                iwantBytesMetric,
		iwantCount:                iwantCountMetric,
		meshSize:                  meshSizeMetric,
		msgIDCount:                make(map[string]int),
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
		t.logger.Info("message_id_mapping",
			"msgID", msgID,
			"message_seq", c,
		)
	}
	return c
}

func (t *prometheusTracer) OnRPCSent(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
	idGen *msgIDGenerator,
) {
	type ihaveStats struct {
		count int
		size  int
	}
	topicIHaves := make(map[string]ihaveStats)
	iWantCount := 0
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				s := topicIHaves[hv.GetTopicID()]
				s.count += len(hv.MessageIDs)
				s.size += len(hv.MessageIDs) * msgIDSize
				topicIHaves[hv.GetTopicID()] = s
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantCount += len(wt.MessageIDs)
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_sent",
		"peerID", peerID.ShortString(),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.ihaveBytes.WithLabelValues(t.nodeID, "sent", topic).Add(float64(s.size))
		t.ihaveCount.WithLabelValues(t.nodeID, "sent", topic).Add(float64(s.count))
		t.logger.Info("topic_ihave_sent",
			"peerID", peerID.ShortString(),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}
	t.iwantBytes.WithLabelValues(t.nodeID, "sent").Add(float64(iWantSize))
	t.iwantCount.WithLabelValues(t.nodeID, "sent").Add(float64(iWantCount))

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_sent",
				"peerID", peerID.ShortString(),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_sent",
				"peerID", peerID.ShortString(),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := idGen.RawID(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.topicMessageBytesSent.WithLabelValues(t.nodeID, msg.GetTopic()).Add(float64(sz))
		t.topicMessageCountSent.WithLabelValues(t.nodeID, msg.GetTopic()).Inc()
		t.logger.Info("topic_message_sent",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID.ShortString(),
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
	type ihaveStats struct {
		count int
		size  int
	}
	topicIHaves := make(map[string]ihaveStats)
	iWantCount := 0
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				s := topicIHaves[hv.GetTopicID()]
				s.count += len(hv.MessageIDs)
				s.size += len(hv.MessageIDs) * msgIDSize
				topicIHaves[hv.GetTopicID()] = s
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantCount += len(wt.MessageIDs)
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_received",
		"peerID", peerID.ShortString(),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.ihaveBytes.WithLabelValues(t.nodeID, "received", topic).Add(float64(s.size))
		t.ihaveCount.WithLabelValues(t.nodeID, "received", topic).Add(float64(s.count))
		t.logger.Info("topic_ihave_received",
			"peerID", peerID.ShortString(),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}
	t.iwantBytes.WithLabelValues(t.nodeID, "received").Add(float64(iWantSize))
	t.iwantCount.WithLabelValues(t.nodeID, "received").Add(float64(iWantCount))

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_received",
				"peerID", peerID.ShortString(),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_received",
				"peerID", peerID.ShortString(),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := idGen.RawID(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.topicMessageBytesReceived.WithLabelValues(t.nodeID, msg.GetTopic()).Add(float64(sz))
		t.topicMessageCountReceived.WithLabelValues(t.nodeID, msg.GetTopic()).Inc()
		t.logger.Info("topic_message_received",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID.ShortString(),
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}
}

func (t *prometheusTracer) OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string, remoteAddr string) {
	t.peerSmoothedRTT.WithLabelValues(t.nodeID, topic).Observe(rtt.Seconds())
	t.logger.Info("mesh_peer_rtt",
		"peerID", peerID.ShortString(),
		"topic", topic,
		"smoothedRTT_ms", rtt.Milliseconds(),
		"transport", transport,
		"remote_addr", remoteAddr,
	)
}

func (t *prometheusTracer) OnMeshSize(topic string, size int) {
	t.meshSize.WithLabelValues(t.nodeID, topic).Set(float64(size))
	t.logger.Info("mesh_size", "topic", topic, "size", size)
}

func (t *prometheusTracer) Close() error {
	return t.logFile.Close()
}

// WithPrometheusTracer enables prometheus metrics and slog file logging
// for gossipsub attestation bandwidth and per-peer RTT.
func WithPrometheusTracer(filepath string) Option {
	return func(ps *PubSub) error {
		nodeID := ps.host.ID().String()
		mt, err := newPrometheusTracer(filepath, nodeID)
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
