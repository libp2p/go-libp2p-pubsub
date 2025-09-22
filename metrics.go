package pubsub

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	metric.MeterProvider
	// TODO
	messageProcessingTime metric.Int64Histogram
	// time spent to read message over network and parse it into an RPC.
	messageParsingTime metric.Int64Histogram // DONE
	// time taken for a heartbeat to complete
	heartbeatTime metric.Int64Histogram // DONE

	// total peers connected to
	totalPeerCount metric.Int64Gauge // DONE
	// total number of topics subscribed to
	totalTopicCount metric.Int64Gauge // DONE
	// total number of subscriptions active in the pubsub router
	totalSubscriptionCount metric.Int64Gauge // DONE
	// depth of the incoming queue in the event loop
	incomingQueueDepth metric.Int64Gauge // DONE
	// depth of the sendMsg queue in the event loop
	sendMsgQueueDepth metric.Int64Gauge // DONE

	// TODO - ideally should be per-topic
	messageSize metric.Int64Histogram // DONE
	// mesh size per topic
	meshMemberCount metric.Int64Gauge
	// fanout network size per topic
	fanoutMemberCount metric.Int64Gauge

	// time spent by different events in the event loop before being picked up
	// for processing.
	eventLoopWaitTime metric.Int64Histogram // DONE
	// time spent by different events
	eventProcessingTime metric.Int64Histogram // DONE

	// the number of times, we have received an IDONTWANT message
	iDontWantMsgRecvd metric.Int64Counter
	// the number of times, we have sent an IDONTWANT message
	iDontWantMsgSent metric.Int64Counter
	// the number of IWantMsgs sent
	iWantMsgSent metric.Int64Counter // DONE
	// the number of IWantMsgs recvd 
	iWantMsgRecvd metric.Int64Counter // DONE
	// the number of prune msgs recvd per topic
	pruneMsgRecvdPerTopic metric.Int64Counter // DONE
	// the number of graft msgs recvd per topic
	graftMsgRecvdPerTopic metric.Int64Counter // DONE
	// the number of prune msgs sent per topic
	pruneMsgSentPerTopic metric.Int64Counter // DONE
	// the number of graft msgs sent per topic
	graftMsgSentPerTopic metric.Int64Counter // DONE

	// the number of messages sent per topic
	topicMsgSent metric.Int64Counter // DONE
	// the bytes sent per topic
	topicBytesSent metric.Int64Counter // DONE
	// the number of messages published per topic
	topicMsgPublished metric.Int64Counter // DONE

	// the number of messages received per topic
	topicMsgRecvd metric.Int64Counter
	// the bytes received per topic
	topicBytesRecvd metric.Int64Counter
	// the number of messages received per topic unfiltered(with duplicates)
	topicMsgRecvdUnfiltered metric.Int64Counter

	// TODO - validation related metrics
}

func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(ps *PubSub) error {
		ps.metrics.MeterProvider = meterProvider
		return nil
	}
}

// TODO: InitMetrics should return a metrics struct which can be assigned to ps.metrics
func InitMetrics(ps *PubSub) error {
	meter := ps.metrics.MeterProvider.Meter("libp2p-pubsub")

	var err error
	ps.metrics.messageProcessingTime, err = meter.Int64Histogram(
		"msg_processing_time.duration",
		metric.WithDescription("The duration of message processing not including validation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	)
	if err != nil {
		return err
	}

	ps.metrics.messageParsingTime, err = meter.Int64Histogram(
		"message_parsing_time.duration",
		metric.WithDescription("The duration of parsing a message received from the network stream to an RPC"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	)
	if err != nil {
		return err
	}

	ps.metrics.heartbeatTime, err = meter.Int64Histogram(
		"heartbeat_time.duration",
		metric.WithDescription("The duration of the heartbeat"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	)
	if err != nil {
		return err
	}

	ps.metrics.totalPeerCount, err = meter.Int64Gauge(
		"total_peer_count",
		metric.WithDescription("The total number of peers that are connected to this pubsub router"),
	)
	if err != nil {
		return err
	}

	ps.metrics.totalTopicCount, err = meter.Int64Gauge(
		"total_topic_count",
		metric.WithDescription("The total number of topics that are subscribed to this pubsub router"),
	)
	if err != nil {
		return err
	}

	ps.metrics.totalSubscriptionCount, err = meter.Int64Gauge(
		"total_subscription_count",
		metric.WithDescription("The total number of subscriptions that are active in this pubsub router"),
	)
	if err != nil {
		return err
	}

	ps.metrics.incomingQueueDepth, err = meter.Int64Gauge(
		"incoming_queue_depth",
		metric.WithDescription("The depth of the incoming queue in the event loop"),
	)
	if err != nil {
		return err
	}

	ps.metrics.sendMsgQueueDepth, err = meter.Int64Gauge(
		"sendmsg_queue_depth",
		metric.WithDescription("The depth of the send message queue in the event loop"),
	)
	if err != nil {
		return err
	}

	ps.metrics.messageSize, err = meter.Int64Histogram(
		"message_size",
		metric.WithDescription("The size of the messages received by the peer in bytes"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return err
	}

	ps.metrics.meshMemberCount, err = meter.Int64Gauge(
		"mesh_member_count",
		metric.WithDescription("The number of mesh members per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.fanoutMemberCount, err = meter.Int64Gauge(
		"fanout_member_count",
		metric.WithDescription("The number of fanout members per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.eventLoopWaitTime, err = meter.Int64Histogram(
		"event_loop_wait_time",
		metric.WithDescription("The time spent by different events in the event loop before being picked up for processing"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return err
	}

	ps.metrics.eventProcessingTime, err = meter.Int64Histogram(
		"event_processing_time",
		metric.WithDescription("The time spent processing different events in the event loop"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return err
	}

	ps.metrics.iDontWantMsgRecvd, err = meter.Int64Counter(
		"i_dont_want_msg_recvd",
		metric.WithDescription("The number of times we have received an IDONTWANT message"),
	)
	if err != nil {
		return err
	}

	ps.metrics.iDontWantMsgSent, err = meter.Int64Counter(
		"i_dont_want_msg_sent",
		metric.WithDescription("The number of times we have sent an IDONTWANT message"),
	)
	if err != nil {
		return err
	}

	ps.metrics.iWantMsgSent, err = meter.Int64Counter(
		"i_want_msg_sent",
		metric.WithDescription("The number of IWANT msgs sent"),
	)
	if err != nil {
		return err
	}

	ps.metrics.iWantMsgRecvd, err = meter.Int64Counter(
		"i_want_msg_recvd",
		metric.WithDescription("The number of IWANT msgs received"),
	)
	if err != nil {
		return err
	}

	ps.metrics.pruneMsgRecvdPerTopic, err = meter.Int64Counter(
		"prune_msg_recvd_per_topic",
		metric.WithDescription("The number of prune msgs received per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.graftMsgRecvdPerTopic, err = meter.Int64Counter(
		"graft_msg_recvd_per_topic",
		metric.WithDescription("The number of graft msgs received per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.pruneMsgSentPerTopic, err = meter.Int64Counter(
		"prune_msg_sent_per_topic",
		metric.WithDescription("The number of prune msgs sent per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.graftMsgSentPerTopic, err = meter.Int64Counter(
		"graft_msg_sent_per_topic",
		metric.WithDescription("The number of graft msgs sent per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicMsgSent, err = meter.Int64Counter(
		"topic_msg_sent",
		metric.WithDescription("The number of messages sent per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicBytesSent, err = meter.Int64Counter(
		"topic_bytes_sent",
		metric.WithDescription("The bytes sent per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicMsgPublished, err = meter.Int64Counter(
		"topic_msg_published",
		metric.WithDescription("The number of messages published per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicMsgRecvd, err = meter.Int64Counter(
		"topic_msg_recvd",
		metric.WithDescription("The number of messages received per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicBytesRecvd, err = meter.Int64Counter(
		"topic_bytes_recvd",
		metric.WithDescription("The bytes received per topic"),
	)
	if err != nil {
		return err
	}

	ps.metrics.topicMsgRecvdUnfiltered, err = meter.Int64Counter(
		"topic_msg_recvd_unfiltered",
		metric.WithDescription("The number of messages received per topic unfiltered(with duplicates)"),
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *metrics) RecordEventLoopWaitTeam(waitTime time.Duration, eventType string) {
	m.eventLoopWaitTime.Record(context.Background(), waitTime.Microseconds(), metric.WithAttributes(attribute.String("event_type", eventType)))
}

func (m *metrics) RecordEventProcessingTime(processingTime time.Duration, eventType string) {
	m.eventProcessingTime.Record(context.Background(), processingTime.Microseconds(), metric.WithAttributes(attribute.String("event_type", eventType)))
}

func (m *metrics) RecordMessageParsingTime(parsingTime time.Duration) {
	m.messageParsingTime.Record(context.Background(), parsingTime.Microseconds())
}

func (m *metrics) RecordMessageSize(msgSize int64) {
	m.messageSize.Record(context.Background(), msgSize)
}

func (m *metrics) RecordHeartbeatTime(heartbeatTime time.Duration) {
	m.heartbeatTime.Record(context.Background(), heartbeatTime.Microseconds())
}

func (m *metrics) RecordPeerCount(peerCount int64) {
	m.totalPeerCount.Record(context.Background(), peerCount)
}

func (m *metrics) RecordTopicCount(topicCount int64) {
	m.totalTopicCount.Record(context.Background(), topicCount)
}

func (m *metrics) RecordSubscriptionCount(subscriptionCount int64) {
	m.totalSubscriptionCount.Record(context.Background(), subscriptionCount)
}

func (m *metrics) RecordIncomingQueueDepth(depth int64) {
	m.incomingQueueDepth.Record(context.Background(), depth)
}

func (m *metrics) RecordSendMsgQueueDepth(depth int64) {
	m.sendMsgQueueDepth.Record(context.Background(), depth)
}

func (m *metrics) IncrementIDontWantMsgRecvdCount() {
	m.iDontWantMsgRecvd.Add(context.Background(), 1)
}

func (m *metrics) IncrementIDontWantMsgSentCount() {
	m.iDontWantMsgSent.Add(context.Background(), 1)
}

func (m *metrics) IncrementIWantMsgSent() {
	m.iWantMsgSent.Add(context.Background(), 1)
}

func (m *metrics) IncrementIWantMsgRecvd() {
	m.iWantMsgRecvd.Add(context.Background(), 1)
}

func (m *metrics) IncrementPruneMsgRecvdPerTopic(topic string) {
	m.pruneMsgRecvdPerTopic.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementGraftMsgRecvdPerTopic(topic string) {
	m.graftMsgRecvdPerTopic.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementPruneMsgSentPerTopic(topic string) {
	m.pruneMsgSentPerTopic.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementGraftMsgSentPerTopic(topic string) {
	m.graftMsgSentPerTopic.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementTopicMsgSent(topic string) {
	m.topicMsgSent.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementTopicBytesSent(bytes int64, topic string) {
	m.topicBytesSent.Add(context.Background(), bytes, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementTopicMsgPublished(topic string) {
	m.topicMsgPublished.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementTopicMsgRecvd(topic string) {
	m.topicMsgRecvd.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) IncrementTopicBytesRecvd(topic string) {
	m.topicBytesRecvd.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}
