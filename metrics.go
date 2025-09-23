package pubsub

import (
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	metric.MeterProvider
	msgProcessingHistogram         metric.Int64Histogram
	peerScoreCalcDurHistogram      metric.Int64Histogram
	peerTopicScoreCalcDurHistogram metric.Int64Histogram
	lateIDONTWANTs                 metric.Int64Counter
	effectiveIDONTWANTs            metric.Int64Counter
}

func (m *metrics) init(meter metric.Meter) (err error) {
	if m.msgProcessingHistogram, err = meter.Int64Histogram(
		"msg_processing.duration",
		metric.WithDescription("The duration of message processing not including validation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if m.peerScoreCalcDurHistogram, err = meter.Int64Histogram(
		"peer_score_calculation.duration",
		metric.WithDescription("The duration of peer score calculation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000),
	); err != nil {
		return err
	}

	if m.peerTopicScoreCalcDurHistogram, err = meter.Int64Histogram(
		"peer_topic_score_calculation.duration",
		metric.WithDescription("The duration of peer score calculation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000),
	); err != nil {
		return err
	}

	if m.lateIDONTWANTs, err = meter.Int64Counter(
		"late_idontwant.count",
		metric.WithDescription("The number of late IDONTWANT messages"),
	); err != nil {
		return err
	}

	if m.effectiveIDONTWANTs, err = meter.Int64Counter(
		"effective_idontwant.count",
		metric.WithDescription("The number of effective IDONTWANT messages"),
	); err != nil {
		return err
	}

	return
}

func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(ps *PubSub) error {
		ps.metrics.MeterProvider = meterProvider
		return nil
	}
}
