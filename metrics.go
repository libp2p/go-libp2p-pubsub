package pubsub

import (
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	metric.MeterProvider
	meshSize                   metric.Int64Gauge
	publishLatencyMilliseconds metric.Float64Histogram
}

func (m *metrics) init(meter metric.Meter) (err error) {
	if m.meshSize, err = meter.Int64Gauge(
		"mesh.size",
		metric.WithDescription("Size of the gossipsub mesh"),
	); err != nil {
		return err
	}

	if m.publishLatencyMilliseconds, err = meter.Float64Histogram(
		"publish.latency.milliseconds",
		metric.WithExplicitBucketBoundaries(
			0.001, 0.01, 0.1, 1, 2, 4, 8, 16, 32, 64, 100, 1000, 2000, 3000,
		),
		metric.WithDescription("Latency of publishing a message (ms)"),
	); err != nil {
		return err
	}

	return nil
}

func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(ps *PubSub) error {
		ps.metrics.MeterProvider = meterProvider
		return nil
	}
}
