package pubsub

import "go.opentelemetry.io/otel/metric"

type metrics struct {
	metric.MeterProvider
	msgProcessingHistogram metric.Int64Histogram
}

func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(ps *PubSub) error {
		ps.metrics.MeterProvider = meterProvider
		return nil
	}
}
