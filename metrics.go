package pubsub

import (
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	metric.MeterProvider
	meshSize metric.Int64Gauge
}

func (m *metrics) init(meter metric.Meter) (err error) {
	if m.meshSize, err = meter.Int64Gauge(
		"mesh.size",
		metric.WithDescription("Size of the gossipsub mesh"),
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
