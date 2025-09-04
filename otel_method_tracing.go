package pubsub

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/libp2p/go-libp2p-pubsub"
)

// Global tracer instance - will be nil if tracing is not enabled
var otelTracer trace.Tracer

// Global topic filter - only trace topics that contain this substring
// Empty string means trace all topics
var otelTopicFilter string

// InitOtelTracing initializes OpenTelemetry tracing for pubsub methods.
// Call this function to enable method-level tracing.
// If this function is never called, all tracing operations are no-ops.
func InitOtelTracing() {
	otelTracer = otel.Tracer(tracerName)
	otelTopicFilter = "" // Default: trace all topics
}

// InitOtelTracingWithTopicFilter initializes OpenTelemetry tracing with topic filtering.
// Only topics containing the specified substring will be traced.
// Use an empty string to trace all topics.
func InitOtelTracingWithTopicFilter(topicFilter string) {
	otelTracer = otel.Tracer(tracerName)
	otelTopicFilter = topicFilter
}

// SetOtelTopicFilter sets the topic filter for tracing.
// Only topics containing this substring will be traced.
// Use an empty string to trace all topics.
func SetOtelTopicFilter(topicFilter string) {
	otelTopicFilter = topicFilter
}

// Helper function to start a span with common attributes
// Returns no-op span if tracing is disabled
func startSpan(ctx context.Context, operationName string) (context.Context, trace.Span) {
	if otelTracer == nil {
		// Return no-op span if tracing not enabled
		return ctx, trace.SpanFromContext(ctx)
	}
	return otelTracer.Start(ctx, operationName)
}

// Helper function to start a span only if the topic matches the filter
// Returns no-op span if tracing is disabled or topic doesn't match filter
func startSpanForTopic(ctx context.Context, operationName string, topic string) (context.Context, trace.Span) {
	if otelTracer == nil {
		// Return no-op span if tracing not enabled
		return ctx, trace.SpanFromContext(ctx)
	}
	
	// If topic filter is set and topic doesn't contain the filter string, return no-op span
	if otelTopicFilter != "" && !strings.Contains(topic, otelTopicFilter) {
		return ctx, trace.SpanFromContext(ctx)
	}
	
	return otelTracer.Start(ctx, operationName)
}

// Helper function to check if a topic should be traced
func shouldTraceTopic(topic string) bool {
	if otelTracer == nil {
		return false
	}
	
	// If no filter is set, trace all topics
	if otelTopicFilter == "" {
		return true
	}
	
	// Only trace if topic contains the filter string
	return strings.Contains(topic, otelTopicFilter)
}

// IsTracingEnabled returns whether OpenTelemetry tracing is currently enabled
func IsTracingEnabled() bool {
	return otelTracer != nil
}

// GetOtelTopicFilter returns the current topic filter
func GetOtelTopicFilter() string {
	return otelTopicFilter
}

// startSpanFromMessage starts a span using the context from a Message for proper nesting
// Falls back to context.Background() if message context is nil
func startSpanFromMessage(msg interface{}, operationName string) (context.Context, trace.Span) {
	var ctx context.Context = context.Background()
	
	// Try to extract context from Message if available
	if m, ok := msg.(*Message); ok && m.Ctx != nil {
		ctx = m.Ctx
		// Debug: Check if context has span info
		if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
			// Context has a valid span - nesting should work
		}
	}
	
	return startSpan(ctx, operationName)
}

// startSpanForTopicFromMessage starts a topic-filtered span using the context from a Message
func startSpanForTopicFromMessage(msg interface{}, operationName, topic string) (context.Context, trace.Span) {
	var ctx context.Context = context.Background()
	
	// Try to extract context from Message if available
	if m, ok := msg.(*Message); ok && m.Ctx != nil {
		ctx = m.Ctx
	}
	
	return startSpanForTopic(ctx, operationName, topic)
}