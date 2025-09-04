package pubsub

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.27.0"
)

func TestOtelTracingDisabled(t *testing.T) {
	// Test that when InitOtelTracing is never called, operations work normally
	// Reset tracer to nil to simulate fresh start
	otelTracer = nil
	
	if IsTracingEnabled() {
		t.Fatal("tracing should be disabled by default")
	}
	
	// Test that spans are no-ops
	_, span := startSpan(context.Background(), "test.operation")
	span.End()
	
	// Should not panic - test passes if we get here
}

func TestOtelTracingEnabled(t *testing.T) {
	// Setup stdout exporter for testing
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()), // Sample everything for testing
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	
	// Enable tracing
	InitOtelTracing()
	
	if !IsTracingEnabled() {
		t.Fatal("tracing should be enabled")
	}
	
	// Test that spans are created
	_, span := startSpan(context.Background(), "test.operation")
	if !span.IsRecording() {
		t.Fatal("span should be recording when tracing enabled")
	}
	span.End()
}

func TestGossipSubWithOtelTracing(t *testing.T) {
	// Setup tracing
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-integration-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	InitOtelTracing()
	
	// Create test environment
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	hosts := getDefaultHosts(t, 3)
	psubs := getGossipsubs(ctx, hosts)
	
	// Connect peers
	connectAll(t, hosts)
	
	topic := "test-otel-tracing"
	
	// Subscribe (this will create join traces)
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	
	// Wait for mesh to form
	time.Sleep(time.Millisecond * 100)
	
	// Publish message (this will create publish and handle_rpc traces)
	testMsg := []byte("test message with otel tracing")
	err = psubs[0].Publish(topic, testMsg)
	if err != nil {
		t.Fatal(err)
	}
	
	// Receive messages (this will create delivery traces)
	for i, sub := range subs[1:] {
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Data) != string(testMsg) {
			t.Fatalf("peer %d received wrong message: expected %s, got %s", i+1, testMsg, msg.Data)
		}
	}
	
	// Wait a bit to ensure all traces are processed
	time.Sleep(time.Millisecond * 100)
	
	// Test completed successfully - traces should be visible in stdout
	t.Log("Test completed successfully. Check stdout for OpenTelemetry traces.")
}

func TestOtelTracingWithTopicFilter(t *testing.T) {
	// Setup tracing with topic filter
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithoutTimestamps(),
	)
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter), // Immediate output
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-topic-filter-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	
	// Enable tracing with topic filter - only trace topics containing "beacon"
	InitOtelTracingWithTopicFilter("beacon")
	
	if !IsTracingEnabled() {
		t.Fatal("tracing should be enabled")
	}
	
	if GetOtelTopicFilter() != "beacon" {
		t.Fatalf("expected topic filter 'beacon', got '%s'", GetOtelTopicFilter())
	}
	
	// Create test environment
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	hosts := getDefaultHosts(t, 3)
	psubs := getGossipsubs(ctx, hosts)
	
	// Connect peers
	connectAll(t, hosts)
	
	// Test with two topics - only beacon topic should be traced
	beaconTopic := "beacon-attestation-test"
	otherTopic := "other-topic-test"
	
	// Subscribe to both topics
	var beaconSubs, otherSubs []*Subscription
	for _, ps := range psubs {
		beaconSub, err := ps.Subscribe(beaconTopic)
		if err != nil {
			t.Fatal(err)
		}
		beaconSubs = append(beaconSubs, beaconSub)
		
		otherSub, err := ps.Subscribe(otherTopic)
		if err != nil {
			t.Fatal(err)
		}
		otherSubs = append(otherSubs, otherSub)
	}
	
	// Wait for mesh to form
	time.Sleep(time.Millisecond * 100)
	
	t.Log("Publishing to beacon topic (should be traced)...")
	// Publish to beacon topic - should be traced
	beaconMsg := []byte("beacon attestation message")
	err = psubs[0].Publish(beaconTopic, beaconMsg)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Log("Publishing to other topic (should NOT be traced)...")
	// Publish to other topic - should NOT be traced
	otherMsg := []byte("other message")
	err = psubs[0].Publish(otherTopic, otherMsg)
	if err != nil {
		t.Fatal(err)
	}
	
	// Receive messages from beacon topic
	for i, sub := range beaconSubs[1:] {
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Data) != string(beaconMsg) {
			t.Fatalf("peer %d received wrong beacon message", i+1)
		}
	}
	
	// Receive messages from other topic
	for i, sub := range otherSubs[1:] {
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Data) != string(otherMsg) {
			t.Fatalf("peer %d received wrong other message", i+1)
		}
	}
	
	// Wait for traces to be processed
	time.Sleep(time.Millisecond * 100)
	
	t.Log("Test completed. Only traces for topics containing 'beacon' should appear above.")
}

func TestOtelTracingSpanNesting(t *testing.T) {
	// Setup tracing
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithoutTimestamps(),
	)
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter), // Immediate output
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-span-nesting-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	
	// Enable tracing
	InitOtelTracing()
	
	if !IsTracingEnabled() {
		t.Fatal("tracing should be enabled")
	}
	
	// Create test environment
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	hosts := getDefaultHosts(t, 2)
	psubs := getGossipsubs(ctx, hosts)
	
	// Connect peers
	connectAll(t, hosts)
	
	topic := "span-nesting-test-topic"
	
	// Create application tracer to simulate calling application (like Prysm)
	appTracer := otel.Tracer("test-application")
	
	t.Log("Creating application span and publishing with context...")
	
	// Create application span (simulating Prysm or other calling application)
	appCtx, appSpan := appTracer.Start(ctx, "app.process_message")
	appSpan.SetAttributes(
		attribute.String("app.operation", "beacon_attestation_processing"),
		attribute.String("app.component", "prysm"),
	)
	
	// Join topic with application context - this should create nested spans
	topicHandle, err := psubs[0].Join(topic)
	if err != nil {
		t.Fatal(err)
	}
	
	// Subscribe with the second peer
	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	
	// Wait for mesh to form
	time.Sleep(time.Millisecond * 100)
	
	// This should create nested spans under the application span
	message := []byte("test message with nested spans")
	err = topicHandle.Publish(appCtx, message)
	if err != nil {
		t.Fatal(err)
	}
	
	appSpan.End()
	
	// Receive the message
	msg, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != string(message) {
		t.Fatal("received wrong message")
	}
	
	// Force flush to ensure all spans are exported
	tp.ForceFlush(context.Background())
	time.Sleep(time.Millisecond * 200)
	
	t.Log("Test completed. Check above for nested spans: app.process_message -> gossipsub.publish -> pubsub.validate_*")
}

func TestHandleNewStreamTracing(t *testing.T) {
	// Setup stdout tracer
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
	)
	defer tp.ForceFlush(context.Background())
	
	InitOtelTracing()
	defer func() {
		otelTracer = nil
	}()
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create test network with 2 peers
	hosts := getDefaultHosts(t, 2)
	psubs := getPubsubs(ctx, hosts)
	
	// Connect peers
	connect(t, hosts[0], hosts[1])
	
	// Subscribe to a topic on both peers
	topic := "test-handleNewStream-topic"
	sub1, err := psubs[0].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Cancel()
	
	sub2, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Cancel()
	
	// Wait for mesh formation
	time.Sleep(100 * time.Millisecond)
	
	// Publish a message from peer 0 to peer 1
	// This should trigger handleNewStream tracing on peer 1
	msg := []byte("test message for handleNewStream tracing")
	err = psubs[0].Publish(topic, msg)
	if err != nil {
		t.Fatal(err)
	}
	
	// Wait for message propagation
	ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	
	select {
	case receivedMsg := <-sub2.ch:
		if string(receivedMsg.Data) != string(msg) {
			t.Errorf("expected %s, got %s", string(msg), string(receivedMsg.Data))
		}
	case <-ctx2.Done():
		t.Fatal("timeout waiting for message")
	}
	
	// Force flush to see traces
	tp.ForceFlush(context.Background())
	
	t.Log("Test completed - check stdout for handleNewStream traces")
}