package main

import (
	"context"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.27.0"
)

func main() {
	// Example 1: Using Jaeger (recommended for production)
	log.Println("=== Example 1: Jaeger Tracing ===")
	runJaegerExample()

	// Example 2: Using stdout (good for development/debugging)
	log.Println("=== Example 2: Stdout Tracing ===")
	runStdoutExample()

	// Example 3: Disabled tracing (production without observability)
	log.Println("=== Example 3: Disabled Tracing ===")
	runDisabledExample()

	// Example 4: Grafana Tempo setup
	log.Println("=== Example 4: Grafana Tempo Setup ===")
	runTempoExample()
}

func runJaegerExample() {
	// Setup OTLP HTTP exporter (works with Jaeger 1.35+ via OTLP)
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("http://localhost:4318"), // Jaeger OTLP HTTP endpoint
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Printf("Failed to create OTLP exporter: %v", err)
		return
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("ethereum-beacon-node"),
			semconv.ServiceVersionKey.String("v1.0.0"),
			semconv.ServiceInstanceIDKey.String("node-001"),
		)),
		// Sample 10% in production, 100% in development
		trace.WithSampler(trace.TraceIDRatioBased(0.1)),
	)
	defer tp.Shutdown(context.Background())

	otel.SetTracerProvider(tp)

	// Enable OpenTelemetry tracing in pubsub
	pubsub.InitOtelTracing()

	runPubSubExample("jaeger-example")
}

func runStdoutExample() {
	// Setup stdout exporter (great for development)
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Printf("Failed to create stdout exporter: %v", err)
		return
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-dev"),
		)),
		trace.WithSampler(trace.AlwaysSample()), // Sample everything for development
	)
	defer tp.Shutdown(context.Background())

	otel.SetTracerProvider(tp)

	// Enable OpenTelemetry tracing in pubsub
	pubsub.InitOtelTracing()

	runPubSubExample("stdout-example")
}

func runDisabledExample() {
	// Don't call InitOtelTracing() - tracing remains disabled by default
	// pubsub.InitOtelTracing() // <- commented out to show disabled state

	log.Printf("Tracing enabled: %v", pubsub.IsTracingEnabled())

	runPubSubExample("disabled-example")
}

func runTempoExample() {
	// Example for Grafana Tempo with OTLP HTTP exporter
	// This would typically connect to Tempo at http://localhost:3200
	
	// For this example, we'll use stdout since Tempo setup is external
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Printf("Failed to create exporter: %v", err)
		return
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("ethereum-consensus-client"),
			semconv.DeploymentEnvironmentKey.String("production"),
		)),
		// Lower sampling for production
		trace.WithSampler(trace.TraceIDRatioBased(0.01)), // 1%
	)
	defer tp.Shutdown(context.Background())

	otel.SetTracerProvider(tp)
	pubsub.InitOtelTracing()

	runPubSubExample("tempo-example")
}

func runPubSubExample(exampleName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create libp2p hosts
	host1, err := libp2p.New()
	if err != nil {
		log.Printf("Failed to create host1: %v", err)
		return
	}
	defer host1.Close()

	host2, err := libp2p.New()
	if err != nil {
		log.Printf("Failed to create host2: %v", err)
		return
	}
	defer host2.Close()

	// Create GossipSub instances
	ps1, err := pubsub.NewGossipSub(ctx, host1)
	if err != nil {
		log.Printf("Failed to create pubsub1: %v", err)
		return
	}

	ps2, err := pubsub.NewGossipSub(ctx, host2)
	if err != nil {
		log.Printf("Failed to create pubsub2: %v", err)
		return
	}

	// Connect the hosts
	host1.Peerstore().AddAddrs(host2.ID(), host2.Addrs(), time.Hour)
	err = host1.Connect(ctx, host2.Peerstore().PeerInfo(host2.ID()))
	if err != nil {
		log.Printf("Failed to connect hosts: %v", err)
		return
	}

	// Simulate beacon attestation topic
	topic := "/eth2/beacon_attestation_0/ssz_snappy"

	// Subscribe (creates join_topic traces)
	sub, err := ps2.Subscribe(topic)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
		return
	}

	// Wait for mesh to form
	time.Sleep(500 * time.Millisecond)

	// Publish attestation (creates publish traces)
	attestationData := []byte("simulated beacon attestation data")
	log.Printf("[%s] Publishing attestation...", exampleName)
	
	err = ps1.Publish(topic, attestationData)
	if err != nil {
		log.Printf("Failed to publish: %v", err)
		return
	}

	// Receive message (creates handle_rpc and deliver traces)
	log.Printf("[%s] Waiting for message...", exampleName)
	msg, err := sub.Next(ctx)
	if err != nil {
		log.Printf("Failed to receive message: %v", err)
		return
	}

	log.Printf("[%s] Received attestation: %d bytes", exampleName, len(msg.Data))

	// Give time for traces to be exported
	time.Sleep(1 * time.Second)
}
