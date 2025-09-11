//go:build pprof-example

package main

import (
	"context"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func main() {
	log.Println("=== PubSub Pprof Hook Example ===")

	// Setup the pprof hook before starting pubsub
	// This enables capturing CPU and heap profiles when SIGUSR1 is received
	if err := pubsub.SetupPprofHook(); err != nil {
		log.Fatalf("Failed to setup pprof hook: %v", err)
	}

	log.Println("Pprof hook enabled!")
	log.Println("Send SIGUSR1 to this process to capture pprof traces:")
	log.Printf("  kill -USR1 %d", os.Getpid())
	log.Println("This will create two files:")
	log.Println("  - pubsub-cpu-profile-YYYYMMDD-HHMMSS.pprof")
	log.Println("  - pubsub-heap-profile-YYYYMMDD-HHMMSS.pprof")
	log.Println("")

	// Create a context that runs for a long time to demonstrate profiling
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create libp2p host
	host, err := libp2p.New()
	if err != nil {
		log.Fatalf("Failed to create host: %v", err)
	}
	defer host.Close()

	// Create GossipSub instance
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		log.Fatalf("Failed to create pubsub: %v", err)
	}

	// Subscribe to a topic to have some pubsub activity
	topic := "/example/chat"
	sub, err := ps.Subscribe(topic)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Simulate some activity to make profiling interesting
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		counter := 0
		for {
			select {
			case <-ticker.C:
				message := []byte("Example message " + string(rune(counter)))
				if err := ps.Publish(topic, message); err != nil {
					log.Printf("Failed to publish: %v", err)
				}
				counter++
			case <-ctx.Done():
				return
			}
		}
	}()

	// Process incoming messages
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Context cancelled, expected
				}
				log.Printf("Failed to receive message: %v", err)
				continue
			}
			log.Printf("Received: %s", string(msg.Data))
		}
	}()

	log.Println("Running pubsub example with pprof hook...")
	log.Println("Try sending SIGUSR1 to trigger profiling:")
	log.Printf("  kill -USR1 %d", os.Getpid())
	log.Println("")

	// Automatically trigger profiling after 10 seconds for demonstration
	go func() {
		time.Sleep(10 * time.Second)
		log.Println("Auto-triggering pprof capture for demonstration...")
		if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
			log.Printf("Failed to send SIGUSR1: %v", err)
		}
	}()

	// Keep the program running
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down...")
	}

	log.Println("Example completed!")
	log.Println("Check for .pprof files in the current directory")
}
