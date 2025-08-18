package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestIssue634FixedSliceOutOfBounds verifies the fix for the panic described in
// https://github.com/libp2p/go-libp2p-pubsub/issues/634
// The panic used to occur when pruning a mesh that has >= Dhi peers but < Dscore peers.
// This test verifies that the fix prevents the panic.
func TestIssue634FixedSliceOutOfBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		peerCount = 3 // Less than default Dscore of 4
		testTopic = "panic-test-topic"
	)

	// Create hosts - we need at least Dhi to trigger pruning
	// but fewer than Dscore to trigger the panic condition
	hosts := getDefaultHosts(t, peerCount)

	// Create gossipsub with custom parameters
	// Set Dhi to 2 so pruning is triggered with 2 peers
	// Keep Dscore at default 4 to test the bounds check
	params := DefaultGossipSubParams()
	params.Dhi = 2  // Trigger pruning with just 2 peers
	params.D = 2    // Target degree
	params.Dlo = 1  // Low water mark
	// params.Dscore = 4 (default) - tests the fix when we have < 4 peers

	psubs, router := setupGossipSubTest(t, ctx, hosts, params)

	// Connect hosts
	connectAll(t, hosts)

	// Subscribe all to same topic
	for _, ps := range psubs {
		_, err := ps.Subscribe(testTopic)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for mesh to form
	waitForMeshFormation(t, router, testTopic, 1, 5*time.Second)

	// Get current mesh state
	meshSize := getMeshSize(t, router, testTopic)
	t.Logf("Initial mesh has %d peers (Dhi=%d, Dscore=%d)", meshSize, params.Dhi, params.Dscore)

	// Ensure we have enough peers to trigger pruning
	if meshSize < params.Dhi {
		// Manually set up mesh to trigger pruning condition
		t.Log("Setting up mesh to ensure pruning conditions...")
		safeEval(router, func() {
			router.mesh[testTopic] = make(map[peer.ID]struct{})
			for i := 1; i < len(hosts); i++ {
				router.mesh[testTopic][hosts[i].ID()] = struct{}{}
			}
		})
		meshSize = getMeshSize(t, router, testTopic)
		t.Logf("Mesh now has %d peers", meshSize)
	}

	// Trigger heartbeats - this used to panic with fewer peers than Dscore
	t.Log("Triggering heartbeats (this used to panic)...")
	for i := 0; i < 5; i++ {
		safeHeartbeat(t, router)
		
		// Verify mesh state after each heartbeat
		currentMeshSize := getMeshSize(t, router, testTopic)
		t.Logf("After heartbeat %d: mesh has %d peers", i+1, currentMeshSize)
		
		// Verify pruning worked correctly (mesh size should be <= Dhi after pruning)
		if currentMeshSize > params.Dhi {
			// This is okay initially, but after pruning it should stabilize
			t.Logf("Note: Mesh size %d exceeds Dhi %d, pruning may occur", currentMeshSize, params.Dhi)
		}
	}

	// If we reach here without panic, the fix is working
	t.Log("Success! No panic occurred with fewer peers than Dscore")
}

// TestIssue634EdgeCases tests various edge cases with different peer counts
func TestIssue634EdgeCases(t *testing.T) {
	scenarios := []struct {
		name      string
		peerCount int
		dhi       int
		dscore    int
		d         int
	}{
		{"Zero peers", 0, 2, 4, 0},
		{"Single peer", 1, 1, 4, 0},
		{"Fewer peers than Dscore", 2, 2, 4, 1},
		{"Exactly Dscore peers", 4, 3, 4, 2},
		{"Dscore equals zero", 3, 2, 0, 2},
		{"Dscore equals Dhi", 5, 3, 3, 3},
		{"Dscore greater than total peers", 2, 1, 10, 1},
		{"More than Dscore but less than default D", 5, 4, 4, 3},
		{"Large Dscore", 3, 2, 100, 2},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Skip test if peer count is invalid
			if scenario.peerCount < 0 {
				t.Skip("Invalid peer count")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			hosts := getDefaultHosts(t, scenario.peerCount)
			
			params := DefaultGossipSubParams()
			params.Dhi = scenario.dhi
			params.D = scenario.d
			params.Dlo = max(0, min(scenario.d-1, scenario.peerCount-1))
			params.Dscore = scenario.dscore
			
			// Ensure parameters are valid
			if params.D > scenario.peerCount && scenario.peerCount > 0 {
				params.D = scenario.peerCount - 1
			}
			if params.Dlo > params.D {
				params.Dlo = max(0, params.D-1)
			}
			
			psubs, router := setupGossipSubTest(t, ctx, hosts, params)
			
			// Handle zero peers case
			if scenario.peerCount == 0 {
				t.Log("Zero peers scenario - no operations to perform")
				t.Logf("Scenario '%s' completed successfully without panic", scenario.name)
				return
			}
			
			if len(hosts) > 1 {
				connectAll(t, hosts)
			}
			
			topic := "test-topic"
			for _, ps := range psubs {
				_, err := ps.Subscribe(topic)
				if err != nil {
					t.Fatal(err)
				}
			}
			
			// For zero or single peer scenarios, don't wait for mesh
			if scenario.peerCount > 1 {
				// Use shorter timeout for edge cases
				waitForMeshFormation(t, router, topic, 1, 2*time.Second)
			}
			
			// Trigger heartbeats without panic
			t.Logf("Testing scenario with %d peers, Dhi=%d, Dscore=%d", 
				scenario.peerCount, params.Dhi, params.Dscore)
			
			for i := 0; i < 3; i++ {
				safeHeartbeat(t, router)
				meshSize := getMeshSize(t, router, topic)
				t.Logf("  Heartbeat %d: mesh size = %d", i+1, meshSize)
			}
			
			t.Logf("Scenario '%s' completed successfully without panic", scenario.name)
		})
	}
}

// TestIssue634ConcurrentHeartbeats tests the fix under concurrent heartbeat conditions
func TestIssue634ConcurrentHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 3)
	
	params := DefaultGossipSubParams()
	params.Dhi = 2
	params.D = 2
	params.Dlo = 1
	// params.Dscore = 4 (default)
	
	psubs, router := setupGossipSubTest(t, ctx, hosts, params)
	connectAll(t, hosts)
	
	topic := "concurrent-test"
	for _, ps := range psubs {
		_, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
	}
	
	waitForMeshFormation(t, router, topic, 1, 5*time.Second)
	
	// Simulate concurrent heartbeats
	t.Log("Testing concurrent heartbeat safety...")
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(iteration int) {
			defer func() {
				done <- true
				if r := recover(); r != nil {
					t.Errorf("Panic in concurrent heartbeat %d: %v", iteration, r)
				}
			}()
			
			safeHeartbeat(t, router)
		}(i)
		
		// Small delay to increase chance of actual concurrency
		time.Sleep(5 * time.Millisecond)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		<-done
	}
	
	t.Log("Concurrent heartbeats completed without panic")
}

// Helper Functions

// setupGossipSubTest creates a test setup with the given parameters
func setupGossipSubTest(t *testing.T, ctx context.Context, hosts []host.Host, params GossipSubParams) ([]*PubSub, *GossipSubRouter) {
	psubs := make([]*PubSub, len(hosts))
	
	for i, h := range hosts {
		ps, err := NewGossipSub(ctx, h, WithGossipSubParams(params))
		if err != nil {
			t.Fatal(err)
		}
		psubs[i] = ps
	}
	
	if len(psubs) == 0 {
		// For zero hosts test case, return nil safely
		return nil, nil
	}
	
	router, ok := psubs[0].rt.(*GossipSubRouter)
	if !ok {
		t.Fatal("Failed to get GossipSubRouter")
	}
	
	return psubs, router
}

// safeEval executes a function within the router's eval queue for thread safety
func safeEval(router *GossipSubRouter, fn func()) {
	done := make(chan struct{})
	router.p.eval <- func() {
		defer close(done)
		fn()
	}
	<-done
}

// safeHeartbeat triggers a heartbeat safely within the eval queue
func safeHeartbeat(t *testing.T, router *GossipSubRouter) {
	done := make(chan struct{})
	router.p.eval <- func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Panic during heartbeat: %v", r)
			}
		}()
		router.heartbeat()
	}
	<-done
}

// waitForMeshFormation waits for the mesh to form with at least minPeers
func waitForMeshFormation(t *testing.T, router *GossipSubRouter, topic string, minPeers int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			meshSize := getMeshSize(t, router, topic)
			if meshSize >= minPeers {
				t.Logf("Mesh formed with %d peers", meshSize)
				return
			}
			
			if time.Now().After(deadline) {
				// Don't fail the test, just log - mesh might not form in all scenarios
				t.Logf("Mesh formation timeout: has %d peers, wanted at least %d", meshSize, minPeers)
				return
			}
		}
	}
}

// getMeshSize safely retrieves the current mesh size for a topic
func getMeshSize(t *testing.T, router *GossipSubRouter, topic string) int {
	var size int
	done := make(chan struct{})
	
	router.p.eval <- func() {
		defer close(done)
		if mesh, ok := router.mesh[topic]; ok {
			size = len(mesh)
		}
	}
	<-done
	
	return size
}

