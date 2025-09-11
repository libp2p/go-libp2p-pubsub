package pubsub

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSetupPprofHook(t *testing.T) {
	// Test that SetupPprofHook can be called without error
	err := SetupPprofHook()
	if err != nil {
		t.Fatalf("SetupPprofHook failed: %v", err)
	}

	// Test that calling it again returns no error (idempotent)
	err = SetupPprofHook()
	if err != nil {
		t.Fatalf("Second call to SetupPprofHook failed: %v", err)
	}
}

func TestSetupPprofHookSignalHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping signal handling test in short mode")
	}

	// Setup the pprof hook
	err := SetupPprofHook()
	if err != nil {
		t.Fatalf("SetupPprofHook failed: %v", err)
	}

	// Give the goroutine time to setup
	time.Sleep(100 * time.Millisecond)

	// Send SIGUSR1 to trigger profile capture
	// Note: This will create profile files in the test directory
	pid := os.Getpid()
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("Failed to find current process: %v", err)
	}

	// Send the signal
	err = process.Signal(syscall.SIGUSR1)
	if err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait a bit for the signal to be processed
	time.Sleep(200 * time.Millisecond)

	// The test passes if no panic occurs during signal handling
	t.Log("Signal handling test completed successfully")

	// Clean up any profile files that might have been created during testing
	// Note: In a real scenario, users would want to keep these files
	// but for testing we clean them up
	cleanupTestProfiles(t)
}

// Helper function to clean up test profile files
func cleanupTestProfiles(t *testing.T) {
	// This is a basic cleanup - in practice you might want to be more specific
	// about which files to remove based on timestamp patterns
	files, err := os.ReadDir(".")
	if err != nil {
		t.Logf("Could not read directory for cleanup: %v", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			name := file.Name()
			// Remove any .pprof files that look like our test files
			if len(name) > 6 && name[len(name)-6:] == ".pprof" &&
				(len(name) > 20 && (name[:20] == "pubsub-cpu-profile-" || name[:21] == "pubsub-heap-profile-")) {
				if err := os.Remove(name); err != nil {
					t.Logf("Could not remove test profile file %s: %v", name, err)
				} else {
					t.Logf("Cleaned up test profile file: %s", name)
				}
			}
		}
	}
}
