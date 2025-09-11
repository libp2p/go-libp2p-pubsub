package pubsub

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var pprofLog = logging.Logger("pubsub-pprof")

var (
	pprofHookOnce sync.Once
	pprofHookErr  error
)

// setupPprofHook sets up a signal handler to capture pprof traces when SIGUSR1 is received.
// This function can only be called once per process and is thread-safe.
func setupPprofHook() error {
	pprofHookOnce.Do(func() {
		pprofHookErr = setupPprofHookOnce()
	})
	return pprofHookErr
}

func setupPprofHookOnce() error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)

	go func() {
		for {
			select {
			case <-sigChan:
				pprofLog.Info("SIGUSR1 received, capturing pprof trace")
				if err := capturePprofTrace(); err != nil {
					pprofLog.Errorf("Failed to capture pprof trace: %v", err)
				}
			}
		}
	}()

	pprofLog.Info("pprof hook setup complete, send SIGUSR1 to capture trace")
	return nil
}

// capturePprofTrace captures a CPU profile and writes it to a timestamped file
func capturePprofTrace() error {
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("pubsub-cpu-profile-%s.pprof", timestamp)

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile file %s: %w", filename, err)
	}
	defer file.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(file); err != nil {
		return fmt.Errorf("could not start CPU profile: %w", err)
	}

	pprofLog.Infof("Starting CPU profile capture, writing to %s", filename)

	// Profile for 30 seconds
	time.Sleep(30 * time.Second)

	// Stop CPU profiling
	pprof.StopCPUProfile()

	pprofLog.Infof("CPU profile capture completed, saved to %s", filename)

	// Also capture a heap profile
	return captureHeapProfile(timestamp)
}

// captureHeapProfile captures a heap profile and writes it to a timestamped file
func captureHeapProfile(timestamp string) error {
	filename := fmt.Sprintf("pubsub-heap-profile-%s.pprof", timestamp)

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create heap profile file %s: %w", filename, err)
	}
	defer file.Close()

	// Force a GC to get up-to-date heap information
	runtime.GC()

	// Write heap profile
	if err := pprof.WriteHeapProfile(file); err != nil {
		return fmt.Errorf("could not write heap profile: %w", err)
	}

	pprofLog.Infof("Heap profile saved to %s", filename)
	return nil
}
