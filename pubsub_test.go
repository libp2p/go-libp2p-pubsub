package pubsub

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// For all tests to work reliably in parallel,
	// we require being able to open many files at a time.
	// Many environments have lower limits,
	// such as Linux's "ulimit -n" soft limit defaulting to 1024.
	// On a machine with 16 CPU threads, 8k seems to be enough.
	const wantFileLimit = 8 << 10
	files := make([]*os.File, 0, wantFileLimit)
	for i := 0; i < wantFileLimit; i++ {
		file, err := os.Open("pubsub_test.go")
		if err != nil {
			if i == 0 {
				panic(err) // the file doesn't exist?
			}
			fmt.Fprintf(os.Stderr, "Skipping parallel runs of tests; open file limit %d is below %d.\n",
				i, wantFileLimit)
			fmt.Fprintf(os.Stderr, "On Linux, consider running: ulimit -Sn %d\n",
				wantFileLimit*2)
			canRunInParallel = false
			break
		}
		files = append(files, file)
	}
	for _, file := range files {
		file.Close()
	}
	os.Exit(m.Run())
}

var canRunInParallel = true

func tryParallel(t *testing.T) {
	if canRunInParallel {
		t.Parallel()
	}
}

// See https://github.com/libp2p/go-libp2p-pubsub/issues/426
func TestPubSubRemovesBlacklistedPeer(t *testing.T) {
	tryParallel(t)

	ctx, cancel := context.WithCancel(context.Background())

	hosts := getNetHosts(t, ctx, 2)

	bl := NewMapBlacklist()

	psubs0 := getPubsub(ctx, hosts[0])
	psubs1 := getPubsub(ctx, hosts[1], WithBlacklist(bl))
	connect(t, hosts[0], hosts[1])

	// Bad peer is blacklisted after it has connected.
	// Calling p.BlacklistPeer directly does the right thing but we should also clean
	// up the peer if it has been added the the blacklist by another means.
	bl.Add(hosts[0].ID())

	_, err := psubs0.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := psubs1.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	psubs0.Publish("test", []byte("message"))

	wctx, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()

	_, _ = sub1.Next(wctx)

	// Explicitly cancel context so PubSub cleans up peer channels.
	// Issue 426 reports a panic due to a peer channel being closed twice.
	cancel()
	time.Sleep(time.Millisecond * 100)
}
