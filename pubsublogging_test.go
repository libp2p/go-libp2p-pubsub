package pubsub

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

type syncBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncBuilder) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuilder) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func TestRPCLoggerIncludesTopicOnPublish(t *testing.T) {
	ctx := t.Context()

	hosts := getDefaultHosts(t, 2)

	const topic = "rpclogger-topic"

	buffers := make([]*syncBuilder, len(hosts))
	psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
		buffers[i] = &syncBuilder{}
		handler := slog.NewTextHandler(buffers[i], &slog.HandlerOptions{Level: slog.LevelDebug})
		return []Option{WithRPCLogger(slog.New(handler))}
	})

	subs := make([]*Subscription, len(psubs))
	for i, ps := range psubs {
		s, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
	}

	connect(t, hosts[0], hosts[1])

	// Let the mesh form so Publish actually sends an RPC.
	time.Sleep(2 * time.Second)

	// Snapshot logs before publish so we can confirm the topic appears
	// specifically as a result of the publish RPC (the subscription RPC's
	// topic is encoded under `topicid=`, not `topic=`).
	before := buffers[0].String() + buffers[1].String()

	if err := psubs[0].Publish(topic, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if _, err := subs[1].Next(wctx); err != nil {
		t.Fatalf("subscriber did not receive message: %v", err)
	}

	after := buffers[0].String() + buffers[1].String()
	delta := strings.TrimPrefix(after, before)

	needle := "topic=" + topic
	if !strings.Contains(delta, needle) {
		t.Fatalf("expected rpc logs after publish to contain %q\nhost0 logs:\n%s\nhost1 logs:\n%s",
			needle, buffers[0].String(), buffers[1].String())
	}
}
