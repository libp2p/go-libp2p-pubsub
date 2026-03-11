package pubsub

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2pyamux "github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	yamux "github.com/libp2p/go-yamux/v5"
)

func newTestPrometheusTracer(t *testing.T) (*prometheusTracer, string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "metrics-*.log")
	if err != nil {
		t.Fatal(err)
	}
	logPath := f.Name()
	f.Close()

	tr, err := newPrometheusTracer(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr, logPath
}

// readLogLines reads and parses all logfmt lines from the log file.
func readLogLines(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var lines []map[string]string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		m := make(map[string]string)
		for _, field := range strings.Fields(scanner.Text()) {
			k, v, ok := strings.Cut(field, "=")
			if ok {
				m[k] = v
			}
		}
		lines = append(lines, m)
	}
	return lines
}

func makeRPC(topic string, data []byte) *pb.RPC {
	return &pb.RPC{
		Publish: []*pb.Message{
			{
				Data:  data,
				Topic: &topic,
			},
		},
	}
}

func TestPrometheusTracer_OnRPCSent_Attestation(t *testing.T) {
	tr, logPath := newTestPrometheusTracer(t)
	pid := peer.ID("peer1")
	idGen := newMsgIdGenerator()

	rpc := makeRPC("beacon_attestation_subnet_0", []byte("test data"))
	tr.OnRPCSent(pid, 5*time.Millisecond, rpc, idGen)

	lines := readLogLines(t, logPath)
	var found bool
	for _, line := range lines {
		if line["msg"] == "topic_message_sent" {
			found = true
			if line["topic"] != "beacon_attestation_subnet_0" {
				t.Fatalf("expected topic=beacon_attestation_subnet_0, got %v", line["topic"])
			}
			bytes, err := strconv.Atoi(line["bytes"])
			if err != nil || bytes <= 0 {
				t.Fatalf("expected bytes > 0, got %v", line["bytes"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected topic_message_sent log line, got lines: %v", lines)
	}
}

func TestPrometheusTracer_OnRPCSent_NonAttestation(t *testing.T) {
	tr, logPath := newTestPrometheusTracer(t)
	pid := peer.ID("peer1")
	idGen := newMsgIdGenerator()

	rpc := makeRPC("beacon_block", []byte("block data"))
	tr.OnRPCSent(pid, 5*time.Millisecond, rpc, idGen)

	lines := readLogLines(t, logPath)
	var found bool
	for _, line := range lines {
		if line["msg"] == "topic_message_sent" && line["topic"] == "beacon_block" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected topic_message_sent log line for beacon_block topic, got lines: %v", lines)
	}
}

func TestPrometheusTracer_OnRPCReceived_Attestation(t *testing.T) {
	tr, logPath := newTestPrometheusTracer(t)
	pid := peer.ID("peer2")
	idGen := newMsgIdGenerator()

	rpc := makeRPC("committee_attestation", []byte("attestation data"))
	tr.OnRPCReceived(pid, 3*time.Millisecond, rpc, idGen)

	lines := readLogLines(t, logPath)
	var found bool
	for _, line := range lines {
		if line["msg"] == "topic_message_received" {
			found = true
			if line["topic"] != "committee_attestation" {
				t.Fatalf("expected topic=committee_attestation, got %v", line["topic"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected topic_message_received log line, got lines: %v", lines)
	}
}

func TestPrometheusTracer_OnPeerRTT(t *testing.T) {
	tr, logPath := newTestPrometheusTracer(t)
	pid := peer.ID("peer3")

	tr.OnPeerRTT(pid, "beacon_attestation_subnet_0", 50*time.Millisecond, "quic", "1.2.3.4:9000")

	lines := readLogLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}
	line := lines[0]
	if line["msg"] != "mesh_peer_rtt" {
		t.Fatalf("expected msg=peer_rtt, got %v", line["msg"])
	}
	if line["topic"] != "beacon_attestation_subnet_0" {
		t.Fatalf("expected topic=beacon_attestation_subnet_0, got %v", line["topic"])
	}
	if line["smoothedRTT_ms"] != "50" {
		t.Fatalf("expected smoothedRTT_ms=50, got %v", line["smoothedRTT_ms"])
	}
}

func TestPrometheusTracer_Close(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "metrics-*.log")
	if err != nil {
		t.Fatal(err)
	}
	logPath := f.Name()
	f.Close()

	tr, err := newPrometheusTracer(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestPrometheusTracer_E2E(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 2)

	logFile0, err := os.CreateTemp(t.TempDir(), "node0-metrics-*.log")
	if err != nil {
		t.Fatal(err)
	}
	logPath0 := logFile0.Name()
	logFile0.Close()

	logFile1, err := os.CreateTemp(t.TempDir(), "node1-metrics-*.log")
	if err != nil {
		t.Fatal(err)
	}
	logPath1 := logFile1.Name()
	logFile1.Close()

	psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
		var logPath string
		if i == 0 {
			logPath = logPath0
		} else {
			logPath = logPath1
		}
		mt, err := newPrometheusTracer(logPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { mt.Close() })
		return []Option{func(ps *PubSub) error {
			ps.metricsTracer = mt
			return nil
		}}
	})

	topic := "beacon_attestation_subnet_0"
	sub0, err := psubs[0].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	sub1, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	connectAll(t, hosts)

	// Wait for mesh to form
	time.Sleep(2 * time.Second)

	msg := []byte("test attestation message data")
	if err := psubs[0].Publish(topic, msg); err != nil {
		t.Fatal(err)
	}

	// Receive on node 1
	recvCtx, recvCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recvCancel()
	got, err := sub1.Next(recvCtx)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(msg) {
		t.Fatal("received wrong message data")
	}
	// Also drain node 0's own subscription
	_, _ = sub0.Next(recvCtx)

	// Give time for async send goroutine to flush logs
	time.Sleep(500 * time.Millisecond)

	// Verify node 0 log file has topic_message_sent
	lines0 := readLogLines(t, logPath0)
	foundSent := false
	for _, line := range lines0 {
		if line["msg"] == "topic_message_sent" &&
			line["topic"] == topic {
			foundSent = true
			if line["peerID"] == "" {
				t.Fatal("peerID should not be empty in sent log")
			}
			bytes, err := strconv.Atoi(line["bytes"])
			if err != nil || bytes <= 0 {
				t.Fatalf("bytes should be > 0, got %v", line["bytes"])
			}
			break
		}
	}
	if !foundSent {
		t.Fatalf("node 0 log missing topic_message_sent entry. Log lines: %v", lines0)
	}

	// Verify node 1 log file has topic_message_received
	lines1 := readLogLines(t, logPath1)
	foundRecv := false
	for _, line := range lines1 {
		if line["msg"] == "topic_message_received" &&
			line["topic"] == topic {
			foundRecv = true
			if line["peerID"] == "" {
				t.Fatal("peerID should not be empty in received log")
			}
			bytes, err := strconv.Atoi(line["bytes"])
			if err != nil || bytes <= 0 {
				t.Fatalf("bytes should be > 0, got %v", line["bytes"])
			}
			break
		}
	}
	if !foundRecv {
		t.Fatalf("node 1 log missing topic_message_received entry. Log lines: %v", lines1)
	}
}

func TestConnAs_Yamux(t *testing.T) {
	h1, err := libp2p.New(
		libp2p.NoTransports,
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer("/yamux/1.0.0", libp2pyamux.DefaultTransport),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New(
		libp2p.NoTransports,
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer("/yamux/1.0.0", libp2pyamux.DefaultTransport),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), time.Hour)
	err = h1.Connect(context.Background(), peer.AddrInfo{ID: h2.ID()})
	if err != nil {
		t.Fatal(err)
	}

	conns := h1.Network().ConnsToPeer(h2.ID())
	if len(conns) == 0 {
		t.Fatal("no connections to peer")
	}

	var ys *yamux.Session
	if ok := conns[0].As(&ys); !ok {
		t.Fatal("Conn.As(*yamux.Session) returned false for TCP+yamux connection")
	}
	t.Logf("yamux RTT: %s", ys.RTT())
}
