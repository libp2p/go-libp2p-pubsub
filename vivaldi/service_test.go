package vivaldi

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestExchangeOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	h1, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	s1 := NewService(h1, nil)
	s2 := NewService(h2, nil)
	defer s1.Close()
	defer s2.Close()

	// Set local state on host2 so that host1 receives a non-zero coord
	s2.SetLocalState(&VivaldiState{Coord: Coord{X: 1.0, Y: 2.0, H: 0.1}, Error: 0.05})
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatal(err)
	}

	rtt, coord, _, err := s1.ExchangeOnce(ctx, h2.ID())
	if err != nil {
		t.Fatal(err)
	}
	if rtt <= 0 {
		t.Fatalf("expected positive rtt, got %v", rtt)
	}
	if coord.X != 1.0 || coord.Y != 2.0 {
		t.Fatalf("unexpected coord received: %v", coord)
	}
}
