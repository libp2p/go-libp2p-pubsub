package vivaldi

import (
	"math"
	"testing"

	peerpkg "github.com/libp2p/go-libp2p/core/peer"
)

func TestUpdateVivaldiAlgorithm1(t *testing.T) {
	res, err := UpdateVivaldi(VivaldiUpdateParams{
		Local:  VivaldiState{Coord: Coord{X: 0, Y: 0, H: 0}, Error: 1},
		Remote: VivaldiState{Coord: Coord{X: 10, Y: 0, H: 0}, Error: 1},
		RTT:    12,
		Ce:     0.25,
		Cc:     0.25,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if math.Abs(res.NewCoord.X-(-0.25)) > 1e-9 {
		t.Fatalf("unexpected new x: %.12f", res.NewCoord.X)
	}
	if math.Abs(res.NewCoord.Y) > 1e-9 || math.Abs(res.NewCoord.H) > 1e-9 {
		t.Fatalf("unexpected coord: %+v", res.NewCoord)
	}
	expectedErr := 0.125*(2.0/12.0) + 0.875*1.0
	if math.Abs(res.NewError-expectedErr) > 1e-9 {
		t.Fatalf("unexpected new error: got %.12f want %.12f", res.NewError, expectedErr)
	}
}

func TestIN3RejectsOutlierForce(t *testing.T) {
	s := &Service{
		forceRandom: []float64{10, 11, 12, 10, 9, 11, 10, 12, 9, 10},
		closePeers:  map[peerpkg.ID]struct{}{},
		randomPeers: map[peerpkg.ID]struct{}{},
	}
	cfg := UpdateConfig{IN3MADKRandom: 5, IN3MinSamples: 8}
	if err := s.checkIN3(peerpkg.ID("peer1"), 100, cfg); err == nil {
		t.Fatal("expected IN3 rejection")
	}
}
