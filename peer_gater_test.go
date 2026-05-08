package pubsub

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPeerGater(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		peerA := peer.ID("A")
		peerAip := "1.2.3.4"

		params := NewPeerGaterParams(.1, .9, .999)
		err := params.validate()
		if err != nil {
			t.Fatal(err)
		}

		pg := newPeerGater(ctx, nil, params, slog.Default())
		pg.getIP = func(p peer.ID) string {
			switch p {
			case peerA:
				return peerAip
			default:
				return "<wtf>"
			}
		}

		pg.OnNewOutboundStream(peerA, "")

		status := pg.AcceptFrom(peerA)
		if status != AcceptAll {
			t.Fatal("expected AcceptAll")
		}

		msg := &Message{ReceivedFrom: peerA}

		pg.ValidateMessage(msg)
		status = pg.AcceptFrom(peerA)
		if status != AcceptAll {
			t.Fatal("expected AcceptAll")
		}

		pg.RejectMessage(msg, RejectValidationQueueFull)
		status = pg.AcceptFrom(peerA)
		if status != AcceptAll {
			t.Fatal("expected AcceptAll")
		}

		pg.RejectMessage(msg, RejectValidationThrottled)
		status = pg.AcceptFrom(peerA)
		if status != AcceptAll {
			t.Fatal("expected AcceptAll")
		}

		for i := 0; i < 100; i++ {
			pg.RejectMessage(msg, RejectValidationIgnored)
			pg.RejectMessage(msg, RejectValidationFailed)
		}

		accepted := false
		for i := 0; !accepted && i < 1000; i++ {
			status = pg.AcceptFrom(peerA)
			if status == AcceptControl {
				accepted = true
			}
		}
		if !accepted {
			t.Fatal("expected AcceptControl")
		}

		for i := 0; i < 100; i++ {
			pg.DeliverMessage(msg)
		}

		accepted = false
		for i := 0; !accepted && i < 1000; i++ {
			status = pg.AcceptFrom(peerA)
			if status == AcceptAll {
				accepted = true
			}
		}
		if !accepted {
			t.Fatal("expected to accept at least once")
		}

		for i := 0; i < 100; i++ {
			pg.decayStats()
		}

		status = pg.AcceptFrom(peerA)
		if status != AcceptAll {
			t.Fatal("expected AcceptAll")
		}

		pg.OnClosedOutboundStream(peerA)
		pg.Lock()
		_, ok := pg.peerStats[peerA]
		pg.Unlock()
		if ok {
			t.Fatal("still have a stat record for peerA")
		}

		pg.Lock()
		_, ok = pg.ipStats[peerAip]
		pg.Unlock()
		if !ok {
			t.Fatal("expected to still have a stat record for peerA's ip")
		}

		pg.Lock()
		pg.ipStats[peerAip].expire = time.Now()
		pg.Unlock()

		time.Sleep(2 * time.Second)

		pg.Lock()
		_, ok = pg.ipStats["1.2.3.4"]
		pg.Unlock()
		if ok {
			t.Fatal("still have a stat record for peerA's ip")
		}
	})
}

func TestPeerGaterRemovesInboundOnlyPeerStats(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		peerA := peer.ID("A")
		peerAip := "1.2.3.4"

		params := NewPeerGaterParams(.1, .9, .999)
		pg := newPeerGater(ctx, nil, params, slog.Default())
		pg.getIP = func(p peer.ID) string {
			if p == peerA {
				return peerAip
			}
			return "<unknown>"
		}

		msg := &Message{ReceivedFrom: peerA}
		pg.RejectMessage(msg, RejectValidationFailed)

		pg.Lock()
		_, ok := pg.peerStats[peerA]
		pg.Unlock()
		if !ok {
			t.Fatal("expected peer stats after validation event")
		}

		pg.OnClosedIncomingStream(peerA, "")

		pg.Lock()
		_, peerStatsOK := pg.peerStats[peerA]
		ipStats, ipStatsOK := pg.ipStats[peerAip]
		connected := 0
		expireIsZero := true
		if ipStatsOK {
			connected = ipStats.connected
			expireIsZero = ipStats.expire.IsZero()
		}
		pg.Unlock()
		if peerStatsOK {
			t.Fatal("inbound close should remove peer stats")
		}
		if !ipStatsOK {
			t.Fatal("expected retained ip stats")
		}
		if connected != 0 {
			t.Fatalf("expected no connected streams, got %d", connected)
		}
		if expireIsZero {
			t.Fatal("expected retained ip stats to have an expiration")
		}

		pg.OnClosedIncomingStream(peerA, "")
		pg.Lock()
		_, peerStatsOK = pg.peerStats[peerA]
		pg.Unlock()
		if peerStatsOK {
			t.Fatal("duplicate inbound close should not recreate peer stats")
		}
	})
}

func TestPeerGaterInboundCloseDoesNotBypassOutboundCleanup(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		peerA := peer.ID("A")
		peerAip := "1.2.3.4"

		params := NewPeerGaterParams(.1, .9, .999)
		pg := newPeerGater(ctx, nil, params, slog.Default())
		pg.getIP = func(p peer.ID) string {
			if p == peerA {
				return peerAip
			}
			return "<unknown>"
		}

		msg := &Message{ReceivedFrom: peerA}
		pg.OnNewOutboundStream(peerA, "")
		pg.RejectMessage(msg, RejectValidationFailed)
		pg.OnClosedIncomingStream(peerA, "")

		pg.Lock()
		_, peerStatsOK := pg.peerStats[peerA]
		connected := pg.ipStats[peerAip].connected
		pg.Unlock()
		if !peerStatsOK {
			t.Fatal("inbound close should not remove stats while outbound stream is active")
		}
		if connected != 1 {
			t.Fatalf("expected one connected outbound stream, got %d", connected)
		}

		pg.OnClosedOutboundStream(peerA)

		pg.Lock()
		_, peerStatsOK = pg.peerStats[peerA]
		connected = pg.ipStats[peerAip].connected
		pg.Unlock()
		if peerStatsOK {
			t.Fatal("outbound close should remove peer stats")
		}
		if connected != 0 {
			t.Fatalf("expected no connected streams after outbound close, got %d", connected)
		}
	})
}
