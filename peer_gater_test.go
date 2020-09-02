package pubsub

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestPeerGater(t *testing.T) {
	pg := &peerGater{threshold: 0.1, decay: .9, stats: make(map[peer.ID]*peerGaterStats)}

	peerA := peer.ID("A")
	pg.AddPeer(peerA, "")

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

	pg.RejectMessage(msg, rejectValidationQueueFull)
	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	pg.RejectMessage(msg, rejectValidationThrottled)
	status = pg.AcceptFrom(peerA)
	if status != AcceptAll {
		t.Fatal("expected AcceptAll")
	}

	for i := 0; i < 100; i++ {
		pg.RejectMessage(msg, rejectValidationIgnored)
		pg.RejectMessage(msg, rejectValidationFailed)
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

	pg.RemovePeer(peerA)
	pg.stats[peerA].expire = time.Now()

	time.Sleep(time.Millisecond)

	pg.decayStats()

	_, ok := pg.stats[peerA]
	if ok {
		t.Fatal("still have a stat record for peerA")
	}
}
