package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestMapBlacklist(t *testing.T) {
	b := NewMapBlacklist()

	p := peer.ID("test")

	b.Add(p)
	if !b.Contains(p) {
		t.Fatal("peer not in the blacklist")
	}

}

func TestTimeCachedBlacklist(t *testing.T) {
	b, err := NewTimeCachedBlacklist(10 * time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	p := peer.ID("test")

	b.Add(p)
	if !b.Contains(p) {
		t.Fatal("peer not in the blacklist")
	}
}

func TestBlacklist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)
	connect(t, hosts[0], hosts[1])

	sub, err := psubs[1].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)
	psubs[1].BlacklistPeer(hosts[0].ID())
	time.Sleep(time.Millisecond * 100)

	psubs[0].Publish("test", []byte("message"))

	wctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err = sub.Next(wctx)

	if err == nil {
		t.Fatal("got message from blacklisted peer")
	}
}

func TestBlacklist2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)
	connect(t, hosts[0], hosts[1])

	_, err := psubs[0].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := psubs[1].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)
	psubs[1].BlacklistPeer(hosts[0].ID())
	time.Sleep(time.Millisecond * 100)

	psubs[0].Publish("test", []byte("message"))

	wctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err = sub1.Next(wctx)

	if err == nil {
		t.Fatal("got message from blacklisted peer")
	}
}

func TestBlacklist3(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	psubs[1].BlacklistPeer(hosts[0].ID())
	time.Sleep(time.Millisecond * 100)
	connect(t, hosts[0], hosts[1])

	sub, err := psubs[1].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	psubs[0].Publish("test", []byte("message"))

	wctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err = sub.Next(wctx)

	if err == nil {
		t.Fatal("got message from blacklisted peer")
	}
}
