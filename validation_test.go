package pubsub

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestRegisterUnregisterValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 1)
	psubs := getPubsubs(ctx, hosts)

	err := psubs[0].RegisterTopicValidator("foo", func(context.Context, peer.ID, *Message) bool {
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[0].UnregisterTopicValidator("foo")
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[0].UnregisterTopicValidator("foo")
	if err == nil {
		t.Fatal("Unregistered bogus topic validator")
	}
}

func TestValidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	err := psubs[1].RegisterTopicValidator(topic, func(ctx context.Context, from peer.ID, msg *Message) bool {
		return !bytes.Contains(msg.Data, []byte("illegal"))
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	msgs := []struct {
		msg       []byte
		validates bool
	}{
		{msg: []byte("this is a legal message"), validates: true},
		{msg: []byte("there also is nothing controversial about this message"), validates: true},
		{msg: []byte("openly illegal content will be censored"), validates: false},
		{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
	}

	for _, tc := range msgs {
		for _, p := range psubs {
			err := p.Publish(topic, tc.msg)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case msg := <-sub.ch:
				if !tc.validates {
					t.Log(msg)
					t.Error("expected message validation to filter out the message")
				}
			case <-time.After(333 * time.Millisecond):
				if tc.validates {
					t.Error("expected message validation to accept the message")
				}
			}
		}
	}
}

func TestValidateOverload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type msg struct {
		msg       []byte
		validates bool
	}

	tcs := []struct {
		msgs []msg

		maxConcurrency int
	}{
		{
			maxConcurrency: 10,
			msgs: []msg{
				{msg: []byte("this is a legal message"), validates: true},
				{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
				{msg: []byte("there also is nothing controversial about this message"), validates: true},
				{msg: []byte("also fine"), validates: true},
				{msg: []byte("still, all good"), validates: true},
				{msg: []byte("this is getting boring"), validates: true},
				{msg: []byte("foo"), validates: true},
				{msg: []byte("foobar"), validates: true},
				{msg: []byte("foofoo"), validates: true},
				{msg: []byte("barfoo"), validates: true},
				{msg: []byte("oh no!"), validates: false},
			},
		},
		{
			maxConcurrency: 2,
			msgs: []msg{
				{msg: []byte("this is a legal message"), validates: true},
				{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
				{msg: []byte("oh no!"), validates: false},
			},
		},
	}

	for _, tc := range tcs {

		hosts := getNetHosts(t, ctx, 2)
		psubs := getPubsubs(ctx, hosts)

		connect(t, hosts[0], hosts[1])
		topic := "foobar"

		block := make(chan struct{})

		err := psubs[1].RegisterTopicValidator(topic,
			func(ctx context.Context, from peer.ID, msg *Message) bool {
				<-block
				return true
			},
			WithValidatorConcurrency(tc.maxConcurrency))

		if err != nil {
			t.Fatal(err)
		}

		sub, err := psubs[1].Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Millisecond * 50)

		if len(tc.msgs) != tc.maxConcurrency+1 {
			t.Fatalf("expected number of messages sent to be maxConcurrency+1. Got %d, expected %d", len(tc.msgs), tc.maxConcurrency+1)
		}

		p := psubs[0]

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			for _, tmsg := range tc.msgs {
				select {
				case msg := <-sub.ch:
					if !tmsg.validates {
						t.Log(msg)
						t.Error("expected message validation to drop the message because all validator goroutines are taken")
					}
				case <-time.After(time.Second):
					if tmsg.validates {
						t.Error("expected message validation to accept the message")
					}
				}
			}
			wg.Done()
		}()

		for _, tmsg := range tc.msgs {
			err := p.Publish(topic, tmsg.msg)
			if err != nil {
				t.Fatal(err)
			}
		}

		// wait a bit before unblocking the validator goroutines
		time.Sleep(500 * time.Millisecond)
		close(block)

		wg.Wait()
	}
}
