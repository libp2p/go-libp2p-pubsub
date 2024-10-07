package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestRegisterUnregisterValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 1)
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

func TestRegisterValidatorEx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 3)
	psubs := getPubsubs(ctx, hosts)

	err := psubs[0].RegisterTopicValidator("test",
		Validator(func(context.Context, peer.ID, *Message) bool {
			return true
		}))
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[1].RegisterTopicValidator("test",
		ValidatorEx(func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		}))
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[2].RegisterTopicValidator("test", "bogus")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 2)
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
		err := psubs[0].Publish(topic, tc.msg)
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

func TestValidate2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 1)
	psubs := getPubsubs(ctx, hosts)

	topic := "foobar"

	err := psubs[0].RegisterTopicValidator(topic, func(ctx context.Context, from peer.ID, msg *Message) bool {
		return !bytes.Contains(msg.Data, []byte("illegal"))
	})
	if err != nil {
		t.Fatal(err)
	}

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
		err := psubs[0].Publish(topic, tc.msg)
		if tc.validates {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if err == nil {
				t.Fatal("expected validation to fail for this message")
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

	for tci, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tci), func(t *testing.T) {
			hosts := getDefaultHosts(t, 2)
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
		})
	}
}

func TestValidateAssortedOptions(t *testing.T) {
	// this test adds coverage for various options that are not covered in other tests
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 10)
	psubs := getPubsubs(ctx, hosts,
		WithValidateQueueSize(10),
		WithValidateThrottle(10),
		WithValidateWorkers(10))

	sparseConnect(t, hosts)

	for _, psub := range psubs {
		err := psub.RegisterTopicValidator("test1",
			func(context.Context, peer.ID, *Message) bool {
				return true
			},
			WithValidatorTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}

		err = psub.RegisterTopicValidator("test2",
			func(context.Context, peer.ID, *Message) bool {
				return true
			},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}
	}

	var subs1, subs2 []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test1")
		if err != nil {
			t.Fatal(err)
		}
		subs1 = append(subs1, sub)

		sub, err = ps.Subscribe("test2")
		if err != nil {
			t.Fatal(err)
		}
		subs2 = append(subs2, sub)
	}

	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))

		psubs[i].Publish("test1", msg)
		for _, sub := range subs1 {
			assertReceive(t, sub, msg)
		}

		psubs[i].Publish("test2", msg)
		for _, sub := range subs2 {
			assertReceive(t, sub, msg)
		}
	}
}
