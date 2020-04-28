package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
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

func TestRegisterValidatorEx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
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

func TestValidateAssortedOptions(t *testing.T) {
	// this test adds coverage for various options that are not covered in other tests
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
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

func TestValidateMultitopic(t *testing.T) {
	// this test adds coverage for multi-topic validation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
	psubs := getPubsubs(ctx, hosts[1:], WithMessageSigning(false))
	for _, ps := range psubs {
		err := ps.RegisterTopicValidator("test1", func(context.Context, peer.ID, *Message) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test2", func(context.Context, peer.ID, *Message) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test3", func(context.Context, peer.ID, *Message) bool {
			return false
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	publisher := &multiTopicPublisher{ctx: ctx, h: hosts[0]}
	hosts[0].SetStreamHandler(FloodSubID, publisher.handleStream)

	connectAll(t, hosts)

	var subs1, subs2, subs3 []*Subscription
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

		sub, err = ps.Subscribe("test3")
		if err != nil {
			t.Fatal(err)
		}
		subs3 = append(subs2, sub)
	}

	time.Sleep(time.Second)

	msg1 := "i am a walrus"

	// this goes to test1 and test2, which is accepted and should be delivered
	publisher.publish(msg1, "test1", "test2")
	for _, sub := range subs1 {
		assertReceive(t, sub, []byte(msg1))
	}
	for _, sub := range subs2 {
		assertReceive(t, sub, []byte(msg1))
	}

	// this goes to test2 and test3, which is rejected by the test3 validator and should not be delivered
	msg2 := "i am not a walrus"
	publisher.publish(msg2, "test2", "test3")

	expectNoMessage := func(sub *Subscription) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		m, err := sub.Next(ctx)
		if err == nil {
			t.Fatal("expected no message, but got ", string(m.Data))
		}
	}

	for _, sub := range subs2 {
		expectNoMessage(sub)
	}

	for _, sub := range subs3 {
		expectNoMessage(sub)
	}
}

func TestValidateMultitopicEx(t *testing.T) {
	// this test adds coverage for multi-topic validation with extended validators
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
	psubs := getPubsubs(ctx, hosts[1:], WithMessageSigning(false))
	for _, ps := range psubs {
		err := ps.RegisterTopicValidator("test1", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test2", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test3", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationIgnore
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test4", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationReject
		})
		if err != nil {
			t.Fatal(err)
		}

		// this is a bogus validator that returns an invalid validation result; the system should interpret
		// this as ValidationIgnore and not crash or do anything other than ignore the message
		err = ps.RegisterTopicValidator("test5", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationResult(1234)
		})
		if err != nil {
			t.Fatal(err)
		}

	}

	publisher := &multiTopicPublisher{ctx: ctx, h: hosts[0]}
	hosts[0].SetStreamHandler(FloodSubID, publisher.handleStream)

	connectAll(t, hosts)

	var subs1, subs2, subs3, subs4, subs5 []*Subscription
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

		sub, err = ps.Subscribe("test3")
		if err != nil {
			t.Fatal(err)
		}
		subs3 = append(subs3, sub)

		sub, err = ps.Subscribe("test4")
		if err != nil {
			t.Fatal(err)
		}
		subs4 = append(subs4, sub)

		sub, err = ps.Subscribe("test5")
		if err != nil {
			t.Fatal(err)
		}
		subs4 = append(subs5, sub)

	}

	time.Sleep(time.Second)

	msg1 := "i am a walrus"

	// this goes to test1 and test2, which is accepted and should be delivered
	publisher.publish(msg1, "test1", "test2")
	for _, sub := range subs1 {
		assertReceive(t, sub, []byte(msg1))
	}
	for _, sub := range subs2 {
		assertReceive(t, sub, []byte(msg1))
	}

	// this goes to test2 and test3, which is ignored by the test3 validator and should not be delivered
	msg2 := "i am not a walrus"
	publisher.publish(msg2, "test2", "test3")

	expectNoMessage := func(sub *Subscription) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		m, err := sub.Next(ctx)
		if err == nil {
			t.Fatal("expected no message, but got ", string(m.Data))
		}
	}

	for _, sub := range subs2 {
		expectNoMessage(sub)
	}

	for _, sub := range subs3 {
		expectNoMessage(sub)
	}

	// this goes to test2 and test4, which is rejected by the test4 validator and should not be delivered
	publisher.publish(msg2, "test2", "test4")

	for _, sub := range subs2 {
		expectNoMessage(sub)
	}

	for _, sub := range subs4 {
		expectNoMessage(sub)
	}

	// this goes to test3 and test4, which is rejected by the test4 validator and should not be delivered
	publisher.publish(msg2, "test3", "test4")

	for _, sub := range subs3 {
		expectNoMessage(sub)
	}

	for _, sub := range subs4 {
		expectNoMessage(sub)
	}

	// this goes to test1 and test5, which by virtue of its bogus validator should result on the message
	// being ignored
	publisher.publish(msg2, "test1", "test5")

	for _, sub := range subs1 {
		expectNoMessage(sub)
	}

	for _, sub := range subs5 {
		expectNoMessage(sub)
	}
}

func TestValidateMultitopicEx2(t *testing.T) {
	// like the previous test, but with all validators inline
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
	psubs := getPubsubs(ctx, hosts[1:], WithMessageSigning(false))
	for _, ps := range psubs {
		err := ps.RegisterTopicValidator("test1", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test2", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationAccept
		},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test3", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationIgnore
		},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}

		err = ps.RegisterTopicValidator("test4", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationReject
		},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}

		// this is a bogus validator that returns an invalid validation result; the system should interpret
		// this as ValidationIgnore and not crash or do anything other than ignore the message
		err = ps.RegisterTopicValidator("test5", func(context.Context, peer.ID, *Message) ValidationResult {
			return ValidationResult(1234)
		},
			WithValidatorInline(true))
		if err != nil {
			t.Fatal(err)
		}

	}

	publisher := &multiTopicPublisher{ctx: ctx, h: hosts[0]}
	hosts[0].SetStreamHandler(FloodSubID, publisher.handleStream)

	connectAll(t, hosts)

	var subs1, subs2, subs3, subs4, subs5 []*Subscription
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

		sub, err = ps.Subscribe("test3")
		if err != nil {
			t.Fatal(err)
		}
		subs3 = append(subs3, sub)

		sub, err = ps.Subscribe("test4")
		if err != nil {
			t.Fatal(err)
		}
		subs4 = append(subs4, sub)

		sub, err = ps.Subscribe("test5")
		if err != nil {
			t.Fatal(err)
		}
		subs4 = append(subs5, sub)

	}

	time.Sleep(time.Second)

	msg1 := "i am a walrus"

	// this goes to test1 and test2, which is accepted and should be delivered
	publisher.publish(msg1, "test1", "test2")
	for _, sub := range subs1 {
		assertReceive(t, sub, []byte(msg1))
	}
	for _, sub := range subs2 {
		assertReceive(t, sub, []byte(msg1))
	}

	// this goes to test2 and test3, which is ignored by the test3 validator and should not be delivered
	msg2 := "i am not a walrus"
	publisher.publish(msg2, "test2", "test3")

	expectNoMessage := func(sub *Subscription) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		m, err := sub.Next(ctx)
		if err == nil {
			t.Fatal("expected no message, but got ", string(m.Data))
		}
	}

	for _, sub := range subs2 {
		expectNoMessage(sub)
	}

	for _, sub := range subs3 {
		expectNoMessage(sub)
	}

	// this goes to test2 and test4, which is rejected by the test4 validator and should not be delivered
	publisher.publish(msg2, "test2", "test4")

	for _, sub := range subs2 {
		expectNoMessage(sub)
	}

	for _, sub := range subs4 {
		expectNoMessage(sub)
	}

	// this goes to test3 and test4, which is rejected by the test4 validator and should not be delivered
	publisher.publish(msg2, "test3", "test4")

	for _, sub := range subs3 {
		expectNoMessage(sub)
	}

	for _, sub := range subs4 {
		expectNoMessage(sub)
	}

	// this goes to test1 and test5, which by virtue of its bogus validator should result on the message
	// being ignored
	publisher.publish(msg2, "test1", "test5")

	for _, sub := range subs1 {
		expectNoMessage(sub)
	}

	for _, sub := range subs5 {
		expectNoMessage(sub)
	}
}

type multiTopicPublisher struct {
	ctx context.Context
	h   host.Host

	mx     sync.Mutex
	out    []network.Stream
	mcount int
}

func (p *multiTopicPublisher) handleStream(s network.Stream) {
	defer s.Close()

	os, err := p.h.NewStream(p.ctx, s.Conn().RemotePeer(), FloodSubID)
	if err != nil {
		panic(err)
	}

	p.mx.Lock()
	p.out = append(p.out, os)
	p.mx.Unlock()

	r := ggio.NewDelimitedReader(s, 1<<20)
	var rpc pb.RPC
	for {
		rpc.Reset()
		err = r.ReadMsg(&rpc)
		if err != nil {
			if err != io.EOF {
				s.Reset()
			}
			return
		}
	}
}

func (p *multiTopicPublisher) publish(msg string, topics ...string) {
	p.mcount++
	rpc := &pb.RPC{
		Publish: []*pb.Message{
			&pb.Message{
				From:     []byte(p.h.ID()),
				Data:     []byte(msg),
				Seqno:    []byte{byte(p.mcount)},
				TopicIDs: topics,
			},
		},
	}

	p.mx.Lock()
	defer p.mx.Unlock()
	for _, os := range p.out {
		w := ggio.NewDelimitedWriter(os)
		err := w.WriteMsg(rpc)
		if err != nil {
			panic(err)
		}
	}
}
