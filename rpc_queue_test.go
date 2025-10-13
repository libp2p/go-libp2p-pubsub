package pubsub

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestNewRpcQueue(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)
	if q.maxSize != maxSize {
		t.Fatalf("rpc queue has wrong max size, expected %d but got %d", maxSize, q.maxSize)
	}
	if q.dataAvailable.L != &q.queueMu {
		t.Fatalf("the dataAvailable field of rpc queue has an incorrect mutex")
	}
	if q.spaceAvailable.L != &q.queueMu {
		t.Fatalf("the spaceAvailable field of rpc queue has an incorrect mutex")
	}
}

func TestRpcQueueUrgentPush(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)

	rpc1 := &RPC{}
	rpc2 := &RPC{}
	rpc3 := &RPC{}
	rpc4 := &RPC{}
	q.Push(rpc1, true)
	q.UrgentPush(rpc2, true)
	q.Push(rpc3, true)
	q.UrgentPush(rpc4, true)
	pop1, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pop2, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pop3, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pop4, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pop1 != rpc2 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
	if pop2 != rpc4 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
	if pop3 != rpc1 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
	if pop4 != rpc3 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
}

func TestRpcQueuePushThenPop(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)

	rpc1 := &RPC{}
	rpc2 := &RPC{}
	q.Push(rpc1, true)
	q.Push(rpc2, true)
	pop1, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pop2, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pop1 != rpc1 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
	if pop2 != rpc2 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
}

func TestRpcQueuePopThenPush(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)

	rpc1 := &RPC{}
	rpc2 := &RPC{}
	go func() {
		// Wait to make sure the main goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		q.Push(rpc1, true)
		q.Push(rpc2, true)
	}()
	pop1, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pop2, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pop1 != rpc1 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
	if pop2 != rpc2 {
		t.Fatalf("get wrong item from rpc queue Pop")
	}
}

func TestRpcQueueBlockPushWhenFull(t *testing.T) {
	maxSize := 1
	q := newRpcQueue(maxSize)

	finished := make(chan struct{})
	q.Push(&RPC{}, true)
	go func() {
		q.Push(&RPC{}, true)
		finished <- struct{}{}
	}()
	// Wait to make sure the goroutine is blocked.
	time.Sleep(1 * time.Millisecond)
	select {
	case <-finished:
		t.Fatalf("blocking rpc queue Push is not blocked when it is full")
	default:
	}
}

func TestRpcQueueNonblockPushWhenFull(t *testing.T) {
	maxSize := 1
	q := newRpcQueue(maxSize)

	q.Push(&RPC{}, true)
	err := q.Push(&RPC{}, false)
	if err != ErrQueueFull {
		t.Fatalf("non-blocking rpc queue Push returns wrong error when it is full")
	}
}

func TestRpcQueuePushAfterClose(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)
	q.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("rpc queue Push does not panick after closed")
		}
	}()
	q.Push(&RPC{}, true)
}

func TestRpcQueuePopAfterClose(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)
	q.Close()
	_, err := q.Pop(context.Background())
	if err != ErrQueueClosed {
		t.Fatalf("rpc queue Pop returns wrong error after closed")
	}
}

func TestRpcQueueCloseWhilePush(t *testing.T) {
	maxSize := 1
	q := newRpcQueue(maxSize)
	q.Push(&RPC{}, true)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("rpc queue Push does not panick when it's closed on the fly")
		}
	}()

	go func() {
		// Wait to make sure the main goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		q.Close()
	}()
	q.Push(&RPC{}, true)
}

func TestRpcQueueCloseWhilePop(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)
	go func() {
		// Wait to make sure the main goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		q.Close()
	}()
	_, err := q.Pop(context.Background())
	if err != ErrQueueClosed {
		t.Fatalf("rpc queue Pop returns wrong error when it's closed on the fly")
	}
}

func TestRpcQueuePushWhenFullThenPop(t *testing.T) {
	maxSize := 1
	q := newRpcQueue(maxSize)

	q.Push(&RPC{}, true)
	go func() {
		// Wait to make sure the main goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		q.Pop(context.Background())
	}()
	q.Push(&RPC{}, true)
}

func TestRpcQueueCancelPop(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Wait to make sure the main goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	_, err := q.Pop(ctx)
	if err != ErrQueueCancelled {
		t.Fatalf("rpc queue Pop returns wrong error when it's cancelled")
	}
}

func TestRPCQueueCancellations(t *testing.T) {
	maxSize := 32
	q := newRpcQueue(maxSize)

	getMesssages := func(n int) ([]*pb.Message, []string) {
		msgs := make([]*pb.Message, n)
		msgIDs := make([]string, n)
		for i := range msgs {
			msgs[i] = &pb.Message{Data: []byte(fmt.Sprintf("message%d", i+1))}
			msgIDs[i] = fmt.Sprintf("msg%d", i+1)
		}
		return msgs, msgIDs
	}

	t.Run("cancel all", func(t *testing.T) {
		msgs, msgIDs := getMesssages(10)
		rpc := &RPC{
			RPC:        pb.RPC{Publish: slices.Clone(msgs)},
			MessageIDs: slices.Clone(msgIDs),
		}
		q.Push(rpc, true)
		q.CancelMessages(msgIDs)
		popped, err := q.Pop(context.Background())
		if err != nil {
			t.Fatalf("failed to pop RPC: %v", err)
		}
		if len(popped.Publish) != 0 {
			t.Fatalf("expected popped.Publish to be empty, got %v", popped.Publish)
		}
		if len(popped.MessageIDs) != 0 {
			t.Fatalf("expected popped.MsgIDs to be empty, got %v", popped.MessageIDs)
		}
		if len(q.queuedMessageIDs) != 0 {
			t.Fatalf("expected q.queuedMsgIDs to be empty, got %v", q.queuedMessageIDs)
		}
		if len(q.cancelledMessageIDs) != 0 {
			t.Fatalf("expected q.cancelledIDs to be empty, got %v", q.cancelledMessageIDs)
		}
	})

	t.Run("cancel some", func(t *testing.T) {
		msgs, msgIDs := getMesssages(10)
		rpc := &RPC{
			RPC:        pb.RPC{Publish: slices.Clone(msgs)},
			MessageIDs: slices.Clone(msgIDs),
		}
		q.Push(rpc, true)
		q.CancelMessages(msgIDs[:3])
		popped, err := q.Pop(context.Background())
		if err != nil {
			t.Fatalf("failed to pop RPC: %v", err)
		}
		if !slices.Equal(msgs[3:], popped.Publish) {
			t.Fatalf("expected popped.Publish to be %v, got %v", msgs[3:], popped.Publish)
		}
		if !slices.Equal(msgIDs[3:], popped.MessageIDs) {
			t.Fatalf("expected popped.MsgIDs to be %v, got %v", msgIDs[3:], popped.MessageIDs)
		}
		if len(q.queuedMessageIDs) != 0 {
			t.Fatalf("expected q.queuedMsgIDs to be empty, got %v", q.queuedMessageIDs)
		}
		if len(q.cancelledMessageIDs) != 0 {
			t.Fatalf("expected q.cancelledIDs to be empty, got %v", q.cancelledMessageIDs)
		}
	})

	t.Run("cancel duplicate", func(t *testing.T) {
		msgs, msgIDs := getMesssages(10)
		rpc := &RPC{
			RPC:        pb.RPC{Publish: slices.Clone(msgs)},
			MessageIDs: slices.Clone(msgIDs),
		}
		q.Push(rpc, true)
		rpc2 := &RPC{
			RPC:        pb.RPC{Publish: slices.Clone(msgs)},
			MessageIDs: slices.Clone(msgIDs),
		}
		q.Push(rpc2, true)
		q.CancelMessages(msgIDs[:3])
		popped, err := q.Pop(context.Background())
		if err != nil {
			t.Fatalf("failed to pop RPC: %v", err)
		}
		if !slices.Equal(msgs[3:], popped.Publish) {
			t.Fatalf("expected popped.Publish to be %v, got %v", msgs[3:], popped.Publish)
		}
		if !slices.Equal(msgIDs[3:], popped.MessageIDs) {
			t.Fatalf("expected popped.MsgIDs to be %v, got %v", msgIDs[3:], popped.MessageIDs)
		}

		popped, err = q.Pop(context.Background())
		if err != nil {
			t.Fatalf("failed to pop RPC: %v", err)
		}
		if !slices.Equal(msgs[3:], popped.Publish) {
			t.Fatalf("expected popped.Publish to be %v, got %v", msgs[3:], popped.Publish)
		}
		if !slices.Equal(msgIDs[3:], popped.MessageIDs) {
			t.Fatalf("expected popped.MsgIDs to be %v, got %v", msgIDs[3:], popped.MessageIDs)
		}
		if len(q.queuedMessageIDs) != 0 {
			t.Fatalf("expected q.queuedMsgIDs to be empty, got %v", q.queuedMessageIDs)
		}
		if len(q.cancelledMessageIDs) != 0 {
			t.Fatalf("expected q.cancelledIDs to be empty, got %v", q.cancelledMessageIDs)
		}
	})
}
