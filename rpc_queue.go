package pubsub

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/metric"
)

var (
	ErrQueueCancelled    = errors.New("rpc queue operation cancelled")
	ErrQueueClosed       = errors.New("rpc queue closed")
	ErrQueueFull         = errors.New("rpc queue full")
	ErrQueuePushOnClosed = errors.New("push on closed rpc queue")
)

type rpcAndMsgIDs struct {
	RPC    *RPC
	MsgIDs []string
}

type priorityQueue struct {
	normal   []rpcAndMsgIDs
	priority []rpcAndMsgIDs
}

func (q *priorityQueue) Len() int {
	return len(q.normal) + len(q.priority)
}

func (q *priorityQueue) NormalPush(rpc *RPC, msgIDs []string) {
	q.normal = append(q.normal, rpcAndMsgIDs{RPC: rpc, MsgIDs: msgIDs})
}

func (q *priorityQueue) PriorityPush(rpc *RPC, msgIDs []string) {
	q.priority = append(q.priority, rpcAndMsgIDs{RPC: rpc, MsgIDs: msgIDs})
}

func (q *priorityQueue) Pop() rpcAndMsgIDs {
	var rpc rpcAndMsgIDs

	if len(q.priority) > 0 {
		rpc = q.priority[0]
		q.priority[0] = rpcAndMsgIDs{}
		q.priority = q.priority[1:]
	} else if len(q.normal) > 0 {
		rpc = q.normal[0]
		q.normal[0] = rpcAndMsgIDs{}
		q.normal = q.normal[1:]
	}

	return rpc
}

type rpcQueue struct {
	dataAvailable  sync.Cond
	spaceAvailable sync.Cond
	lateIDONTWANTs metric.Int64Counter
	// Mutex used to access queue
	queueMu      sync.Mutex
	queue        priorityQueue
	queuedMsgIDs map[string]struct{}

	closed  bool
	maxSize int
}

func newRpcQueue(maxSize int, lateIDONTWANTs metric.Int64Counter) *rpcQueue {
	q := &rpcQueue{maxSize: maxSize, lateIDONTWANTs: lateIDONTWANTs}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	q.queuedMsgIDs = make(map[string]struct{})
	return q
}

func (q *rpcQueue) Push(rpc *RPC, block bool, msgIDs []string) error {
	return q.push(rpc, false, block, msgIDs)
}

func (q *rpcQueue) UrgentPush(rpc *RPC, block bool, msgIDs []string) error {
	return q.push(rpc, true, block, msgIDs)
}

func (q *rpcQueue) Cancel(msgID string) {
	var found bool
	q.queueMu.Lock()
	_, found = q.queuedMsgIDs[msgID]
	if found {
		delete(q.queuedMsgIDs, msgID)
	}
	q.queueMu.Unlock()
	if found {
		q.lateIDONTWANTs.Add(context.Background(), 1)
	}
}

func (q *rpcQueue) push(rpc *RPC, urgent bool, block bool, msgIDs []string) error {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		panic(ErrQueuePushOnClosed)
	}

	for q.queue.Len() == q.maxSize {
		if block {
			q.spaceAvailable.Wait()
			// It can receive a signal because the queue is closed.
			if q.closed {
				panic(ErrQueuePushOnClosed)
			}
		} else {
			return ErrQueueFull
		}
	}
	if urgent {
		q.queue.PriorityPush(rpc, msgIDs)
	} else {
		q.queue.NormalPush(rpc, msgIDs)
	}
	for _, msgID := range msgIDs {
		q.queuedMsgIDs[msgID] = struct{}{}
	}

	q.dataAvailable.Signal()
	return nil
}

// Note that, when the queue is empty and there are two blocked Pop calls, it
// doesn't mean that the first Pop will get the item from the next Push. The
// second Pop will probably get it instead.
func (q *rpcQueue) Pop(ctx context.Context) (*RPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	unregisterAfterFunc := context.AfterFunc(ctx, func() {
		// Wake up all the waiting routines. The only routine that correponds
		// to this Pop call will return from the function. Note that this can
		// be expensive, if there are too many waiting routines.
		q.dataAvailable.Broadcast()
	})
	defer unregisterAfterFunc()

	for q.queue.Len() == 0 {
		select {
		case <-ctx.Done():
			return nil, ErrQueueCancelled
		default:
		}
		q.dataAvailable.Wait()
		// It can receive a signal because the queue is closed.
		if q.closed {
			return nil, ErrQueueClosed
		}
	}
	r := q.queue.Pop()
	for _, msgID := range r.MsgIDs {
		delete(q.queuedMsgIDs, msgID)
	}
	q.spaceAvailable.Signal()
	return r.RPC, nil
}

func (q *rpcQueue) Close() {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	q.closed = true
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}
