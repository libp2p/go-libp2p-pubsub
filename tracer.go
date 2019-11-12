package pubsub

import (
	"encoding/json"
	"io"
	"os"
	"sync"
)

// JSONTracer is a tracer that writes events to a file, encoded in json.
type JSONTracer struct {
	w   io.WriteCloser
	ch  chan struct{}
	mx  sync.Mutex
	buf []interface{}
}

// NewJsonTracer creates a new JSON tracer writing to file.
func NewJSONTracer(file string) (*JSONTracer, error) {
	return OpenJSONTracer(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

// OpenJsonTracer creates a new JSON tracer, with explicit control of OpenFile flags and permissions.
func OpenJSONTracer(file string, flags int, perm os.FileMode) (*JSONTracer, error) {
	f, err := os.OpenFile(file, flags, perm)
	if err != nil {
		return nil, err
	}

	tr := &JSONTracer{w: f, ch: make(chan struct{}, 1)}
	go tr.doWrite()

	return tr, nil
}

func (t *JSONTracer) Trace(evt interface{}) {
	t.mx.Lock()
	t.buf = append(t.buf, evt)
	t.mx.Unlock()

	select {
	case t.ch <- struct{}{}:
	default:
	}
}

func (t *JSONTracer) Close() {
	close(t.ch)
}

func (t *JSONTracer) doWrite() {
	var buf []interface{}
	enc := json.NewEncoder(t.w)
	for {
		_, ok := <-t.ch

		t.mx.Lock()
		tmp := t.buf
		t.buf = buf[:0]
		buf = tmp
		t.mx.Unlock()

		for i, evt := range buf {
			err := enc.Encode(evt)
			if err != nil {
				log.Errorf("error writing event trace: %s", err.Error())
			}
			buf[i] = nil
		}

		if !ok {
			t.w.Close()
			return
		}
	}
}
