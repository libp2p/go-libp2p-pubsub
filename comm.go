package pubsub

import (
	"context"
	"errors"
	"io"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
	proto "github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ms "github.com/multiformats/go-multistream"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC
	for t := range p.mySubs {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s network.Stream) {
	r := ggio.NewDelimitedReader(s, 1<<20)
	for {
		rpc := new(RPC)
		err := r.ReadMsg(&rpc.RPC)
		if err != nil {
			if err != io.EOF {
				s.Reset()
				log.Infof("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			} else {
				// Just be nice. They probably won't read this
				// but it doesn't hurt to send it.
				s.Close()
			}
			return
		}

		rpc.from = s.Conn().RemotePeer()
		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			// Close is useless because the other side isn't reading.
			s.Reset()
			return
		}
	}
}

func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing <-chan *RPC) {
	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		log.Warning("opening new stream to peer: ", err, pid)

		var ch chan peer.ID
		if err == ms.ErrNotSupported {
			ch = p.newPeerError
		} else {
			ch = p.peerDead
		}

		select {
		case ch <- pid:
		case <-ctx.Done():
		}
		return
	}

	go p.handleSendingMessages(ctx, s, outgoing)
	go p.handlePeerEOF(ctx, s)
	select {
	case p.newPeerStream <- s:
	case <-ctx.Done():
	}
}

func (p *PubSub) handlePeerEOF(ctx context.Context, s network.Stream) {
	r := ggio.NewDelimitedReader(s, 1<<20)
	rpc := new(RPC)
	for {
		err := r.ReadMsg(&rpc.RPC)
		if err != nil {
			select {
			case p.peerDead <- s.Conn().RemotePeer():
			case <-ctx.Done():
			}
			return
		}
		log.Warning("unexpected message from ", s.Conn().RemotePeer())
	}
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing <-chan *RPC) {
	bufw := newBufferedWriter(s)
	wc := ggio.NewDelimitedWriter(bufw)

	defer bufw.Release()
	defer helpers.FullClose(s)
	for {
		select {
		case rpc, ok := <-outgoing:
			if !ok {
				return
			}

			err := wc.WriteMsg(&rpc.RPC)
			if err != nil {
				s.Reset()
				log.Infof("error writing message to %s: %s", s.Conn().RemotePeer(), err)
				return
			}

		coalesce:
			for {
				select {
				case rpc, ok = <-outgoing:
					if !ok {
						break coalesce
					}

					err = wc.WriteMsg(&rpc.RPC)
					if err != nil {
						s.Reset()
						log.Infof("error writing message to %s: %s", s.Conn().RemotePeer(), err)
						return
					}

				case <-ctx.Done():
					// don't just return, flush the buffer first
					break coalesce

				default:
					break coalesce
				}
			}

			err = bufw.Flush()
			if err != nil {
				s.Reset()
				log.Infof("error flushing messages to %s: %s", s.Conn().RemotePeer(), err)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

type bufferedWriter struct {
	w   io.Writer
	buf []byte
	n   int
}

func newBufferedWriter(w io.Writer) *bufferedWriter {
	return &bufferedWriter{w: w}
}

func (b *bufferedWriter) Write(p []byte) (nn int, err error) {
	if len(p) > 1<<20 {
		// that's the max message limit for deserialization (and the buffer size)
		// error if this happens, we will not be able to deserialize at the other size
		return 0, errors.New("max message size exceeded")
	}

	if b.buf == nil {
		b.buf = pool.Get(1 << 20)
	}

	for len(p) > b.available() && err == nil {
		// large write, doesn't fit in the buffer; we need to flush some
		n := copy(b.buf[b.n:], p)
		b.n += n
		nn += n
		p = p[n:]
		err = b.doWrite()
	}

	if err != nil {
		return nn, err
	}

	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}

func (b *bufferedWriter) Flush() (err error) {
	if b.n > 0 {
		err = b.doWrite()
	}
	pool.Put(b.buf)
	b.buf = nil
	return err
}

func (b *bufferedWriter) Release() {
	if b.buf != nil {
		pool.Put(b.buf)
		b.buf = nil
		b.n = 0
	}
}

func (b *bufferedWriter) doWrite() error {
	_, err := b.w.Write(b.buf[:b.n])
	b.n = 0
	return err
}

func (b *bufferedWriter) available() int {
	return len(b.buf) - b.n
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}

func rpcWithControl(msgs []*pb.Message,
	ihave []*pb.ControlIHave,
	iwant []*pb.ControlIWant,
	graft []*pb.ControlGraft,
	prune []*pb.ControlPrune) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Publish: msgs,
			Control: &pb.ControlMessage{
				Ihave: ihave,
				Iwant: iwant,
				Graft: graft,
				Prune: prune,
			},
		},
	}
}

func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)
	*res = *rpc
	if rpc.Control != nil {
		res.Control = new(pb.ControlMessage)
		*res.Control = *rpc.Control
	}
	return res
}
