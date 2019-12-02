# go-libp2p-pubsub

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://protocol.ai)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](http://github.com/libp2p/libp2p)
[![](https://img.shields.io/badge/freenode-%23libp2p-yellow.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23libp2p)
[![Discourse posts](https://img.shields.io/discourse/https/discuss.libp2p.io/posts.svg)](https://discuss.libp2p.io)

> A pubsub system with flooding and gossiping variants.

This is the canonical pubsub implementation for libp2p.

We currently provide three implementations:
- floodsub, which is the baseline flooding protocol.
- gossipsub, which is a more advanced router with mesh formation and gossip propagation.
  See [spec](https://github.com/libp2p/specs/tree/master/pubsub/gossipsub) and  [implementation](https://github.com/libp2p/go-libp2p-pubsub/blob/master/gossipsub.go) for more details.
- randomsub, which is a simple probabilistic router that propagates to random subsets of peers.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Tracing](#tracing)
- [Contribute](#contribute)
- [License](#license)

## Install

```
go get github.com/libp2p/go-libp2p-pubsub
```

## Usage

To be used for messaging in p2p instrastructure (as part of libp2p) such as IPFS, Ethereum, other blockchains, etc.

## Implementations

See [libp2p/specs/pubsub#Implementations](https://github.com/libp2p/specs/tree/master/pubsub#Implementations).

## Documentation

See the [libp2p specs](https://github.com/libp2p/specs/tree/master/pubsub) for high level documentation
and [godoc](https://godoc.org/github.com/libp2p/go-libp2p-pubsub) for API documentation.


## Tracing

The pubsub system supports _tracing_, which collects all events pertaining to the internals of the system.
This allows you to recreate the complete message flow and state of the system for analysis purposes.

To enable tracing, instantiate the pubsub system using the `WithEventTracer` option; the option
accepts a tracer with three available implementations in-package (trace to json, pb, or a remote peer).
If you want to trace using a remote peer, you can do so using the `traced` daemon from [go-libp2p-pubsub-tracer](https://github.com/libp2p/go-libp2p-pubsub-tracer). The package also includes a utility program, `tracestat`, for analyzing the traces collected by the daemon.

For instance, to capture the trace as a json file, you can use the following option:
```go
pubsub.NewGossipSub(..., pubsub.NewEventTracer(pubsub.NewJSONTracer("/path/to/trace.json")))
```

To capture the trace as a protobuf, you can use the following option:
```go
pubsub.NewGossipSub(..., pubsub.NewEventTracer(pubsub.NewPBTracer("/path/to/trace.pb")))
```

Finally, to use the remote tracer, you can use the following incantations:
```go
// assuming that your tracer runs in x.x.x.x and has a peer ID of QmTracer
pi, err := peer.AddrInfoFromP2pAddr(ma.StringCast("/ip4/x.x.x.x/tcp/4001/p2p/QmTracer"))
if err != nil {
  panic(err)
}

tracer, err := pubsub.NewRemoteTracer(ctx, host, pi)
if err != nil {
  panic(err)
}

ps, err := pubsub.NewGossipSub(..., pubsub.WithEventTracer(tracer))
```

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/libp2p/go-libp2p-pubsub/issues).

Check out our [contributing document](https://github.com/libp2p/community/blob/master/contributing.md) for more information on how we work, and about contributing in general. Please be aware that all interactions related to multiformats are subject to the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](LICENSE) © Jeromy Johnson

---

The last gx published version of this module was: 0.11.16: QmfB4oDUTiaGEqT13P1JqCEhqW7cB1wpKtq3PP4BN8PhQd
