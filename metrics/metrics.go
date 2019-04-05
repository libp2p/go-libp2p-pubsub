package metrics

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

var (
	MOutgoingMsgs = stats.Int64("pubsub/outgoing_msg_size", "Outgoing message size", "By")
	MIncomingMsgs = stats.Int64("pubsub/incoming_msg_size", "Incoming message size", "By")
	MTopics       = stats.Int64("pubsub/topics", "Number of topics currently subscribed", "1")
	MPeers        = stats.Int64("pubsub/peers", "Number of pubsub peers", "1")

	OutgoingMsgCountView = &view.View{
		Name:        "pubsub/outgoing_msg_count",
		Description: "Number of outgoing messages sent",
		Measure:     MOutgoingMsgs,
		Aggregation: view.Count(),
	}

	OutgoingMsgSizeView = &view.View{
		Name:        "pubsub/outgoing_msg_size",
		Description: "Sizes of outgoing messages sent",
		Measure:     MOutgoingMsgs,
		Aggregation: view.Distribution(0, 128, 512, 1024, 1024*1024),
	}

	IncomingMsgCountView = &view.View{
		Name:        "pubsub/outgoing_msg_count",
		Description: "Number of outgoing messages sent",
		Measure:     MIncomingMsgs,
		Aggregation: view.Count(),
	}

	IncomingMsgSizeView = &view.View{
		Name:        "pubsub/outgoing_msg_size",
		Description: "Sizes of outgoing messages sent",
		Measure:     MIncomingMsgs,
		Aggregation: view.Distribution(0, 128, 512, 1024, 1024*1024),
	}

	TopicsGaugeView = &view.View{
		Name:        "pubsub/topics",
		Description: "Topics the pubsub instance is routing",
		Measure:     MTopics,
		Aggregation: view.LastValue(),
	}

	PeersGaugeView = &view.View{
		Name:        "pubsub/peers",
		Description: "Pubsub peers the host is connected to",
		Measure:     MPeers,
		Aggregation: view.LastValue(),
	}
)

func Register() error {
	return view.Register(
		OutgoingMsgCountView,
		OutgoingMsgSizeView,
		IncomingMsgCountView,
		IncomingMsgSizeView,
		TopicsGaugeView,
		PeersGaugeView,
	)
}
