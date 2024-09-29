package nats

import "github.com/nats-io/nats.go/jetstream"

type SubscriberInfo struct {
	jsConsumerInfo *jetstream.ConsumerInfo
}

func (si *SubscriberInfo) HasPendingMessages() bool {
	return si.jsConsumerInfo.NumPending > 0 || si.jsConsumerInfo.NumAckPending > 0
}
