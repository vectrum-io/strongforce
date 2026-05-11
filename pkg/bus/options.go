package bus

import (
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

var DefaultSubscriptionOptions = SubscriptionOptions{
	FilterSubjects:   []string{},
	GuaranteeOrder:   false,
	MaxDeliveryTries: 10,
}

type SubscriptionOptions struct {
	FilterSubjects   []string
	GuaranteeOrder   bool
	MaxDeliveryTries int
	DeliveryPolicy   DeliveryPolicy
	Durable          bool
	Deserializer     serialization.Serializer
}

type DeliveryPolicy int

const (
	DeliverAll DeliveryPolicy = iota
	DeliverNew
)

type SubscribeOption func(*SubscriptionOptions)

func WithFilterSubject(subject string) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.FilterSubjects = append(options.FilterSubjects, subject)
	}
}

// WithFilterSubjects appends each subject to the consumer's FilterSubjects list.
// Use when a single consumer needs to filter on multiple subjects (NATS >= 2.10).
// Equivalent to chaining WithFilterSubject calls but clearer at the call site.
func WithFilterSubjects(subjects ...string) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.FilterSubjects = append(options.FilterSubjects, subjects...)
	}
}

func WithGuaranteeOrder() SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.GuaranteeOrder = true
	}
}

func WithMaxDeliveryTries(maxTries int) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.MaxDeliveryTries = maxTries
	}
}

func WithDeliveryPolicy(policy DeliveryPolicy) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.DeliveryPolicy = policy
	}
}

func WithDurable() SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Durable = true
	}
}

func WithDeserializer(deserializer serialization.Serializer) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Deserializer = deserializer
	}
}
