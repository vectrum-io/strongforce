package bus

var DefaultSubscriptionOptions = SubscriptionOptions{
	FilterSubject:    "",
	GuaranteeOrder:   false,
	MaxDeliveryTries: 10,
}

type SubscriptionOptions struct {
	FilterSubject    string
	GuaranteeOrder   bool
	MaxDeliveryTries int
	DeliveryPolicy   DeliveryPolicy
	Durable          bool
}

type DeliveryPolicy int

const (
	DeliverAll DeliveryPolicy = iota
	DeliverNew
)

type SubscribeOption func(*SubscriptionOptions)

func WithFilterSubject(subject string) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.FilterSubject = subject
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
