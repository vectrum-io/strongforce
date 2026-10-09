<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/vectrum-io/strongforce">
    <img src="assets/logo.png" alt="Logo" width="200">
  </a>

<h3 align="center">Strongforce</h3>

  <p align="center">
    Create highly consistent microservice architectures
    <br />
    <a href="https://github.com/vectrum-io/strongforce"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/vectrum-io/strongforce">View Demo</a>
    ·
    <a href="https://github.com/vectrum-io/strongforce/issues">Report Bug</a>
    ·
    <a href="https://github.com/vectrum-io/strongforce/issues">Request Feature</a>
  </p>
</div>

<!-- ABOUT THE PROJECT -->
## About The Project

Create highly consistent microservice architectures using the outbox pattern combined with the NATS eventing platform.

<!-- GETTING STARTED -->
## Getting Started

TODO

### Prerequisites

TODO

<!-- USAGE EXAMPLES -->
## Usage

### Subscribing

Declare the handlers of a subscription with `bus.Handle`. The consumer only receives the subjects it has handlers for, and the payload is decoded into the handler's event type:

```go
sub, err := sf.Bus().Subscribe(ctx, "incidents", "tasks",
	bus.WithDurable(),
	bus.Handle("tasks.v1.task.updated.assignee", handleTaskAssigned),
)
if err != nil {
	return err
}
sub.Start(ctx)

func handleTaskAssigned(ctx context.Context, event *eventsv1.TaskAssignedEvent, msg bus.InboundMessage) error {
	// ...
}
```

A handler settles its message by what it returns:

| Return | Outcome |
|---|---|
| `nil` | acked |
| `bus.Skip("incident %s was deleted", id)` | acked, logged and counted as skipped |
| any other error | retried with exponential backoff, dead-lettered once the attempts are used up |
| `bus.Permanent(err)` | dead-lettered right away; undecodable payloads are permanent |

Handlers run with a deadline of nine tenths of the consumer's AckWait, so the broker does not redeliver a message that is still being handled. Raise it with `bus.WithAckWait` for slow handlers.

`nats.Options.Middleware` wraps the handlers of every subscription of a bus, e.g. to prepare the handler context. Handler tests build messages with `bustest.Message(id, subject)`.

### Batches and pinned consumers

`bus.HandleBatch` hands the messages of a subject to the handler in batches of up to `bus.BatchSize(n)`, collected for at most `bus.BatchWait(d)` after the first one. Batches run one at a time; the handler returns one outcome per message (`nil`, `bus.Skip`, `bus.Permanent` or another error) and every message is settled by its own outcome. While a batch runs, its messages and the ones waiting for the next batch are kept from redelivery with `InProgress` every quarter AckWait; a batch is cancelled after ten AckWaits.

`bus.WithPinnedPriorityGroup(group, ttl)` (NATS 2.11+) lets only one of all subscribers of the consumer receive messages at a time. The subscriber keeps pulling while a batch runs, so it keeps the pin; when it stops pulling the server moves the pin after `ttl`. `Subscription.Shutdown(ctx)` finishes the running batch, releases waiting messages for immediate redelivery and gives up the pin right away:

```go
subscription, err := sf.Bus().Subscribe(ctx, "alerts-aggregation-p00", "alert-aggregation",
	bus.HandleBatch("aggregator.v2.alerts.created.0.>", handleAlerts, bus.BatchSize(500), bus.BatchWait(10*time.Second)),
	bus.WithDurable(),
	bus.WithPinnedPriorityGroup("aggregation", 30*time.Second),
	bus.WithAckWait(2*time.Minute),
)
```

## Contribution guide

### Guidelines

- Use conventional commit messages
- Always create a PR against the dev branch
- Use the [pre-commit](https://pre-commit.com/) tool to verify your code before committing

### Setup pre-commit hooks

```bash
brew install pre-commit
pre-commit install
```

## License

MIT
