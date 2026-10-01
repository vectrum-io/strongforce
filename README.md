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
