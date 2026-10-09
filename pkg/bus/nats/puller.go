package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.uber.org/zap"
)

const (
	// pinIDHeader carries the pin id on messages of a pinned priority group.
	pinIDHeader = "Nats-Pin-Id"
	// maxPullExpiry bounds how long one pull request waits for messages.
	maxPullExpiry = 30 * time.Second
	minPullExpiry = time.Second
	// pullRetryDelay is the pause after a pull request failed.
	pullRetryDelay = time.Second
)

// pullExpiry is how long one pull request of a batch subscription waits.
// The server keeps a pin only while its holder sends new pull requests, so a
// pinned subscriber pulls at least three times per pinned TTL.
func pullExpiry(pinned *bus.PinnedGroup) time.Duration {
	expiry := maxPullExpiry
	if pinned != nil {
		expiry = min(expiry, pinned.TTL/3)
	}
	return max(expiry, minPullExpiry)
}

// batchPuller feeds a batch subscription by pulling from its consumer without
// pause, also while a batch is being handled: the server only keeps a pin
// while its holder keeps pulling. The consumer's MaxAckPending bounds how many
// messages it holds.
type batchPuller struct {
	consumer  jetstream.Consumer
	stream    jetstream.Stream
	group     string
	batchSize int
	expiry    time.Duration
	msgChan   chan<- bus.InboundMessage
	toInbound func(msg jetstream.Msg) bus.InboundMessage
	metrics   *bus.Metrics
	logger    *zap.SugaredLogger

	mu     sync.Mutex
	pinID  string
	cancel context.CancelFunc
	done   chan struct{}
}

// start begins pulling in the background until stop.
func (p *batchPuller) start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done != nil {
		return
	}

	ctx, p.cancel = context.WithCancel(ctx)
	p.done = make(chan struct{})
	go p.run(ctx)
}

// stop ends pulling and waits until every pulled message was handed on.
func (p *batchPuller) stop() {
	p.mu.Lock()
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if done == nil {
		return
	}
	cancel()
	<-done
}

func (p *batchPuller) run(ctx context.Context) {
	defer close(p.done)

	for ctx.Err() == nil {
		if err := p.pull(ctx); err != nil && ctx.Err() == nil {
			p.logger.Warnf("failed to pull messages: %s", err)
			select {
			case <-ctx.Done():
			case <-time.After(pullRetryDelay):
			}
		}
	}
}

// pull sends one pull request and hands on the messages it returns.
func (p *batchPuller) pull(ctx context.Context) error {
	pullCtx, cancel := context.WithTimeout(ctx, p.expiry)
	defer cancel()

	opts := []jetstream.FetchOpt{jetstream.FetchContext(pullCtx)}
	if p.group != "" {
		opts = append(opts, jetstream.FetchPriorityGroup(p.group))
	}

	batch, err := p.consumer.Fetch(p.batchSize, opts...)
	if err != nil {
		return err
	}

	for msg := range batch.Messages() {
		if pinID := msg.Headers().Get(pinIDHeader); pinID != "" {
			p.setPin(ctx, pinID)
		}
		select {
		case p.msgChan <- p.toInbound(msg):
		case <-ctx.Done():
			_ = msg.Nak()
		}
	}

	err = batch.Error()
	switch {
	case err == nil, errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return nil
	case errors.Is(err, jetstream.ErrPinIDMismatch):
		p.logger.Infof("lost the pin of priority group %s", p.group)
		p.setPin(ctx, "")
		return nil
	default:
		return err
	}
}

func (p *batchPuller) setPin(ctx context.Context, pinID string) {
	p.mu.Lock()
	was := p.pinID != ""
	p.pinID = pinID
	p.mu.Unlock()

	if now := pinID != ""; now != was {
		info := p.consumer.CachedInfo()
		p.metrics.RecordPinned(context.WithoutCancel(ctx), info.Stream, info.Name, p.group, now)
		if now {
			p.logger.Infof("holding the pin of priority group %s", p.group)
		}
	}
}

func (p *batchPuller) pinned() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinID != ""
}

// unpin releases the pin if this puller still holds it. The server unpins
// whichever client is pinned, so the pin id is compared first.
func (p *batchPuller) unpin(ctx context.Context) error {
	p.mu.Lock()
	pinID := p.pinID
	p.mu.Unlock()
	if pinID == "" {
		return nil
	}
	defer p.setPin(ctx, "")

	info, err := p.consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consumer info: %w", err)
	}

	for _, state := range info.PriorityGroups {
		if state.Group != p.group || state.PinnedClientID != pinID {
			continue
		}
		if err := p.stream.UnpinConsumer(ctx, info.Name, p.group); err != nil {
			return err
		}
		p.logger.Infof("released the pin of priority group %s", p.group)
	}

	return nil
}
