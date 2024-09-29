package testhelper

import (
	"context"
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"time"
)

type OutboxHelpers struct {
	bus bus.Bus
	db  db.DB
}

func (oh *OutboxHelpers) WaitUntilEventProcessed(ctx context.Context, timeout time.Duration, interval time.Duration, outboxTableName string, streamName string, consumerName string) error {
	if err := oh.WaitUntilOutboxEmpty(ctx, timeout, interval, outboxTableName); err != nil {
		return fmt.Errorf("failed to wait until outbox is empty: %w", err)
	}

	if err := oh.WaitUntilBusEmpty(ctx, timeout, interval, streamName, consumerName); err != nil {
		return fmt.Errorf("failed to wait until bus is empty: %w", err)
	}

	return nil
}

func (oh *OutboxHelpers) WaitUntilOutboxEmpty(ctx context.Context, timeout time.Duration, interval time.Duration, outboxTableName string) error {
	// Create a context with timeout to enforce the maximum wait duration
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sql := oh.db.Connection().Rebind("SELECT COUNT(*) FROM " + outboxTableName)

	for {
		select {
		case <-timeoutCtx.Done():
			// Timeout or cancellation reached
			return fmt.Errorf("timed out waiting for the outbox to be empty: %w", timeoutCtx.Err())
		case <-ticker.C:
			var count int
			if err := oh.db.Connection().GetContext(ctx, &count, sql); err != nil {
				return fmt.Errorf("failed to get outbox count: %w", err)
			}

			if count == 0 {
				// Outbox is empty, return nil
				return nil
			}
		}
	}
}

func (oh *OutboxHelpers) WaitUntilBusEmpty(ctx context.Context, timeout time.Duration, interval time.Duration, streamName string, consumerName string) error {
	// Create a context with timeout to enforce the maximum wait duration
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			// Timeout or cancellation reached
			return fmt.Errorf("timed out waiting for the bus to be empty: %w", timeoutCtx.Err())
		case <-ticker.C:
			subscriberInfo, err := oh.bus.SubscriberInfo(ctx, streamName, consumerName)
			if err != nil {
				return fmt.Errorf("failed to get subscriber info: %w", err)
			}

			if !subscriberInfo.HasPendingMessages() {
				return nil
			}
		}
	}
}
