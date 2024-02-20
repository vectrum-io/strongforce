package forwarder

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
	"time"
)

type DBForwarder struct {
	db              db.DB
	bus             bus.Bus
	serializer      serialization.Serializer
	pollingInterval time.Duration
	outboxTableName string
	logger          *zap.Logger
	stopChan        chan bool
}

func New(db db.DB, bus bus.Bus, options *Options) (*DBForwarder, error) {
	if options == nil {
		options = DefaultOptions
	} else {
		if err := options.validate(); err != nil {
			return nil, fmt.Errorf("failed to validate options: %w", err)
		}
	}

	return &DBForwarder{
		db:              db,
		bus:             bus,
		serializer:      options.Serializer,
		pollingInterval: options.PollingInterval,
		outboxTableName: options.OutboxTableName,
		logger:          options.Logger,
		stopChan:        make(chan bool),
	}, nil
}

func (fw *DBForwarder) Stop() error {
	fw.stopChan <- true
	return nil
}

func (fw *DBForwarder) Start(ctx context.Context) error {
	//goland:noinspection SqlNoDataSourceInspection
	query := fmt.Sprintf(`
		SELECT id, topic, payload, created_at
		FROM %s
	`, fw.outboxTableName)

	ticker := time.NewTicker(fw.pollingInterval)

	for {
		select {
		case <-ticker.C:
			if err := fw.processEvents(ctx, query); err != nil {
				fw.logger.Sugar().Warnf("failed to process events: %s", err.Error())
			}
		case <-fw.stopChan:
			return nil
		}
	}
}

func (fw *DBForwarder) processEvents(ctx context.Context, query string) error {
	var eventRows []*outbox.EventEntity
	err := fw.db.Connection().SelectContext(ctx, &eventRows, query)
	if err != nil {
		return err
	}

	if len(eventRows) == 0 {
		return nil
	}

	var publishedIds []events.EventID

	for _, row := range eventRows {
		event, err := row.ToSerializedEvent()
		if err != nil {
			fw.logger.Error("failed to convert db entity to event spec: " + err.Error())
			continue
		}

		if err := fw.emitEvent(ctx, event); err != nil {
			fw.logger.Warn("failed to publish event: " + err.Error())
			continue
		}

		publishedIds = append(publishedIds, event.Metadata.Id)
	}

	if len(publishedIds) == 0 {
		return nil
	}

	// remove published events
	queryString := fmt.Sprintf("DELETE FROM %s WHERE id IN (?)", fw.outboxTableName)
	query, args, err := sqlx.In(queryString, publishedIds)
	if err != nil {
		return fmt.Errorf("failed to construct deletion query: %w", err)
	}

	query = fw.db.Connection().Rebind(query)

	_, err = fw.db.Connection().ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete published events: %w", err)
	}

	return nil
}

func (fw *DBForwarder) emitEvent(ctx context.Context, event *events.SerializedEvent) error {
	message := &bus.OutboundMessage{
		Id:      event.Metadata.Id.String(),
		Subject: event.Metadata.Topic,
		Data:    event.SerializedPayload,
	}

	return fw.bus.Publish(ctx, message)
}
