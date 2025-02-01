package forwarder

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
	"time"
)

type DebeziumForwarder struct {
	db              db.DB
	bus             bus.Bus
	serializer      serialization.Serializer
	debeziumSubject string
	debeziumStream  string
	subscriberName  string
	logger          *zap.Logger
	stopChan        chan bool
}

type DebeziumMessage struct {
	Payload struct {
		Before interface{} `json:"before"`
		After  struct {
			ID        string `json:"id"`
			Topic     string `json:"topic"`
			Payload   []byte `json:"payload"`
			CreatedAt int64  `json:"created_at"`
		} `json:"after"`
		Source struct {
			Version   string      `json:"version"`
			Connector string      `json:"connector"`
			Name      string      `json:"name"`
			TsMs      int64       `json:"ts_ms"`
			Snapshot  string      `json:"snapshot"`
			Db        string      `json:"db"`
			Sequence  interface{} `json:"sequence"`
			TsUs      int64       `json:"ts_us"`
			TsNs      int64       `json:"ts_ns"`
			Table     string      `json:"table"`
			ServerID  int         `json:"server_id"`
			Gtid      interface{} `json:"gtid"`
			File      string      `json:"file"`
			Pos       int         `json:"pos"`
			Row       int         `json:"row"`
			Thread    int         `json:"thread"`
			Query     interface{} `json:"query"`
		} `json:"source"`
		Transaction interface{} `json:"transaction"`
		Op          string      `json:"op"`
		TsMs        int64       `json:"ts_ms"`
		TsUs        int64       `json:"ts_us"`
		TsNs        int64       `json:"ts_ns"`
	} `json:"payload"`
}

func NewDebeziumForwarder(db db.DB, bus bus.Bus, options *DebeziumOptions) (*DebeziumForwarder, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	return &DebeziumForwarder{
		db:              db,
		bus:             bus,
		serializer:      options.Serializer,
		debeziumSubject: options.DebeziumSubject,
		debeziumStream:  options.DebeziumStream,
		subscriberName:  options.SubscriberName,
		logger:          options.Logger,
		stopChan:        make(chan bool),
	}, nil
}

func (fw *DebeziumForwarder) Stop() error {
	fw.stopChan <- true
	return nil
}

func (fw *DebeziumForwarder) Start(ctx context.Context) error {

	subscription, err := fw.bus.Subscribe(ctx, fw.subscriberName, fw.debeziumStream, bus.WithFilterSubject(fw.debeziumSubject), bus.WithDurable(), bus.WithGuaranteeOrder())
	if err != nil {
		return fmt.Errorf("failed to subscribe to debezium stream: %w", err)
	}

	if err := subscription.AddHandler(fw.debeziumSubject, func(ctx context.Context, message bus.InboundMessage) error {
		debeziumMessage := &DebeziumMessage{}

		if err := json.Unmarshal(message.Data, debeziumMessage); err != nil {
			fw.logger.Sugar().Errorf("failed to emit event: %s", err.Error())
			return fmt.Errorf("failed to unmarshal debezium message: %w", err)
		}

		return fw.processDebeziumMessage(ctx, debeziumMessage)
	}); err != nil {
		fw.logger.Sugar().Errorf("failed to emit event: %s", err.Error())
		return fmt.Errorf("failed to add handler to debezium stream: %w", err)
	}

	subscription.Start(ctx)

	for range fw.stopChan {
		subscription.Stop()
	}

	return nil
}

func (fw *DebeziumForwarder) processDebeziumMessage(ctx context.Context, message *DebeziumMessage) error {
	if message.Payload.After.ID == "" || message.Payload.After.Topic == "" {
		return nil
	}

	event := &events.SerializedEvent{
		Metadata: &events.EventMetadata{
			Id:        events.EventID(message.Payload.After.ID),
			Topic:     message.Payload.After.Topic,
			CreatedAt: time.Unix(0, message.Payload.After.CreatedAt),
		},
		SerializedPayload: message.Payload.After.Payload,
	}

	if err := fw.emitEvent(ctx, event); err != nil {
		fw.logger.Sugar().Errorf("failed to emit event %+v: %s", event.Metadata.Topic, err.Error())
		return fmt.Errorf("failed to emit event: %w", err)
	}

	if err := fw.removeEvent(ctx, message.Payload.Source.Table, events.EventID(message.Payload.After.ID)); err != nil {
		fw.logger.Sugar().Errorf("failed to remove event: %s", err.Error())
		return fmt.Errorf("failed to remove event: %w", err)
	}

	return nil
}

func (fw *DebeziumForwarder) emitEvent(ctx context.Context, event *events.SerializedEvent) error {
	message := &bus.OutboundMessage{
		Id:      event.Metadata.Id.String(),
		Subject: event.Metadata.Topic,
		Data:    event.SerializedPayload,
	}

	return fw.bus.Publish(ctx, message)
}

func (fw *DebeziumForwarder) removeEvent(ctx context.Context, tableName string, eventID events.EventID) error {
	// remove published events
	queryString := fmt.Sprintf("DELETE FROM %s WHERE id IN (?)", tableName)
	query, args, err := sqlx.In(queryString, eventID)
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
