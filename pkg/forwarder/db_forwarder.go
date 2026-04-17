package forwarder

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
)

var (
	ErrDirectEmitRequiresDB  = errors.New("direct emit requires a non-nil db")
	ErrDirectEmitRequiresBus = errors.New("direct emit requires a non-nil bus")
)

type directJob struct {
	event      *events.SerializedEvent
	enqueuedAt time.Time
}

type DBForwarder struct {
	db                     db.DB
	bus                    bus.Bus
	serializer             serialization.Serializer
	pollingInterval        time.Duration
	outboxTableName        string
	logger                 *zap.Logger
	stopChan               chan struct{}
	directEmit             bool
	directWorkers          int
	directQueue            chan directJob
	outboxDepthSampleEvery int
	metrics                *Metrics

	workerWg sync.WaitGroup
}

func New(db db.DB, bus bus.Bus, options *Options) (*DBForwarder, error) {
	if options == nil {
		options = DefaultOptions
	}
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	// Direct emit publishes to the bus and deletes from the outbox table
	// after each successful publish. Both dependencies are mandatory when
	// the option is on; fail fast here rather than nil-panic at runtime.
	if options.DirectEmit {
		if db == nil {
			return nil, ErrDirectEmitRequiresDB
		}
		if bus == nil {
			return nil, ErrDirectEmitRequiresBus
		}
	}

	return &DBForwarder{
		db:                     db,
		bus:                    bus,
		serializer:             options.Serializer,
		pollingInterval:        options.PollingInterval,
		outboxTableName:        options.OutboxTableName,
		logger:                 options.Logger,
		stopChan:               make(chan struct{}),
		directEmit:             options.DirectEmit,
		directWorkers:          options.DirectWorkers,
		directQueue:            make(chan directJob, options.DirectQueueSize),
		outboxDepthSampleEvery: options.OutboxDepthSampleEvery,
		metrics:                options.Metrics,
	}, nil
}

func (fw *DBForwarder) Stop() error {
	select {
	case <-fw.stopChan:
		return nil
	default:
		close(fw.stopChan)
	}
	fw.workerWg.Wait()
	return nil
}

func (fw *DBForwarder) Start(ctx context.Context) error {
	//goland:noinspection SqlNoDataSourceInspection
	query := fmt.Sprintf(`
		SELECT id, topic, payload, created_at
		FROM %s
		FOR UPDATE
	`, fw.outboxTableName)

	if fw.directEmit {
		for i := 0; i < fw.directWorkers; i++ {
			fw.workerWg.Add(1)
			go fw.directWorker(ctx)
		}
	}

	ticker := time.NewTicker(fw.pollingInterval)
	defer ticker.Stop()

	var tickCount uint64

	for {
		select {
		case <-ticker.C:
			if err := fw.processEvents(ctx, query); err != nil {
				fw.logger.Sugar().Warnf("failed to process events: %s", err.Error())
			}
			tickCount++
			if fw.outboxDepthSampleEvery > 0 && tickCount%uint64(fw.outboxDepthSampleEvery) == 0 {
				fw.sampleOutboxDepth(ctx)
			}
		case <-fw.stopChan:
			return nil
		}
	}
}

// NotifyCommitted implements outbox.CommitNotifier. It enqueues events for
// the direct-emit worker pool. When the queue is full, events are dropped and
// the poller handles them on its next cycle.
func (fw *DBForwarder) NotifyCommitted(ctx context.Context, evs []*events.SerializedEvent) {
	if !fw.directEmit {
		return
	}
	now := time.Now()
	for _, e := range evs {
		select {
		case fw.directQueue <- directJob{event: e, enqueuedAt: now}:
			fw.metrics.incDirectEnqueued(ctx)
		default:
			fw.metrics.incDirectDropped(ctx)
			fw.logger.Sugar().Debugf("direct-emit queue full, event %s dropped to poller", e.Metadata.Id.String())
		}
	}
}

func (fw *DBForwarder) directWorker(ctx context.Context) {
	defer fw.workerWg.Done()
	for {
		select {
		case <-fw.stopChan:
			return
		case job, ok := <-fw.directQueue:
			if !ok {
				return
			}
			fw.processDirect(ctx, job)
		}
	}
}

func (fw *DBForwarder) processDirect(ctx context.Context, job directJob) {
	if err := fw.emitEvent(ctx, job.event); err != nil {
		fw.metrics.incDirectFailed(ctx)
		fw.logger.Sugar().Warnf("direct emit publish failed for %s: %s", job.event.Metadata.Id.String(), err.Error())
		return
	}

	fw.metrics.observeEmitLatency(ctx, time.Since(job.enqueuedAt).Seconds())
	fw.metrics.incDirectPublished(ctx)

	if err := fw.deleteEvent(ctx, job.event.Metadata.Id); err != nil {
		fw.metrics.incDirectDeleteFailed(ctx)
		fw.logger.Sugar().Warnf("direct emit delete failed for %s: %s — poller will retry", job.event.Metadata.Id.String(), err.Error())
	}
}

func (fw *DBForwarder) deleteEvent(ctx context.Context, id events.EventID) error {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE id = ?", fw.outboxTableName)
	query := fw.db.Connection().Rebind(queryString)
	_, err := fw.db.Connection().ExecContext(ctx, query, id.String())
	return err
}

func (fw *DBForwarder) sampleOutboxDepth(ctx context.Context) {
	var count int64
	//goland:noinspection SqlNoDataSourceInspection
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", fw.outboxTableName)
	if err := fw.db.Connection().GetContext(ctx, &count, q); err != nil {
		fw.logger.Sugar().Debugf("failed to sample outbox depth: %s", err.Error())
		return
	}
	fw.metrics.setOutboxDepth(ctx, count)
}

func (fw *DBForwarder) processEvents(ctx context.Context, query string) error {
	var eventRows []*outbox.EventEntity

	return fw.db.Tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := tx.SelectContext(ctx, &eventRows, query)
		if err != nil {
			return err
		}

		if len(eventRows) == 0 {
			return nil
		}

		serializedEvents := make([]*events.SerializedEvent, 0, len(eventRows))
		for _, row := range eventRows {
			event, err := row.ToSerializedEvent()
			if err != nil {
				fw.logger.Error("failed to convert db entity to event spec: " + err.Error())
				continue
			}
			serializedEvents = append(serializedEvents, event)
		}

		if len(serializedEvents) == 0 {
			return nil
		}

		publishedIds := fw.publishBatch(ctx, serializedEvents)
		if len(publishedIds) == 0 {
			return nil
		}

		queryString := fmt.Sprintf("DELETE FROM %s WHERE id IN (?)", fw.outboxTableName)
		delQuery, args, err := sqlx.In(queryString, publishedIds)
		if err != nil {
			return fmt.Errorf("failed to construct deletion query: %w", err)
		}

		delQuery = tx.Rebind(delQuery)
		if _, err := tx.ExecContext(ctx, delQuery, args...); err != nil {
			return fmt.Errorf("failed to delete published events: %w", err)
		}

		return nil
	})
}

// publishBatch publishes events concurrently, bounded by directWorkers (or
// sequentially when direct emit is disabled / workers == 0). Returns the ids
// of successfully published events so the caller can delete them in a single
// query.
func (fw *DBForwarder) publishBatch(ctx context.Context, evs []*events.SerializedEvent) []events.EventID {
	// Pick the concurrency cap. A batch of 3 events with directWorkers=8 only
	// needs 3 goroutines; no point allocating 8 slots we'll never fill.
	concurrency := fw.directWorkers
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(evs) {
		concurrency = len(evs)
	}

	var (
		// mu guards the ids slice: multiple goroutines append concurrently.
		mu sync.Mutex
		// ids collects successfully published event ids so the caller can
		// issue a single batched DELETE.
		ids = make([]events.EventID, 0, len(evs))
		// sem is a counting semaphore implemented as a buffered channel.
		// Capacity == max goroutines allowed to run emitEvent at once.
		// A token is a struct{}{} value — empty struct uses zero memory.
		sem = make(chan struct{}, concurrency)
		// wg tracks when all spawned goroutines have finished so we can
		// return a complete ids slice to the caller.
		wg sync.WaitGroup
	)

	for _, ev := range evs {
		wg.Add(1)
		// Acquire a slot BEFORE spawning the goroutine.
		// If the buffer is full this blocks.
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			// Release the slot when the goroutine exits so the next
			// iteration of the for-loop can acquire one and proceed.
			defer func() { <-sem }()

			if err := fw.emitEvent(ctx, ev); err != nil {
				fw.metrics.incPollerFailed(ctx)
				fw.logger.Warn("failed to publish event: " + err.Error())
				return
			}
			fw.metrics.incPollerPublished(ctx)
			mu.Lock()
			ids = append(ids, ev.Metadata.Id)
			mu.Unlock()
		}()
	}

	wg.Wait()
	return ids
}

func (fw *DBForwarder) emitEvent(ctx context.Context, event *events.SerializedEvent) error {
	message := &bus.OutboundMessage{
		Id:      event.Metadata.Id.String(),
		Subject: event.Metadata.Topic,
		Data:    event.SerializedPayload,
	}

	return fw.bus.Publish(ctx, message)
}
