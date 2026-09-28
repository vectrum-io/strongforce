package forwarder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
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
	pollerBatchSize        int
	pollerBatchBudget      time.Duration
	pollerGracePeriod      time.Duration
	pollerMaxBackoff       time.Duration
	publishTimeout         time.Duration
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
		pollerBatchSize:        options.PollerBatchSize,
		pollerBatchBudget:      options.PollerBatchBudget,
		pollerGracePeriod:      options.PollerGracePeriod,
		pollerMaxBackoff:       options.PollerMaxBackoff,
		publishTimeout:         options.PublishTimeout,
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
	if fw.directEmit {
		for i := 0; i < fw.directWorkers; i++ {
			fw.workerWg.Add(1)
			go fw.directWorker(ctx)
		}
	}

	timer := time.NewTimer(fw.pollingInterval)
	defer timer.Stop()

	delay := fw.pollingInterval
	var pollCount uint64

	for {
		select {
		case <-timer.C:
			result, err := fw.processEvents(ctx)
			delay = fw.nextPollDelay(delay, result, err)
			if err != nil {
				fw.logger.Sugar().Warnf("failed to process events, next poll in %s: %s", delay, err.Error())
			}

			pollCount++
			if fw.outboxDepthSampleEvery > 0 && pollCount%uint64(fw.outboxDepthSampleEvery) == 0 {
				fw.sampleOutbox(ctx)
			}

			timer.Reset(delay)
		case <-fw.stopChan:
			return nil
		}
	}
}

// nextPollDelay backs off exponentially while polls fail, polls again right
// away while a backlog is draining, and otherwise waits one polling interval.
func (fw *DBForwarder) nextPollDelay(previous time.Duration, result pollResult, err error) time.Duration {
	if err != nil {
		next := previous * 2
		if next < fw.pollingInterval {
			next = fw.pollingInterval
		}
		if next > fw.pollerMaxBackoff {
			next = fw.pollerMaxBackoff
		}
		return next
	}

	if result.hasMore {
		return 0
	}

	return fw.pollingInterval
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
	if err := fw.publishWithTimeout(ctx, job.event); err != nil {
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

// sampleOutbox records the outbox depth and the age of its oldest row, read
// from the timestamp encoded in the smallest ULID id.
func (fw *DBForwarder) sampleOutbox(ctx context.Context) {
	var sample struct {
		Count  int64          `db:"depth"`
		Oldest sql.NullString `db:"oldest_id"`
	}
	//goland:noinspection SqlNoDataSourceInspection
	q := fmt.Sprintf("SELECT COUNT(*) AS depth, MIN(id) AS oldest_id FROM %s", fw.outboxTableName)
	if err := fw.db.Connection().GetContext(ctx, &sample, q); err != nil {
		fw.logger.Sugar().Debugf("failed to sample outbox: %s", err.Error())
		return
	}
	fw.metrics.setOutboxDepth(ctx, sample.Count)

	if !sample.Oldest.Valid {
		fw.metrics.setOutboxOldestAge(ctx, 0)
		return
	}
	oldest, err := ulid.Parse(sample.Oldest.String)
	if err != nil {
		return
	}
	fw.metrics.setOutboxOldestAge(ctx, time.Since(oldest.Timestamp()).Seconds())
}

type pollResult struct {
	// hasMore is set when the poll may have left rows behind: its batch was
	// full or it ran out of budget.
	hasMore bool
}

// processEvents publishes one batch of outbox rows in id order. It locks only
// the rows of the batch (READ COMMITTED takes no gap locks, so concurrent
// inserts into the outbox never wait on the poller) and skips rows another
// poller holds. It stops at the first failed publish, deletes the rows
// published before it and returns the publish error. Once the batch budget is
// spent it stops early too, so row locks are held for at most the budget plus
// one PublishTimeout.
func (fw *DBForwarder) processEvents(ctx context.Context) (pollResult, error) {
	start := time.Now()
	query, args := fw.pollQuery(start)

	tx, err := fw.db.Connection().BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return pollResult{}, fmt.Errorf("failed to begin poll transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var eventRows []*outbox.EventEntity
	if err := tx.SelectContext(ctx, &eventRows, tx.Rebind(query), args...); err != nil {
		return pollResult{}, fmt.Errorf("failed to select outbox rows: %w", err)
	}

	if len(eventRows) == 0 {
		return pollResult{}, nil
	}

	publishedIds := make([]events.EventID, 0, len(eventRows))
	var publishErr error
	outOfBudget := false
	for _, row := range eventRows {
		// Always publish at least one row, so a slow bus still makes progress.
		if len(publishedIds) > 0 && time.Since(start) >= fw.pollerBatchBudget {
			outOfBudget = true
			break
		}

		event, err := row.ToSerializedEvent()
		if err != nil {
			fw.logger.Error("failed to convert db entity to event spec: " + err.Error())
			continue
		}

		if err := fw.publishWithTimeout(ctx, event); err != nil {
			fw.metrics.incPollerFailed(ctx)
			publishErr = fmt.Errorf("failed to publish event %s: %w", event.Metadata.Id.String(), err)
			break
		}
		fw.metrics.incPollerPublished(ctx)
		publishedIds = append(publishedIds, event.Metadata.Id)
	}

	if len(publishedIds) > 0 {
		deleteQuery, deleteArgs, err := sqlx.In(fmt.Sprintf("DELETE FROM %s WHERE id IN (?)", fw.outboxTableName), publishedIds)
		if err != nil {
			return pollResult{}, fmt.Errorf("failed to construct deletion query: %w", err)
		}
		if _, err := tx.ExecContext(ctx, tx.Rebind(deleteQuery), deleteArgs...); err != nil {
			return pollResult{}, fmt.Errorf("failed to delete published events: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return pollResult{}, fmt.Errorf("failed to commit poll transaction: %w", err)
	}

	return pollResult{hasMore: outOfBudget || len(eventRows) == fw.pollerBatchSize}, publishErr
}

// pollQuery selects the next batch in id order. With direct emit, rows younger
// than the grace period are left to the direct-emit workers: ULIDs start with
// their creation time, so a ULID with the cutoff time and zero entropy bounds
// them on the primary key.
func (fw *DBForwarder) pollQuery(now time.Time) (string, []interface{}) {
	//goland:noinspection SqlNoDataSourceInspection
	query := fmt.Sprintf("SELECT id, topic, payload, created_at FROM %s", fw.outboxTableName)
	var args []interface{}

	if fw.directEmit && fw.pollerGracePeriod > 0 {
		var cutoff ulid.ULID
		_ = cutoff.SetTime(ulid.Timestamp(now.Add(-fw.pollerGracePeriod)))
		query += " WHERE id < ?"
		args = append(args, cutoff.String())
	}

	query += " ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED"
	args = append(args, fw.pollerBatchSize)

	return query, args
}

func (fw *DBForwarder) publishWithTimeout(ctx context.Context, event *events.SerializedEvent) error {
	ctx, cancel := context.WithTimeout(ctx, fw.publishTimeout)
	defer cancel()
	return fw.emitEvent(ctx, event)
}

func (fw *DBForwarder) emitEvent(ctx context.Context, event *events.SerializedEvent) error {
	message := &bus.OutboundMessage{
		Id:      event.Metadata.Id.String(),
		Subject: event.Metadata.Topic,
		Data:    event.SerializedPayload,
	}

	return fw.bus.Publish(ctx, message)
}
