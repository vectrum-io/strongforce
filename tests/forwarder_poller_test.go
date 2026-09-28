package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// recordingBus records published message ids in order. hook, when set, runs
// before a publish is recorded and can fail or delay it.
type recordingBus struct {
	mu        sync.Mutex
	published []string
	hook      func(ctx context.Context, message *bus.OutboundMessage) error
}

func (b *recordingBus) Publish(ctx context.Context, message *bus.OutboundMessage) error {
	if b.hook != nil {
		if err := b.hook(ctx, message); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, message.Id)
	return nil
}

func (b *recordingBus) Subscribe(context.Context, string, string, ...bus.SubscribeOption) (*bus.Subscription, error) {
	return nil, errors.New("not supported")
}

func (b *recordingBus) Migrate(context.Context) error {
	return nil
}

func (b *recordingBus) SubscriberInfo(context.Context, string, string) (bus.SubscriberInfo, error) {
	return nil, errors.New("not supported")
}

func (b *recordingBus) publishedIds() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.published...)
}

var pollerDrivers = []string{"mysql", "postgres"}

func newPollerDB(t *testing.T, driver, tableName string) db.DB {
	t.Helper()
	d := newDirectEmitDB(t, driver, tableName)
	assert.NoError(t, d.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(d, tableName))
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func insertOutboxRows(t *testing.T, d db.DB, tableName string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		//goland:noinspection SqlNoDataSourceInspection
		_, err := d.Connection().Exec(d.Connection().Rebind(
			fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?)", tableName),
		), id, "poller.topic", []byte("payload"), time.Now())
		assert.NoError(t, err)
	}
}

func outboxIds(t *testing.T, d db.DB, tableName string) []string {
	t.Helper()
	rows, err := sharedtest.GetEventEntities(d, tableName)
	assert.NoError(t, err)
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Id.String)
	}
	return ids
}

func startForwarder(t *testing.T, d db.DB, b bus.Bus, options *forwarder.Options) {
	t.Helper()
	fw, err := forwarder.New(d, b, options)
	assert.NoError(t, err)
	go func() {
		_ = fw.Start(context.Background())
	}()
	t.Cleanup(func() { _ = fw.Stop() })
}

func TestPollerDoesNotBlockOutboxInsertsWhilePublishing(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_lock"
			d := newPollerDB(t, driver, tableName)

			publishing := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			b := &recordingBus{hook: func(ctx context.Context, message *bus.OutboundMessage) error {
				once.Do(func() {
					close(publishing)
					<-release
				})
				return nil
			}}

			insertOutboxRows(t, d, tableName, "row-001")
			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval: 20 * time.Millisecond,
				Serializer:      serialization.NewJSONSerializer(),
				OutboxTableName: tableName,
				PublishTimeout:  10 * time.Second,
			})

			select {
			case <-publishing:
			case <-time.After(5 * time.Second):
				t.Fatal("poller never published")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			//goland:noinspection SqlNoDataSourceInspection
			_, err := d.Connection().ExecContext(ctx, d.Connection().Rebind(
				fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?)", tableName),
			), "row-002", "poller.topic", []byte("payload"), time.Now())
			assert.NoError(t, err, "outbox insert waited on the poller's locks")

			close(release)
			assertOutboxEmpty(t, d, tableName, 5*time.Second)
			assert.Equal(t, []string{"row-001", "row-002"}, b.publishedIds())
		})
	}
}

func TestPollerStopsAtFirstFailedPublishAndKeepsOrder(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_stop"
			d := newPollerDB(t, driver, tableName)

			b := &recordingBus{hook: func(ctx context.Context, message *bus.OutboundMessage) error {
				if message.Id == "row-002" {
					return errors.New("bus down")
				}
				return nil
			}}

			insertOutboxRows(t, d, tableName, "row-003", "row-001", "row-002")
			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval:  20 * time.Millisecond,
				PollerMaxBackoff: 100 * time.Millisecond,
				Serializer:       serialization.NewJSONSerializer(),
				OutboxTableName:  tableName,
			})

			assert.Eventually(t, func() bool {
				return len(b.publishedIds()) == 1
			}, 5*time.Second, 10*time.Millisecond)
			time.Sleep(300 * time.Millisecond)

			assert.Equal(t, []string{"row-001"}, b.publishedIds())
			assert.ElementsMatch(t, []string{"row-002", "row-003"}, outboxIds(t, d, tableName))
		})
	}
}

func TestPollerSkipsRowsYoungerThanGracePeriod(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_grace"
			d := newPollerDB(t, driver, tableName)
			b := &recordingBus{}

			oldId := ulid.MustNew(ulid.Timestamp(time.Now().Add(-time.Minute)), ulid.DefaultEntropy()).String()
			freshId := ulid.Make().String()
			insertOutboxRows(t, d, tableName, oldId, freshId)

			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval:   20 * time.Millisecond,
				Serializer:        serialization.NewJSONSerializer(),
				OutboxTableName:   tableName,
				DirectEmit:        true,
				DirectWorkers:     0,
				DirectQueueSize:   0,
				PollerGracePeriod: time.Second,
			})

			assert.Eventually(t, func() bool {
				return len(b.publishedIds()) == 1
			}, 900*time.Millisecond, 10*time.Millisecond)
			assert.Equal(t, []string{oldId}, b.publishedIds())

			assertOutboxEmpty(t, d, tableName, 3*time.Second)
			assert.Equal(t, []string{oldId, freshId}, b.publishedIds())
		})
	}
}

func TestPollerDrainsBacklogWithoutWaitingForInterval(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_drain"
			d := newPollerDB(t, driver, tableName)
			b := &recordingBus{}

			ids := make([]string, 250)
			for i := range ids {
				ids[i] = fmt.Sprintf("row-%03d", i)
			}
			insertOutboxRows(t, d, tableName, ids...)

			// The first poll runs after one interval; without draining, 250
			// rows at 100 per poll would need three intervals.
			start := time.Now()
			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval: time.Second,
				PollerBatchSize: 100,
				Serializer:      serialization.NewJSONSerializer(),
				OutboxTableName: tableName,
			})

			assertOutboxEmpty(t, d, tableName, 5*time.Second)
			assert.Less(t, time.Since(start), 2*time.Second)
			assert.Equal(t, ids, b.publishedIds())
		})
	}
}

func TestPollerCommitsWhenBatchBudgetIsSpent(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_budget"
			d := newPollerDB(t, driver, tableName)
			b := &recordingBus{hook: func(ctx context.Context, message *bus.OutboundMessage) error {
				time.Sleep(50 * time.Millisecond)
				return nil
			}}

			ids := make([]string, 10)
			for i := range ids {
				ids[i] = fmt.Sprintf("row-%03d", i)
			}
			insertOutboxRows(t, d, tableName, ids...)

			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval:   20 * time.Millisecond,
				PollerBatchBudget: 120 * time.Millisecond,
				Serializer:        serialization.NewJSONSerializer(),
				OutboxTableName:   tableName,
			})

			// Published rows are deleted while later ones are still pending,
			// so the first poll committed before publishing the whole batch.
			assert.Eventually(t, func() bool {
				remaining := len(outboxIds(t, d, tableName))
				return remaining > 0 && remaining < len(ids)
			}, 3*time.Second, 10*time.Millisecond)

			assertOutboxEmpty(t, d, tableName, 5*time.Second)
			assert.Equal(t, ids, b.publishedIds())
		})
	}
}

func TestConcurrentPollersPublishEachRowOnce(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_concurrent"
			d := newPollerDB(t, driver, tableName)
			b := &recordingBus{}

			ids := make([]string, 300)
			for i := range ids {
				ids[i] = fmt.Sprintf("row-%03d", i)
			}
			insertOutboxRows(t, d, tableName, ids...)

			for i := 0; i < 2; i++ {
				startForwarder(t, d, b, &forwarder.Options{
					PollingInterval: 10 * time.Millisecond,
					PollerBatchSize: 10,
					Serializer:      serialization.NewJSONSerializer(),
					OutboxTableName: tableName,
				})
			}

			assertOutboxEmpty(t, d, tableName, 10*time.Second)
			assert.ElementsMatch(t, ids, b.publishedIds())
		})
	}
}

func TestPollerReportsOldestOutboxRowAge(t *testing.T) {
	for _, driver := range pollerDrivers {
		t.Run(driver, func(t *testing.T) {
			tableName := "event_outbox_poller_age"
			d := newPollerDB(t, driver, tableName)
			b := &recordingBus{hook: func(ctx context.Context, message *bus.OutboundMessage) error {
				return errors.New("bus down")
			}}

			stuckId := ulid.MustNew(ulid.Timestamp(time.Now().Add(-time.Hour)), ulid.DefaultEntropy()).String()
			insertOutboxRows(t, d, tableName, stuckId)

			metrics, reader := newTestMetrics(t)
			startForwarder(t, d, b, &forwarder.Options{
				PollingInterval:        20 * time.Millisecond,
				Serializer:             serialization.NewJSONSerializer(),
				OutboxTableName:        tableName,
				OutboxDepthSampleEvery: 1,
				Metrics:                metrics,
			})

			assert.Eventually(t, func() bool {
				return readFloatGauge(t, reader, "strongforce.forwarder.outbox.oldest_age") >= time.Hour.Seconds()
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}

// readFloatGauge returns the last value of a Float64 gauge, or 0 if unset.
func readFloatGauge(t *testing.T, reader *sdkmetric.ManualReader, name string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				t.Fatalf("metric %q is not a float64 Gauge (got %T)", name, m.Data)
			}
			if len(gauge.DataPoints) == 0 {
				return 0
			}
			return gauge.DataPoints[len(gauge.DataPoints)-1].Value
		}
	}
	return 0
}
