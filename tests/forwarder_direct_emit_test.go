package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"github.com/vectrum-io/strongforce/tests/mocks"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
)

// newTestMetrics wires the forwarder's OTel Metrics against a ManualReader so
// tests can introspect counter/histogram values synchronously.
func newTestMetrics(t *testing.T) (*forwarder.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := forwarder.NewMetrics(mp)
	assert.NoError(t, err)
	return m, reader
}

// readCounter collects the current value of an Int64 sum metric by name.
// Returns 0 if the metric has not been recorded yet.
func readCounter(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is not an int64 Sum (got %T)", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

type outboxProvider interface {
	Outbox() *outbox.Outbox
}

// jsonPayloadForDirect is a tiny payload type so the test matches the bus mock
// on exact serialized bytes.
type jsonPayloadForDirect struct {
	Data string `json:"data"`
}

func newDirectEmitDB(t *testing.T, driver, tableName string) db.DB {
	t.Helper()
	switch driver {
	case "mysql":
		d, err := mysql.New(mysql.Options{
			DSN: sharedtest.MySQLDSN,
			OutboxOptions: &outbox.Options{
				TableName:  tableName,
				Serializer: serialization.NewJSONSerializer(),
			},
		})
		assert.NoError(t, err)
		return d
	case "postgres":
		d, err := postgres.New(postgres.Options{
			DSN: sharedtest.PostgresDSN,
			OutboxOptions: &outbox.Options{
				TableName:  tableName,
				Serializer: serialization.NewJSONSerializer(),
			},
		})
		assert.NoError(t, err)
		return d
	}
	t.Fatalf("unsupported driver %q", driver)
	return nil
}

func attachForwarder(t *testing.T, d db.DB, fw *forwarder.DBForwarder) {
	t.Helper()
	op, ok := d.(outboxProvider)
	assert.True(t, ok, "db must expose Outbox()")
	op.Outbox().SetNotifier(fw)
}

func TestDirectEmitMySQL(t *testing.T) {
	testDirectEmitHappyPath(t, "mysql", "event_outbox_direct_1")
}

func TestDirectEmitPostgres(t *testing.T) {
	testDirectEmitHappyPath(t, "postgres", "event_outbox_direct_1")
}

// testDirectEmitHappyPath asserts that after EventTx commits the event is
// published via the direct-emit worker pool (well before any poller tick)
// and the outbox row is deleted. The polling interval is set long enough
// that the poller cannot be the thing doing the work.
func testDirectEmitHappyPath(t *testing.T, driver, tableName string) {
	mockBus := &mocks.Bus{}
	d := newDirectEmitDB(t, driver, tableName)
	assert.NoError(t, d.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(d, tableName))
	defer d.Close()

	metrics, reader := newTestMetrics(t)

	fw, err := forwarder.New(d, mockBus, &forwarder.Options{
		PollingInterval: 10 * time.Second, // effectively disables poller
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
		DirectEmit:      true,
		DirectWorkers:   2,
		DirectQueueSize: 16,
		Metrics:         metrics,
	})
	assert.NoError(t, err)
	attachForwarder(t, d, fw)

	go fw.Start(context.Background())
	defer fw.Stop()

	// Expected serialized payload: json.Marshal of jsonPayloadForDirect{Data:"hello"}
	expectedData := []byte(`{"data":"hello"}`)
	eventBuilder := &events.Builder{}
	mockBus.On("Publish", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		msg := args.Get(0).(bus.OutboundMessage)
		assert.Equal(t, "direct.topic", msg.Subject)
		assert.Equal(t, expectedData, msg.Data)
	}).Once()

	_, err = d.EventTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error) {
		return eventBuilder.New("direct.topic", &jsonPayloadForDirect{Data: "hello"})
	})
	assert.NoError(t, err)

	// Direct emit should be fast enough that well under the 10s poll interval
	// is plenty. Poll the outbox until empty.
	assertOutboxEmpty(t, d, tableName, 2*time.Second)

	assert.Equal(t, int64(1), readCounter(t, reader, "strongforce.forwarder.direct.enqueued"))
	assert.Equal(t, int64(1), readCounter(t, reader, "strongforce.forwarder.direct.published"))
	assert.Equal(t, int64(0), readCounter(t, reader, "strongforce.forwarder.direct.failed"))
	mockBus.AssertExpectations(t)
}

func TestDirectEmitBatchMySQL(t *testing.T) {
	testDirectEmitBatch(t, "mysql", "event_outbox_direct_batch")
}

func TestDirectEmitBatchPostgres(t *testing.T) {
	testDirectEmitBatch(t, "postgres", "event_outbox_direct_batch")
}

func testDirectEmitBatch(t *testing.T, driver, tableName string) {
	mockBus := &mocks.Bus{}
	d := newDirectEmitDB(t, driver, tableName)
	assert.NoError(t, d.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(d, tableName))
	defer d.Close()

	metrics, reader := newTestMetrics(t)
	fw, err := forwarder.New(d, mockBus, &forwarder.Options{
		PollingInterval: 10 * time.Second,
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
		DirectEmit:      true,
		DirectWorkers:   4,
		DirectQueueSize: 16,
		Metrics:         metrics,
	})
	assert.NoError(t, err)
	attachForwarder(t, d, fw)

	go fw.Start(context.Background())
	defer fw.Stop()

	mockBus.On("Publish", mock.Anything).Return(nil).Times(5)

	eventBuilder := &events.Builder{}
	_, err = d.EventsTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) ([]*events.EventSpec, error) {
		specs := make([]*events.EventSpec, 0, 5)
		for i := 0; i < 5; i++ {
			e, err := eventBuilder.New(fmt.Sprintf("batch.%d", i), &jsonPayloadForDirect{Data: fmt.Sprintf("v%d", i)})
			if err != nil {
				return nil, err
			}
			specs = append(specs, e)
		}
		return specs, nil
	})
	assert.NoError(t, err)

	assertOutboxEmpty(t, d, tableName, 2*time.Second)
	assert.Equal(t, int64(5), readCounter(t, reader, "strongforce.forwarder.direct.enqueued"))
	assert.Equal(t, int64(5), readCounter(t, reader, "strongforce.forwarder.direct.published"))
	mockBus.AssertExpectations(t)
}

func TestDirectEmitFallbackOnBusErrorMySQL(t *testing.T) {
	testDirectEmitFallbackOnBusError(t, "mysql", "event_outbox_direct_fb")
}

func TestDirectEmitFallbackOnBusErrorPostgres(t *testing.T) {
	testDirectEmitFallbackOnBusError(t, "postgres", "event_outbox_direct_fb")
}

// testDirectEmitFallbackOnBusError: the direct path fails on first publish,
// the row stays in the outbox, and the poller retries successfully.
func testDirectEmitFallbackOnBusError(t *testing.T, driver, tableName string) {
	mockBus := &mocks.Bus{}
	d := newDirectEmitDB(t, driver, tableName)
	assert.NoError(t, d.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(d, tableName))
	defer d.Close()

	metrics, reader := newTestMetrics(t)
	fw, err := forwarder.New(d, mockBus, &forwarder.Options{
		PollingInterval: 100 * time.Millisecond,
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
		DirectEmit:      true,
		DirectWorkers:   1,
		DirectQueueSize: 4,
		Metrics:         metrics,
	})
	assert.NoError(t, err)
	attachForwarder(t, d, fw)

	// First publish (direct path) errors. Second publish (poller) succeeds.
	mockBus.On("Publish", mock.Anything).Return(errors.New("bus down")).Once()
	mockBus.On("Publish", mock.Anything).Return(nil).Once()

	go fw.Start(context.Background())
	defer fw.Stop()

	eventBuilder := &events.Builder{}
	_, err = d.EventTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error) {
		return eventBuilder.New("fallback.topic", &jsonPayloadForDirect{Data: "recover"})
	})
	assert.NoError(t, err)

	assertOutboxEmpty(t, d, tableName, 2*time.Second)
	assert.Equal(t, int64(1), readCounter(t, reader, "strongforce.forwarder.direct.failed"))
	assert.GreaterOrEqual(t, readCounter(t, reader, "strongforce.forwarder.poller.published"), int64(1))
	mockBus.AssertExpectations(t)
}

func TestDirectEmitDroppedGoesToPollerMySQL(t *testing.T) {
	testDirectEmitDroppedGoesToPoller(t, "mysql", "event_outbox_direct_drop")
}

func TestDirectEmitDroppedGoesToPollerPostgres(t *testing.T) {
	testDirectEmitDroppedGoesToPoller(t, "postgres", "event_outbox_direct_drop")
}

// testDirectEmitDroppedGoesToPoller: DirectQueueSize=0 and DirectWorkers=0
// forces every event to be dropped from the direct path; the poller must
// still deliver them.
func testDirectEmitDroppedGoesToPoller(t *testing.T, driver, tableName string) {
	mockBus := &mocks.Bus{}
	d := newDirectEmitDB(t, driver, tableName)
	assert.NoError(t, d.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(d, tableName))
	defer d.Close()

	metrics, reader := newTestMetrics(t)
	fw, err := forwarder.New(d, mockBus, &forwarder.Options{
		PollingInterval: 100 * time.Millisecond,
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
		DirectEmit:      true,
		DirectWorkers:   0, // no workers → every enqueue drops
		DirectQueueSize: 0,
		Metrics:         metrics,
	})
	assert.NoError(t, err)
	attachForwarder(t, d, fw)

	mockBus.On("Publish", mock.Anything).Return(nil).Once()

	go fw.Start(context.Background())
	defer fw.Stop()

	eventBuilder := &events.Builder{}
	_, err = d.EventTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error) {
		return eventBuilder.New("drop.topic", &jsonPayloadForDirect{Data: "polled"})
	})
	assert.NoError(t, err)

	assertOutboxEmpty(t, d, tableName, 2*time.Second)
	assert.Equal(t, int64(1), readCounter(t, reader, "strongforce.forwarder.direct.dropped"))
	assert.Equal(t, int64(0), readCounter(t, reader, "strongforce.forwarder.direct.published"))
	assert.GreaterOrEqual(t, readCounter(t, reader, "strongforce.forwarder.poller.published"), int64(1))
	mockBus.AssertExpectations(t)
}

// assertOutboxEmpty polls the outbox table until it is empty or the timeout
// elapses. Avoids flakiness from fixed sleeps.
func assertOutboxEmpty(t *testing.T, d db.DB, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := sharedtest.GetEventEntities(d, tableName)
		assert.NoError(t, err)
		if len(rows) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, _ := sharedtest.GetEventEntities(d, tableName)
	t.Fatalf("outbox %q not empty after %s: %d rows remain", tableName, timeout, len(rows))
}
