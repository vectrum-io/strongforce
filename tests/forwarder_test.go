package tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"github.com/vectrum-io/strongforce/tests/mocks"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"testing"
	"time"
)

var (
	expectedOutboundMessageOk = bus.OutboundMessage{
		Id:      "test-event",
		Subject: "test",
		Data:    []byte{69, 42, 0},
	}
	expectedOutboundMessageTwoOk = bus.OutboundMessage{
		Id:      "test-event-2",
		Subject: "test",
		Data:    []byte{69, 42, 0},
	}
	expectedOutboundMessageFail = bus.OutboundMessage{
		Id:      "test-event-fail",
		Subject: "test",
		Data:    []byte{69, 42, 0},
	}
)

func TestForwardMySQL(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_1"

	db, err := mysql.New(mysql.Options{
		DSN: sharedtest.MySQLDSN,
	})
	assert.NoError(t, err)

	testForward(t, mockBus, db, tableName)
}

func TestForwardFailedMySQL(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_2"

	db, err := mysql.New(mysql.Options{
		DSN: sharedtest.MySQLDSN,
	})
	assert.NoError(t, err)

	testForwardFailed(t, mockBus, db, tableName)
}

func TestForwardPostgres(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_1"

	db, err := postgres.New(postgres.Options{
		DSN: sharedtest.PostgresDSN,
	})
	assert.NoError(t, err)

	testForward(t, mockBus, db, tableName)
}

func TestForwardFailedPostgres(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_2"

	db, err := postgres.New(postgres.Options{
		DSN: sharedtest.PostgresDSN,
	})
	assert.NoError(t, err)

	testForwardFailed(t, mockBus, db, tableName)
}

func testForward(t *testing.T, mockBus *mocks.Bus, db db.DB, tableName string) {
	assert.NoError(t, db.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(db, tableName))

	fw, err := forwarder.New(db, mockBus, &forwarder.Options{
		PollingInterval: 100 * time.Millisecond,
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
	})
	assert.NoError(t, err)

	go func() {
		fw.Start(context.Background())
	}()

	mockBus.On("Publish", expectedOutboundMessageOk).Return(nil).Once()
	mockBus.On("Publish", expectedOutboundMessageTwoOk).Return(nil).Once()

	// insert first event into outbox
	//goland:noinspection ALL
	_, err = db.Connection().Exec(db.Connection().Rebind(
		fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?)", tableName),
	), "test-event", "test", []byte{69, 42, 0}, time.Now())
	assert.NoError(t, err)

	// insert second event into outbox
	//goland:noinspection ALL
	_, err = db.Connection().Exec(db.Connection().Rebind(
		fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?)", tableName),
	), "test-event-2", "test", []byte{69, 42, 0}, time.Now())
	assert.NoError(t, err)

	time.Sleep(150 * time.Millisecond)

	// check if event has been deleted from table
	obEvents, err := sharedtest.GetEventEntities(db, tableName)
	assert.NoError(t, err)
	assert.Len(t, obEvents, 0)

	mockBus.AssertExpectations(t)
	assert.NoError(t, fw.Stop())
	assert.NoError(t, db.Close())
}

func testForwardFailed(t *testing.T, mockBus *mocks.Bus, db db.DB, tableName string) {
	assert.NoError(t, db.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTable(db, tableName))

	fw, err := forwarder.New(db, mockBus, &forwarder.Options{
		PollingInterval: 100 * time.Millisecond,
		Serializer:      serialization.NewJSONSerializer(),
		OutboxTableName: tableName,
	})
	assert.NoError(t, err)

	mockBus.On("Publish", expectedOutboundMessageFail).Return(errors.New("dummy error")).Once()
	mockBus.On("Publish", expectedOutboundMessageOk).Return(nil).Once()

	go func() {
		fw.Start(context.Background())
	}()

	// insert one good and one failed event into db
	//goland:noinspection ALL
	_, err = db.Connection().Exec(
		db.Connection().Rebind(fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?), (?, ?, ?, ?)", tableName)),
		"test-event-fail", "test", []byte{69, 42, 0}, time.Now(),
		"test-event", "test", []byte{69, 42, 0}, time.Now(),
	)
	assert.NoError(t, err)

	time.Sleep(150 * time.Millisecond)

	// check if good event was deleted and failed event is still in db
	obEvents, err := sharedtest.GetEventEntities(db, tableName)
	assert.NoError(t, err)
	assert.Len(t, obEvents, 1)
	assert.Equal(t, obEvents[0].Id.String, "test-event-fail")

	mockBus.AssertExpectations(t)
	assert.NoError(t, fw.Stop())
	assert.NoError(t, db.Close())
}
