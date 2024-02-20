package tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"github.com/vectrum-io/strongforce/tests/mocks"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"log"
	"testing"
	"time"
)

var (
	expectedOutboundMessageOk = bus.OutboundMessage{
		Id:      "test-event",
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
		DSN: sharedtest.DSN,
	})
	assert.NoError(t, err)

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

	// insert event into outbox
	//goland:noinspection ALL
	_, err = db.Connection().Exec(
		fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?)", tableName),
		"test-event", "test", []byte{69, 42, 0}, time.Now(),
	)
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
func TestForwardPostgres(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_4"

	db, err := postgres.New(postgres.Options{
		DSN: sharedtest.PostgresDSN,
	})
	assert.NoError(t, err)

	assert.NoError(t, db.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTablePostgres(db, tableName))

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

	// insert event into outbox
	//goland:noinspection ALL
	stmt, err := db.Connection().Preparex(fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES ($1, $2, $3, $4)", tableName))
	if err != nil {
		log.Fatalln(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec("test-event", "test", []byte{69, 42, 0}, time.Now())
	if err != nil {
		fmt.Println(err)
	}
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

func TestForwardFailedMySQL(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_2"

	db, err := mysql.New(mysql.Options{
		DSN: sharedtest.DSN,
	})
	assert.NoError(t, err)
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
		fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES (?, ?, ?, ?), (?, ?, ?, ?)", tableName),
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

func TestForwardFailedPostgres(t *testing.T) {
	mockBus := &mocks.Bus{}
	tableName := "event_outbox_fw_2"

	db, err := postgres.New(postgres.Options{
		DSN: sharedtest.PostgresDSN,
	})
	assert.NoError(t, err)
	assert.NoError(t, db.Connect())
	assert.NoError(t, sharedtest.CreateOutboxTablePostgres(db, tableName))

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
	stmt, err := db.Connection().Preparex(fmt.Sprintf("INSERT INTO %s (id, topic, payload, created_at) VALUES ($1,$2,$3,$4), ($5,$6,$7,$8)", tableName))
	assert.NoError(t, err)
	_, err = stmt.Exec(
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
