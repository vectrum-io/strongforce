package tests

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	modelsv1 "github.com/vectrum-io/strongforce/protobuf/gen/strongforce/models/v1"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"google.golang.org/protobuf/proto"
	"testing"
)

type outboxTestCase struct {
	name                string
	serializer          serialization.Serializer
	payload             interface{}
	validatePayloadFunc func(t *testing.T, testCase *outboxTestCase, dbPayload []byte)
}
type jsonPayload struct {
	Data string `json:"data"`
}

var dbDrivers = []string{"mysql", "postgres"}

func createDB(driver string, tableName string, serializer serialization.Serializer) (db.DB, error) {
	if driver == "mysql" {
		return mysql.New(mysql.Options{
			DSN: sharedtest.MySQLDSN,
			OutboxOptions: &outbox.Options{
				TableName:  tableName,
				Serializer: serializer,
			},
		})
	} else if driver == "postgres" {
		return postgres.New(postgres.Options{
			DSN: sharedtest.PostgresDSN,
			OutboxOptions: &outbox.Options{
				TableName:  tableName,
				Serializer: serializer,
			},
		})
	}

	return nil, fmt.Errorf("unsupported driver: %s", driver)
}

func TestOutbox(t *testing.T) {
	testCases := []outboxTestCase{
		{
			name:       "JSON",
			serializer: serialization.NewJSONSerializer(),
			payload:    &jsonPayload{Data: "test"},
			validatePayloadFunc: func(t *testing.T, testCase *outboxTestCase, dbPayload []byte) {
				payload := &jsonPayload{}
				err := testCase.serializer.Deserialize(dbPayload, payload)
				assert.NoError(t, err)
				assert.Equal(t, testCase.payload, payload)
			},
		},
		{
			name:       "Protobuf",
			serializer: serialization.NewProtobufSerializer(),
			payload:    &modelsv1.TestEvent{Data: "test"},
			validatePayloadFunc: func(t *testing.T, testCase *outboxTestCase, dbPayload []byte) {
				payload := &modelsv1.TestEvent{}
				err := testCase.serializer.Deserialize(dbPayload, payload)
				assert.NoError(t, err)
				assert.True(t, proto.Equal(testCase.payload.(proto.Message), payload))
			},
		},
	}

	eventBuilder := events.Builder{}

	for _, driver := range dbDrivers {
		for i, testCase := range testCases {
			tableName := fmt.Sprintf("event_outbox_ob_1_%d", i)

			t.Run(testCase.name, func(t *testing.T) {
				t.Logf("Running test case: %s", testCase.name)
				db, err := createDB(driver, tableName, testCase.serializer)
				assert.NoError(t, err)
				t.Logf("Connecting to %s database", driver)
				assert.NoError(t, db.Connect())
				t.Logf("Creating outbox table: %s", tableName)
				assert.NoError(t, sharedtest.CreateOutboxTable(db, tableName))

				// run test cases
				t.Logf("creating event with payload: %v", testCase.payload)
				eventId, err := db.EventTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error) {
					return eventBuilder.New("test", testCase.payload)
				})

				assert.NoError(t, err)
				assert.NotEmpty(t, eventId)

				// check if event exists in outbox table
				obEvents, err := sharedtest.GetEventEntities(db, tableName)
				assert.NoError(t, err)

				// check if event is correct
				assert.Len(t, obEvents, 1)

				assert.Equal(t, obEvents[0].Id.String, eventId.String())
				assert.Equal(t, obEvents[0].Topic.String, "test")

				testCase.validatePayloadFunc(t, &testCase, obEvents[0].Payload)
				assert.NoError(t, db.Close())
			})
		}
	}
}

func TestMultiEventOutbox(t *testing.T) {
	tableName := "multi_outbox"
	eventBuilder := events.Builder{}

	for _, driver := range dbDrivers {
		t.Run(driver, func(t *testing.T) {
			db, err := createDB(driver, tableName, serialization.NewJSONSerializer())
			assert.NoError(t, err)
			assert.NoError(t, db.Connect())
			assert.NoError(t, sharedtest.CreateOutboxTable(db, tableName))

			// run test cases
			eventIds, err := db.EventsTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) ([]*events.EventSpec, error) {
				e1, err := eventBuilder.New("topic.1", &jsonPayload{Data: "test1"})
				assert.NoError(t, err)

				e2, err := eventBuilder.New("topic.2", &jsonPayload{Data: "test2"})
				assert.NoError(t, err)

				return []*events.EventSpec{e1, e2}, nil
			})

			assert.NoError(t, err)
			assert.Len(t, eventIds, 2)

			// check if event exists in outbox table
			obEvents, err := sharedtest.GetEventEntities(db, tableName)
			assert.NoError(t, err)

			assert.Len(t, obEvents, 2)

			assert.Equal(t, obEvents[0].Topic.String, "topic.1")
			assert.Equal(t, obEvents[1].Topic.String, "topic.2")
		})
	}
}
