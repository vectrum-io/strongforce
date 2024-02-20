package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"testing"
)

func TestClientCreationMySQL(t *testing.T) {

	outboxTable := "client_creation_1"

	// create new client
	client, err := strongforce.New(
		strongforce.WithMySQL(&mysql.Options{
			DSN: sharedtest.MySQLDSN,
			OutboxOptions: &outbox.Options{
				TableName: outboxTable,
			},
		}),
		strongforce.WithForwarder(&forwarder.Options{
			OutboxTableName: outboxTable,
		}),
		strongforce.WithNATS(&nats.Options{
			NATSAddress: sharedtest.NATS,
		}),
	)

	assert.NoError(t, err)

	assert.NotNil(t, client.DB())
	assert.NotNil(t, client.Bus())
	assert.NotNil(t, client.EventBuilder())

	assert.NoError(t, client.Init())

	assert.NoError(t, client.DB().Connection().Ping())
}

func TestClientCreationPostgres(t *testing.T) {

	outboxTable := "client_creation_2"

	// create new client
	client, err := strongforce.New(
		strongforce.WithPostgres(&postgres.Options{
			DSN: sharedtest.PostgresDSN,
			OutboxOptions: &outbox.Options{
				TableName: outboxTable,
			},
		}),
		strongforce.WithForwarder(&forwarder.Options{
			OutboxTableName: outboxTable,
		}),
		strongforce.WithNATS(&nats.Options{
			NATSAddress: sharedtest.NATS,
		}),
	)

	assert.NoError(t, err)

	assert.NotNil(t, client.DB())
	assert.NotNil(t, client.Bus())
	assert.NotNil(t, client.EventBuilder())

	assert.NoError(t, client.Init())

	assert.NoError(t, client.DB().Connection().Ping())
}
