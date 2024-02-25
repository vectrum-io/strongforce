package main

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/db/migrator"
	"github.com/vectrum-io/strongforce/pkg/db/postgres"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

func main() {

	sf, err := strongforce.New(
		strongforce.WithPostgres(&postgres.Options{
			DSN: "postgresql://strongforce:strongforce@127.0.0.1:64003/strongforce?sslmode=disable",
			OutboxOptions: &outbox.Options{
				TableName:  "event_outbox",
				Serializer: serialization.NewJSONSerializer(),
			},
			Migrator: migrator.NewAtlasMigrator(&migrator.AtlasOptions{
				MigrationDir: "./atlas/migrations",
			}),
		}))

	if err != nil {
		panic(err)
	}

	if err := sf.Init(); err != nil {
		panic(err)
	}

	if _, err := sf.DB().Migrate(context.Background(), &db.MigrationOptions{}); err != nil {
		panic(err)
	}

	eventId, err := sf.DB().EventTx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error) {
		if _, err := tx.ExecContext(ctx, "INSERT INTO test (value) VALUES ($1)", "test user"); err != nil {
			return nil, fmt.Errorf("failed to insert test user: %w", err)
		}

		return sf.EventBuilder().New("user.created", "test event")
	})

	if err != nil {
		panic(fmt.Errorf("failed to create event: %w", err))
	}

	fmt.Println("event id: " + eventId.String())
}
