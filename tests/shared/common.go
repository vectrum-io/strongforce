package sharedtest

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
)

//goland:noinspection SqlNoDataSourceInspection
func CreateOutboxTable(db db.DB, name string) error {
	_, err := db.Connection().Exec(fmt.Sprintf(
		"DROP TABLE IF EXISTS %s",
		name,
	))
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		  id char(36) NOT NULL,
		  topic varchar(255) NOT NULL,
		  payload blob NOT NULL,
		  created_at datetime NULL DEFAULT CURRENT_TIMESTAMP,
		  PRIMARY KEY (id)
		)
	`, name)

	_, err = db.Connection().Exec(query)
	return err
}

func GetEventEntities(db db.DB, tableName string) ([]*outbox.EventEntity, error) {
	var obEvents []*outbox.EventEntity
	if err := db.Connection().Select(&obEvents, fmt.Sprintf("SELECT * FROM %s", tableName)); err != nil {
		return nil, err
	}

	return obEvents, nil
}

func CreateNatsStream(address string, name string, subject string) error {
	nc, err := nats.Connect(address)
	if err != nil {
		return err
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return err
	}

	// ignore error if stream does not exist
	_ = js.DeleteStream(name)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
	})

	return err
}

func GetNATSStream(address string, name string) (*nats.StreamInfo, error) {
	nc, err := nats.Connect(address)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	return js.StreamInfo(name)
}
