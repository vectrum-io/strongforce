package testhelper

import (
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
)

type TestHelper struct {
	DB  db.DB
	Bus bus.Bus
}

func (th *TestHelper) Outbox() (*OutboxHelpers, error) {
	if th.Bus == nil || th.DB == nil {
		return nil, fmt.Errorf("bus and db must be set to use outbox test helpers")
	}

	return &OutboxHelpers{
		bus: th.Bus,
		db:  th.DB,
	}, nil
}
