package strongforce

import (
	"context"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"go.uber.org/zap"
)

type Client struct {
	db           db.DB
	eventBuilder *events.Builder
	forwarder    *forwarder.DBForwarder
	bus          bus.Bus
}

type Options struct {
	DB     db.DB
	Bus    bus.Bus
	Logger *zap.Logger
}

func New(opts ...Option) (*Client, error) {
	options := &clientOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options.CreateClient()
}

func (sf *Client) Init() error {
	if sf.db != nil {
		if err := sf.db.Connect(); err != nil {
			return err
		}
	}
	if sf.forwarder != nil {
		go func() {
			err := sf.forwarder.Start(context.Background())
			zap.L().Error("failed to run forwarder: " + err.Error())
		}()
	}

	return nil
}

func (sf *Client) DB() db.DB {
	return sf.db
}

func (sf *Client) Bus() bus.Bus {
	return sf.bus
}

func (sf *Client) EventBuilder() *events.Builder {
	return sf.eventBuilder
}
