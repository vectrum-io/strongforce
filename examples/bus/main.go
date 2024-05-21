package main

import (
	"context"
	"fmt"
	nats2 "github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"time"
)

func main() {

	tp, err := setupTracing()
	if err != nil {
		panic(err)
	}

	if err := SimpleNATSPubSub(); err != nil {
		panic(err)
	}

	tp.Shutdown(context.Background())
}

func setupTracing() (*sdktrace.TracerProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.NewClient("127.0.0.1:32317",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)

	return tracerProvider, nil
}

func SimpleNATSPubSub() error {
	logger, _ := zap.NewDevelopment()
	tracer := otel.Tracer("test-tracer")

	sf, err := strongforce.New(strongforce.WithNATS(&nats.Options{
		NATSAddress: "nats://localhost:64002",
		Logger:      logger,
		Streams: []nats2.StreamConfig{
			{
				Name:        "test",
				Description: "test stream",
				Subjects: []string{
					"test.>",
				},
				Retention:    nats2.LimitsPolicy,
				MaxConsumers: -1,
				MaxMsgs:      -1,
				MaxBytes:     -1,
				Discard:      nats2.DiscardOld,
				Storage:      nats2.FileStorage,
				Duplicates:   time.Minute,
			},
		},
		OTelPropagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}))

	if err != nil {
		return fmt.Errorf("failed to create strongforce client: %w", err)
	}

	if err := sf.Bus().Migrate(context.Background()); err != nil {
		return fmt.Errorf("failed to migrate bus: %w", err)
	}

	subscription, subscribeErr := sf.Bus().Subscribe(context.Background(), "test", "test",
		bus.WithFilterSubject("test.>"),
	)

	if subscribeErr != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", subscribeErr)
	}

	if err := subscription.AddHandler("test.howdy", func(ctx context.Context, message bus.InboundMessage) error {
		_, span := tracer.Start(ctx, "received message")
		defer span.End()

		fmt.Printf("RECEIVED FROM NATS: %s\n", string(message.Data))
		return nil
	}); err != nil {
		return fmt.Errorf("failed to add handler: %w", err)
	}

	subscription.Start(context.Background())

	ctx, span := tracer.Start(context.Background(), "pre publish")

	if err := sf.Bus().Publish(ctx, &bus.OutboundMessage{
		Id:      "1234",
		Subject: "test.howdy",
		Data:    []byte("hello world"),
	}); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	span.End()

	time.Sleep(1 * time.Second)

	return nil
}
