package bus

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

type mockContext struct {
	mock.Mock
}

func (m *mockContext) Stop() {
	m.Called()
}

type mockMessage struct {
	mock.Mock
	msg       *InboundMessage
	processed bool
}

func (m *mockMessage) Ack() error {
	args := m.Called()
	m.processed = true
	return args.Error(0)
}

func (m *mockMessage) Nak(retryAfter time.Duration) error {
	args := m.Called(retryAfter)
	m.processed = true
	return args.Error(0)
}

func (m *mockMessage) WaitUntilProcessed() {
	for {
		if m.processed {
			return
		}
		time.Sleep(100 * time.Nanosecond)
	}
}

func createMockMessage(id string, subject string) *mockMessage {
	msg := &InboundMessage{
		Id:      id,
		Subject: subject,
		Data:    []byte{},
	}

	m := &mockMessage{
		msg: msg,
	}

	msg.Ack = m.Ack
	msg.Nak = m.Nak

	return m
}

func TestSubscription(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)
	sub := NewSubscription(mockChan, nil, mockCtx.Stop)

	err := sub.AddHandler("test.*", func(ctx context.Context, message InboundMessage) error {
		return nil
	})
	assert.NoError(t, err)

	mocks := make([]*mock.Mock, 10)
	for i := 0; i < 10; i++ {
		msg := createMockMessage(fmt.Sprintf("%d", i), "test.a")
		msg.On("Ack").Once().Return(nil)

		mocks[i] = &msg.Mock
		mockChan <- *msg.msg
	}

	// start handler
	sub.Start(context.Background())

	// wait until all messages were processed
	for {
		if len(mockChan) == 0 {
			break
		}
	}

	for _, msgMock := range mocks {
		msgMock.AssertExpectations(t)
	}
}

func TestSubscriptionAddHandlerTwice(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)
	sub := NewSubscription(mockChan, nil, mockCtx.Stop)

	err := sub.AddHandler("test.*", func(ctx context.Context, message InboundMessage) error {
		return nil
	})
	assert.NoError(t, err)

	errTwo := sub.AddHandler("test.*", func(ctx context.Context, message InboundMessage) error {
		return nil
	})
	assert.ErrorIs(t, errTwo, ErrHandlerRegistrationFailed)
}

func TestSubscriptionClose(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)
	sub := NewSubscription(mockChan, nil, mockCtx.Stop)

	sub.Start(context.Background())

	mockCtx.On("Stop").Once()
	sub.Stop()

	mockCtx.AssertExpectations(t)
}

func TestRouteMessageToTwoHandlers(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)
	sub := NewSubscription(mockChan, nil, mockCtx.Stop)

	receivedOne := false
	receivedTwo := false
	receivedThree := false

	subOneErr := sub.AddHandler("test.*", func(ctx context.Context, message InboundMessage) error {
		receivedOne = true
		return nil
	})
	assert.NoError(t, subOneErr)

	subTwoErr := sub.AddHandler("test.wow", func(ctx context.Context, message InboundMessage) error {
		receivedTwo = true
		return nil
	})
	assert.NoError(t, subTwoErr)

	subThreeErr := sub.AddHandler("test.nothere", func(ctx context.Context, message InboundMessage) error {
		receivedThree = true
		return nil
	})
	assert.NoError(t, subThreeErr)

	sub.Start(context.Background())

	msg := createMockMessage("1", "test.wow")
	msg.On("Ack").Return(nil).Once()
	mockChan <- *msg.msg

	// wait until messages was acked

	msg.WaitUntilProcessed()

	assert.Truef(t, receivedOne, "message not received in handler one")
	assert.Truef(t, receivedTwo, "message not received in handler two")
	assert.Falsef(t, receivedThree, "message received in handler three")

	msg.AssertExpectations(t)
}

func TestErrorCallbackHandlersFailed(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)

	sub := NewSubscription(mockChan, nil, mockCtx.Stop)
	sub.Start(context.Background())

	subOneErr := sub.AddHandler("test.*", func(ctx context.Context, message InboundMessage) error {
		return fmt.Errorf("dummy one")
	})
	assert.NoError(t, subOneErr)

	subTwoErr := sub.AddHandler("test.wow", func(ctx context.Context, message InboundMessage) error {
		return fmt.Errorf("dummy two")
	})
	assert.NoError(t, subTwoErr)

	var subErrors []error
	sub.OnError(func(err error) {
		subErrors = append(subErrors, err)
	})

	msg := createMockMessage("1", "test.wow")
	mockChan <- *msg.msg

	assert.Eventually(t, func() bool {
		return len(subErrors) == 1
	}, 1*time.Millisecond, 10*time.Nanosecond)

	assert.ErrorIs(t, subErrors[0], ErrMessageHandlerFailed)
	assert.ErrorContains(t, subErrors[0], "dummy one")
	assert.ErrorContains(t, subErrors[0], "dummy two")

	msg.AssertExpectations(t)
}

func TestErrorCallbackNotRoutable(t *testing.T) {
	mockCtx := &mockContext{}
	mockChan := make(chan InboundMessage, 256)

	sub := NewSubscription(mockChan, nil, mockCtx.Stop)
	sub.Start(context.Background())

	var subErrors []error
	sub.OnError(func(err error) {
		subErrors = append(subErrors, err)
	})

	msg := createMockMessage("1", "test.wow")
	mockChan <- *msg.msg

	assert.Eventually(t, func() bool {
		return len(subErrors) == 1
	}, 1*time.Millisecond, 10*time.Nanosecond)

	assert.ErrorIs(t, subErrors[0], ErrMessageNotRoutable)

	msg.AssertExpectations(t)
}
