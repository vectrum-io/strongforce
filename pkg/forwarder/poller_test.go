package forwarder

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func newPollerForwarder(t *testing.T, options *Options) *DBForwarder {
	t.Helper()
	fw, err := New(nil, nil, options)
	assert.NoError(t, err)
	return fw
}

func TestNextPollDelayBacksOffUntilCapOnFailure(t *testing.T) {
	fw := newPollerForwarder(t, &Options{
		PollingInterval:  time.Second,
		PollerMaxBackoff: 5 * time.Second,
	})
	failure := errors.New("bus down")

	delay := fw.pollingInterval
	var delays []time.Duration
	for i := 0; i < 4; i++ {
		delay = fw.nextPollDelay(delay, pollResult{}, failure)
		delays = append(delays, delay)
	}

	assert.Equal(t, []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}, delays)
}

func TestNextPollDelayResetsAfterSuccess(t *testing.T) {
	fw := newPollerForwarder(t, &Options{PollingInterval: time.Second})

	assert.Equal(t, time.Second, fw.nextPollDelay(time.Minute, pollResult{}, nil))
}

func TestNextPollDelayBacksOffFromIntervalAfterDrain(t *testing.T) {
	fw := newPollerForwarder(t, &Options{PollingInterval: time.Second})

	assert.Equal(t, time.Second, fw.nextPollDelay(0, pollResult{}, errors.New("bus down")))
}

func TestNextPollDelayPollsImmediatelyWhileRowsAreLeft(t *testing.T) {
	fw := newPollerForwarder(t, &Options{PollingInterval: time.Second})

	assert.Zero(t, fw.nextPollDelay(time.Second, pollResult{hasMore: true}, nil))
}

func TestPollQueryWithoutDirectEmitHasNoCutoff(t *testing.T) {
	fw := newPollerForwarder(t, &Options{OutboxTableName: "outbox", PollerBatchSize: 50})

	query, args := fw.pollQuery(time.Now())

	assert.Equal(t, "SELECT id, topic, payload, created_at FROM outbox ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", query)
	assert.Equal(t, []interface{}{50}, args)
}

func TestPollQueryCutsOffRowsYoungerThanGracePeriod(t *testing.T) {
	fw := &DBForwarder{
		outboxTableName:   "outbox",
		directEmit:        true,
		pollerGracePeriod: 10 * time.Second,
		pollerBatchSize:   100,
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

	query, args := fw.pollQuery(now)

	assert.Contains(t, query, "WHERE id < ?")
	if assert.Len(t, args, 2) {
		cutoff := ulid.MustParse(args[0].(string))
		assert.Equal(t, now.Add(-10*time.Second), cutoff.Timestamp().UTC())
		assert.True(t, strings.HasSuffix(args[0].(string), strings.Repeat("0", 16)))
		assert.Equal(t, 100, args[1])
	}

	olderId := ulid.MustNew(ulid.Timestamp(now.Add(-11*time.Second)), ulid.DefaultEntropy()).String()
	youngerId := ulid.MustNew(ulid.Timestamp(now.Add(-9*time.Second)), ulid.DefaultEntropy()).String()
	assert.Less(t, olderId, args[0].(string))
	assert.Greater(t, youngerId, args[0].(string))
}

func TestPollQueryIgnoresDisabledGracePeriod(t *testing.T) {
	fw := &DBForwarder{
		outboxTableName:   "outbox",
		directEmit:        true,
		pollerGracePeriod: -1,
		pollerBatchSize:   100,
	}

	query, _ := fw.pollQuery(time.Now())

	assert.NotContains(t, query, "WHERE")
}

func TestOptionsDefaultPollerSettings(t *testing.T) {
	options := &Options{PollingInterval: 5 * time.Second}
	assert.NoError(t, options.validate())

	assert.Equal(t, DefaultPollerBatchSize, options.PollerBatchSize)
	assert.Equal(t, DefaultPollerBatchBudget, options.PollerBatchBudget)
	assert.Equal(t, DefaultPollerGracePeriod, options.PollerGracePeriod)
	assert.Equal(t, DefaultPollerMaxBackoff, options.PollerMaxBackoff)
	assert.Equal(t, DefaultPublishTimeout, options.PublishTimeout)
}

func TestOptionsMaxBackoffNeverBelowPollingInterval(t *testing.T) {
	options := &Options{PollingInterval: 2 * time.Minute}
	assert.NoError(t, options.validate())

	assert.Equal(t, 2*time.Minute, options.PollerMaxBackoff)
}
