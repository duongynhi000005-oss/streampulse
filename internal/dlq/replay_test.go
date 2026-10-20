// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayNoBrokers(t *testing.T) {
	_, err := Replay(context.Background(), ReplayOptions{Topic: "payments.dlq"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestReplayNotDLQTopic(t *testing.T) {
	_, err := Replay(context.Background(), ReplayOptions{Brokers: []string{"localhost:9093"}, Topic: "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a DLQ topic")
}

func TestParseFilter(t *testing.T) {
	key, value, err := ParseFilter("error=DB_TIMEOUT")
	require.NoError(t, err)
	assert.Equal(t, "error", key)
	assert.Equal(t, "DB_TIMEOUT", value)

	key, value, err = ParseFilter("retry=3")
	require.NoError(t, err)
	assert.Equal(t, "retry", key)
	assert.Equal(t, "3", value)

	_, _, err = ParseFilter("novalue")
	require.Error(t, err)

	_, _, err = ParseFilter("=v")
	require.Error(t, err)

	_, _, err = ParseFilter("")
	require.Error(t, err)
}

func TestReplayDefaults(t *testing.T) {
	assert.Equal(t, 50, DefaultReplayBatchSize)
	assert.Equal(t, "streampulse-replay", DefaultReplayGroup)
	assert.Equal(t, "x-streampulse-replayed", ReplayHeader)
	assert.Equal(t, "x-streampulse-source-offset", SourceOffsetHeader)
}

func TestReplayFilterExclude(t *testing.T) {
	now := time.Now()
	m := kafka.Message{
		Time: now.Add(-2 * time.Hour),
		Headers: []kafka.Header{
			{Key: "error", Value: []byte("DB_TIMEOUT")},
		},
	}

	matches := replayFilter{key: "error", value: "DB_TIMEOUT", olderThan: time.Hour}
	assert.False(t, matches.exclude(m, now), "header matches and old enough")

	wrongValue := replayFilter{key: "error", value: "OTHER"}
	assert.True(t, wrongValue.exclude(m, now), "header value mismatch excluded")

	wrongKey := replayFilter{key: "source", value: "DB_TIMEOUT"}
	assert.True(t, wrongKey.exclude(m, now), "missing header excluded")

	tooFresh := replayFilter{key: "error", value: "DB_TIMEOUT", olderThan: 3 * time.Hour}
	assert.True(t, tooFresh.exclude(m, now), "not old enough for older-than excluded")

	old := kafka.Message{Time: now.Add(-5 * time.Hour)}
	tooOld := replayFilter{newerThan: 3 * time.Hour}
	assert.True(t, tooOld.exclude(old, now), "older than the newer-than window excluded")
	inWindow := replayFilter{newerThan: time.Hour}
	assert.True(t, inWindow.exclude(m, now), "2h-old message excluded by newer-than-1h window")
	recent := kafka.Message{Time: now.Add(-30 * time.Minute)}
	assert.False(t, inWindow.exclude(recent, now), "30min-old message inside the newer-than-1h window")

	noFilters := replayFilter{}
	assert.False(t, noFilters.exclude(m, now), "no filters excludes nothing")
}

func TestHasHeader(t *testing.T) {
	m := kafka.Message{Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}}
	assert.True(t, hasHeader(m, "error"))
	assert.False(t, hasHeader(m, "other"))
	assert.False(t, hasHeader(kafka.Message{}, "error"))
}

func TestReplayHeaderPreserved(t *testing.T) {
	m := kafka.Message{Partition: 0, Offset: 42, Key: []byte("k"), Value: []byte("v"),
		Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}}
	out := replayMessage("payments", m)
	assert.Equal(t, "payments", out.Topic)
	assert.Equal(t, []byte("k"), out.Key)
	assert.Equal(t, []byte("v"), out.Value)
	assert.Equal(t, "true", headerValue(out, ReplayHeader))
	assert.Equal(t, "42", headerValue(out, SourceOffsetHeader))
	assert.Equal(t, "DB_TIMEOUT", headerValue(out, "error"), "original headers preserved")
}
