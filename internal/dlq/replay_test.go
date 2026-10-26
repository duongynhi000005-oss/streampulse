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


func TestRecordMarkedKeyScansBeyondLimitWindow(t *testing.T) {
	// Simulate 5 original messages at offsets 0..4 and 5 marker copies at
	// offsets 5..9 (past a Limit of 3). Full-topic marker scanning visits all
	// 10 messages so keys k0..k2 inside the [0, 3) window are recognized as
	// already replayed.
	fullPartition := []kafka.Message{
		{Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 3, Key: []byte("k3"), Value: []byte("v3")},
		{Offset: 4, Key: []byte("k4"), Value: []byte("v4")},
		replayMessage("payments.dlq", kafka.Message{Offset: 0, Key: []byte("k0"), Value: []byte("v0")}),
		replayMessage("payments.dlq", kafka.Message{Offset: 1, Key: []byte("k1"), Value: []byte("v1")}),
		replayMessage("payments.dlq", kafka.Message{Offset: 2, Key: []byte("k2"), Value: []byte("v2")}),
		replayMessage("payments.dlq", kafka.Message{Offset: 3, Key: []byte("k3"), Value: []byte("v3")}),
		replayMessage("payments.dlq", kafka.Message{Offset: 4, Key: []byte("k4"), Value: []byte("v4")}),
	}

	marked := make(map[string]struct{})
	for _, m := range fullPartition {
		recordMarkedKey(m, marked)
	}

	limitWindow := fullPartition[:3]
	var replayed, skipped int
	for _, m := range limitWindow {
		if hasHeader(m, SourceOffsetHeader) {
			skipped++
			continue
		}
		if _, ok := marked[string(m.Key)]; ok {
			skipped++
			continue
		}
		replayed++
	}
	assert.Equal(t, 0, replayed, "messages with markers past Limit must not be replayed again")
	assert.Equal(t, 3, skipped, "all messages in the limited window are skipped")
}
