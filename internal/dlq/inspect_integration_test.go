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

func TestInspectLastNMessagesIntegration(t *testing.T) {
	brokers := requireBroker(t)
	dlq, _ := createScratchTopic(t, "inspect", 2)

	before := time.Now()
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1"), Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})
	produceToPartition(t, brokers, dlq, 1, []kafka.Message{
		{Key: []byte("k4"), Value: []byte("v4")},
		{Key: []byte("k5"), Value: []byte("v5")},
		{Key: []byte("k6"), Value: []byte("v6")},
	})
	after := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs, err := Inspect(ctx, brokers, dlq, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 5, "limit respected across partitions")

	var keys []string
	for _, m := range msgs {
		keys = append(keys, string(m.Key))
	}
	// Tails are p0[k0 k1 k2 k3] and p1[k4 k5 k6]; round-robin interleave
	// of the tails capped at the limit.
	assert.Equal(t, []string{"k0", "k4", "k1", "k5", "k2"}, keys,
		"last 5 messages round-robin across partitions")

	var found bool
	for _, m := range msgs {
		assert.Equal(t, dlq, m.Topic)
		assert.GreaterOrEqual(t, m.Offset, int64(0))
		assert.True(t, m.Timestamp.After(before.Add(-time.Minute)),
			"timestamp not before produce window")
		assert.True(t, m.Timestamp.Before(after.Add(time.Minute)),
			"timestamp not after produce window")
		if string(m.Key) == "k1" {
			found = true
			assert.Equal(t, []byte("v1"), m.Value, "value round-trips")
			assert.Equal(t, "DB_TIMEOUT", m.Headers["error"], "header round-trips")
		}
	}
	assert.True(t, found, "message with headers was read")
}

func TestInspectEmptyTopicIntegration(t *testing.T) {
	brokers := requireBroker(t)
	dlq, _ := createScratchTopic(t, "inspect-empty", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs, err := Inspect(ctx, brokers, dlq, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs, "empty DLQ yields clean empty output")
}
