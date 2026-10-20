// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicOffsetsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.TopicOffsets(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestTopicOffsetsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offsets, err := client.TopicOffsets(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	require.Contains(t, offsets, "orders", "orders topic expected in offsets")
	require.Len(t, offsets["orders"], 6, "orders has 6 partitions")
	for partition, offset := range offsets["orders"] {
		require.GreaterOrEqual(t, offset, int64(0), "partition %d", partition)
	}
	for name := range offsets {
		assert.False(t, isInternalTopic(name), "internal topic %q must not be collected", name)
	}
}

func TestGroupLagNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.GroupLag(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestLagFromOffsets(t *testing.T) {
	hw := map[string]map[int]int64{
		"orders":   {0: 100, 1: 100, 2: 100},
		"payments": {0: 50},
	}
	cases := []struct {
		name      string
		committed map[string]map[int]int64
		want      map[string]int64
	}{
		{
			name:      "partial commit counts missing partitions as full lag",
			committed: map[string]map[int]int64{"orders": {0: 90, 2: 100}},
			want:      map[string]int64{"orders": 110}, // p0: 10, p1 no commit: 100, p2: 0
		},
		{
			name:      "fully consumed topic has zero lag",
			committed: map[string]map[int]int64{"orders": {0: 100, 1: 100, 2: 100}},
			want:      map[string]int64{},
		},
		{
			name:      "commit ahead of high-watermark floors to zero",
			committed: map[string]map[int]int64{"orders": {0: 120}},
			want:      map[string]int64{"orders": 200}, // p0 floored, p1/p2 no commit
		},
		{
			name:      "topic with no high-watermark is skipped",
			committed: map[string]map[int]int64{"other": {0: 10}},
			want:      map[string]int64{},
		},
		{
			name:      "no commits yields empty lag",
			committed: nil,
			want:      map[string]int64{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, lagFromOffsets(tc.committed, hw))
		})
	}
}

func TestGroupLagIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lag, err := client.GroupLag(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	require.Contains(t, lag, "orders-processor", "orders-processor group expected")
	for topic, l := range lag["orders-processor"] {
		require.GreaterOrEqual(t, l, int64(0), "topic %s", topic)
	}
}
