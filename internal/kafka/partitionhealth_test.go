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

func TestPartitionHealthNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, _, err := c.PartitionHealth(context.Background(), "orders")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestPartitionHealthIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	partitions, errored, err := client.PartitionHealth(ctx, "orders")
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	assert.Equal(t, 6, partitions, "orders has 6 partitions")
	assert.Zero(t, errored, "no errored partitions expected")
}
