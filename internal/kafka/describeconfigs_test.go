// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeConfigsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.DescribeConfigs(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestDescribeConfigsUnknownResourceType(t *testing.T) {
	c := NewClient([]string{"localhost:9093"})
	_, err := c.DescribeConfigs(context.Background(), []DescribeConfigResource{
		{Type: "bogus", Name: "orders", ConfigNames: []string{"retention.ms"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource type")
}

func TestDescribeConfigsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	configs, err := client.DescribeConfigs(ctx, []DescribeConfigResource{
		{Type: "topic", Name: "orders", ConfigNames: []string{"retention.ms", "retention.bytes"}},
	})
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	require.Contains(t, configs, "orders", "orders topic configs expected")
	require.Contains(t, configs["orders"], "retention.ms", "retention.ms expected for orders")
	require.Contains(t, configs["orders"], "retention.bytes", "retention.bytes expected for orders")

	ms, err := strconv.ParseInt(configs["orders"]["retention.ms"], 10, 64)
	require.NoError(t, err, "retention.ms must be numeric, got %q", configs["orders"]["retention.ms"])
	assert.Positive(t, ms, "retention.ms must be positive")
}
