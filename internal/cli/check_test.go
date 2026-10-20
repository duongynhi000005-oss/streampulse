// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/pulsedev/streampulse/internal/check"
	"github.com/pulsedev/streampulse/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckInvalidTimeout asserts a malformed --timeout is a usage error
// surfaced by the flag parser before any check runs.
func TestCheckInvalidTimeout(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"check", "--timeout", "bogus"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

// TestCheckResultsIntegration runs the real checks against the docker compose
// cluster (skipped when the broker is down): orders must pass with 6
// partitions; a dead broker must yield verdict 2.
func TestCheckResultsIntegration(t *testing.T) {
	if !brokerAvailable() {
		t.Skip("no local Kafka broker (docker compose) available")
	}

	cfg := config.DefaultConfig()
	cfg.Brokers = []string{"localhost:9093"}

	results, verdict, err := newCheckResults(context.Background(), cfg, check.Flags{
		Topics: []string{"orders"}, MinPartitions: 6,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, verdict)
	for _, r := range results {
		assert.Equal(t, check.StatusPass, r.Status, r.Name)
	}

	dead := config.DefaultConfig()
	dead.Brokers = []string{"127.0.0.1:1"}
	_, verdict, err = newCheckResults(context.Background(), dead, check.Flags{})
	require.NoError(t, err)
	assert.Equal(t, 2, verdict, "dead broker must yield a connectivity verdict of 2")
}

// TestCheckJSONRoundTrip verifies the --json payload shape through the
// command's rendering helper against a live broker.
func TestCheckJSONRoundTrip(t *testing.T) {
	if !brokerAvailable() {
		t.Skip("no local Kafka broker (docker compose) available")
	}

	cfg := config.DefaultConfig()
	cfg.Brokers = []string{"localhost:9093"}
	results, verdict, err := newCheckResults(context.Background(), cfg, check.Flags{
		Topics: []string{"orders"}, MinPartitions: 6,
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, printCheckJSON(&buf, results, verdict))

	var out struct {
		Results  []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"results"`
		Verdict  int `json:"verdict"`
		ExitCode int `json:"exit_code"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	require.Len(t, out.Results, 2)
	assert.Equal(t, "connectivity", out.Results[0].Name)
	assert.Equal(t, "topic orders", out.Results[1].Name)
	assert.Equal(t, 0, out.Verdict)
	assert.Equal(t, 0, out.ExitCode)
}
