package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsedev/streampulse/internal/dlq"
)

// brokerAvailable reports whether a local test broker (docker compose,
// localhost:9093) answers a TCP dial.
func brokerAvailable() bool {
	conn, err := net.DialTimeout("tcp", "localhost:9093", time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func TestDLQInspectRequiresTopic(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "inspect"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--topic")
}

func TestDLQReplayRequiresTopic(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "replay", "--dry-run"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--topic")
}

func TestDLQReplayDryRunLineCountsCandidates(t *testing.T) {
	res := &dlq.ReplayResult{DryRun: true, Total: 12}
	assert.Equal(t,
		"would replay 12 of 12 messages (0 filtered, 0 skipped)\n",
		replayDryRunLine(res),
		"dry run reports candidates, not produced messages")

	res = &dlq.ReplayResult{DryRun: true, Total: 10, Filtered: 3, Skipped: 2}
	assert.Equal(t,
		"would replay 5 of 10 messages (3 filtered, 2 skipped)\n",
		replayDryRunLine(res),
		"filtered and skipped messages are excluded from candidates")
}

func TestDLQReplayDryRunCommandReportsCandidates(t *testing.T) {
	orig := dlqReplay
	dlqReplay = func(context.Context, dlq.ReplayOptions) (*dlq.ReplayResult, error) {
		return &dlq.ReplayResult{DryRun: true, Total: 12, Filtered: 1, Skipped: 1}, nil
	}
	t.Cleanup(func() { dlqReplay = orig })

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "replay", "--topic", "x.dlq", "--dry-run"})
	require.NoError(t, root.Execute())
	assert.Contains(t, buf.String(), "would replay 10 of 12 messages",
		"dry-run command prints the candidate count, not produced messages")
}

func TestDLQReplayRejectsBadFilter(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "replay", "--topic", "x.dlq", "--filter", "novalue"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key=value")
}

// TestDLQListJSONIntegration lists DLQ topics against the docker compose
// cluster (skipped when the broker is down) and asserts the convention-based
// discovery finds payments.dlq with its original topic resolved.
func TestDLQListJSONIntegration(t *testing.T) {
	if !brokerAvailable() {
		t.Skip("no local Kafka broker (docker compose) available")
	}

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "list", "--json"})
	require.NoError(t, root.Execute())

	var rows []struct {
		Name           string `json:"name"`
		OriginalTopic  string `json:"original_topic"`
		OriginalExists bool   `json:"original_exists"`
		MessageCount   int64  `json:"message_count"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.NotEmpty(t, rows, "expected at least one DLQ topic")

	found := false
	for _, r := range rows {
		if r.Name == "payments.dlq" {
			found = true
			assert.Equal(t, "payments", r.OriginalTopic)
			assert.True(t, r.OriginalExists)
			break
		}
	}
	assert.True(t, found, fmt.Sprintf("payments.dlq missing from %+v", rows))
}
