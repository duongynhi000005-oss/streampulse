// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seededAlertStateDB writes two alert_state rows (one firing, one ok) into a
// temp SQLite file and returns its path.
func seededAlertStateDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := storage.NewSQLiteStore(path)
	require.NoError(t, err)
	fired := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "consumer-lag", Status: "firing", LastFired: fired, LastValue: 2400, NotifyCount: 3,
	}))
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "dlq-growth", Status: "ok", LastValue: 2.5, NotifyCount: 0,
	}))
	require.NoError(t, store.Close())
	return path
}

func TestAlertsPrintsTable(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", seededAlertStateDB(t))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"alerts"})
	require.NoError(t, root.Execute())

	out := buf.String()
	for _, want := range []string{"RULE", "STATUS", "consumer-lag", "firing", "2026-08-12T10:00:00Z", "2400.00", "3", "dlq-growth", "ok"} {
		assert.Contains(t, out, want)
	}
}

func TestAlertsJSON(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", seededAlertStateDB(t))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"alerts", "--json"})
	require.NoError(t, root.Execute())

	var rows []storage.AlertStateRow
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 2)
	assert.Equal(t, "consumer-lag", rows[0].RuleName)
	assert.Equal(t, "firing", rows[0].Status)
	assert.Equal(t, 3, rows[0].NotifyCount)
}

func TestAlertsNoState(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"alerts"})
	require.NoError(t, root.Execute())
	assert.Equal(t, "no alerts\n", buf.String())
}
