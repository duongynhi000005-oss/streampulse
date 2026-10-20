// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootLoadsConfig(t *testing.T) {
	root := NewRootCommand("test")
	root.SetArgs([]string{"--config", "/nonexistent", "serve"})
	err := root.Execute()
	require.Error(t, err) // nonexistent file → error before serve runs
}

func TestValidateAlertConditions(t *testing.T) {
	valid := &config.Config{Alerts: []config.AlertRule{
		{Name: "consumer-lag", Condition: "lag > 1000"},
		{Name: "under-replicated", Condition: "under_replicated > 0"},
		{Name: "keep-builtin", Condition: ""}, // empty keeps the builtin condition
	}}
	require.NoError(t, validateAlertConditions(valid))

	bad := &config.Config{Alerts: []config.AlertRule{
		{Name: "consumer-lag", Condition: "lag >"},
	}}
	err := validateAlertConditions(bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer-lag")
	assert.Contains(t, err.Error(), "condition")
}

func TestRootRejectsBadAlertCondition(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "streampulse.yml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
alerts:
  - name: consumer-lag
    condition: "lag >"
`), 0o644))

	root := NewRootCommand("test")
	root.SetArgs([]string{"--config", cfgPath, "check"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer-lag")
	assert.Contains(t, err.Error(), "condition")
}

func TestRootAcceptsValidAlertConditions(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "streampulse.yml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
brokers: ["localhost:1"]
storage:
  type: sqlite
  sqlite:
    path: %s
alerts:
  - name: consumer-lag
    condition: "lag > 1000"
`, filepath.Join(dir, "state.db"))), 0o644))

	root := NewRootCommand("test")
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{"--config", cfgPath, "alerts"})
	require.NoError(t, root.Execute())
	assert.Contains(t, buf.String(), "no alerts")
}
