// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"localhost:9093"}, cfg.Brokers)
	assert.Equal(t, "5s", cfg.ScrapeInterval)
	assert.Equal(t, "sqlite", cfg.Storage.Type)
}

func TestLoadFileOverridesDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "streampulse.yaml")
	require.NoError(t, os.WriteFile(path, []byte("brokers: [kafka:9092]\nscrape_interval: 10s\n"), 0o644))
	cfg, err := loadFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, []string{"kafka:9092"}, cfg.Brokers)
	assert.Equal(t, "10s", cfg.ScrapeInterval)
}

func TestLoadEnvOverridesFile(t *testing.T) {
	t.Setenv("STREAMPULSE_BROKERS", "env:9092")
	cfg, err := Load(nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"env:9092"}, cfg.Brokers)
}

func TestLoadFlagOverridesEnv(t *testing.T) {
	t.Setenv("STREAMPULSE_BROKERS", "env:9092")
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.StringSlice("brokers", []string{"flag:9092"}, "")
	cfg, err := Load(flags)
	require.NoError(t, err)
	assert.Equal(t, []string{"flag:9092"}, cfg.Brokers)
}
