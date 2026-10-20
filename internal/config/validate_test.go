// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package config

import (
	"fmt"
	"testing"

	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"no brokers", func(c *Config) { c.Brokers = nil }, "brokers"},
		{"bad interval", func(c *Config) { c.ScrapeInterval = "3" }, "scrape_interval"},
		{"unknown storage", func(c *Config) { c.Storage.Type = "postgress" }, "storage type"},
		{"unknown sasl", func(c *Config) { c.Kafka.SASL.Mechanism = "kerberos" }, "sasl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(cfg)
			_, err := Validate(cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestRedacted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Kafka.SASL.PasswordEnv = "PW"
	r := cfg.Redacted()
	assert.NotContains(t, fmt.Sprint(r), "PW")
}

func TestValidateAcceptsValidStorageTypes(t *testing.T) {
	for _, typ := range storage.ValidTypes() {
		cfg := DefaultConfig()
		cfg.Storage.Type = typ
		_, err := Validate(cfg)
		assert.NoError(t, err, "storage type %q must validate", typ)
	}
}
