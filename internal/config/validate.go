// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/pulsedev/streampulse/internal/storage"
)

// Validate checks structural config rules and returns cfg when valid, or a
// single joined error listing all violations.
func Validate(cfg *Config) (*Config, error) {
	var errs []error

	if len(cfg.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("brokers: at least one broker address is required"))
	}
	if _, err := cfg.ParseScrapeInterval(); err != nil {
		errs = append(errs, err)
	}

	if cfg.Storage.Type != "" && !slices.Contains(storage.ValidTypes(), cfg.Storage.Type) {
		// Text matches storage.NewStore's error format.
		errs = append(errs, fmt.Errorf("unknown storage type %q", cfg.Storage.Type))
	}

	switch m := cfg.Kafka.SASL.Mechanism; m {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		errs = append(errs, fmt.Errorf("kafka.sasl.mechanism: unsupported mechanism %q", m))
	}
	if strings.HasPrefix(cfg.Kafka.SASL.Mechanism, "scram") {
		if cfg.Kafka.SASL.Username == "" {
			errs = append(errs, fmt.Errorf("kafka.sasl.username: required for %s", cfg.Kafka.SASL.Mechanism))
		}
		if cfg.Kafka.SASL.PasswordEnv == "" || os.Getenv(cfg.Kafka.SASL.PasswordEnv) == "" {
			errs = append(errs, fmt.Errorf("kafka.sasl.password_env: env var %q not set", cfg.Kafka.SASL.PasswordEnv))
		}
	}

	for i, r := range cfg.Alerts {
		if r.Name == "" {
			errs = append(errs, fmt.Errorf("alerts[%d].name: required", i))
		}
		// TODO(wave2): validate condition syntax via alerts.ParseCondition
		// (Phase 5 wires the parser in).
	}

	return cfg, errors.Join(errs...)
}

// Redacted returns a deep copy of the config with secret-bearing values
// (password env var names, webhook URLs) replaced by "<set>". Safe to log.
func (c *Config) Redacted() map[string]any {
	b, err := json.Marshal(c)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{"error": err.Error()}
	}
	redact(m)
	return m
}

// redact replaces secret values in a decoded config map, descending into
// nested maps and slices of maps.
func redact(m map[string]any) {
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			redact(val)
		case []any:
			for _, item := range val {
				if im, ok := item.(map[string]any); ok {
					redact(im)
				}
			}
		case string:
			switch k {
			case "PasswordEnv", "Webhook":
				if val != "" {
					m[k] = "<set>"
				}
			}
		}
	}
}
