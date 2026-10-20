// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package cli provides the cobra command tree for StreamPulse.
package cli

import (
	"context"
	"fmt"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/spf13/cobra"
)

type cfgKey struct{}

// NewRootCommand creates the root streampulse command with all subcommands.
func NewRootCommand(version string) *cobra.Command {
	root := &cobra.Command{
		Use:   "streampulse",
		Short: "StreamPulse — the k9s for Apache Kafka",
		Long: `StreamPulse is a real-time Kafka observability tool with a k9s-style terminal UI.

Single binary. Zero dependencies. All views auto-refresh — no manual reload.

  streampulse                              # interactive TUI (reads from daemon's store)
  streampulse --brokers localhost:9092     # interactive TUI connected directly to Kafka
  streampulse serve --brokers kafka:9092   # daemon mode (24/7 monitoring)
  streampulse check --topic orders         # CI/CD health gate
  streampulse dlq list                     # DLQ management`,
		Version: version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cmd.Flags())
			if err != nil {
				return err
			}
			if err := validateAlertConditions(cfg); err != nil {
				return err
			}
			cmd.SetContext(context.WithValue(cmd.Context(), cfgKey{}, cfg))
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			return runTUI(cfg)
		},
	}

	root.PersistentFlags().String("config", "", "Path to YAML config file")
	// Registered without a default: DefaultConfig supplies localhost:9093 so
	// file and STREAMPULSE_BROKERS keep their precedence below the flag.
	root.PersistentFlags().StringSlice("brokers", nil, "Kafka broker addresses (comma-separated)")

	root.AddCommand(newServeCommand())
	root.AddCommand(newCheckCommand())
	root.AddCommand(newDLQCommand())
	root.AddCommand(newAnalyzeCommand())
	root.AddCommand(newAlertsCommand())

	return root
}

// CfgFromContext returns the config loaded by the root PersistentPreRunE.
func CfgFromContext(ctx context.Context) (*config.Config, error) {
	cfg, ok := ctx.Value(cfgKey{}).(*config.Config)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("config not loaded")
	}
	return cfg, nil
}
