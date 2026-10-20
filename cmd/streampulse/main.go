// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package main is the entry point for StreamPulse.
// StreamPulse is a k9s-style terminal UI for Apache Kafka observability.
//
// Usage:
//
//	streampulse                          # interactive TUI
//	streampulse serve --brokers kafka:9092  # daemon mode (metrics + alerts 24/7)
//	streampulse check --topic orders       # CI/CD health gate
//	streampulse dlq list                   # DLQ management
package main

import (
	"fmt"
	"os"

	"github.com/pulsedev/streampulse/internal/cli"
)

var (
	version = "v0.1.0-dev" // set via ldflags at build time
)

func main() {
	root := cli.NewRootCommand(version)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
