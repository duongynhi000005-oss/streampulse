// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package alerts implements the StreamPulse alert engine: a tiny condition
// language, a state machine over persisted alert_state rows, and the engine
// that evaluates built-in and user-configured rules against scraped metrics.
package alerts

import (
	"fmt"
	"strconv"
	"strings"
)

// Condition is a compiled alert condition: one metric compared to a
// threshold with a single operator. The metric names are from the scraper
// metric set; bare aliases ("lag", "up") expand to full names.
type Condition struct {
	Metric    string
	Op        string
	Threshold float64
}

// metricAliases maps short condition names to full scraper metric names.
var metricAliases = map[string]string{
	"lag":              "kafka.group.lag",
	"replica":          "kafka.broker.replica_partitions",
	"leader":           "kafka.broker.leader_partitions",
	"growth_rate":      "dlq.topic.growth_rate",
	"up":               "kafka.broker.up",
	"skew":             "kafka.cluster.partition_skew",
	"under_replicated": "kafka.cluster.under_replicated_partitions",
	// Daemon-internal counters evaluated by the engine, not part of the
	// scraper batch; registered so conditions can reference them.
	"scrape_errors_total": "scrape_errors_total",
}

// validOps is the supported operator whitelist.
var validOps = map[string]bool{
	">": true, ">=": true, "<": true, "<=": true, "==": true, "!=": true,
}

// ParseCondition parses the condition language: `metric [operator] value`,
// e.g. "lag > 1000" or "skew >= 1.5". The metric may be a bare alias or a
// full dotted metric name; operators are > >= < <= == !=; the value is a
// float (0/1 for boolean metrics).
func ParseCondition(s string) (*Condition, error) {
	fields := strings.Fields(s)
	if len(fields) != 3 {
		return nil, fmt.Errorf("condition %q: want \"metric op value\", got %d fields", s, len(fields))
	}
	name, op, raw := fields[0], fields[1], fields[2]

	metric, ok := metricAliases[name]
	if !ok {
		if !strings.Contains(name, ".") {
			return nil, fmt.Errorf("condition %q: unknown metric %q", s, name)
		}
		metric = name
	}
	if !validOps[op] {
		return nil, fmt.Errorf("condition %q: unsupported operator %q", s, op)
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("condition %q: threshold %q is not a number", s, raw)
	}

	return &Condition{Metric: metric, Op: op, Threshold: value}, nil
}

// Evaluate reports whether v satisfies the condition for the given metric
// name. A metric name mismatch is always false.
func (c *Condition) Evaluate(metric string, v float64) bool {
	if c == nil || metric != c.Metric {
		return false
	}
	switch c.Op {
	case ">":
		return v > c.Threshold
	case ">=":
		return v >= c.Threshold
	case "<":
		return v < c.Threshold
	case "<=":
		return v <= c.Threshold
	case "==":
		return v == c.Threshold
	case "!=":
		return v != c.Threshold
	}
	return false
}

// mustCondition parses s and panics on failure; for tests and built-in rules
// whose syntax is verified at compile time.
func mustCondition(s string) *Condition {
	c, err := ParseCondition(s)
	if err != nil {
		panic(fmt.Sprintf("bad condition %q: %v", s, err))
	}
	return c
}
