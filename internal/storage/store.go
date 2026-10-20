// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package storage defines the MetricsStore interface and SQLite implementation.
package storage

import (
	"context"
	"fmt"
	"time"
)

// ─── Domain Types ──────────────────────────────────────────────────────────

// Metric represents a single scraped metric data point.
type Metric struct {
	TS         time.Time         `json:"ts"`
	ClusterID  string            `json:"cluster_id"`
	Metric     string            `json:"metric"`
	EntityType string            `json:"entity_type"` // topic, consumer_group, broker
	EntityName string            `json:"entity_name"`
	Tags       map[string]string `json:"tags,omitempty"`
	Value      float64           `json:"value"`
}

// MetricRow is a query result row (aggregated or raw).
type MetricRow struct {
	TimeStart  time.Time         `json:"time_start"`
	ClusterID  string            `json:"cluster_id"`
	Metric     string            `json:"metric"`
	EntityType string            `json:"entity_type"`
	EntityName string            `json:"entity_name"`
	Tags       map[string]string `json:"tags,omitempty"`
	Avg        float64           `json:"avg"`
	Min        float64           `json:"min"`
	Max        float64           `json:"max"`
	P50        float64           `json:"p50"`
	P95        float64           `json:"p95"`
	P99        float64           `json:"p99"`
	Count      int64             `json:"count"`
	Sum        float64           `json:"sum"`
}

// StateTransition is a consecutive value change for one entity.
type StateTransition struct {
	Time   time.Time
	Entity string
	From   float64 // previous sampled value
	To     float64 // new value
}

// QueryParams filters metric queries.
type QueryParams struct {
	ClusterID  string
	Metric     string
	EntityType string
	EntityName string
	From       time.Time
	To         time.Time
	Limit      int
}

// Retention defines data retention policies.
type Retention struct {
	Raw    time.Duration
	Hourly time.Duration
	Daily  time.Duration
}

// AlertStateRow is one persisted alert rule state.
type AlertStateRow struct {
	RuleName    string    `json:"rule_name"`
	Status      string    `json:"status"` // "ok" | "pending" | "firing"
	LastFired   time.Time `json:"last_fired,omitempty"`
	LastValue   float64   `json:"last_value"`
	NotifyCount int       `json:"notify_count"`
}

// ─── Interface ─────────────────────────────────────────────────────────────

// MetricsStore is the pluggable storage backend for metrics and analytics.
// Implementations: SQLite (v0.1), PostgreSQL (v0.2), ClickHouse (v0.3).
type MetricsStore interface {
	// WriteBatch writes raw scraped metrics.
	WriteBatch(ctx context.Context, metrics []Metric) error

	// Query methods for analytics layers.
	QueryRaw(ctx context.Context, params QueryParams) ([]MetricRow, error)
	QueryHourly(ctx context.Context, params QueryParams) ([]MetricRow, error)
	QueryDaily(ctx context.Context, params QueryParams) ([]MetricRow, error)

	// QueryStateTransitions returns the state-value transitions (consecutive
	// samples where the value changed) for a metric, in time order.
	QueryStateTransitions(ctx context.Context, params QueryParams) ([]StateTransition, error)

	// Rollup aggregates raw → hourly and hourly → daily.
	Rollup(ctx context.Context, resolution string) error

	// QueryAlertState returns the persisted alert states for all rules,
	// ordered by rule name.
	QueryAlertState(ctx context.Context) ([]AlertStateRow, error)

	// SaveAlertState upserts one rule's alert state.
	SaveAlertState(ctx context.Context, row AlertStateRow) error

	// Purge removes expired data according to retention policy.
	Purge(ctx context.Context, retention Retention) error

	// Ping checks connectivity.
	Ping(ctx context.Context) error

	// Migrate runs schema migrations.
	Migrate(ctx context.Context) error

	// Close cleans up resources.
	Close() error
}

// ValidTypes returns the storage backend types NewStore supports, in the
// canonical order used by validation and error messages.
func ValidTypes() []string {
	return []string{"sqlite", "postgres", "clickhouse"}
}

// NewStore creates a MetricsStore based on configuration type.
func NewStore(storeType, dsn string) (MetricsStore, error) {
	switch storeType {
	case "sqlite", "":
		return NewSQLiteStore(dsn)
	case "postgres":
		return nil, fmt.Errorf("storage type %q not implemented (planned for v0.2)", storeType)
	case "clickhouse":
		return nil, fmt.Errorf("storage type %q not implemented (planned for v0.3)", storeType)
	default:
		return nil, fmt.Errorf("unknown storage type %q", storeType)
	}
}
