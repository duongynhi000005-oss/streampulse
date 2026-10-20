// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package storage

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateVersionedSchema(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	var version int
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&version))
	assert.Equal(t, 2, version, "fresh DB must be migrated to schema version 2")

	for _, table := range []string{"raw_metrics", "alert_state", "hourly_metrics", "daily_metrics"} {
		var sqlText string
		require.NoError(t, s.db.QueryRow(
			`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
		).Scan(&sqlText), "table %s must exist", table)
		if table == "hourly_metrics" || table == "daily_metrics" {
			assert.Contains(t, sqlText, "PRIMARY KEY", "table %s must declare a PRIMARY KEY", table)
		}
	}

	for _, idx := range []string{"idx_raw_ts", "idx_raw_entity", "idx_hourly_lookup", "idx_daily_lookup"} {
		var name string
		require.NoError(t, s.db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, idx,
		).Scan(&name), "index %s must exist", idx)
	}

	// A second Migrate call is a no-op and must not reset the version.
	require.NoError(t, s.Migrate(context.Background()))
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&version))
	assert.Equal(t, 2, version)
}

func TestNewSQLiteStoreWalMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := NewSQLiteStore(path)
	require.NoError(t, err)
	defer s.Close()

	var mode string
	require.NoError(t, s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode))
	assert.Equal(t, "wal", mode, "file-backed store must run in WAL mode")
}

func seedMetric(t *testing.T, s *SQLiteStore, ts time.Time, value float64) {
	t.Helper()
	require.NoError(t, s.WriteBatch(context.Background(), []Metric{
		{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: value},
	}))
}

func scanAggregate(t *testing.T, s *SQLiteStore, table string, bucket int64) MetricRow {
	t.Helper()
	var r MetricRow
	var ts int64
	var tags string
	err := s.db.QueryRow(`SELECT bucket, cluster_id, metric, entity_type, entity_name, tags,
		avg, min, max, p50, p95, p99, count, sum FROM `+table+` WHERE bucket = ?`, bucket,
	).Scan(&ts, &r.ClusterID, &r.Metric, &r.EntityType, &r.EntityName,
		&tags, &r.Avg, &r.Min, &r.Max, &r.P50, &r.P95, &r.P99, &r.Count, &r.Sum)
	require.NoError(t, err)
	r.TimeStart = time.UnixMilli(ts).UTC()
	require.NoError(t, json.Unmarshal([]byte(tags), &r.Tags))
	if len(r.Tags) == 0 {
		r.Tags = nil
	}
	return r
}

func TestRollupHourly(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	// Hour A: values 10, 20, 30.
	for i, v := range []float64{10, 20, 30} {
		seedMetric(t, s, base.Add(time.Duration(i)*15*time.Minute), v)
	}
	// Hour B: values 40, 50, 60.
	for i, v := range []float64{40, 50, 60} {
		seedMetric(t, s, base.Add(time.Hour+time.Duration(i)*15*time.Minute), v)
	}

	require.NoError(t, s.Rollup(context.Background(), "hourly"))

	hourA := scanAggregate(t, s, "hourly_metrics", base.UnixMilli())
	assert.Equal(t, MetricRow{
		TimeStart: base, ClusterID: "c1", Metric: "msg_rate",
		EntityType: "topic", EntityName: "orders",
		Avg: 20, Min: 10, Max: 30, P50: 20, P95: 30, P99: 30, Count: 3, Sum: 60,
	}, hourA)

	hourB := scanAggregate(t, s, "hourly_metrics", base.Add(time.Hour).UnixMilli())
	assert.Equal(t, MetricRow{
		TimeStart: base.Add(time.Hour), ClusterID: "c1", Metric: "msg_rate",
		EntityType: "topic", EntityName: "orders",
		Avg: 50, Min: 40, Max: 60, P50: 50, P95: 60, P99: 60, Count: 3, Sum: 150,
	}, hourB)

	// Idempotency: a second run must produce identical rows.
	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM hourly_metrics`).Scan(&count))
	assert.Equal(t, 2, count)
	assert.Equal(t, hourA, scanAggregate(t, s, "hourly_metrics", base.UnixMilli()))
	assert.Equal(t, hourB, scanAggregate(t, s, "hourly_metrics", base.Add(time.Hour).UnixMilli()))
}

func TestRollupHourlyTagsGroupedSeparately(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	require.NoError(t, s.WriteBatch(context.Background(), []Metric{
		{TS: base, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "1"}, Value: 10},
		{TS: base.Add(30 * time.Minute), ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "1"}, Value: 20},
		{TS: base, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "2"}, Value: 100},
	}))

	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	assert.Equal(t, 2, len(aggregateRows(t, s, "hourly_metrics")))
}

func aggregateRows(t *testing.T, s *SQLiteStore, table string) []MetricRow {
	t.Helper()
	rows, err := s.db.Query(`SELECT bucket, cluster_id, metric, entity_type, entity_name, tags,
		avg, min, max, p50, p95, p99, count, sum FROM ` + table + ` ORDER BY bucket`)
	require.NoError(t, err)
	defer rows.Close()
	var out []MetricRow
	for rows.Next() {
		var r MetricRow
		var ts int64
		var tags string
		require.NoError(t, rows.Scan(&ts, &r.ClusterID, &r.Metric, &r.EntityType, &r.EntityName,
			&tags, &r.Avg, &r.Min, &r.Max, &r.P50, &r.P95, &r.P99, &r.Count, &r.Sum))
		r.TimeStart = time.UnixMilli(ts).UTC()
		require.NoError(t, json.Unmarshal([]byte(tags), &r.Tags))
	if len(r.Tags) == 0 {
		r.Tags = nil
	}
		out = append(out, r)
	}
	return out
}

func TestRollupDailyFromHourly(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	day1 := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	// Day 1: values 1, 2, 3 at 10:00/11:00/12:00.
	for i, v := range []float64{1, 2, 3} {
		seedMetric(t, s, day1.Add(time.Duration(i)*time.Hour), v)
	}
	// Day 2: values 10, 20, 30.
	for i, v := range []float64{10, 20, 30} {
		seedMetric(t, s, day2.Add(time.Duration(i)*time.Hour), v)
	}

	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	require.NoError(t, s.Rollup(context.Background(), "daily"))

	d1 := scanAggregate(t, s, "daily_metrics", day1.Truncate(24*time.Hour).UnixMilli())
	assert.Equal(t, MetricRow{
		TimeStart: day1.Truncate(24 * time.Hour), ClusterID: "c1", Metric: "msg_rate",
		EntityType: "topic", EntityName: "orders",
		Avg: 2, Min: 1, Max: 3, P50: 2, P95: 3, P99: 3, Count: 3, Sum: 6,
	}, d1)
	d2 := scanAggregate(t, s, "daily_metrics", day2.Truncate(24*time.Hour).UnixMilli())
	assert.Equal(t, MetricRow{
		TimeStart: day2.Truncate(24 * time.Hour), ClusterID: "c1", Metric: "msg_rate",
		EntityType: "topic", EntityName: "orders",
		Avg: 20, Min: 10, Max: 30, P50: 20, P95: 30, P99: 30, Count: 3, Sum: 60,
	}, d2)

	// Idempotent re-run.
	require.NoError(t, s.Rollup(context.Background(), "daily"))
	assert.Equal(t, 2, len(aggregateRows(t, s, "daily_metrics")))
}

func TestRollupRejectsUnknownResolution(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()
	err = s.Rollup(context.Background(), "weekly")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid rollup resolution "weekly"`)
}

func TestRollupEmptyStore(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	require.NoError(t, s.Rollup(context.Background(), "daily"))
	assert.Empty(t, aggregateRows(t, s, "hourly_metrics"))
	assert.Empty(t, aggregateRows(t, s, "daily_metrics"))
}

func TestQueryRawFilters(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	require.NoError(t, s.WriteBatch(context.Background(), []Metric{
		{TS: base, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 10},
		{TS: base, ClusterID: "c2", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 20},
		{TS: base, ClusterID: "c1", Metric: "lag", EntityType: "consumer_group", EntityName: "orders-processor", Value: 30},
	}))

	rows, err := s.QueryRaw(context.Background(), QueryParams{ClusterID: "c1"})
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	rows, err = s.QueryRaw(context.Background(), QueryParams{ClusterID: "c1", Metric: "msg_rate"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "topic", rows[0].EntityType)
	assert.Equal(t, "orders", rows[0].EntityName)

	rows, err = s.QueryRaw(context.Background(), QueryParams{Metric: "lag", EntityType: "consumer_group"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "orders-processor", rows[0].EntityName)

	rows, err = s.QueryRaw(context.Background(), QueryParams{EntityName: "orders"})
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	rows, err = s.QueryRaw(context.Background(), QueryParams{Metric: "nonexistent"})
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestQueryRawTimeWindow(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	for i, v := range []float64{1, 2, 3} {
		seedMetric(t, s, base.Add(time.Duration(i)*5*time.Second), v)
	}

	rows, err := s.QueryRaw(context.Background(), QueryParams{From: base, To: base.Add(10 * time.Second)})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, base, rows[0].TimeStart)
	assert.Equal(t, base.Add(5*time.Second), rows[1].TimeStart)

	rows, err = s.QueryRaw(context.Background(), QueryParams{From: base.Add(5 * time.Second)})
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestQueryRawLimit(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	metrics := make([]Metric, 10001)
	for i := range metrics {
		metrics[i] = Metric{
			TS: base.Add(time.Duration(i) * time.Millisecond), ClusterID: "c1",
			Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: float64(i),
		}
	}
	require.NoError(t, s.WriteBatch(context.Background(), metrics))

	rows, err := s.QueryRaw(context.Background(), QueryParams{})
	require.NoError(t, err)
	assert.Len(t, rows, 1000, "default limit is 1000")

	rows, err = s.QueryRaw(context.Background(), QueryParams{Limit: 5})
	require.NoError(t, err)
	assert.Len(t, rows, 5)

	rows, err = s.QueryRaw(context.Background(), QueryParams{Limit: 20000})
	require.NoError(t, err)
	assert.Len(t, rows, 10000, "limit is clamped to 10000")
}

func TestQueryRawRowShape(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	require.NoError(t, s.WriteBatch(context.Background(), []Metric{
		{TS: base, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "b"}, Value: 12.5},
	}))

	rows, err := s.QueryRaw(context.Background(), QueryParams{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, base, rows[0].TimeStart)
	assert.Equal(t, map[string]string{"a": "b"}, rows[0].Tags)
	assert.Equal(t, 12.5, rows[0].Avg)
	assert.Equal(t, 12.5, rows[0].Min)
	assert.Equal(t, 12.5, rows[0].Max)
	assert.Equal(t, 12.5, rows[0].Sum)
	assert.Equal(t, int64(1), rows[0].Count)
	assert.Equal(t, 0.0, rows[0].P50)
	assert.Equal(t, 0.0, rows[0].P95)
	assert.Equal(t, 0.0, rows[0].P99)
}

func TestQueryHourlyDaily(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	for i, v := range []float64{10, 20, 30} {
		seedMetric(t, s, base.Add(time.Duration(i)*15*time.Minute), v)
	}
	for i, v := range []float64{40, 50, 60} {
		seedMetric(t, s, base.Add(time.Hour+time.Duration(i)*15*time.Minute), v)
	}
	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	require.NoError(t, s.Rollup(context.Background(), "daily"))

	rows, err := s.QueryHourly(context.Background(), QueryParams{Metric: "msg_rate"})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, base, rows[0].TimeStart)
	assert.Equal(t, 20.0, rows[0].Avg)
	assert.Equal(t, 10.0, rows[0].Min)
	assert.Equal(t, 30.0, rows[0].Max)
	assert.Equal(t, 20.0, rows[0].P50)
	assert.Equal(t, 30.0, rows[0].P95)
	assert.Equal(t, 30.0, rows[0].P99)
	assert.Equal(t, int64(3), rows[0].Count)
	assert.Equal(t, 60.0, rows[0].Sum)

	rows, err = s.QueryDaily(context.Background(), QueryParams{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, base.Truncate(24*time.Hour), rows[0].TimeStart)
	assert.Equal(t, 35.0, rows[0].Avg)
	assert.Equal(t, 6, int(rows[0].Count))
	assert.Equal(t, 210.0, rows[0].Sum)
}

func TestQueryFromAfterTo(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	for i, v := range []float64{1, 2, 3} {
		seedMetric(t, s, base.Add(time.Duration(i)*time.Hour), v)
	}
	require.NoError(t, s.Rollup(context.Background(), "hourly"))

	params := QueryParams{From: base.Add(24 * time.Hour), To: base}
	rows, err := s.QueryRaw(context.Background(), params)
	require.NoError(t, err)
	assert.Empty(t, rows)
	rows, err = s.QueryHourly(context.Background(), params)
	require.NoError(t, err)
	assert.Empty(t, rows)
	rows, err = s.QueryDaily(context.Background(), params)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestPurgePerResolution(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	for _, ago := range []time.Duration{40 * 24 * time.Hour, 96 * time.Hour, 25 * time.Hour, 2 * time.Hour, 0} {
		seedMetric(t, s, now.Add(-ago), 1)
	}
	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	require.NoError(t, s.Rollup(context.Background(), "daily"))

	require.NoError(t, s.Purge(context.Background(), Retention{
		Raw:    24 * time.Hour,
		Hourly: 48 * time.Hour,
		Daily:  30 * 24 * time.Hour,
	}))

	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
	assert.Equal(t, 2, count, "raw rows younger than 24h survive")

	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM hourly_metrics`).Scan(&count))
	assert.Equal(t, 3, count, "hourly buckets younger than 48h survive")

	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM daily_metrics`).Scan(&count))
	assert.Equal(t, 3, count, "daily buckets younger than 30d survive")
}

func TestPurgeZeroRetentionIsNoop(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	seedMetric(t, s, time.Now().UTC().Add(-40*24*time.Hour), 1)
	require.NoError(t, s.Rollup(context.Background(), "hourly"))
	require.NoError(t, s.Rollup(context.Background(), "daily"))

	require.NoError(t, s.Purge(context.Background(), Retention{}))

	var count int
	for _, table := range []string{"raw_metrics", "hourly_metrics", "daily_metrics"} {
		require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&count))
		assert.Equal(t, 1, count, "no retention deletes nothing in %s", table)
	}
}

func TestWriteBatchRejectsNonFinite(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	ts := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		err := s.WriteBatch(context.Background(), []Metric{
			{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 1},
			{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: v},
		})
		require.Error(t, err, "value %v must be rejected", v)
		var count int
		require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
		assert.Equal(t, 0, count, "no rows may be written when a value is non-finite")
	}

	// A valid batch still works afterwards.
	require.NoError(t, s.WriteBatch(context.Background(), []Metric{
		{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 1},
	}))
	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
	assert.Equal(t, 1, count)
}

func TestQueryStateTransitions(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	base := time.Now().Truncate(time.Hour).Add(-48 * time.Hour)
	seq := []float64{1, 2, 3, 1, 1, 2, 1, 2, 2, 3, 1} // states: 3 rebalances into Preparing (idx1, idx5, idx7)
	metrics := make([]Metric, 0, len(seq))
	for i, v := range seq {
		metrics = append(metrics, Metric{
			TS:         base.Add(time.Duration(i) * 5 * time.Second),
			ClusterID:  "c1",
			Metric:     "kafka.group.state",
			EntityType: "consumer_group",
			EntityName: "orders-processor",
			Value:      v,
		})
	}
	require.NoError(t, s.WriteBatch(context.Background(), metrics))

	rows, err := s.QueryStateTransitions(context.Background(), QueryParams{
		Metric:     "kafka.group.state",
		EntityName: "orders-processor",
		From:       base,
		To:         base.Add(2 * time.Hour),
	})
	require.NoError(t, err)

	// One row per consecutive value change: idx1 1→2, idx2 2→3, idx3 3→1,
	// idx5 1→2, idx6 2→1, idx7 1→2, idx9 2→3, idx10 3→1.
	assert.Len(t, rows, 8)

	// Rebalance semantics: transitions into Preparing (To == 2 with From != 2)
	// at idx1, idx5, idx7 — deduping repeated 2→2 samples.
	preparing := 0
	for _, tr := range rows {
		if tr.To == 2 && tr.From != 2 {
			preparing++
		}
	}
	assert.Equal(t, 3, preparing)
}
