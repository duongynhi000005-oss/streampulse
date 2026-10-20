// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure Go SQLite driver
)

// SQLiteStore implements MetricsStore using an embedded SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates a new SQLite-backed store.
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	if dsn == "" {
		dsn = "~/.streampulse/state.db"
	}

	if strings.HasPrefix(dsn, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("home dir: %w", err)
		}
		dsn = filepath.Join(home, dsn[2:])
	}

	dir := filepath.Dir(dsn)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", dir, err)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Performance tuning
	db.SetMaxOpenConns(1) // SQLite: single writer
	db.SetMaxIdleConns(1)

	// WAL mode enables concurrent readers while the daemon writes; both
	// PRAGMAs are per-connection, so set them before any other use (the
	// pool is capped at one connection above). Memory-backed stores have
	// no journal, so skip them.
	if dsn != ":memory:" {
		if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			return nil, fmt.Errorf("enable WAL: %w", err)
		}
		if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
			return nil, fmt.Errorf("set busy timeout: %w", err)
		}
	}

	store := &SQLiteStore{db: db}
	if err := store.Migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return store, nil
}

func (s *SQLiteStore) WriteBatch(ctx context.Context, metrics []Metric) error {
	// Reject non-finite values up front: NaN/Inf would corrupt aggregate
	// computations (percentiles, avg, min/max) downstream.
	for _, m := range metrics {
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			return fmt.Errorf("non-finite value %v for %s/%s", m.Value, m.EntityType, m.EntityName)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO raw_metrics (ts, cluster_id, metric, entity_type, entity_name, tags, value)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range metrics {
		tagsJSON := "{}"
		if len(m.Tags) > 0 {
			b, err := json.Marshal(m.Tags)
			if err != nil {
				return fmt.Errorf("marshal tags for %s/%s: %w", m.EntityType, m.EntityName, err)
			}
			tagsJSON = string(b)
		}

		if _, err := stmt.ExecContext(ctx,
			m.TS.UnixMilli(), m.ClusterID, m.Metric,
			m.EntityType, m.EntityName, tagsJSON, m.Value,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) QueryRaw(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return s.query(ctx, "raw_metrics", "ts", params)
}

func (s *SQLiteStore) QueryHourly(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return s.query(ctx, "hourly_metrics", "bucket", params)
}

func (s *SQLiteStore) QueryDaily(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return s.query(ctx, "daily_metrics", "bucket", params)
}

// QueryStateTransitions returns the consecutive value changes for one entity
// within the window, in time order, via a LAG window function. A first sample
// (no predecessor) and unchanged consecutive samples are skipped. From is
// inclusive, To exclusive; a window with From >= To matches nothing.
func (s *SQLiteStore) QueryStateTransitions(ctx context.Context, params QueryParams) ([]StateTransition, error) {
	where := []string{"1=1"}
	args := []any{}
	if params.Metric != "" {
		where = append(where, "metric = ?")
		args = append(args, params.Metric)
	}
	if params.EntityType != "" {
		where = append(where, "entity_type = ?")
		args = append(args, params.EntityType)
	}
	if params.EntityName != "" {
		where = append(where, "entity_name = ?")
		args = append(args, params.EntityName)
	}
	if !params.From.IsZero() {
		where = append(where, "ts >= ?")
		args = append(args, params.From.UnixMilli())
	}
	if !params.To.IsZero() {
		where = append(where, "ts < ?")
		args = append(args, params.To.UnixMilli())
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		limit = 10000
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT entity_name, ts, value,
		       LAG(value) OVER (PARTITION BY entity_name ORDER BY ts) AS prev
		FROM raw_metrics
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY entity_name, ts
		LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("query state transitions: %w", err)
	}
	defer rows.Close()

	var out []StateTransition
	for rows.Next() {
		var entity string
		var ts, value int64
		var prevNull sql.NullInt64
		if err := rows.Scan(&entity, &ts, &value, &prevNull); err != nil {
			return nil, fmt.Errorf("scan transition: %w", err)
		}
		if !prevNull.Valid || prevNull.Int64 == value {
			continue
		}
		out = append(out, StateTransition{
			Time:   time.UnixMilli(ts).UTC(),
			Entity: entity,
			From:   float64(prevNull.Int64),
			To:     float64(value),
		})
	}
	return out, rows.Err()
}

// query runs a filtered, ordered, limited SELECT over one metrics table.
// Raw rows are grouped by (ts, identity, tags) into one MetricRow per point;
// hourly/daily rows map their aggregate columns directly. A window with
// From >= To naturally matches nothing and is not an error.
func (s *SQLiteStore) query(ctx context.Context, table, tsCol string, params QueryParams) ([]MetricRow, error) {
	var clauses []string
	var args []any
	add := func(clause string, arg any) {
		clauses = append(clauses, clause)
		args = append(args, arg)
	}
	if params.ClusterID != "" {
		add("cluster_id = ?", params.ClusterID)
	}
	if params.Metric != "" {
		add("metric = ?", params.Metric)
	}
	if params.EntityType != "" {
		add("entity_type = ?", params.EntityType)
	}
	if params.EntityName != "" {
		add("entity_name = ?", params.EntityName)
	}
	if !params.From.IsZero() {
		add(tsCol+" >= ?", params.From.UnixMilli())
	}
	if !params.To.IsZero() {
		add(tsCol+" < ?", params.To.UnixMilli())
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		limit = 10000
	}

	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}

	selectCols := tsCol + ", cluster_id, metric, entity_type, entity_name, tags"
	groupBy := ""
	if table == "raw_metrics" {
		selectCols += ", AVG(value), MIN(value), MAX(value), COUNT(*), SUM(value)"
		groupBy = " GROUP BY " + tsCol + ", cluster_id, metric, entity_type, entity_name, tags"
	} else {
		selectCols += ", avg, min, max, p50, p95, p99, count, sum"
	}

	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+selectCols+" FROM "+table+where+groupBy+" ORDER BY "+tsCol+" LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MetricRow
	for rows.Next() {
		var r MetricRow
		var ts int64
		var tags string
		if table == "raw_metrics" {
			if err := rows.Scan(&ts, &r.ClusterID, &r.Metric, &r.EntityType, &r.EntityName, &tags,
				&r.Avg, &r.Min, &r.Max, &r.Count, &r.Sum); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&ts, &r.ClusterID, &r.Metric, &r.EntityType, &r.EntityName, &tags,
				&r.Avg, &r.Min, &r.Max, &r.P50, &r.P95, &r.P99, &r.Count, &r.Sum); err != nil {
				return nil, err
			}
		}
		r.TimeStart = time.UnixMilli(ts).UTC()
		if len(tags) > 0 {
			if err := json.Unmarshal([]byte(tags), &r.Tags); err != nil {
				return nil, fmt.Errorf("parse tags for %s at %d: %w", r.Metric, ts, err)
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) Rollup(ctx context.Context, resolution string) error {
	t, err := rollupTargets(resolution)
	if err != nil {
		return err
	}

	// Watermark: start from the newest already-aggregated bucket so re-runs
	// re-compute the last (possibly still growing) bucket in full; a first
	// run has no watermark and processes everything.
	var watermark int64 = 1
	if err := s.db.QueryRowContext(ctx,
		`SELECT MAX(bucket) FROM `+t.dst,
	).Scan(&watermark); err == nil {
		watermark = t.trunc(watermark)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	valueStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		SELECT %[4]s FROM %[1]s
		WHERE (%[2]s/%[3]d)*%[3]d = ?
		  AND cluster_id = ? AND metric = ? AND entity_type = ? AND entity_name = ? AND tags = ?`,
		t.src, t.tsCol, t.div, t.valueCol))
	if err != nil {
		return err
	}
	defer valueStmt.Close()

	upsertStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %[1]s (bucket, cluster_id, metric, entity_type, entity_name, tags,
		                   avg, min, max, p50, p95, p99, count, sum)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket, cluster_id, metric, entity_type, entity_name, tags)
		DO UPDATE SET avg=excluded.avg, min=excluded.min, max=excluded.max,
		              p50=excluded.p50, p95=excluded.p95, p99=excluded.p99,
		              count=excluded.count, sum=excluded.sum`, t.dst))
	if err != nil {
		return err
	}
	defer upsertStmt.Close()

	const batchSize = 500
	for offset := 0; ; offset += batchSize {
		groups, err := s.rollupGroups(ctx, tx, t, watermark, batchSize, offset)
		if err != nil {
			return err
		}
		for _, g := range groups {
			values, count, sum, err := s.rollupValues(valueStmt, t, g)
			if err != nil {
				return err
			}
			aggs, err := computeAggregate(values, count, sum)
			if err != nil {
				return fmt.Errorf("rollup %s %s/%s/%s bucket %d: %w",
					resolution, g.metric, g.entityType, g.entityName, g.bucket, err)
			}
			if _, err := upsertStmt.ExecContext(ctx,
				g.bucket, g.clusterID, g.metric, g.entityType, g.entityName, g.tags,
				aggs.avg, aggs.min, aggs.max, aggs.p50, aggs.p95, aggs.p99, aggs.count, aggs.sum,
			); err != nil {
				return err
			}
		}
		if len(groups) < batchSize {
			break
		}
	}

	return tx.Commit()
}

const hourMs = 3600000
const dayMs = 86400000

// rollupSpec describes one rollup pass: source and destination tables, the
// source timestamp column, and the bucket truncation.
type rollupSpec struct {
	src, dst, tsCol, valueCol string
	div                       int64
	trunc                     func(int64) int64
}

// rollupTargets resolves a rollup resolution to its source and destination
// tables, timestamp column, and bucket truncation function.
func rollupTargets(resolution string) (rollupSpec, error) {
	var t rollupSpec
	switch resolution {
	case "hourly":
		t.src, t.dst, t.tsCol, t.valueCol, t.div = "raw_metrics", "hourly_metrics", "ts", "value", hourMs
	case "daily":
		// Daily aggregates the hourly avg values (count/sum columns are
		// summed separately so daily totals stay exact).
		t.src, t.dst, t.tsCol, t.valueCol, t.div = "hourly_metrics", "daily_metrics", "bucket", "avg, count, sum", dayMs
	default:
		return t, fmt.Errorf("invalid rollup resolution %q", resolution)
	}
	t.trunc = func(ms int64) int64 { return (ms / t.div) * t.div }
	return t, nil
}

type rollupGroup struct {
	bucket     int64
	clusterID  string
	metric     string
	entityType string
	entityName string
	tags       string
}

// rollupGroups fetches one batch of (bucket, identity, tags) groups from the
// source table, bounded by the watermark and batchSize.
func (s *SQLiteStore) rollupGroups(ctx context.Context, tx *sql.Tx, t rollupSpec, watermark int64, batchSize, offset int) ([]rollupGroup, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT (%[2]s/%[3]d)*%[3]d AS bucket, cluster_id, metric, entity_type, entity_name, tags
		FROM %[1]s
		WHERE %[2]s >= ?
		GROUP BY bucket, cluster_id, metric, entity_type, entity_name, tags
		ORDER BY bucket, cluster_id, metric, entity_type, entity_name, tags
		LIMIT ? OFFSET ?`, t.src, t.tsCol, t.div), watermark, batchSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []rollupGroup
	for rows.Next() {
		var g rollupGroup
		if err := rows.Scan(&g.bucket, &g.clusterID, &g.metric, &g.entityType, &g.entityName, &g.tags); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// rollupValues loads the source rows for one aggregation group. For the
// hourly pass the source row value is the raw metric; for the daily pass it
// is the hourly avg, with count and sum propagated for exact totals.
func (s *SQLiteStore) rollupValues(stmt *sql.Stmt, t rollupSpec, g rollupGroup) (values []float64, count int64, sum float64, err error) {
	rows, err := stmt.Query(g.bucket, g.clusterID, g.metric, g.entityType, g.entityName, g.tags)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()
	if t.dst == "daily_metrics" {
		for rows.Next() {
			var v, c, sm float64
			if err := rows.Scan(&v, &c, &sm); err != nil {
				return nil, 0, 0, err
			}
			values = append(values, v)
			count += int64(c)
			sum += sm
		}
	} else {
		for rows.Next() {
			var v float64
			if err := rows.Scan(&v); err != nil {
				return nil, 0, 0, err
			}
			values = append(values, v)
			sum += v
		}
		count = int64(len(values))
	}
	return values, count, sum, rows.Err()
}

type aggregate struct {
	avg, min, max, p50, p95, p99 float64
	count                        int64
	sum                          float64
}

func computeAggregate(values []float64, count int64, sum float64) (aggregate, error) {
	var out aggregate
	if len(values) == 0 || count == 0 {
		return out, fmt.Errorf("no values")
	}
	out.min, out.max = values[0], values[0]
	for _, v := range values {
		if v < out.min {
			out.min = v
		}
		if v > out.max {
			out.max = v
		}
	}
	out.count = count
	out.sum = sum
	out.avg = out.sum / float64(out.count)
	ps, err := percentiles(values, []float64{0.5, 0.95, 0.99})
	if err != nil {
		return out, err
	}
	out.p50, out.p95, out.p99 = ps[0.5], ps[0.95], ps[0.99]
	return out, nil
}

// Purge deletes data older than each per-resolution retention window. A zero
// duration disables retention for that resolution.
func (s *SQLiteStore) Purge(ctx context.Context, retention Retention) error {
	now := time.Now()
	if retention.Raw > 0 {
		cutoff := now.Add(-retention.Raw).UnixMilli()
		if _, err := s.db.ExecContext(ctx, `DELETE FROM raw_metrics WHERE ts < ?`, cutoff); err != nil {
			return fmt.Errorf("purge raw_metrics: %w", err)
		}
	}
	if retention.Hourly > 0 {
		cutoff := now.Add(-retention.Hourly).UnixMilli()
		if _, err := s.db.ExecContext(ctx, `DELETE FROM hourly_metrics WHERE bucket < ?`, cutoff); err != nil {
			return fmt.Errorf("purge hourly_metrics: %w", err)
		}
	}
	if retention.Daily > 0 {
		cutoff := now.Add(-retention.Daily).UnixMilli()
		if _, err := s.db.ExecContext(ctx, `DELETE FROM daily_metrics WHERE bucket < ?`, cutoff); err != nil {
			return fmt.Errorf("purge daily_metrics: %w", err)
		}
	}
	return nil
}

// QueryAlertState returns the persisted alert states for all rules, ordered
// by rule name. A rule that never fired has a zero LastFired.
func (s *SQLiteStore) QueryAlertState(ctx context.Context) ([]AlertStateRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT rule_name, status, last_fired, last_value, notify_count
		FROM alert_state
		ORDER BY rule_name
	`)
	if err != nil {
		return nil, fmt.Errorf("query alert_state: %w", err)
	}
	defer rows.Close()

	var out []AlertStateRow
	for rows.Next() {
		var r AlertStateRow
		var lastFired *int64
		if err := rows.Scan(&r.RuleName, &r.Status, &lastFired, &r.LastValue, &r.NotifyCount); err != nil {
			return nil, fmt.Errorf("scan alert_state: %w", err)
		}
		if lastFired != nil {
			r.LastFired = time.UnixMilli(*lastFired).UTC()
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveAlertState upserts one rule's alert state.
func (s *SQLiteStore) SaveAlertState(ctx context.Context, row AlertStateRow) error {
	var lastFired any
	if !row.LastFired.IsZero() {
		lastFired = row.LastFired.UnixMilli()
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO alert_state (rule_name, status, last_fired, last_value, notify_count)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(rule_name) DO UPDATE SET
			status = excluded.status,
			last_fired = excluded.last_fired,
			last_value = excluded.last_value,
			notify_count = excluded.notify_count
	`, row.RuleName, row.Status, lastFired, row.LastValue, row.NotifyCount); err != nil {
		return fmt.Errorf("save alert_state %s: %w", row.RuleName, err)
	}
	return nil
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	// v1: baseline schema (idempotent).
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS raw_metrics (
			ts          INTEGER NOT NULL,
			cluster_id  TEXT NOT NULL,
			metric      TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_name TEXT NOT NULL,
			tags        TEXT DEFAULT '{}',
			value       REAL NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_metrics(ts);
		CREATE INDEX IF NOT EXISTS idx_raw_entity ON raw_metrics(cluster_id, metric, entity_type, entity_name, ts);

		CREATE TABLE IF NOT EXISTS alert_state (
			rule_name    TEXT PRIMARY KEY,
			status       TEXT DEFAULT 'ok',
			last_fired   INTEGER,
			last_value   REAL,
			notify_count INTEGER DEFAULT 0
		);
	`); err != nil {
		return fmt.Errorf("migrate v1: %w", err)
	}

	// v2: aggregate tables for hourly/daily rollup, guarded by user_version.
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version < 2 {
		if _, err := s.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS hourly_metrics (
				bucket      INTEGER NOT NULL,   -- truncated-to-hour unix ms
				cluster_id  TEXT NOT NULL,
				metric      TEXT NOT NULL,
				entity_type TEXT NOT NULL,
				entity_name TEXT NOT NULL,
				tags        TEXT NOT NULL DEFAULT '{}',
				avg REAL, min REAL, max REAL,
				p50 REAL, p95 REAL, p99 REAL,
				count INTEGER, sum REAL,
				PRIMARY KEY (bucket, cluster_id, metric, entity_type, entity_name, tags)
			);
			CREATE TABLE IF NOT EXISTS daily_metrics (
				bucket      INTEGER NOT NULL,   -- truncated-to-day unix ms
				cluster_id  TEXT NOT NULL,
				metric      TEXT NOT NULL,
				entity_type TEXT NOT NULL,
				entity_name TEXT NOT NULL,
				tags        TEXT NOT NULL DEFAULT '{}',
				avg REAL, min REAL, max REAL,
				p50 REAL, p95 REAL, p99 REAL,
				count INTEGER, sum REAL,
				PRIMARY KEY (bucket, cluster_id, metric, entity_type, entity_name, tags)
			);
			CREATE INDEX IF NOT EXISTS idx_hourly_lookup ON hourly_metrics(cluster_id, metric, entity_type, entity_name, bucket);
			CREATE INDEX IF NOT EXISTS idx_daily_lookup  ON daily_metrics(cluster_id, metric, entity_type, entity_name, bucket);
			PRAGMA user_version = 2;
		`); err != nil {
			return fmt.Errorf("migrate v2: %w", err)
		}
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// Compile-time check that SQLiteStore implements MetricsStore.
var _ MetricsStore = (*SQLiteStore)(nil)
