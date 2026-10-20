// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedGrowthStore writes 3 days of kafka.topic.messages raw points and rolls
// them up so QueryDaily returns daily aggregates for the given topics.
func seedGrowthStore(t *testing.T, path string, topics map[string]float64) {
	t.Helper()
	store, err := storage.NewSQLiteStore(path)
	require.NoError(t, err)
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Hour)
	var batch []storage.Metric
	day := 0
	for d := 2; d >= 0; d-- {
		for h := 0; h < 12; h++ {
			for name, rate := range topics {
				batch = append(batch, storage.Metric{
					TS:        now.Add(-time.Duration(d)*24*time.Hour + time.Duration(h)*2*time.Hour),
					ClusterID: "local-dev", Metric: scraper.MetricTopicMessages,
					EntityType: "topic", EntityName: name, Value: rate * float64(h+1),
				})
			}
		}
		day++
	}
	require.NoError(t, store.WriteBatch(context.Background(), batch))
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	require.NoError(t, store.Rollup(context.Background(), "daily"))
}

func TestAnalyzePrintsGrowthSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	seedGrowthStore(t, path, map[string]float64{"orders": 100, "payments": 50})
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "24h", "--topics", "orders"})
	require.NoError(t, root.Execute())

	out := buf.String()
	assert.Contains(t, out, "Growth")
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "▁") // sparkline glyphs
}

func TestAnalyzeJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	seedGrowthStore(t, path, map[string]float64{"orders": 100})
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "24h", "--json"})
	require.NoError(t, root.Execute())

	var out struct {
		Growth []struct {
			Topic     string          `json:"topic"`
			Window    json.RawMessage `json:"window"`
			Sparkline string          `json:"sparkline"`
		} `json:"growth"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	require.Len(t, out.Growth, 1)
	assert.Equal(t, "orders", out.Growth[0].Topic)
	assert.NotEmpty(t, out.Growth[0].Sparkline)
}

func TestAnalyzeNoData(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze"})
	require.NoError(t, root.Execute())
	assert.Equal(t, "no data\n", buf.String())
}

func TestAnalyzeInvalidWindow(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "3x"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "window")
}

// ─── L2 seeds ───────────────────────────────────────────────────────────────

// openSeedStore opens (and closes at cleanup) a file-backed SQLite store.
func openSeedStore(t *testing.T, path string) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.NewSQLiteStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

// seedAnomalyStore writes 4 weeks of hourly kafka.group.lag points for g1: a
// baseline alternating 95/105 around 100, then a 500 spike in the last two
// hours (z >> 4 against the seasonal hour-of-week baseline).
func seedAnomalyStore(t *testing.T, store *storage.SQLiteStore) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(time.Hour)
	metrics := make([]storage.Metric, 0, 4*7*24)
	for h := -(4*7*24 - 1); h <= -3; h++ {
		v := 105.0
		if h%2 == 0 {
			v = 95.0
		}
		metrics = append(metrics, storage.Metric{
			TS:        t0.Add(time.Duration(h) * time.Hour),
			ClusterID: "local-dev", Metric: scraper.MetricGroupLag,
			EntityType: "consumer_group", EntityName: "g1", Value: v,
		})
	}
	metrics = append(metrics,
		storage.Metric{TS: t0.Add(-2 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
		storage.Metric{TS: t0.Add(-1 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
	)
	require.NoError(t, store.WriteBatch(context.Background(), metrics))
}

// seedTransitionStore writes kafka.group.state samples for two groups with
// known rebalances: g1 two days ago (2 transitions into PreparingRebalance),
// g2 yesterday (1 transition).
func seedTransitionStore(t *testing.T, store *storage.SQLiteStore) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	ctx := context.Background()
	metrics := []storage.Metric{
		{TS: t0.Add(-48 * time.Hour), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 4},
		{TS: t0.Add(-48*time.Hour + 5*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		{TS: t0.Add(-48*time.Hour + 10*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 2},
		{TS: t0.Add(-48*time.Hour + 15*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 3},
		{TS: t0.Add(-48*time.Hour + 20*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		{TS: t0.Add(-48*time.Hour + 25*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 2},
		{TS: t0.Add(-48*time.Hour + 30*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 3},
		{TS: t0.Add(-48*time.Hour + 35*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		{TS: t0.Add(-24 * time.Hour), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 1},
		{TS: t0.Add(-24*time.Hour + 5*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 2},
		{TS: t0.Add(-24*time.Hour + 10*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 3},
		{TS: t0.Add(-24*time.Hour + 15*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 1},
	}
	require.NoError(t, store.WriteBatch(ctx, metrics))
}

// seedPatternStore writes 3 days of hourly kafka.topic.msg_rate points for
// "orders": 100 at 09:00 UTC, 10 at every other hour.
func seedPatternStore(t *testing.T, store *storage.SQLiteStore) {
	t.Helper()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	metrics := make([]storage.Metric, 0, 72)
	for i := 0; i < 72; i++ {
		v := 10.0
		if (i % 24) == 9 {
			v = 100.0
		}
		metrics = append(metrics, storage.Metric{
			TS:        t0.Add(time.Duration(i-72) * time.Hour),
			ClusterID: "local-dev", Metric: scraper.MetricTopicMsgRate,
			EntityType: "topic", EntityName: "orders", Value: v,
		})
	}
	require.NoError(t, store.WriteBatch(context.Background(), metrics))
}

// seedL2Store writes all three L2 datasets into a single store, then rolls up
// the hourly aggregates once (a later rollup would skip buckets below the
// existing aggregation watermark).
func seedL2Store(t *testing.T, path string) {
	t.Helper()
	store := openSeedStore(t, path)
	seedAnomalyStore(t, store)
	seedTransitionStore(t, store)
	seedPatternStore(t, store)
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
}

// runAnalyze executes the analyze command with the given args and returns its
// output.
func runAnalyze(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs(append([]string{"analyze"}, args...))
	err := root.Execute()
	return buf.String(), err
}

// ─── L2 sections ────────────────────────────────────────────────────────────

func TestAnalyzeAnomaliesSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSeedStore(t, path)
	seedAnomalyStore(t, store)
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	out, err := runAnalyze(t, "--window", "168h", "--anomalies", "lag")
	require.NoError(t, err)
	assert.Contains(t, out, "ANOMALIES")
	assert.Contains(t, out, "kafka.group.lag")
	assert.Contains(t, out, "g1")
	assert.Contains(t, out, "UTC")
	assert.Contains(t, out, "500.00")
	assert.Contains(t, out, "high")
	assert.Contains(t, out, "warning")
}

func TestAnalyzeRebalancesSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSeedStore(t, path)
	seedTransitionStore(t, store)
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	out, err := runAnalyze(t, "--window", "168h", "--rebalances", "")
	require.NoError(t, err)
	assert.Contains(t, out, "REBALANCES")
	assert.Contains(t, out, "g1")
	assert.Contains(t, out, "g2")

	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	assert.Contains(t, out, t0.Add(-48*time.Hour).Format("2006-01-02"))
	assert.Contains(t, out, t0.Add(-24*time.Hour).Format("2006-01-02"))
}

func TestAnalyzePatternsSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSeedStore(t, path)
	seedPatternStore(t, store)
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	out, err := runAnalyze(t, "--window", "168h", "--patterns", "msg_rate", "--topics", "orders")
	require.NoError(t, err)
	assert.Contains(t, out, "PATTERNS")
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "09:00") // peak hour
	assert.Contains(t, out, "█")     // Bars hourly profile glyphs
	assert.Contains(t, out, "forecast")
}

func TestAnalyzeL2JSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	seedL2Store(t, path)
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	out, err := runAnalyze(t, "--window", "168h", "--anomalies", "lag", "--rebalances", "", "--patterns", "msg_rate", "--json")
	require.NoError(t, err)

	var sections map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(out), &sections))
	for _, key := range []string{"growth", "patterns", "anomalies", "rebalances"} {
		assert.Contains(t, sections, key, "json section %q present", key)
	}

	var anomalies []analytics.Anomaly
	require.NoError(t, json.Unmarshal(sections["anomalies"], &anomalies))
	require.Len(t, anomalies, 2) // the two spike hours
	for _, an := range anomalies {
		assert.Equal(t, "g1", an.Entity)
		assert.Equal(t, "high", an.Direction)
		assert.Equal(t, "warning", an.Severity)
	}

	var rebalances []analytics.RebalanceReport
	require.NoError(t, json.Unmarshal(sections["rebalances"], &rebalances))
	require.Len(t, rebalances, 2)
	assert.Equal(t, "g1", rebalances[0].Group)
	assert.Equal(t, 2, rebalances[0].Count)
	assert.Equal(t, "g2", rebalances[1].Group)
	assert.Equal(t, 1, rebalances[1].Count)

	var patterns []analytics.ThroughputReport
	require.NoError(t, json.Unmarshal(sections["patterns"], &patterns))
	require.Len(t, patterns, 1)
	assert.Equal(t, "orders", patterns[0].Topic)
	assert.Equal(t, 9, patterns[0].PeakHour)
}

func TestAnalyzeL2NoData(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	out, err := runAnalyze(t, "--window", "168h", "--anomalies", "", "--rebalances", "", "--patterns", "msg_rate")
	require.NoError(t, err)
	assert.Equal(t, "no data\n", out)
}

func TestAnalyzeInvalidAnomalyMetric(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	_, err := runAnalyze(t, "--anomalies", "foo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anomalies")
}

func TestAnalyzeInvalidPatternMetric(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	_, err := runAnalyze(t, "--patterns", "foo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patterns")
}
