// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Fakes ─────────────────────────────────────────────────────────────────

// fakeClient satisfies the analytics Client interface. clusterErr/configsErr
// simulate per-operation failures.
type fakeClient struct {
	cluster    *kafka.ClusterInfo
	configs    map[string]map[string]string
	clusterErr error
	configsErr error
}

func (f *fakeClient) DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error) {
	return f.cluster, f.clusterErr
}

func (f *fakeClient) DescribeConfigs(ctx context.Context, resources []kafka.DescribeConfigResource) (map[string]map[string]string, error) {
	return f.configs, f.configsErr
}

// fakeStore implements storage.MetricsStore with canned query results and a
// record of the query parameters used.
type fakeStore struct {
	daily   map[string][]storage.MetricRow // keyed metric + ":" + entity
	raw     map[string][]storage.MetricRow
	err     error
	queries []storage.QueryParams
}

func key(metric, entity string) string { return metric + ":" + entity }

func (f *fakeStore) QueryDaily(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	f.queries = append(f.queries, params)
	if f.err != nil {
		return nil, f.err
	}
	return f.daily[key(params.Metric, params.EntityName)], nil
}

func (f *fakeStore) QueryStateTransitions(ctx context.Context, params storage.QueryParams) ([]storage.StateTransition, error) {
	return nil, nil
}

func (f *fakeStore) QueryRaw(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	f.queries = append(f.queries, params)
	if f.err != nil {
		return nil, f.err
	}
	return f.raw[key(params.Metric, params.EntityName)], nil
}

func (f *fakeStore) WriteBatch(ctx context.Context, metrics []storage.Metric) error { return nil }
func (f *fakeStore) QueryHourly(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}
func (f *fakeStore) Rollup(ctx context.Context, resolution string) error { return nil }
func (f *fakeStore) Purge(ctx context.Context, retention storage.Retention) error {
	return nil
}
func (f *fakeStore) Ping(ctx context.Context) error    { return nil }
func (f *fakeStore) Migrate(ctx context.Context) error { return nil }
func (f *fakeStore) Close() error                      { return nil }
func (f *fakeStore) QueryAlertState(ctx context.Context) ([]storage.AlertStateRow, error) {
	return nil, nil
}
func (f *fakeStore) SaveAlertState(ctx context.Context, row storage.AlertStateRow) error {
	return nil
}

// seededStore opens a fresh in-memory SQLite store.
func seededStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

// seedGrowth writes kafka.topic.messages raw points for orders (3 days) and
// payments (2 days) relative to midnight UTC today, then rolls up to daily.
func seedGrowth(t *testing.T, store *storage.SQLiteStore) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	metrics := []storage.Metric{
		// orders: 86400 cumulative messages per day, one point per 12h.
		{TS: t0.Add(-48 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 0},
		{TS: t0.Add(-36 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 86400},
		{TS: t0.Add(-24 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 86400},
		{TS: t0.Add(-12 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 172800},
		{TS: t0, ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 172800},
		{TS: t0.Add(6 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 259200},
		// payments: smaller volume, only the last two days.
		{TS: t0.Add(-24 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 1000},
		{TS: t0.Add(-12 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 2000},
		{TS: t0, ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 2000},
		{TS: t0.Add(6 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 3000},
	}
	require.NoError(t, store.WriteBatch(ctx, metrics))
	require.NoError(t, store.Rollup(ctx, "hourly"))
	require.NoError(t, store.Rollup(ctx, "daily"))
}

// ─── Growth ─────────────────────────────────────────────────────────────────

func TestAnalyzerGrowth(t *testing.T) {
	store := seededStore(t)
	seedGrowth(t, store)
	a := &Analyzer{store: store, client: &fakeClient{}}

	reports, err := a.Growth(context.Background(), nil, 72*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 2, "orders and payments expected")

	require.Equal(t, "orders", reports[0].Topic)
	assert.Equal(t, 72*time.Hour, reports[0].Window)
	require.Len(t, reports[0].Points, 3, "72h window covers three daily buckets")
	// orders daily avgs: 43200, 129600, 216000.
	assert.Equal(t, 43200.0, reports[0].Points[0].Rate)
	assert.Equal(t, 129600.0, reports[0].Points[1].Rate)
	assert.Equal(t, 216000.0, reports[0].Points[2].Rate)
	assert.Equal(t, t0Point(t, -48*time.Hour), reports[0].Points[0].Time)
	// 172800 messages over 48h.
	assert.InDelta(t, 1.0, reports[0].Delta, 0.001)
	assert.Equal(t, "▁▅█", reports[0].Sparkline)

	require.Equal(t, "payments", reports[1].Topic)
	require.Len(t, reports[1].Points, 2, "payments has two days of data")
	assert.InDelta(t, 1000.0/86400.0, reports[1].Delta, 0.001)
}

func TestAnalyzerGrowthTopicFilter(t *testing.T) {
	store := seededStore(t)
	seedGrowth(t, store)
	a := &Analyzer{store: store, client: &fakeClient{}}

	reports, err := a.Growth(context.Background(), []string{"orders"}, 72*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 1)
	assert.Equal(t, "orders", reports[0].Topic)
}

func TestAnalyzerGrowthNoData(t *testing.T) {
	store := seededStore(t)
	a := &Analyzer{store: store, client: &fakeClient{}}

	reports, err := a.Growth(context.Background(), nil, 72*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, reports)
}

func TestAnalyzerGrowthInvalidWindow(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: &fakeClient{}}
	_, err := a.Growth(context.Background(), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "window")
}

func TestAnalyzerGrowthPartialWindow(t *testing.T) {
	store := seededStore(t)
	seedGrowth(t, store)
	a := &Analyzer{store: store, client: &fakeClient{}}

	// A 24h window reaches only the final daily bucket (or, at a day
	// boundary, the last two); delta is consistent with the point count.
	reports, err := a.Growth(context.Background(), nil, 24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 2)
	orders := reports[0]
	require.NotEmpty(t, orders.Points)
	if len(orders.Points) == 1 {
		assert.Equal(t, 0.0, orders.Delta)
	} else {
		assert.InDelta(t, 1.0, orders.Delta, 0.001)
	}
}

// t0Point computes the expected daily bucket time for an offset from midnight
// UTC today.
func t0Point(t *testing.T, offset time.Duration) time.Time {
	t.Helper()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	return t0.Add(offset).UTC()
}

// ─── Skew ───────────────────────────────────────────────────────────────────

func TestAnalyzerSkew(t *testing.T) {
	cases := []struct {
		name     string
		leaders  map[string]int
		ratio    float64
		balanced bool
	}{
		{"balanced at threshold", map[string]int{"1": 10, "2": 5, "3": 5}, 1.5, true},
		{"unbalanced", map[string]int{"1": 10, "2": 2, "3": 2}, 10.0 / (14.0 / 3.0), false},
		{"single broker", map[string]int{"1": 7}, 1.0, true},
		{"zero leaders", map[string]int{"1": 0, "2": 0}, 0.0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brokers := make([]kafka.BrokerInfo, 0, len(tc.leaders))
			for id, n := range tc.leaders {
				brokers = append(brokers, kafka.BrokerInfo{ID: atoi(id), LeaderPartitions: n})
			}
			client := &fakeClient{cluster: &kafka.ClusterInfo{BrokerCount: len(brokers), Brokers: brokers}}
			a := &Analyzer{store: seededStore(t), client: client}

			reports, err := a.Skew(context.Background())
			require.NoError(t, err)
			require.Len(t, reports, 1)
			assert.InDelta(t, tc.ratio, reports[0].Ratio, 0.0001)
			assert.Equal(t, tc.balanced, reports[0].Balanced)
			assert.Equal(t, tc.leaders, reports[0].Leaders)
		})
	}
}

func TestAnalyzerSkewNoBrokers(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: &fakeClient{cluster: &kafka.ClusterInfo{}}}
	reports, err := a.Skew(context.Background())
	require.NoError(t, err)
	assert.Empty(t, reports)
}

func TestAnalyzerSkewClientError(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: &fakeClient{clusterErr: errors.New("cluster down")}}
	_, err := a.Skew(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "describe cluster")
}

// ─── Retention ──────────────────────────────────────────────────────────────

func TestAnalyzerRetention(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		daily: map[string][]storage.MetricRow{
			key(scraper.MetricTopicBytesRate, "orders"): {
				{TimeStart: now.Add(-24 * time.Hour), Avg: 2_000_000}, // bytes/sec
			},
		},
		raw: map[string][]storage.MetricRow{
			key(scraper.MetricTopicMessages, "orders"): {
				{TimeStart: now.Add(-9 * 24 * time.Hour)}, // oldest persisted point
			},
		},
	}
	client := &fakeClient{configs: map[string]map[string]string{
		"orders":   {"retention.ms": "604800000", "retention.bytes": "172800000000"}, // 7 days / ~1 day of data
		"payments": {"retention.ms": "-1", "retention.bytes": "-1"},                  // unset
	}}
	a := &Analyzer{store: store, client: client}

	reports, err := a.Retention(context.Background(), []string{"orders", "payments", "ghost"})
	require.NoError(t, err)
	require.Len(t, reports, 3)

	orders := reports[0]
	assert.Equal(t, "orders", orders.Topic)
	assert.Equal(t, 7*24*time.Hour, orders.RetentionMS)
	// 172800000000 bytes / (2e6 bytes/sec * 86400 sec/day) = 1 day.
	assert.InDelta(t, 1.0, orders.EstimateFillDays, 0.001)
	assert.InDelta(t, 9*24*time.Hour, orders.OldestOffsetAge, float64(30*time.Second))
	assert.True(t, orders.AtRisk, "fill days (1) < retention (7) is at risk")

	payments := reports[1]
	assert.Equal(t, "payments", payments.Topic)
	assert.Equal(t, time.Duration(0), payments.RetentionMS, "unset retention is unknown")
	assert.Equal(t, 0.0, payments.EstimateFillDays)
	assert.False(t, payments.AtRisk)

	ghost := reports[2]
	assert.Equal(t, "ghost", ghost.Topic)
	assert.Equal(t, time.Duration(0), ghost.RetentionMS, "missing config is unknown")
	assert.False(t, ghost.AtRisk)
}

func TestAnalyzerRetentionBoundary(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name        string
		retentionMS string
		retentionB  string
		atRisk      bool
	}{
		{"fill days below retention is at risk", "604800000", "172800000000", true},   // 1 day < 7
		{"fill days equal to retention is safe", "604800000", "1209600000000", false}, // 7 == 7
		{"fill days above retention is safe", "604800000", "1728000000000", false},    // 10 > 7
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{
				daily: map[string][]storage.MetricRow{
					key(scraper.MetricTopicBytesRate, "orders"): {
						{TimeStart: now.Add(-24 * time.Hour), Avg: 2_000_000},
					},
				},
			}
			client := &fakeClient{configs: map[string]map[string]string{
				"orders": {"retention.ms": tc.retentionMS, "retention.bytes": tc.retentionB},
			}}
			a := &Analyzer{store: store, client: client}

			reports, err := a.Retention(context.Background(), []string{"orders"})
			require.NoError(t, err)
			require.Len(t, reports, 1)
			assert.Equal(t, tc.atRisk, reports[0].AtRisk)
		})
	}
}

func TestAnalyzerRetentionDescribeConfigsError(t *testing.T) {
	a := &Analyzer{store: &fakeStore{}, client: &fakeClient{configsErr: errors.New("acl denied")}}
	_, err := a.Retention(context.Background(), []string{"orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "describe configs")
}

func TestAnalyzerRetentionNoTopics(t *testing.T) {
	a := &Analyzer{store: &fakeStore{}, client: &fakeClient{}}
	reports, err := a.Retention(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, reports)
}

func TestAnalyzerRetentionStoreError(t *testing.T) {
	a := &Analyzer{store: &fakeStore{err: errors.New("db locked")}, client: &fakeClient{configs: map[string]map[string]string{
		"orders": {"retention.ms": "604800000", "retention.bytes": "172800000000"},
	}}}
	_, err := a.Retention(context.Background(), []string{"orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query")
}

// ─── Per-report error isolation ─────────────────────────────────────────────

func TestAnalyzerReportIsolation(t *testing.T) {
	store := seededStore(t)
	seedGrowth(t, store)
	client := &fakeClient{
		cluster:    &kafka.ClusterInfo{Brokers: []kafka.BrokerInfo{{ID: 1, LeaderPartitions: 5}}},
		configsErr: errors.New("acl denied"),
	}
	a := &Analyzer{store: store, client: client}

	_, err := a.Retention(context.Background(), []string{"orders"})
	require.Error(t, err, "describe configs failure fails retention only")

	reports, err := a.Growth(context.Background(), nil, 72*time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, reports)

	skew, err := a.Skew(context.Background())
	require.NoError(t, err)
	require.Len(t, skew, 1)
}

func atoi(s string) int {
	var n int
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}
