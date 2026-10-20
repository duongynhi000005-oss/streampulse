// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"context"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seededTransitionStore writes kafka.group.state samples for two groups with
// known rebalances:
//   - g1, two days ago: 2 transitions into PreparingRebalance (plus a
//     Dead→Empty transition that must not count)
//   - g2, yesterday: 1 transition into PreparingRebalance
func seededTransitionStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	ctx := context.Background()
	metrics := []storage.Metric{
		// g1: Dead(4)→Empty(1)→Preparing(2)→Completing(3)→Empty(1)→Preparing(2)
		// →Completing(3)→Empty(1): two rebalances.
		{TS: t0.Add(-48 * time.Hour), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 4},
		{TS: t0.Add(-48*time.Hour + 5*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		{TS: t0.Add(-48*time.Hour + 10*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 2},
		{TS: t0.Add(-48*time.Hour + 15*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 3},
		{TS: t0.Add(-48*time.Hour + 20*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		{TS: t0.Add(-48*time.Hour + 25*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 2},
		{TS: t0.Add(-48*time.Hour + 30*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 3},
		{TS: t0.Add(-48*time.Hour + 35*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: 1},
		// g2: one rebalance.
		{TS: t0.Add(-24 * time.Hour), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 1},
		{TS: t0.Add(-24*time.Hour + 5*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 2},
		{TS: t0.Add(-24*time.Hour + 10*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 3},
		{TS: t0.Add(-24*time.Hour + 15*time.Second), ClusterID: "c1", Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: 1},
	}
	require.NoError(t, store.WriteBatch(ctx, metrics))
	return store
}

func TestRebalancesCountsPreparingTransitions(t *testing.T) {
	store := seededTransitionStore(t)
	a := &Analyzer{store: store, client: nil}

	reports, err := a.Rebalances(context.Background(), nil, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 2)

	// ordered by group, then day
	g1 := reports[0]
	assert.Equal(t, "g1", g1.Group)
	assert.Equal(t, t0Point(t, -48*time.Hour), g1.Day, "g1 day is UTC midnight")
	assert.Equal(t, 2, g1.Count)

	g2 := reports[1]
	assert.Equal(t, "g2", g2.Group)
	assert.Equal(t, t0Point(t, -24*time.Hour), g2.Day, "g2 day is UTC midnight")
	assert.Equal(t, 1, g2.Count)
}

func TestRebalancesGroupFilter(t *testing.T) {
	store := seededTransitionStore(t)
	a := &Analyzer{store: store, client: nil}

	reports, err := a.Rebalances(context.Background(), []string{"g2"}, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reports, 1)
	assert.Equal(t, "g2", reports[0].Group)
	assert.Equal(t, 1, reports[0].Count)
}

func TestRebalancesNoData(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: nil}

	reports, err := a.Rebalances(context.Background(), nil, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, reports)
}

func TestRebalancesInvalidWindow(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: nil}

	_, err := a.Rebalances(context.Background(), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "window")
}
