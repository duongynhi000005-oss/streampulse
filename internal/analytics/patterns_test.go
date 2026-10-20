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

// seededPatternStore seeds 3 days of hourly kafka.topic.msg_rate points for
// "orders": 100 at 09:00 UTC, 10 at every other hour.
func seededPatternStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store := seededStore(t)
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	metrics := make([]storage.Metric, 0, 72)
	for i := 0; i < 72; i++ {
		v := 10.0
		if (i % 24) == 9 {
			v = 100.0
		}
		metrics = append(metrics, storage.Metric{
			TS:         t0.Add(time.Duration(i-72) * time.Hour),
			ClusterID:  "local-dev",
			Metric:     scraper.MetricTopicMsgRate,
			EntityType: "topic",
			EntityName: "orders",
			Value:      v,
		})
	}
	require.NoError(t, store.WriteBatch(context.Background(), metrics))
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	return store
}

// ─── Patterns ───────────────────────────────────────────────────────────────

func TestPatternsProfiles(t *testing.T) {
	store := seededPatternStore(t) // 3 days of hourly kafka.topic.msg_rate for "orders": 100 at 09:00, 10 at other hours
	a := &Analyzer{store: store, client: nil}

	reps, err := a.Patterns(context.Background(), []string{"orders"}, scraper.MetricTopicMsgRate, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, reps, 1)
	p := reps[0]
	assert.Equal(t, 9, p.PeakHour)
	assert.Greater(t, p.HourlyProfile[9], p.HourlyProfile[0])
	// The series is flat (identical days), but the 09:00 peak is asymmetric
	// within the window (positions 9/33/57 vs center 35.5), so the fit
	// yields a tiny negative slope rather than exactly zero.
	assert.InDelta(t, 0, p.Slope, 1e-4)
}
