// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
)

// AnomalyMetrics are the metrics the anomaly detector evaluates.
var AnomalyMetrics = []string{
	scraper.MetricGroupLag,
	scraper.MetricTopicMsgRate,
	scraper.MetricTopicBytesRate,
}

// minSeasonalSamples is the minimum baseline samples per seasonal bucket.
const minSeasonalSamples = 3

// Anomalies scans hourly aggregates over the window for each requested metric
// and flags points whose seasonal or rolling z-score exceeds the threshold.
// A metric with insufficient history yields no anomalies (not an error).
func (a *Analyzer) Anomalies(ctx context.Context, metrics []string, window time.Duration) ([]Anomaly, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}
	if len(metrics) == 0 {
		metrics = AnomalyMetrics
	}

	now := time.Now()
	from := now.Add(-window)
	var out []Anomaly

	for _, metric := range metrics {
		rows, err := a.store.QueryHourly(ctx, storage.QueryParams{
			Metric: metric,
			From:   from,
			To:     now,
		})
		if err != nil {
			return nil, fmt.Errorf("query hourly %s: %w", metric, err)
		}

		byEntity := make(map[string][]storage.MetricRow)
		for _, r := range rows {
			byEntity[r.EntityName] = append(byEntity[r.EntityName], r)
		}

		for _, entity := range sortedKeys(byEntity) {
			pts := byEntity[entity]
			for i := range pts {
				z, expected, ok := a.scorePoint(metric, pts, i)
				if !ok {
					continue
				}
				sev, dir := classifyZ(z)
				if sev == "" {
					continue
				}
				out = append(out, Anomaly{
					Metric: metric, Entity: entity, Time: pts[i].TimeStart,
					Value: pts[i].Avg, Expected: expected, ZScore: z,
					Direction: dir, Severity: sev,
				})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Metric != out[j].Metric {
			return out[i].Metric < out[j].Metric
		}
		if out[i].Entity != out[j].Entity {
			return out[i].Entity < out[j].Entity
		}
		return out[i].Time.Before(out[j].Time)
	})
	return out, nil
}

// scorePoint z-scores point i against its seasonal bucket (hour-of-week over
// the available history) when the bucket has enough baseline samples, else the
// rolling window of the preceding points.
func (a *Analyzer) scorePoint(metric string, pts []storage.MetricRow, i int) (z, expected float64, ok bool) {
	// seasonal bucket: same hour-of-week slot across the window
	slot := ProfileIndex(pts[i].TimeStart, 168)
	slotVals := []float64{}
	for j := 0; j <= i; j++ {
		if ProfileIndex(pts[j].TimeStart, 168) == slot {
			slotVals = append(slotVals, pts[j].Avg)
		}
	}
	if len(slotVals) >= minSeasonalSamples+1 {
		z, err := SeasonalZScore(slotVals)
		if err == nil {
			mean, _ := meanStd(slotVals[:len(slotVals)-1])
			return z, mean, true
		}
	}
	// fallback: rolling window over the preceding hourly points
	if i >= 2 {
		vals := make([]float64, 0, i+1)
		for j := 0; j <= i; j++ {
			vals = append(vals, pts[j].Avg)
		}
		z, mean, err := RollingZScore(vals)
		if err == nil {
			return z, mean, true
		}
	}
	return 0, 0, false
}

// classifyZ maps a z-score to severity and direction; "" severity = no flag.
func classifyZ(z float64) (severity, direction string) {
	if z >= 2.0 {
		return "warning", "high"
	}
	if z <= -2.0 {
		return "warning", "low"
	}
	return "", ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
