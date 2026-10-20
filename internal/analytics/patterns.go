// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
)

// Patterns returns throughput profiles per topic over the window. A topic
// without data is omitted. window <= 0 is an error.
func (a *Analyzer) Patterns(ctx context.Context, topics []string, metric string, window time.Duration) ([]ThroughputReport, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}
	if metric == "" {
		return nil, fmt.Errorf("metric required")
	}

	now := time.Now()
	rows, err := a.store.QueryHourly(ctx, storage.QueryParams{
		Metric: metric,
		From:   now.Add(-window),
		To:     now,
	})
	if err != nil {
		return nil, fmt.Errorf("query hourly %s: %w", metric, err)
	}

	byTopic := make(map[string][]storage.MetricRow)
	for _, r := range rows {
		if len(topics) > 0 && !contains(topics, r.EntityName) {
			continue
		}
		byTopic[r.EntityName] = append(byTopic[r.EntityName], r)
	}

	names := make([]string, 0, len(byTopic))
	for n := range byTopic {
		names = append(names, n)
	}
	sort.Strings(names)

	reports := make([]ThroughputReport, 0, len(names))
	for _, name := range names {
		pts := byTopic[name]
		rep := ThroughputReport{Topic: name, Metric: metric, Window: window}

		hourSums := [24]float64{}
		hourCounts := [24]int{}
		daySums := [7]float64{}
		dayCounts := [7]int{}
		for _, p := range pts {
			h := p.TimeStart.Hour()
			hourSums[h] += p.Avg
			hourCounts[h]++
			d := int(p.TimeStart.Weekday())
			daySums[d] += p.Avg
			dayCounts[d]++
		}
		for h := 0; h < 24; h++ {
			if hourCounts[h] > 0 {
				rep.HourlyProfile[h] = hourSums[h] / float64(hourCounts[h])
			}
		}
		for d := 0; d < 7; d++ {
			if dayCounts[d] > 0 {
				rep.DailyProfile[d] = daySums[d] / float64(dayCounts[d])
			}
		}
		rep.PeakHour = peakIndex(rep.HourlyProfile[:])
		rep.PeakDay = peakIndex(rep.DailyProfile[:])

		// least-squares linear fit on (unix hours, rate)
		slope, intercept := linearFit(pts)
		rep.Slope = slope
		last := pts[len(pts)-1].TimeStart
		rep.Forecast7d = slope*float64(last.Add(7*24*time.Hour).Unix()) + intercept

		reports = append(reports, rep)
	}
	return reports, nil
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func peakIndex(vals []float64) int {
	idx := 0
	for i := 1; i < len(vals); i++ {
		if vals[i] > vals[idx] {
			idx = i
		}
	}
	return idx
}

// linearFit returns the slope (per second) and intercept of the least-squares
// line through the points' timestamps and averages.
func linearFit(pts []storage.MetricRow) (slope, intercept float64) {
	n := len(pts)
	if n < 2 {
		return 0, 0
	}
	var sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := float64(p.TimeStart.Unix())
		y := p.Avg
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	denom := float64(n)*sxx - sx*sx
	if denom == 0 {
		return 0, 0
	}
	slope = (float64(n)*sxy - sx*sy) / denom
	intercept = (sy - slope*sx) / float64(n)
	return slope, intercept
}
