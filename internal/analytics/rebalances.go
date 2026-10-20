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

// statePreparingRebalance is the mapped kafka.group.state value for
// PreparingRebalance (scraper mapping: 0 Empty, 1 Stable, 2 Preparing, 3 Completing, 4 Dead).
const statePreparingRebalance = 2

// Rebalances counts per-group per-day transitions into PreparingRebalance over
// the window, derived from the persisted kafka.group.state samples. Detection
// is sampling-based (5s scrape): sub-5s rebalances may be missed. Groups with
// no transitions are omitted.
func (a *Analyzer) Rebalances(ctx context.Context, groups []string, window time.Duration) ([]RebalanceReport, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}

	now := time.Now()
	transitions, err := a.store.QueryStateTransitions(ctx, storage.QueryParams{
		Metric:     scraper.MetricGroupState,
		EntityType: "consumer_group",
		From:       now.Add(-window),
		To:         now,
	})
	if err != nil {
		return nil, fmt.Errorf("query state transitions: %w", err)
	}

	want := make(map[string]bool, len(groups))
	for _, g := range groups {
		want[g] = true
	}

	type dayKey struct {
		group string
		day   time.Time
	}
	counts := make(map[dayKey]int)
	for _, tr := range transitions {
		if tr.To != statePreparingRebalance {
			continue
		}
		if len(want) > 0 && !want[tr.Entity] {
			continue
		}
		day := tr.Time.Truncate(24 * time.Hour)
		counts[dayKey{tr.Entity, day}]++
	}

	reports := make([]RebalanceReport, 0, len(counts))
	for k, n := range counts {
		reports = append(reports, RebalanceReport{Group: k.group, Day: k.day, Count: n})
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].Group != reports[j].Group {
			return reports[i].Group < reports[j].Group
		}
		return reports[i].Day.Before(reports[j].Day)
	})
	return reports, nil
}
