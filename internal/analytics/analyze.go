// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package analytics computes growth, skew, and retention reports from the
// persisted metrics store and the live Kafka cluster.
package analytics

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
)

// skewBalancedThreshold is the max/avg leader ratio below which the cluster
// is considered balanced.
const skewBalancedThreshold = 1.5

const (
	// retentionMsKey and retentionBytesKey are the topic configs the
	// retention report reads.
	retentionMsKey    = "retention.ms"
	retentionBytesKey = "retention.bytes"
	// retentionSampleWindow is how far back the byte rate is averaged.
	retentionSampleWindow = 24 * time.Hour
)

// Client is the subset of the kafka client the analyzer depends on.
type Client interface {
	DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error)
	DescribeConfigs(ctx context.Context, resources []kafka.DescribeConfigResource) (map[string]map[string]string, error)
}

// Compile-time check that the concrete kafka client satisfies the interface.
var _ Client = (*kafka.Client)(nil)

// Analyzer computes analytics reports from persisted metrics and the live
// cluster. The three reports are independent: a failure in one never affects
// the others.
type Analyzer struct {
	store  storage.MetricsStore
	client Client
}

// NewAnalyzer creates an Analyzer over the given store and kafka client.
func NewAnalyzer(store storage.MetricsStore, client *kafka.Client) *Analyzer {
	return &Analyzer{store: store, client: client}
}

// Growth returns one report per topic with daily aggregation points over the
// window, ordered by topic name. Topics without any data in the window are
// omitted; a window larger than the available data renders a partial window.
func (a *Analyzer) Growth(ctx context.Context, topics []string, window time.Duration) ([]GrowthReport, error) {
	if window <= 0 {
		return nil, fmt.Errorf("invalid window %s", window)
	}

	now := time.Now()
	rows, err := a.store.QueryDaily(ctx, storage.QueryParams{
		Metric: scraper.MetricTopicMessages,
		From:   now.Add(-window),
		To:     now,
	})
	if err != nil {
		return nil, fmt.Errorf("query daily messages: %w", err)
	}

	want := make(map[string]bool, len(topics))
	for _, t := range topics {
		want[t] = true
	}

	byTopic := make(map[string][]storage.MetricRow)
	for _, r := range rows {
		if len(want) > 0 && !want[r.EntityName] {
			continue
		}
		byTopic[r.EntityName] = append(byTopic[r.EntityName], r)
	}

	names := make([]string, 0, len(byTopic))
	for name := range byTopic {
		names = append(names, name)
	}
	sort.Strings(names)

	reports := make([]GrowthReport, 0, len(names))
	for _, name := range names {
		pts := byTopic[name]
		report := GrowthReport{Topic: name, Window: window}
		report.Points = make([]Point, len(pts))
		rates := make([]float64, len(pts))
		for i, r := range pts {
			report.Points[i] = Point{Time: r.TimeStart, Rate: r.Avg}
			rates[i] = r.Avg
		}
		if len(pts) >= 2 {
			elapsed := pts[len(pts)-1].TimeStart.Sub(pts[0].TimeStart).Seconds()
			if elapsed > 0 {
				report.Delta = (pts[len(pts)-1].Avg - pts[0].Avg) / elapsed
			}
		}
		report.Sparkline = Sparkline(rates)
		reports = append(reports, report)
	}
	return reports, nil
}

// Skew reports the cluster-wide partition leadership distribution derived
// from DescribeCluster, one report describing the whole cluster (Topic "").
// An empty broker list yields no reports.
func (a *Analyzer) Skew(ctx context.Context) ([]SkewReport, error) {
	info, err := a.client.DescribeCluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe cluster: %w", err)
	}
	if len(info.Brokers) == 0 {
		return nil, nil
	}

	leaders := make(map[string]int, len(info.Brokers))
	total, max := 0, 0
	for _, b := range info.Brokers {
		leaders[strconv.Itoa(b.ID)] = b.LeaderPartitions
		total += b.LeaderPartitions
		if b.LeaderPartitions > max {
			max = b.LeaderPartitions
		}
	}

	avg := float64(total) / float64(len(info.Brokers))
	ratio := 0.0
	if avg > 0 {
		ratio = float64(max) / avg
	}
	return []SkewReport{{
		Leaders:  leaders,
		Ratio:    ratio,
		Balanced: ratio <= skewBalancedThreshold,
	}}, nil
}

// Retention returns one report per topic combining the broker's retention
// config with the persisted byte rate and oldest data age. A topic with no
// usable config (missing, "-1", or ACL-denied) reports zero retention as
// "unknown". An empty topic list yields no reports.
func (a *Analyzer) Retention(ctx context.Context, topics []string) ([]RetentionReport, error) {
	if len(topics) == 0 {
		return nil, nil
	}

	resources := make([]kafka.DescribeConfigResource, len(topics))
	for i, t := range topics {
		resources[i] = kafka.DescribeConfigResource{
			Type:        "topic",
			Name:        t,
			ConfigNames: []string{retentionMsKey, retentionBytesKey},
		}
	}
	configs, err := a.client.DescribeConfigs(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("describe configs: %w", err)
	}

	now := time.Now()
	reports := make([]RetentionReport, 0, len(topics))
	for _, topic := range topics {
		cfg := configs[topic]
		report := RetentionReport{Topic: topic}

		report.RetentionMS = parseRetentionDuration(cfg[retentionMsKey])
		retentionBytes := parseFloatConfig(cfg[retentionBytesKey])

		rateRows, err := a.store.QueryDaily(ctx, storage.QueryParams{
			Metric:     scraper.MetricTopicBytesRate,
			EntityName: topic,
			From:       now.Add(-retentionSampleWindow),
			To:         now,
		})
		if err != nil {
			return nil, fmt.Errorf("query bytes rate %s: %w", topic, err)
		}
		bytesPerDay := 0.0
		if len(rateRows) > 0 {
			sum := 0.0
			for _, r := range rateRows {
				sum += r.Avg
			}
			bytesPerDay = sum / float64(len(rateRows)) * float64(retentionSampleWindow/time.Second)
		}
		if retentionBytes > 0 && bytesPerDay > 0 {
			report.EstimateFillDays = retentionBytes / bytesPerDay
		}

		age, err := a.oldestAge(ctx, topic, now)
		if err != nil {
			return nil, err
		}
		report.OldestOffsetAge = age

		if report.RetentionMS > 0 && report.EstimateFillDays > 0 {
			retentionDays := report.RetentionMS.Seconds() / float64(retentionSampleWindow/time.Second)
			report.AtRisk = report.EstimateFillDays < retentionDays
		}
		reports = append(reports, report)
	}
	return reports, nil
}

// oldestAge returns how long ago the oldest persisted metric of the topic was
// recorded, or zero when the topic has no data.
func (a *Analyzer) oldestAge(ctx context.Context, topic string, now time.Time) (time.Duration, error) {
	rows, err := a.store.QueryRaw(ctx, storage.QueryParams{
		Metric:     scraper.MetricTopicMessages,
		EntityName: topic,
		Limit:      1,
	})
	if err != nil {
		return 0, fmt.Errorf("query oldest messages %s: %w", topic, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	age := now.Sub(rows[0].TimeStart)
	if age < 0 {
		age = 0
	}
	return age, nil
}

// parseRetentionDuration parses a Kafka retention.ms config value. Missing,
// unset ("-1"), or malformed values yield zero (unknown).
func parseRetentionDuration(s string) time.Duration {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// parseFloatConfig parses a numeric config value; missing, unset ("-1"), or
// malformed values yield zero.
func parseFloatConfig(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}
