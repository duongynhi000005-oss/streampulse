// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package scraper collects Kafka cluster metrics and persists them through the
// storage backend.
package scraper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
)

const (
	// scrapeTimeout bounds one full scrape cycle.
	scrapeTimeout = 5 * time.Second
	// defaultScrapeInterval is used when no interval is configured.
	defaultScrapeInterval = 5 * time.Second
)

// Client is the subset of the kafka client the collectors depend on.
type Client interface {
	DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error)
	ListTopics(ctx context.Context) ([]kafka.TopicInfo, error)
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error)
	TopicOffsets(ctx context.Context) (map[string]map[int]int64, error)
	GroupLag(ctx context.Context) (map[string]map[string]int64, error)
}

// Compile-time check that the concrete kafka client satisfies the interface.
var _ Client = (*kafka.Client)(nil)

// Collector scrapes one metric family into storage.Metric rows.
type Collector interface {
	Collect(ctx context.Context, now time.Time) ([]storage.Metric, error)
}

// Scraper runs the collectors sequentially within one timeout context and
// persists the resulting metrics in a single batch per cycle.
type Scraper struct {
	collectors []Collector
	store      storage.MetricsStore
	clusterID  string
}

// New creates a Scraper with the default collector set (broker, topic,
// group). The interval drives topic rate gap detection.
func New(clusterID string, client Client, store storage.MetricsStore, interval time.Duration) *Scraper {
	if interval <= 0 {
		interval = defaultScrapeInterval
	}
	return &Scraper{
		clusterID: clusterID,
		store:     store,
		collectors: []Collector{
			newBrokerCollector(client, clusterID),
			newTopicCollector(client, clusterID, interval),
			newGroupCollector(client, clusterID),
		},
	}
}

// NewWithCollectors creates a Scraper with a custom collector set.
func NewWithCollectors(clusterID string, store storage.MetricsStore, collectors []Collector) *Scraper {
	return &Scraper{clusterID: clusterID, store: store, collectors: collectors}
}

// Collect runs every collector sequentially and returns their metrics. A
// failing collector does not prevent the others from reporting; its error is
// returned joined with the rest.
func (s *Scraper) Collect(ctx context.Context) ([]storage.Metric, error) {
	scrapeCtx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()
	now := time.Now()

	var all []storage.Metric
	var errs []error
	for _, c := range s.collectors {
		metrics, err := c.Collect(scrapeCtx, now)
		if err != nil {
			errs = append(errs, err)
		}
		all = append(all, metrics...)
	}
	for i := range all {
		all[i].ClusterID = s.clusterID
	}
	return all, errors.Join(errs...)
}

// ScrapeAndStore runs one scrape cycle and persists the results in a single
// batch. Partial results are written even when a collector fails; empty result
// sets are not written.
func (s *Scraper) ScrapeAndStore(ctx context.Context) error {
	metrics, err := s.Collect(ctx)
	if err != nil {
		err = fmt.Errorf("collect: %w", err)
	}
	if len(metrics) == 0 {
		return err
	}
	if werr := s.store.WriteBatch(ctx, metrics); werr != nil {
		err = errors.Join(err, fmt.Errorf("write batch: %w", werr))
	}
	return err
}
