// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package dlq implements the dead-letter queue module: discovery of DLQ
// topics by name convention, message inspection, and replay to the original
// topic.
package dlq

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pulsedev/streampulse/internal/kafka"
)

// DefaultSuffixes is the DLQ name convention used when suffixes is nil.
var DefaultSuffixes = []string{".dlq", ".dead", ".error", ".failed"}

// Client is the subset of the kafka client the DLQ module needs. It is
// satisfied by *kafka.Client.
type Client interface {
	ListTopics(ctx context.Context) ([]kafka.TopicInfo, error)
	TopicOffsets(ctx context.Context) (map[string]map[int]int64, error)
}

var _ Client = (*kafka.Client)(nil)

// Topic describes one discovered dead-letter topic.
type Topic struct {
	Name           string
	OriginalTopic  string
	OriginalExists bool
	MessageCount   int64
	GrowthRate     float64
}

// Discover finds DLQ topics by name convention: a topic is a DLQ when its
// name ends in one of suffixes (nil uses DefaultSuffixes), and the original
// topic is the name with the longest matching suffix stripped. Internal
// topics are excluded. MessageCount is the sum of high-watermark offsets
// across all partitions.
func Discover(ctx context.Context, client Client, suffixes []string) ([]Topic, error) {
	if suffixes == nil {
		suffixes = DefaultSuffixes
	}
	suffixes = sortedByLenDesc(suffixes)

	topics, err := client.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("dlq: list topics: %w", err)
	}
	offsets, err := client.TopicOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("dlq: topic offsets: %w", err)
	}

	known := make(map[string]bool, len(topics))
	for _, t := range topics {
		known[t.Name] = true
	}

	var dlqs []Topic
	for _, t := range topics {
		if strings.HasPrefix(t.Name, "__") {
			continue
		}
		original, ok := stripSuffix(t.Name, suffixes)
		if !ok {
			continue
		}
		var count int64
		for _, hw := range offsets[t.Name] {
			if hw > 0 {
				count += hw
			}
		}
		dlqs = append(dlqs, Topic{
			Name:           t.Name,
			OriginalTopic:  original,
			OriginalExists: known[original],
			MessageCount:   count,
		})
	}
	sort.Slice(dlqs, func(i, j int) bool { return dlqs[i].Name < dlqs[j].Name })
	return dlqs, nil
}

// stripSuffix reports whether name ends in one of suffixes and returns the
// name with the longest matching suffix removed.
func stripSuffix(name string, suffixes []string) (string, bool) {
	for _, s := range suffixes {
		if strings.HasSuffix(name, s) {
			return name[:len(name)-len(s)], true
		}
	}
	return "", false
}

func sortedByLenDesc(suffixes []string) []string {
	out := append([]string(nil), suffixes...)
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}
