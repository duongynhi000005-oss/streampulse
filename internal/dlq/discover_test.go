// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"errors"
	"testing"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClient is a stand-in for *kafka.Client satisfying the dlq Client
// interface for discovery.
type fakeClient struct {
	topics  []kafka.TopicInfo
	offsets map[string]map[int]int64
	err     error
}

func (f *fakeClient) ListTopics(ctx context.Context) ([]kafka.TopicInfo, error) {
	return f.topics, f.err
}

func (f *fakeClient) TopicOffsets(ctx context.Context) (map[string]map[int]int64, error) {
	return f.offsets, f.err
}

func TestDiscoverSuffixTable(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "payments.dlq", Partitions: 2},
			{Name: "orders.dead", Partitions: 1},
			{Name: "audit.error", Partitions: 1},
			{Name: "jobs.failed", Partitions: 1},
			{Name: "payments", Partitions: 3},
			{Name: "orders", Partitions: 6},
		},
		offsets: map[string]map[int]int64{
			"payments.dlq": {0: 10, 1: 20},
			"orders.dead":  {0: 5},
			"audit.error":  {0: 0},
			"jobs.failed":  {0: 100},
		},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	require.Len(t, topics, 4, "all four default suffixes must match")

	got := make(map[string]Topic)
	for _, tp := range topics {
		got[tp.Name] = tp
	}

	assert.Equal(t, "payments", got["payments.dlq"].OriginalTopic)
	assert.Equal(t, "orders", got["orders.dead"].OriginalTopic)
	assert.Equal(t, "audit", got["audit.error"].OriginalTopic)
	assert.Equal(t, "jobs", got["jobs.failed"].OriginalTopic)

	assert.True(t, got["payments.dlq"].OriginalExists, "payments exists")
	assert.Equal(t, int64(30), got["payments.dlq"].MessageCount, "sum of partition offsets")
	assert.Equal(t, int64(0), got["audit.error"].MessageCount, "empty DLQ has zero messages")
}

func TestDiscoverCustomSuffixes(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "orders.dlq", Partitions: 1},
			{Name: "orders.retry", Partitions: 1},
		},
		offsets: map[string]map[int]int64{"orders.dlq": {0: 3}},
	}

	topics, err := Discover(context.Background(), f, []string{".dlq", ".retry"})
	require.NoError(t, err)
	require.Len(t, topics, 2)
}

func TestDiscoverNestedSuffixLongestMatch(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "a.dead.error", Partitions: 1},
			{Name: "a.dead", Partitions: 1},
		},
		offsets: map[string]map[int]int64{"a.dead.error": {0: 7}},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	require.Len(t, topics, 2, "a.dead matches .dead and is itself a DLQ")

	var nested Topic
	for _, tp := range topics {
		if tp.Name == "a.dead.error" {
			nested = tp
		}
	}
	assert.Equal(t, "a.dead", nested.OriginalTopic, "longest suffix .error stripped")
	assert.True(t, nested.OriginalExists, "original a.dead exists")
	assert.Equal(t, int64(7), nested.MessageCount)
}

func TestDiscoverExcludesNonSuffixTopics(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "orders", Partitions: 6},
			{Name: "payments", Partitions: 3},
			{Name: "my.dlqfile", Partitions: 1},
		},
		offsets: map[string]map[int]int64{},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	assert.Empty(t, topics, "suffix must be at the end of the name")
}

func TestDiscoverExcludesInternalTopics(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "__consumer_offsets", Partitions: 50},
			{Name: "__consumer_offsets.dlq", Partitions: 1},
			{Name: "payments.dlq", Partitions: 2},
		},
		offsets: map[string]map[int]int64{"payments.dlq": {0: 4}},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.Equal(t, "payments.dlq", topics[0].Name)
}

func TestDiscoverOriginalMissingFlagged(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "payments.dlq", Partitions: 2},
		},
		offsets: map[string]map[int]int64{"payments.dlq": {0: 4, 1: 6}},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.False(t, topics[0].OriginalExists, "original topic payments missing")
	assert.Equal(t, int64(10), topics[0].MessageCount)
}

func TestDiscoverClientError(t *testing.T) {
	f := &fakeClient{err: errors.New("dial tcp: connection refused")}
	_, err := Discover(context.Background(), f, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestDiscoverSorted(t *testing.T) {
	f := &fakeClient{
		topics: []kafka.TopicInfo{
			{Name: "zzz.failed", Partitions: 1},
			{Name: "aaa.dlq", Partitions: 1},
		},
		offsets: map[string]map[int]int64{"zzz.failed": {0: 1}, "aaa.dlq": {0: 1}},
	}

	topics, err := Discover(context.Background(), f, nil)
	require.NoError(t, err)
	assert.Equal(t, "aaa.dlq", topics[0].Name)
	assert.Equal(t, "zzz.failed", topics[1].Name)
}
