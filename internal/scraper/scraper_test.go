// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package scraper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testClusterID = "local-dev"

// fakeClient satisfies the scraper Client interface. The interface grows per
// task: 2A adds the existing kafka methods, 2B TopicOffsets, 2C GroupLag.
type fakeClient struct {
	cluster *kafka.ClusterInfo
	topics  []kafka.TopicInfo
	groups  []kafka.GroupInfo
	offsets map[string]map[int]int64
	lags    map[string]map[string]int64
	err     error
}

func (f *fakeClient) DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error) {
	return f.cluster, f.err
}

func (f *fakeClient) ListTopics(ctx context.Context) ([]kafka.TopicInfo, error) {
	return f.topics, f.err
}

func (f *fakeClient) ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error) {
	return f.groups, f.err
}

func (f *fakeClient) TopicOffsets(ctx context.Context) (map[string]map[int]int64, error) {
	return f.offsets, f.err
}

func (f *fakeClient) GroupLag(ctx context.Context) (map[string]map[string]int64, error) {
	return f.lags, f.err
}

type fakeCollector struct {
	metrics []storage.Metric
	err     error
}

func (f *fakeCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	return f.metrics, f.err
}

// fakeStore implements storage.MetricsStore and records WriteBatch calls.
type fakeStore struct {
	batches [][]storage.Metric
	err     error
}

func (f *fakeStore) WriteBatch(ctx context.Context, metrics []storage.Metric) error {
	if f.err != nil {
		return f.err
	}
	f.batches = append(f.batches, metrics)
	return nil
}

func (f *fakeStore) QueryRaw(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) QueryHourly(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) QueryDaily(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) QueryStateTransitions(ctx context.Context, params storage.QueryParams) ([]storage.StateTransition, error) {
	return nil, nil
}

func (f *fakeStore) Rollup(ctx context.Context, resolution string) error {
	return nil
}

func (f *fakeStore) Purge(ctx context.Context, retention storage.Retention) error {
	return nil
}

func (f *fakeStore) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeStore) Migrate(ctx context.Context) error {
	return nil
}

func (f *fakeStore) QueryAlertState(ctx context.Context) ([]storage.AlertStateRow, error) {
	return nil, nil
}

func (f *fakeStore) SaveAlertState(ctx context.Context, row storage.AlertStateRow) error {
	return nil
}

func (f *fakeStore) Close() error {
	return nil
}

func TestMetricNames(t *testing.T) {
	names := map[string]string{
		"kafka.broker.leader_partitions":  MetricBrokerLeaderPartitions,
		"kafka.broker.replica_partitions": MetricBrokerReplicaPartitions,
		"kafka.topic.partition_count":     MetricTopicPartitionCount,
		"kafka.topic.messages":            MetricTopicMessages,
		"kafka.topic.msg_rate":            MetricTopicMsgRate,
		"kafka.topic.bytes_rate":          MetricTopicBytesRate,
		"kafka.group.lag":                 MetricGroupLag,
		"kafka.group.member_count":        MetricGroupMemberCount,
		"kafka.group.state":               MetricGroupState,
	}
	for want, got := range names {
		assert.Equal(t, want, got, "metric constant %q", want)
	}
}

func TestBrokerCollector(t *testing.T) {
	now := time.Now()
	client := &fakeClient{cluster: &kafka.ClusterInfo{
		ControllerID:              1,
		UnderReplicatedPartitions: 2,
		Brokers: []kafka.BrokerInfo{
			{Host: "broker-1", Port: 9092, ID: 1, LeaderPartitions: 4, ReplicaPartitions: 6},
			{Host: "broker-2", Port: 9093, ID: 2, LeaderPartitions: 2, ReplicaPartitions: 3},
		},
	}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), now)
	require.NoError(t, err)
	require.Len(t, metrics, 6)

	// One cluster-level metric: the under-replicated partition count.
	under, ok := metricWithEntityType(metrics, MetricClusterUnderReplicatedPartitions, "cluster")
	require.True(t, ok)
	assert.Equal(t, "cluster", under.EntityName)
	assert.Equal(t, 2.0, under.Value)

	got := make(map[string]map[string]float64)
	for _, m := range metrics {
		if m.EntityType != "broker" {
			continue
		}
		assert.Equal(t, now, m.TS)
		assert.Equal(t, testClusterID, m.ClusterID)
		if got[m.EntityName] == nil {
			got[m.EntityName] = make(map[string]float64)
		}
		got[m.EntityName][m.Metric] = m.Value
	}
	assert.Equal(t, 4.0, got["broker-1:9092"][MetricBrokerLeaderPartitions])
	assert.Equal(t, 6.0, got["broker-1:9092"][MetricBrokerReplicaPartitions])
	assert.Equal(t, 2.0, got["broker-2:9093"][MetricBrokerLeaderPartitions])
	assert.Equal(t, 3.0, got["broker-2:9093"][MetricBrokerReplicaPartitions])
}

func TestPartitionSkew(t *testing.T) {
	cases := []struct {
		name    string
		leaders []int
		want    float64
	}{
		{"three brokers skewed", []int{5, 2, 2}, 5.0 / 3.0},
		{"balanced", []int{3, 3, 3}, 1.0},
		{"single broker", []int{7}, 0},
		{"no brokers", nil, 0},
		{"zero leaders", []int{0, 0, 0}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.InDelta(t, tc.want, partitionSkew(tc.leaders), 0.001)
		})
	}
}

func TestBrokerCollectorPartitionSkew(t *testing.T) {
	now := time.Now()
	client := &fakeClient{cluster: &kafka.ClusterInfo{
		ControllerID: 1,
		Brokers: []kafka.BrokerInfo{
			{Host: "broker-1", Port: 9092, ID: 1, LeaderPartitions: 5},
			{Host: "broker-2", Port: 9093, ID: 2, LeaderPartitions: 2},
			{Host: "broker-3", Port: 9094, ID: 3, LeaderPartitions: 2},
		},
	}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), now)
	require.NoError(t, err)
	skew, ok := metricWithEntityType(metrics, MetricClusterPartitionSkew, "cluster")
	require.True(t, ok)
	assert.Equal(t, "cluster", skew.EntityName)
	assert.InDelta(t, 5.0/3.0, skew.Value, 0.001)
}

func TestBrokerCollectorNoUnderReplicated(t *testing.T) {
	client := &fakeClient{cluster: &kafka.ClusterInfo{
		ControllerID: 1,
		Brokers:      []kafka.BrokerInfo{{Host: "broker-1", Port: 9092, ID: 1, LeaderPartitions: 4, ReplicaPartitions: 6}},
	}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)
	under, ok := metricWithEntityType(metrics, MetricClusterUnderReplicatedPartitions, "cluster")
	require.True(t, ok)
	assert.Equal(t, 0.0, under.Value)
}

func TestBrokerCollectorOfflineBrokerName(t *testing.T) {
	client := &fakeClient{cluster: &kafka.ClusterInfo{Brokers: []kafka.BrokerInfo{{ID: 3, LeaderPartitions: 1, ReplicaPartitions: 1}}}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, metrics, 4)
	assert.Equal(t, "3", metrics[0].EntityName)
}

func TestBrokerCollectorError(t *testing.T) {
	client := &fakeClient{err: errors.New("cluster down")}
	c := newBrokerCollector(client, testClusterID)

	_, err := c.Collect(context.Background(), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster down")
}

func TestScraperCollect(t *testing.T) {
	store := &fakeStore{}
	s := NewWithCollectors(testClusterID, store, []Collector{
		&fakeCollector{metrics: []storage.Metric{{Metric: "m1", Value: 1}}},
		&fakeCollector{metrics: []storage.Metric{{Metric: "m2", Value: 2}}},
	})

	metrics, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, metrics, 2)
	for _, m := range metrics {
		assert.Equal(t, testClusterID, m.ClusterID)
	}
}

// topicMetricsIndexes maps a metric name to the slice of values collected for
// it, for the topic collector.
func topicMetricsIndexes(metrics []storage.Metric, name string) map[string]float64 {
	out := make(map[string]float64)
	for _, m := range metrics {
		if m.Metric == name {
			out[m.EntityName] = m.Value
		}
	}
	return out
}

func TestTopicCollectorFirstCollectNoRates(t *testing.T) {
	client := &fakeClient{
		topics:  []kafka.TopicInfo{{Name: "orders", Partitions: 3}},
		offsets: map[string]map[int]int64{"orders": {0: 100, 1: 100, 2: 100}},
	}
	c := newTopicCollector(client, testClusterID, 5*time.Second)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)

	partitionCount := topicMetricsIndexes(metrics, MetricTopicPartitionCount)
	messages := topicMetricsIndexes(metrics, MetricTopicMessages)
	assert.Equal(t, 3.0, partitionCount["orders"])
	assert.Equal(t, 300.0, messages["orders"])
	assert.NotContains(t, topicMetricsIndexes(metrics, MetricTopicMsgRate), "orders")
	assert.NotContains(t, topicMetricsIndexes(metrics, MetricTopicBytesRate), "orders")
}

func TestTopicCollectorRates(t *testing.T) {
	client := &fakeClient{
		topics:  []kafka.TopicInfo{{Name: "orders", Partitions: 3}, {Name: "payments", Partitions: 2}},
		offsets: map[string]map[int]int64{"orders": {0: 100, 1: 100, 2: 100}, "payments": {0: 50, 1: 50}},
	}
	c := newTopicCollector(client, testClusterID, 5*time.Second)
	t0 := time.Now()

	_, err := c.Collect(context.Background(), t0)
	require.NoError(t, err)

	client.offsets = map[string]map[int]int64{"orders": {0: 200, 1: 100, 2: 100}, "payments": {0: 60, 1: 50}}
	metrics, err := c.Collect(context.Background(), t0.Add(5*time.Second))
	require.NoError(t, err)

	// orders grew by 100 messages in 5s -> 20 msgs/s; payments by 10 -> 2/s.
	rate := topicMetricsIndexes(metrics, MetricTopicMsgRate)
	bytesRate := topicMetricsIndexes(metrics, MetricTopicBytesRate)
	assert.InDelta(t, 20.0, rate["orders"], 0.001)
	assert.InDelta(t, 2.0, rate["payments"], 0.001)
	// bytes_rate assumes a 100-byte average message.
	assert.InDelta(t, 2000.0, bytesRate["orders"], 0.001)
	assert.InDelta(t, 200.0, bytesRate["payments"], 0.001)

	// Cumulative messages reflect the new high-watermarks.
	messages := topicMetricsIndexes(metrics, MetricTopicMessages)
	assert.Equal(t, 400.0, messages["orders"])
	assert.Equal(t, 110.0, messages["payments"])
}

func TestTopicCollectorGapResetsRate(t *testing.T) {
	client := &fakeClient{
		topics:  []kafka.TopicInfo{{Name: "orders", Partitions: 1}},
		offsets: map[string]map[int]int64{"orders": {0: 100}},
	}
	c := newTopicCollector(client, testClusterID, 5*time.Second)
	t0 := time.Now()

	_, err := c.Collect(context.Background(), t0)
	require.NoError(t, err)
	_, err = c.Collect(context.Background(), t0.Add(5*time.Second))
	require.NoError(t, err)

	// 3 intervals later the delta must not spike: rate resets to 0.
	client.offsets = map[string]map[int]int64{"orders": {0: 10000}}
	metrics, err := c.Collect(context.Background(), t0.Add(20*time.Second))
	require.NoError(t, err)
	rate := topicMetricsIndexes(metrics, MetricTopicMsgRate)
	assert.InDelta(t, 0.0, rate["orders"], 0.001)

	// A normal interval after the gap computes a sane rate from the reset point.
	client.offsets = map[string]map[int]int64{"orders": {0: 10050}}
	metrics, err = c.Collect(context.Background(), t0.Add(25*time.Second))
	require.NoError(t, err)
	rate = topicMetricsIndexes(metrics, MetricTopicMsgRate)
	assert.InDelta(t, 10.0, rate["orders"], 0.001)
}

func TestTopicCollectorError(t *testing.T) {
	client := &fakeClient{err: errors.New("broker down")}
	c := newTopicCollector(client, testClusterID, 5*time.Second)

	_, err := c.Collect(context.Background(), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broker down")
}

func TestGroupStateMapping(t *testing.T) {
	cases := map[string]float64{
		"Empty":               0,
		"Stable":              1,
		"PreparingRebalance":  2,
		"CompletingRebalance": 3,
		"Dead":                4,
		"Bogus":               -1,
	}
	for in, want := range cases {
		assert.Equal(t, want, groupStateValue(in), "state %q", in)
	}
}

// groupMetricsByEntity groups collector output by entity name.
func groupMetricsByEntity(metrics []storage.Metric) map[string][]storage.Metric {
	out := make(map[string][]storage.Metric)
	for _, m := range metrics {
		out[m.EntityName] = append(out[m.EntityName], m)
	}
	return out
}

// metricWithTags finds the first metric with the given name and tag set.
func metricWithTags(ms []storage.Metric, name string, tags map[string]string) (storage.Metric, bool) {
	for _, m := range ms {
		if m.Metric != name {
			continue
		}
		if len(m.Tags) != len(tags) {
			continue
		}
		match := true
		for k, v := range tags {
			if m.Tags[k] != v {
				match = false
				break
			}
		}
		if match {
			return m, true
		}
	}
	return storage.Metric{}, false
}

// metricWithEntityType finds the first metric with the given name and entity
// type.
func metricWithEntityType(ms []storage.Metric, name, entityType string) (storage.Metric, bool) {
	for _, m := range ms {
		if m.Metric == name && m.EntityType == entityType {
			return m, true
		}
	}
	return storage.Metric{}, false
}

func TestGroupCollector(t *testing.T) {
	client := &fakeClient{
		groups: []kafka.GroupInfo{
			{Name: "orders-processor", State: "Stable", Members: 3},
			{Name: "dead-group", State: "Dead", Members: 0},
		},
		lags: map[string]map[string]int64{
			"orders-processor": {"orders": 150, "payments": 10},
			"dead-group":       {},
		},
	}
	c := newGroupCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)
	byEntity := groupMetricsByEntity(metrics)
	require.Len(t, byEntity["orders-processor"], 5)
	require.Len(t, byEntity["dead-group"], 3)

	for _, m := range metrics {
		assert.Equal(t, testClusterID, m.ClusterID)
		assert.Equal(t, "consumer_group", m.EntityType)
	}

	orders := byEntity["orders-processor"]
	dead := byEntity["dead-group"]

	total, ok := metricWithTags(orders, MetricGroupLag, nil)
	require.True(t, ok)
	assert.Equal(t, 160.0, total.Value)

	perTopic, ok := metricWithTags(orders, MetricGroupLag, map[string]string{"topic": "orders"})
	require.True(t, ok)
	assert.Equal(t, 150.0, perTopic.Value)

	members, ok := metricWithTags(orders, MetricGroupMemberCount, nil)
	require.True(t, ok)
	assert.Equal(t, 3.0, members.Value)

	state, ok := metricWithTags(orders, MetricGroupState, nil)
	require.True(t, ok)
	assert.Equal(t, 1.0, state.Value)

	deadTotal, ok := metricWithTags(dead, MetricGroupLag, nil)
	require.True(t, ok)
	assert.Equal(t, 0.0, deadTotal.Value)

	deadState, ok := metricWithTags(dead, MetricGroupState, nil)
	require.True(t, ok)
	assert.Equal(t, 4.0, deadState.Value)

	deadMembers, ok := metricWithTags(dead, MetricGroupMemberCount, nil)
	require.True(t, ok)
	assert.Equal(t, 0.0, deadMembers.Value)
}

func TestGroupCollectorNoGroups(t *testing.T) {
	client := &fakeClient{}
	c := newGroupCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestGroupCollectorError(t *testing.T) {
	client := &fakeClient{err: errors.New("cluster down")}
	c := newGroupCollector(client, testClusterID)

	_, err := c.Collect(context.Background(), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster down")
}

func TestNewDefaultCollectors(t *testing.T) {
	s := New(testClusterID, &fakeClient{}, &fakeStore{}, 0)
	require.Len(t, s.collectors, 3)
	assert.IsType(t, &brokerCollector{}, s.collectors[0])
	assert.IsType(t, &topicCollector{}, s.collectors[1])
	assert.IsType(t, &groupCollector{}, s.collectors[2])
}

func TestScrapeAndStoreBatch(t *testing.T) {
	store := &fakeStore{}
	ok := &fakeCollector{metrics: []storage.Metric{{Metric: "a", Value: 1}, {Metric: "b", Value: 2}}}
	fail := &fakeCollector{err: errors.New("boom")}
	s := NewWithCollectors(testClusterID, store, []Collector{ok, fail, ok})

	err := s.ScrapeAndStore(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")

	// One batch per cycle; the failing collector's metrics are dropped but the
	// others are persisted, stamped with the cluster ID.
	require.Len(t, store.batches, 1)
	require.Len(t, store.batches[0], 4)
	for _, m := range store.batches[0] {
		assert.Equal(t, testClusterID, m.ClusterID)
	}
}

func TestScrapeAndStoreEmptyNoWrite(t *testing.T) {
	store := &fakeStore{}
	fail := &fakeCollector{err: errors.New("boom")}
	s := NewWithCollectors(testClusterID, store, []Collector{fail})

	err := s.ScrapeAndStore(context.Background())
	require.Error(t, err)
	assert.Empty(t, store.batches)
}

func TestScrapeAndStoreWriteError(t *testing.T) {
	store := &fakeStore{err: errors.New("disk full")}
	s := NewWithCollectors(testClusterID, store, []Collector{&fakeCollector{metrics: []storage.Metric{{Metric: "a", Value: 1}}}})

	err := s.ScrapeAndStore(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write batch")
	assert.Contains(t, err.Error(), "disk full")
}
