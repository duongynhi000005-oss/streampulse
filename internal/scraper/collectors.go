// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package scraper

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
)

// brokerCollector emits per-broker leader and replica partition counts from
// DescribeCluster.
type brokerCollector struct {
	client    Client
	clusterID string
}

func newBrokerCollector(client Client, clusterID string) *brokerCollector {
	return &brokerCollector{client: client, clusterID: clusterID}
}

func (b *brokerCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	info, err := b.client.DescribeCluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe cluster: %w", err)
	}
	metrics := make([]storage.Metric, 0, len(info.Brokers)*2+2)
	for _, br := range info.Brokers {
		metrics = append(metrics,
			storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricBrokerLeaderPartitions, EntityType: "broker", EntityName: brokerName(br), Value: float64(br.LeaderPartitions)},
			storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricBrokerReplicaPartitions, EntityType: "broker", EntityName: brokerName(br), Value: float64(br.ReplicaPartitions)},
		)
	}
	leaders := make([]int, len(info.Brokers))
	for i, br := range info.Brokers {
		leaders[i] = br.LeaderPartitions
	}
	metrics = append(metrics,
		storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricClusterUnderReplicatedPartitions, EntityType: "cluster", EntityName: "cluster", Value: float64(info.UnderReplicatedPartitions)},
		storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricClusterPartitionSkew, EntityType: "cluster", EntityName: "cluster", Value: partitionSkew(leaders)},
	)
	return metrics, nil
}

// partitionSkew is the ratio of the most-led broker to the average leader
// count: 1 is a perfectly balanced cluster, >1 means leadership is
// concentrated. It is 0 when there is no meaningful distribution (fewer than
// two brokers, or no partitions led at all).
func partitionSkew(leaders []int) float64 {
	if len(leaders) <= 1 {
		return 0
	}
	max := 0
	sum := 0
	for _, n := range leaders {
		if n > max {
			max = n
		}
		sum += n
	}
	if sum == 0 {
		return 0
	}
	return float64(max) / (float64(sum) / float64(len(leaders)))
}

// brokerName formats a broker as host:port, falling back to the broker ID when
// the host is unknown (offline broker).
func brokerName(b kafka.BrokerInfo) string {
	if b.Host == "" {
		return strconv.Itoa(b.ID)
	}
	return net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
}

const (
	// avgMessageSize is the assumed mean payload size in bytes used to derive
	// bytes_rate from message counts (v0.1 has no broker-side byte counters).
	avgMessageSize = 100
)

// topicSnapshot is the per-topic state carried between collect cycles.
type topicSnapshot struct {
	messages int64
	bytes    int64
	ts       time.Time
}

// topicCollector emits per-topic partition counts, cumulative message counts,
// and per-second message and byte rates. Rates are computed against the
// previous cycle's snapshot and reset after a gap larger than two intervals so
// missed cycles never produce a bogus spike.
type topicCollector struct {
	client    Client
	clusterID string
	interval  time.Duration
	prev      map[string]topicSnapshot
}

func newTopicCollector(client Client, clusterID string, interval time.Duration) *topicCollector {
	return &topicCollector{client: client, clusterID: clusterID, interval: interval}
}

func (t *topicCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	topics, err := t.client.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	offsets, err := t.client.TopicOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("topic offsets: %w", err)
	}

	snap := make(map[string]topicSnapshot, len(topics))
	metrics := make([]storage.Metric, 0, len(topics)*4)
	for _, topic := range topics {
		messages := int64(0)
		if parts := offsets[topic.Name]; parts != nil {
			for _, off := range parts {
				if off > 0 {
					messages += off
				}
			}
		}
		metrics = append(metrics,
			storage.Metric{TS: now, ClusterID: t.clusterID, Metric: MetricTopicPartitionCount, EntityType: "topic", EntityName: topic.Name, Value: float64(topic.Partitions)},
			storage.Metric{TS: now, ClusterID: t.clusterID, Metric: MetricTopicMessages, EntityType: "topic", EntityName: topic.Name, Value: float64(messages)},
		)
		snap[topic.Name] = topicSnapshot{messages: messages, bytes: messages * avgMessageSize, ts: now}
	}

	for name, prev := range t.prev {
		cur, ok := snap[name]
		if !ok {
			continue
		}
		msgRate, byteRate := 0.0, 0.0
		if dt := cur.ts.Sub(prev.ts); dt > 0 && dt <= 2*t.interval {
			if delta := cur.messages - prev.messages; delta > 0 {
				msgRate = float64(delta) / dt.Seconds()
				byteRate = float64(cur.bytes-prev.bytes) / dt.Seconds()
			}
		}
		metrics = append(metrics,
			storage.Metric{TS: now, ClusterID: t.clusterID, Metric: MetricTopicMsgRate, EntityType: "topic", EntityName: name, Value: msgRate},
			storage.Metric{TS: now, ClusterID: t.clusterID, Metric: MetricTopicBytesRate, EntityType: "topic", EntityName: name, Value: byteRate},
		)
	}
	t.prev = snap
	return metrics, nil
}

// groupCollector emits per-group total lag, member count, and mapped state,
// plus per-topic lag rows tagged with the topic name.
type groupCollector struct {
	client    Client
	clusterID string
}

func newGroupCollector(client Client, clusterID string) *groupCollector {
	return &groupCollector{client: client, clusterID: clusterID}
}

func (g *groupCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	groups, err := g.client.ListConsumerGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	lag, err := g.client.GroupLag(ctx)
	if err != nil {
		return nil, fmt.Errorf("group lag: %w", err)
	}

	metrics := make([]storage.Metric, 0, len(groups)*3)
	for _, grp := range groups {
		perTopic := lag[grp.Name]
		total := int64(0)
		for _, l := range perTopic {
			total += l
		}
		metrics = append(metrics,
			storage.Metric{TS: now, ClusterID: g.clusterID, Metric: MetricGroupLag, EntityType: "consumer_group", EntityName: grp.Name, Value: float64(total)},
			storage.Metric{TS: now, ClusterID: g.clusterID, Metric: MetricGroupMemberCount, EntityType: "consumer_group", EntityName: grp.Name, Value: float64(grp.Members)},
			storage.Metric{TS: now, ClusterID: g.clusterID, Metric: MetricGroupState, EntityType: "consumer_group", EntityName: grp.Name, Value: groupStateValue(grp.State)},
		)
		for topic, l := range perTopic {
			metrics = append(metrics, storage.Metric{TS: now, ClusterID: g.clusterID, Metric: MetricGroupLag, EntityType: "consumer_group", EntityName: grp.Name, Tags: map[string]string{"topic": topic}, Value: float64(l)})
		}
	}
	return metrics, nil
}

// groupStateValue maps a Kafka group state string to the scraper.md enum:
// 0=Empty 1=Stable 2=PreparingRebalance 3=CompletingRebalance 4=Dead.
func groupStateValue(state string) float64 {
	switch state {
	case "Empty":
		return 0
	case "Stable":
		return 1
	case "PreparingRebalance":
		return 2
	case "CompletingRebalance":
		return 3
	case "Dead":
		return 4
	default:
		return -1
	}
}

// dlqCollector is a placeholder for Phase 6 DLQ discovery; it is not part of
// the default collector set until then.
type dlqCollector struct{}

func (dlqCollector) Collect(context.Context, time.Time) ([]storage.Metric, error) {
	return nil, nil
}
