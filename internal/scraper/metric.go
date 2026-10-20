// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package scraper

// Metric names follow the scraper.md design table: dotted, prometheus-flavored.
const (
	// MetricBrokerLeaderPartitions counts the partitions led by a broker.
	MetricBrokerLeaderPartitions = "kafka.broker.leader_partitions"
	// MetricBrokerReplicaPartitions counts the partitions replicated by a broker.
	MetricBrokerReplicaPartitions = "kafka.broker.replica_partitions"

	// MetricClusterUnderReplicatedPartitions counts partitions whose ISR is
	// smaller than the replica set.
	MetricClusterUnderReplicatedPartitions = "kafka.cluster.under_replicated_partitions"
	// MetricClusterPartitionSkew is the ratio of the most-led broker to the
	// average leader count (0 with 0 or 1 brokers).
	MetricClusterPartitionSkew = "kafka.cluster.partition_skew"

	// MetricTopicPartitionCount is the number of partitions of a topic.
	MetricTopicPartitionCount = "kafka.topic.partition_count"
	// MetricTopicMessages is the cumulative high-watermark message count.
	MetricTopicMessages = "kafka.topic.messages"
	// MetricTopicMsgRate is the per-second message delta.
	MetricTopicMsgRate = "kafka.topic.msg_rate"
	// MetricTopicBytesRate is the per-second byte delta.
	MetricTopicBytesRate = "kafka.topic.bytes_rate"

	// MetricGroupLag is the total lag of a consumer group.
	MetricGroupLag = "kafka.group.lag"
	// MetricGroupMemberCount is the number of members of a consumer group.
	MetricGroupMemberCount = "kafka.group.member_count"
	// MetricGroupState is the mapped consumer group state (0..4).
	MetricGroupState = "kafka.group.state"
)
