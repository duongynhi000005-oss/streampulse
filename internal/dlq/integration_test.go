// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// testBroker returns the broker address for integration tests, defaulting to
// the docker compose cluster.
func testBroker() string {
	if b := os.Getenv("STREAMPULSE_TEST_BROKER"); b != "" {
		return b
	}
	return "localhost:9093"
}

// requireBroker skips the test when no Kafka broker is reachable.
func requireBroker(t *testing.T) []string {
	t.Helper()
	brokers := []string{testBroker()}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := kafka.DialContext(ctx, "tcp", brokers[0]); err != nil {
		t.Skipf("Kafka not available at %s: %v", brokers[0], err)
	}
	return brokers
}

// createScratchTopic creates a unique DLQ topic (with the given partition
// count) and its original topic, registering cleanup to delete both.
func createScratchTopic(t *testing.T, name string, partitions int) (dlqTopic, origTopic string) {
	t.Helper()
	brokers := requireBroker(t)
	dlqTopic = fmt.Sprintf("%s-%d.dlq", name, time.Now().UnixNano())
	origTopic = dlqTopic[:len(dlqTopic)-len(".dlq")]
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.CreateTopics(
		kafka.TopicConfig{Topic: dlqTopic, NumPartitions: partitions, ReplicationFactor: 1},
	))
	require.NoError(t, conn.CreateTopics(
		kafka.TopicConfig{Topic: origTopic, NumPartitions: 1, ReplicationFactor: 1},
	))
	t.Cleanup(func() {
		c, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.DeleteTopics(dlqTopic, origTopic)
	})
	return dlqTopic, origTopic
}

// createScratchDlqOnly creates a unique DLQ topic without its original,
// registering cleanup to delete it.
func createScratchDlqOnly(t *testing.T, name string) string {
	t.Helper()
	brokers := requireBroker(t)
	dlqTopic := fmt.Sprintf("%s-%d.dlq", name, time.Now().UnixNano())
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.CreateTopics(
		kafka.TopicConfig{Topic: dlqTopic, NumPartitions: 1, ReplicationFactor: 1},
	))
	t.Cleanup(func() {
		c, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.DeleteTopics(dlqTopic)
	})
	return dlqTopic
}

// produceToPartition writes messages to a specific partition of a topic.
func produceToPartition(t *testing.T, brokers []string, topic string, partition int, msgs []kafka.Message) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, partition)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.WriteMessages(msgs...)
	require.NoError(t, err)
}

// produceWithWriter produces messages through the kafka-go Writer, which
// assigns partitions by key hash.
func produceWithWriter(t *testing.T, brokers []string, topic string, msgs []kafka.Message) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: int(kafka.RequireAll),
	})
	w.AllowAutoTopicCreation = false
	defer w.Close()
	require.NoError(t, w.WriteMessages(ctx, msgs...))
}

// countMessages returns the number of messages currently in a topic.
func countMessages(t *testing.T, brokers []string, topic string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	require.NoError(t, err)
	var total int64
	for _, p := range partitions {
		if p.Error != nil {
			continue
		}
		c, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		require.NoError(t, err)
		hw, err := c.ReadLastOffset()
		c.Close()
		require.NoError(t, err)
		total += hw
	}
	return total
}
