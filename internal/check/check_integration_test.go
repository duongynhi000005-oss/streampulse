// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package check

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	spkafka "github.com/pulsedev/streampulse/internal/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
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

// TestCheckIntegrationFullPass runs the check suite against the docker
// compose cluster: connectivity plus the orders topic (6 partitions) and the
// orders-processor consumer group must all pass with verdict 0.
func TestCheckIntegrationFullPass(t *testing.T) {
	brokers := requireBroker(t)
	client := spkafka.NewClient(brokers)
	defer client.Close()

	results := RunAll(context.Background(), Env{
		Client: client,
		Flags: Flags{
			Topics: []string{"orders"}, MinPartitions: 6,
			Groups: []string{"orders-processor"},
		},
	})
	for _, r := range results {
		assert.Equal(t, StatusPass, r.Status, r.Name)
	}
	assert.Equal(t, 0, Verdict(results))
}

// TestCheckIntegrationGroupLagFails creates a scratch topic with a committed
// consumer-group offset behind the high-watermark, so the group check must
// fail on lag with verdict 1.
func TestCheckIntegrationGroupLagFails(t *testing.T) {
	brokers := requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	topic := fmt.Sprintf("check-lag-%d", time.Now().UnixNano())
	group := fmt.Sprintf("check-lag-group-%d", time.Now().UnixNano())

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(
		kafka.TopicConfig{Topic: topic, NumPartitions: 2, ReplicationFactor: 1},
	))
	conn.Close()
	t.Cleanup(func() {
		c, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.DeleteTopics(topic)
	})

	msgs := make([]kafka.Message, 5)
	for i := range msgs {
		msgs[i] = kafka.Message{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}
	}
	// The topic leader may not be assigned yet right after creation;
	// retry the produce with a short backoff.
	// The topic leader may not be assigned yet right after creation;
	// retry the produce with a fresh leader connection per attempt (a
	// failed write corrupts the connection state).
	var werr error
	for attempt := 0; attempt < 5; attempt++ {
		pconn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, 0)
		if err == nil {
			_, werr = pconn.WriteMessages(msgs...)
			pconn.Close()
		} else {
			werr = err
		}
		if werr == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	require.NoError(t, werr)

	// Commit offset 2 on partition 0: the group lags the 5 produced
	// messages by 3, above the max lag of 1.
	admin := &kafka.Client{Transport: &kafka.Transport{Dial: kafka.DefaultDialer.DialFunc}}
	_, err = admin.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		Addr: kafka.TCP(brokers[0]), GroupID: group, GenerationID: -1,
		Topics: map[string][]kafka.OffsetCommit{topic: {{Partition: 0, Offset: 2}}},
	})
	require.NoError(t, err)

	client := spkafka.NewClient(brokers)
	defer client.Close()

	results := RunAll(ctx, Env{
		Client: client,
		Flags:  Flags{Groups: []string{group}, MaxLag: 1},
	})

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status, "a lagging group must fail the check")
	assert.Contains(t, results[1].Message, "lag 3, max 1")
	assert.Equal(t, 1, Verdict(results))
}

// TestCheckIntegrationConnectivityFail points the client at a dead broker:
// the connectivity check must fail and the verdict must be 2 (pipeline
// problem, not a health verdict).
func TestCheckIntegrationConnectivityFail(t *testing.T) {
	client := spkafka.NewClient([]string{"127.0.0.1:1"})
	defer client.Close()

	results := RunAll(context.Background(), Env{Client: client, Flags: Flags{}})

	require.Len(t, results, 1)
	assert.Equal(t, StatusFail, results[0].Status)
	assert.Equal(t, 2, Verdict(results))
}
