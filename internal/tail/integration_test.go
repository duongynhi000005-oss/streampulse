// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tail

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotAndReadNewIntegration exercises Snapshot and ReadNew against a
// live broker (docker compose; skipped when unreachable). The scratch topic is
// single-partition so ordering is deterministic.
func TestSnapshotAndReadNewIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}
	topic := "streampulse-tail-test"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := &kafka.Client{Addr: kafka.TCP(broker)}
	if _, err := admin.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}}); err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}
	defer func() { admin.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}}) }()

	produce := func(n int) {
		w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic}
		defer w.Close()
		msgs := make([]kafka.Message, n)
		for i := range msgs {
			msgs[i] = kafka.Message{Key: []byte("k"), Value: []byte("v")}
		}
		require.NoError(t, w.WriteMessages(ctx, msgs...))
	}

	produce(60)

	b := NewBroker([]string{broker})
	msgs, err := Snapshot(ctx, b, topic, 50)
	require.NoError(t, err)
	require.Len(t, msgs, 50, "snapshot returns the last 50 of 60")
	assert.Equal(t, int64(59), msgs[49].Offset, "snapshot ends at the newest message")
	for i := 1; i < len(msgs); i++ {
		assert.False(t, msgs[i].Offset < msgs[i-1].Offset, "snapshot is offset-ordered")
	}

	offsets := map[int]int64{0: msgs[49].Offset + 1}

	produce(3)
	news, next, err := ReadNew(ctx, b, topic, offsets, 10)
	require.NoError(t, err)
	require.Len(t, news, 3, "follow picks up exactly the 3 new messages")
	assert.Equal(t, int64(63), next[0])
}
