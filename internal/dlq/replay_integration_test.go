// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayIntegration(t *testing.T) {
	brokers := requireBroker(t)
	dlq, orig := createScratchTopic(t, "replay", 1)
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte(`{"error":"DB_TIMEOUT"}`), Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}},
		{Key: []byte("k1"), Value: []byte(`{"id":1}`)},
		{Key: []byte("k2"), Value: []byte(`{"id":2}`)},
		{Key: []byte("k3"), Value: []byte(`{"id":3}`)},
		{Key: []byte("k4"), Value: []byte(`{"id":4}`)},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := Replay(ctx, ReplayOptions{Brokers: brokers, Topic: dlq})
	require.NoError(t, err)
	assert.False(t, res.DryRun)
	assert.Equal(t, int64(5), res.Total)
	assert.Equal(t, int64(5), res.Replayed)
	assert.Equal(t, int64(0), res.Filtered)
	assert.Equal(t, int64(0), res.Skipped)
	assert.Equal(t, int64(0), res.Failed)
	assert.Equal(t, 1, res.Batches)

	msgs := readAllMessages(t, brokers, orig)
	require.Len(t, msgs, 5, "all messages replayed to original topic")
	byKey := make(map[string]kafka.Message, len(msgs))
	for _, m := range msgs {
		byKey[string(m.Key)] = m
	}

	m0 := byKey["k0"]
	assert.Equal(t, `{"error":"DB_TIMEOUT"}`, string(m0.Value), "value preserved")
	assert.Equal(t, "DB_TIMEOUT", headerValue(m0, "error"), "original header preserved")
	assert.Equal(t, "true", headerValue(m0, ReplayHeader), "replay marker header added")
	assert.Equal(t, "0", headerValue(m0, SourceOffsetHeader), "source offset header added")
	for i := 1; i < 5; i++ {
		m := byKey[fmt.Sprintf("k%d", i)]
		assert.Equal(t, "true", headerValue(m, ReplayHeader), "k%d marker", i)
		assert.Equal(t, fmt.Sprintf("%d", i), headerValue(m, SourceOffsetHeader), "k%d source offset", i)
	}
}

func TestReplayDryRunProducesNothing(t *testing.T) {
	brokers := requireBroker(t)
	dlq, orig := createScratchTopic(t, "replay-dry", 1)
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := Replay(ctx, ReplayOptions{Brokers: brokers, Topic: dlq, DryRun: true})
	require.NoError(t, err)
	assert.True(t, res.DryRun)
	assert.Equal(t, int64(3), res.Total)
	assert.Equal(t, int64(0), res.Replayed)
	require.Len(t, res.Sample, 3, "dry-run samples the candidates")
	assert.Equal(t, []byte("v0"), res.Sample[0].Value)
	assert.Equal(t, int64(0), countMessages(t, brokers, orig), "dry-run produces nothing")
}

func TestReplayFilterSelectsSubset(t *testing.T) {
	brokers := requireBroker(t)
	dlq, orig := createScratchTopic(t, "replay-filter", 1)
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}},
		{Key: []byte("k1")},
		{Key: []byte("k2"), Headers: []kafka.Header{{Key: "error", Value: []byte("DB_TIMEOUT")}}},
		{Key: []byte("k3")},
		{Key: []byte("k4")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := Replay(ctx, ReplayOptions{Brokers: brokers, Topic: dlq, Filter: "error=DB_TIMEOUT"})
	require.NoError(t, err)
	assert.Equal(t, int64(5), res.Total)
	assert.Equal(t, int64(2), res.Replayed)
	assert.Equal(t, int64(3), res.Filtered, "messages without the header excluded")

	msgs := readAllMessages(t, brokers, orig)
	require.Len(t, msgs, 2, "only the matching subset replayed")
	keys := map[string]bool{}
	for _, m := range msgs {
		keys[string(m.Key)] = true
	}
	assert.True(t, keys["k0"])
	assert.True(t, keys["k2"])
}

func TestReplaySkipExistingNoDuplicates(t *testing.T) {
	brokers := requireBroker(t)
	dlq, orig := createScratchTopic(t, "replay-skip", 1)
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first, err := Replay(ctx, ReplayOptions{Brokers: brokers, Topic: dlq, SkipExisting: true})
	require.NoError(t, err)
	assert.Equal(t, int64(5), first.Replayed)

	// A fresh consumer group re-reads everything from the start; the marker
	// copies left by the first run must be skipped via the source-offset
	// header, and their keys via the marker scan.
	second, err := Replay(ctx, ReplayOptions{
		Brokers:      brokers,
		Topic:        dlq,
		SkipExisting: true,
		groupID:      fmt.Sprintf("streampulse-replay-verify-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(10), second.Total, "originals plus marker copies")
	assert.Equal(t, int64(0), second.Replayed)
	assert.Equal(t, int64(10), second.Skipped, "marked copies and their keys skipped")

	assert.Equal(t, int64(5), countMessages(t, brokers, orig), "no duplicates on the original topic")
}

func TestReplayOriginalMissingErrors(t *testing.T) {
	brokers := requireBroker(t)
	dlq := createScratchDlqOnly(t, "replay-missing")
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := Replay(ctx, ReplayOptions{Brokers: brokers, Topic: dlq})
	require.Error(t, err)
	assert.Contains(t, err.Error(), dlq[:len(dlq)-len(".dlq")], "error names the original topic")
}

// failingWriter is a test producer that fails on the n-th WriteMessages call.
type failingWriter struct {
	calls    int
	failAt   int
	produced []kafka.Message
}

func (f *failingWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		f.calls++
		if f.calls == f.failAt {
			return errors.New("simulated produce failure")
		}
		f.produced = append(f.produced, m)
	}
	return nil
}

func (f *failingWriter) Close() error { return nil }

func TestReplayBatchCommitBoundary(t *testing.T) {
	brokers := requireBroker(t)
	dlq, _ := createScratchTopic(t, "replay-batch", 1)
	produceToPartition(t, brokers, dlq, 0, []kafka.Message{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	writer := &failingWriter{failAt: 3}
	res, err := Replay(ctx, ReplayOptions{
		Brokers:         brokers,
		Topic:           dlq,
		BatchSize:       2,
		producerFactory: func() messageWriter { return writer },
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated produce failure")
	assert.Equal(t, int64(2), res.Replayed, "only the first batch produced")

	admin := &kafka.Client{Transport: &kafka.Transport{Dial: kafka.DefaultDialer.DialFunc}}
	committed, err := fetchCommitted(ctx, admin, brokers, DefaultReplayGroup, dlq, []int{0})
	require.NoError(t, err)
	assert.Equal(t, int64(2), committed[0], "offsets not committed past the batch boundary")

	require.Len(t, writer.produced, 2, "failed batch not produced")
	assert.Equal(t, []byte("k1"), writer.produced[1].Key, "produced messages keep keys")
}

func readAllMessages(t *testing.T, brokers []string, topic string) []kafka.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	require.NoError(t, err)
	defer conn.Close()

	first, err := conn.ReadFirstOffset()
	require.NoError(t, err)
	hw, err := conn.ReadLastOffset()
	require.NoError(t, err)
	if _, err := conn.Seek(first, kafka.SeekAbsolute); err != nil {
		require.NoError(t, err)
	}

	batch := conn.ReadBatchWith(kafka.ReadBatchConfig{MinBytes: 1, MaxBytes: 10e6})
	defer batch.Close()

	var msgs []kafka.Message
	for {
		m, err := batch.ReadMessage()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		msgs = append(msgs, m)
		if int64(len(msgs)) == hw-first {
			break
		}
	}
	return msgs
}
