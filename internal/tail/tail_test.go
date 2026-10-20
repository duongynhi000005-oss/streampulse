// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tail

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBroker is a scripted Broker for unit tests.
type fakeBroker struct {
	parts []kafka.Partition
	hws   map[int]int64
	msgs  map[int][]kafka.Message // partition -> messages, offsets assumed contiguous from 0
}

func (f *fakeBroker) Partitions(ctx context.Context, topic string) ([]kafka.Partition, error) {
	return f.parts, nil
}

func (f *fakeBroker) HighWatermark(ctx context.Context, topic string, partition int) (int64, error) {
	return f.hws[partition], nil
}

func (f *fakeBroker) ReadRange(ctx context.Context, topic string, partition int, start, end int64) ([]kafka.Message, error) {
	msgs := f.msgs[partition]
	out := make([]kafka.Message, 0, end-start)
	for i := start; i < end; i++ {
		if i < 0 || i >= int64(len(msgs)) {
			break
		}
		out = append(out, msgs[i])
	}
	return out, nil
}

func mkMsg(topic string, partition int, offset int64, ts time.Time, key, value string) kafka.Message {
	return kafka.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     []byte(value),
		Time:      ts,
	}
}

func TestDisplayValue(t *testing.T) {
	assert.Equal(t, "", DisplayValue(nil, 0))
	assert.Equal(t, "hello", DisplayValue([]byte("hello"), 0))
	assert.Equal(t, "hel...(+2 more bytes)", DisplayValue([]byte("hello"), 3))
	assert.Equal(t, "0001ff", DisplayValue([]byte{0x00, 0x01, 0xff}, 0))
	assert.Equal(t, "0001...(+3 more bytes)", DisplayValue([]byte{0x00, 0x01, 0x02, 0x03, 0x04}, 2))
	assert.Equal(t, "café", DisplayValue([]byte("café"), 0))
}

func TestMergeChronological(t *testing.T) {
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	a := []Message{
		{Topic: "t", Partition: 0, Offset: 10, Timestamp: base.Add(2 * time.Second)},
		{Topic: "t", Partition: 0, Offset: 11, Timestamp: base.Add(5 * time.Second)},
	}
	b := []Message{
		{Topic: "t", Partition: 1, Offset: 4, Timestamp: base.Add(1 * time.Second)},
		{Topic: "t", Partition: 1, Offset: 5, Timestamp: base.Add(4 * time.Second)},
	}
	merged := mergeChronological([][]Message{a, b})
	require.Len(t, merged, 4)
	assert.Equal(t, []int{1, 0, 1, 0}, []int{merged[0].Partition, merged[1].Partition, merged[2].Partition, merged[3].Partition})
}

func TestMergeChronologicalTiebreak(t *testing.T) {
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	merged := mergeChronological([][]Message{
		{{Topic: "t", Partition: 1, Offset: 9, Timestamp: base}},
		{{Topic: "t", Partition: 0, Offset: 9, Timestamp: base}},
	})
	require.Len(t, merged, 2)
	assert.Equal(t, 0, merged[0].Partition, "same timestamp+offset → lower partition first")
	assert.Equal(t, 1, merged[1].Partition)
}

func TestSnapshotDistributesAcrossPartitions(t *testing.T) {
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0}, {Topic: "t", ID: 1}, {Topic: "t", ID: 2}},
		hws:   map[int]int64{0: 100, 1: 100, 2: 100},
		msgs:  map[int][]kafka.Message{},
	}
	for p := 0; p < 3; p++ {
		for i := int64(0); i < 100; i++ {
			b.msgs[p] = append(b.msgs[p], mkMsg("t", p, i, base.Add(time.Duration(int64(p)*1000+i)*time.Millisecond), "", ""))
		}
	}

	msgs, err := Snapshot(context.Background(), b, "t", 50)
	require.NoError(t, err)
	require.Len(t, msgs, 50)
	// chronological order, all three partitions represented
	seen := map[int]bool{}
	for _, m := range msgs {
		seen[m.Partition] = true
	}
	assert.Len(t, seen, 3)
	for i := 1; i < len(msgs); i++ {
		assert.False(t, msgs[i].Timestamp.Before(msgs[i-1].Timestamp))
	}
}

func TestSnapshotEmptyTopic(t *testing.T) {
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0}},
		hws:   map[int]int64{0: 0},
	}
	msgs, err := Snapshot(context.Background(), b, "t", 50)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestSnapshotErroredPartitionsReported(t *testing.T) {
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0, Error: errors.New("leader not available")}},
	}
	msgs, err := Snapshot(context.Background(), b, "t", 50)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "leader not available")
	assert.Empty(t, msgs)
}

func TestReadNewAdvancesOffsets(t *testing.T) {
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0}},
		hws:   map[int]int64{0: 25},
		msgs:  map[int][]kafka.Message{},
	}
	for i := int64(0); i < 25; i++ {
		b.msgs[0] = append(b.msgs[0], mkMsg("t", 0, i, base.Add(time.Duration(i)*time.Second), "", "m"))
	}

	// nil offsets → start from high-watermark, nothing returned
	msgs, offs, err := ReadNew(context.Background(), b, "t", nil, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs)
	assert.Equal(t, int64(25), offs[0])

	// advance the topic by 7
	b.hws[0] = 32
	for i := int64(25); i < 32; i++ {
		b.msgs[0] = append(b.msgs[0], mkMsg("t", 0, i, base.Add(time.Duration(i)*time.Second), "", "m"))
	}

	msgs, offs, err = ReadNew(context.Background(), b, "t", offs, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 7)
	assert.Equal(t, int64(32), offs[0], "offsets advance to the new watermark")

	// per-partition bound: jump far ahead, only perPartition returned
	b.hws[0] = 100
	for i := int64(32); i < 100; i++ {
		b.msgs[0] = append(b.msgs[0], mkMsg("t", 0, i, base.Add(time.Duration(i)*time.Second), "", "m"))
	}
	msgs, offs, err = ReadNew(context.Background(), b, "t", offs, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 10)
	assert.Equal(t, int64(42), offs[0])
}

func TestFollowDeliversUntilCancel(t *testing.T) {
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0}},
		hws:   map[int]int64{0: 1},
		msgs:  map[int][]kafka.Message{},
	}
	b.msgs[0] = append(b.msgs[0], mkMsg("t", 0, 0, base, "", "one"))

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan Message, 10)
	var gotErr error
	done := make(chan error, 1)
	go func() {
		done <- Follow(ctx, b, "t", map[int]int64{0: 0}, 10*time.Millisecond, ch, func(e error) { gotErr = e })
	}()

	select {
	case m := <-ch:
		assert.Equal(t, "one", string(m.Value))
	case <-time.After(2 * time.Second):
		t.Fatal("follow did not deliver the message")
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "follow returns nil on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("follow did not stop after cancel")
	}
	assert.Nil(t, gotErr, "no transient errors expected")
}

func TestFollowTransientErrorReported(t *testing.T) {
	b := &fakeBroker{
		parts: []kafka.Partition{{Topic: "t", ID: 0}},
		hws:   map[int]int64{0: 5},
	}
	// a failing broker: ReadRange always errors
	failing := &errorBroker{fake: b}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan Message, 10)
	var count atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- Follow(ctx, failing, "t", map[int]int64{0: 0}, 10*time.Millisecond, ch, func(e error) { count.Add(1) })
	}()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && count.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	assert.Positive(t, count.Load(), "transient errors are reported via onErr")
}

type errorBroker struct {
	fake *fakeBroker
}

func (e *errorBroker) Partitions(ctx context.Context, topic string) ([]kafka.Partition, error) {
	return e.fake.Partitions(ctx, topic)
}

func (e *errorBroker) HighWatermark(ctx context.Context, topic string, partition int) (int64, error) {
	return e.fake.HighWatermark(ctx, topic, partition)
}

func (e *errorBroker) ReadRange(ctx context.Context, topic string, partition int, start, end int64) ([]kafka.Message, error) {
	return nil, errors.New("boom")
}

func TestNewBrokerNoBrokers(t *testing.T) {
	b := NewBroker(nil)
	_, err := b.Partitions(context.Background(), "t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers configured")
}

func TestFormatMessage(t *testing.T) {
	ts := time.Date(2026, 8, 12, 12, 34, 56, 789000000, time.UTC)
	m := Message{
		Topic: "orders", Partition: 2, Offset: 1243,
		Key: []byte("order-42"), Value: []byte(`{"id":"ord_42"}`),
		Headers: map[string]string{"x-trace": "abc"}, Timestamp: ts,
	}
	line := FormatMessage(m)
	assert.True(t, strings.HasPrefix(line, "[p 2|o 1243|12:34:56.789]"), line)
	assert.Contains(t, line, `key="order-42"`)
	assert.Contains(t, line, `{"id":"ord_42"}`)
	assert.Contains(t, line, "(1 headers)")
}
