// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package tail provides snapshot and live-follow reading of Kafka topics
// without creating consumer groups. All reads are direct partition reads with
// in-memory offsets, so nothing is written to __consumer_offsets and nothing
// appears in the cluster's consumer group list.
package tail

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	// DefaultSnapshotLimit is the number of messages Snapshot reads.
	DefaultSnapshotLimit = 50
	// DefaultFollowInterval is the Follow poll cadence.
	DefaultFollowInterval = time.Second
	// DefaultPerPartition bounds how many messages ReadNew reads per
	// partition in one poll.
	DefaultPerPartition = 10
	// DefaultDisplayMaxBytes is the truncation budget DisplayValue applies.
	DefaultDisplayMaxBytes = 200
)

// Message is one message read from a topic.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Broker reads partitions of a Kafka topic. The concrete implementation
// (*kafkaBroker) uses plaintext direct connections; tests inject fakes.
type Broker interface {
	// Partitions lists the topic's partitions.
	Partitions(ctx context.Context, topic string) ([]kafka.Partition, error)
	// HighWatermark returns the partition's current end offset.
	HighWatermark(ctx context.Context, topic string, partition int) (int64, error)
	// ReadRange reads the messages of one partition in the offset range
	// [start, end), oldest-first.
	ReadRange(ctx context.Context, topic string, partition int, start, end int64) ([]kafka.Message, error)
}

// NewBroker returns a Broker dialing the given brokers. Plaintext only in
// v0.1 (same limitation as the dlq module); TLS/SASL pass-through is a
// follow-up.
func NewBroker(brokers []string) Broker {
	return &kafkaBroker{brokers: brokers}
}

type kafkaBroker struct {
	brokers []string
}

func (b *kafkaBroker) dial(ctx context.Context) (*kafka.Conn, error) {
	if len(b.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	return kafka.DialContext(ctx, "tcp", b.brokers[0])
}

func (b *kafkaBroker) Partitions(ctx context.Context, topic string) ([]kafka.Partition, error) {
	conn, err := b.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.ReadPartitions(topic)
}

func (b *kafkaBroker) HighWatermark(ctx context.Context, topic string, partition int) (int64, error) {
	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", b.brokers[0], topic, partition)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.ReadLastOffset()
}

func (b *kafkaBroker) ReadRange(ctx context.Context, topic string, partition int, start, end int64) ([]kafka.Message, error) {
	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", b.brokers[0], topic, partition)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.Seek(start, kafka.SeekAbsolute); err != nil {
		return nil, err
	}

	batch := conn.ReadBatchWith(kafka.ReadBatchConfig{MinBytes: 1, MaxBytes: 10e6})
	defer batch.Close()

	msgs := make([]kafka.Message, 0, end-start)
	for i := start; i < end; i++ {
		m, err := batch.ReadMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return msgs, nil
			}
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// Snapshot reads the last limit messages of a topic across all partitions,
// merged chronologically (timestamp, then offset). Partitions that fail are
// reported in the joined error alongside any partial results.
func Snapshot(ctx context.Context, b Broker, topic string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = DefaultSnapshotLimit
	}

	parts, err := b.Partitions(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("tail: partitions %q: %w", topic, err)
	}

	ok := make([]kafka.Partition, 0, len(parts))
	var errs []error
	for _, p := range parts {
		if p.Error != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, p.Error))
			continue
		}
		ok = append(ok, p)
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i].ID < ok[j].ID })

	perPart := int(math.Ceil(float64(limit)/float64(max(len(ok), 1)))) + 1

	var tails [][]Message
	for _, p := range ok {
		tailMsgs, err := snapshotPartition(ctx, b, topic, p.ID, perPart)
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, err))
			continue
		}
		tails = append(tails, tailMsgs)
	}

	merged := mergeChronological(tails)
	if len(merged) > limit {
		merged = merged[len(merged)-limit:]
	}
	if len(merged) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("tail: snapshot %q: %w", topic, errors.Join(errs...))
	}
	return merged, errors.Join(errs...)
}

func snapshotPartition(ctx context.Context, b Broker, topic string, partition, limit int) ([]Message, error) {
	hw, err := b.HighWatermark(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	if hw <= 0 {
		return nil, nil
	}
	start := hw - int64(limit)
	if start < 0 {
		start = 0
	}
	raw, err := b.ReadRange(ctx, topic, partition, start, hw)
	if err != nil {
		return nil, err
	}
	out := make([]Message, len(raw))
	for i, m := range raw {
		out[i] = toMessage(topic, m)
	}
	return out, nil
}

// ReadNew reads messages past the given offsets (per partition, bounded by
// perPartition), returning them and the updated offsets. A nil offsets map
// starts from the current high-watermarks, i.e. only future messages.
func ReadNew(ctx context.Context, b Broker, topic string, offsets map[int]int64, perPartition int) ([]Message, map[int]int64, error) {
	parts, err := b.Partitions(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("tail: partitions %q: %w", topic, err)
	}
	if perPartition <= 0 {
		perPartition = DefaultPerPartition
	}

	if offsets == nil {
		offsets = make(map[int]int64, len(parts))
		for _, p := range parts {
			if p.Error != nil {
				continue
			}
			hw, err := b.HighWatermark(ctx, topic, p.ID)
			if err != nil {
				return nil, nil, fmt.Errorf("tail: watermark %s/%d: %w", topic, p.ID, err)
			}
			offsets[p.ID] = hw
		}
		return nil, offsets, nil
	}

	var out []Message
	var errs []error
	next := make(map[int]int64, len(offsets))
	for _, p := range parts {
		if p.Error != nil {
			continue
		}
		from, ok := offsets[p.ID]
		if !ok {
			continue
		}
		hw, err := b.HighWatermark(ctx, topic, p.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, err))
			continue
		}
		if hw <= from {
			next[p.ID] = from
			continue
		}
		to := min(hw, from+int64(perPartition))
		raw, err := b.ReadRange(ctx, topic, p.ID, from, to)
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, err))
			next[p.ID] = from
			continue
		}
		for _, m := range raw {
			out = append(out, toMessage(topic, m))
		}
		last := from
		if len(raw) > 0 {
			last = raw[len(raw)-1].Offset + 1
		}
		next[p.ID] = last
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp.Equal(out[j].Timestamp) {
			if out[i].Offset == out[j].Offset {
				return out[i].Partition < out[j].Partition
			}
			return out[i].Offset < out[j].Offset
		}
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out, next, errors.Join(errs...)
}

// Follow polls ReadNew every interval, sending messages on ch and transient
// errors to onErr until ctx is canceled. Returns nil on ctx cancel.
func Follow(ctx context.Context, b Broker, topic string, offsets map[int]int64, interval time.Duration, ch chan<- Message, onErr func(error)) error {
	if interval <= 0 {
		interval = DefaultFollowInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	current := offsets
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			msgs, next, err := ReadNew(ctx, b, topic, current, DefaultPerPartition)
			if err != nil {
				if onErr != nil {
					onErr(err)
				}
				continue
			}
			current = next
			for _, m := range msgs {
				select {
				case ch <- m:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

// mergeChronological merges per-partition tails (each oldest-first) into one
// timestamp-ordered slice.
func mergeChronological(tails [][]Message) []Message {
	total := 0
	for _, t := range tails {
		total += len(t)
	}
	out := make([]Message, 0, total)
	idx := make([]int, len(tails))
	for {
		best := -1
		for i, t := range tails {
			if idx[i] >= len(t) {
				continue
			}
			if best == -1 {
				best = i
				continue
			}
			a, b := t[idx[i]], tails[best][idx[best]]
			if a.Timestamp.Before(b.Timestamp) ||
				(a.Timestamp.Equal(b.Timestamp) && a.Offset < b.Offset) ||
				(a.Timestamp.Equal(b.Timestamp) && a.Offset == b.Offset && a.Partition < b.Partition) {
				best = i
			}
		}
		if best == -1 {
			break
		}
		out = append(out, tails[best][idx[best]])
		idx[best]++
	}
	return out
}

func toMessage(topic string, m kafka.Message) Message {
	headers := make(map[string]string, len(m.Headers))
	for _, h := range m.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Message{
		Topic:     topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
		Timestamp: m.Time,
	}
}
