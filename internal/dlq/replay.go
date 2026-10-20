// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Headers added to messages replayed to the original topic.
const (
	// ReplayHeader marks a message as replayed by StreamPulse.
	ReplayHeader = "x-streampulse-replayed"
	// SourceOffsetHeader records the offset the message was replayed from.
	SourceOffsetHeader = "x-streampulse-source-offset"
)

// DefaultReplayBatchSize is the number of messages produced before DLQ
// offsets are committed.
const DefaultReplayBatchSize = 50

// DefaultReplayGroup is the consumer group replay commits offsets to.
const DefaultReplayGroup = "streampulse-replay"

// ReplayOptions configures a replay run.
type ReplayOptions struct {
	Brokers []string
	Topic   string

	// DryRun counts and samples the messages that would be replayed without
	// producing anything or committing offsets.
	DryRun bool

	// Limit caps the number of messages read from the DLQ; zero or negative
	// means no limit.
	Limit int

	// Filter restricts replay to messages with a header matching key=value.
	Filter string

	// OlderThan / NewerThan restrict replay to messages whose timestamp is
	// outside / inside the given window.
	OlderThan time.Duration
	NewerThan time.Duration

	// SkipExisting skips messages already carrying the replay marker header.
	SkipExisting bool

	// BatchSize is the number of messages produced between offset commits.
	BatchSize int

	// groupID is the consumer group used for offset commits (tests only).
	groupID string

	// producerFactory builds the writer used to produce (tests only).
	producerFactory func() messageWriter
}

// ReplayResult reports what a replay run did.
type ReplayResult struct {
	DryRun   bool
	Total    int64
	Replayed int64
	Filtered int64
	Skipped  int64
	Failed   int64
	Batches  int
	// Sample holds the first 10 candidates that would be replayed, populated
	// on dry runs.
	Sample []Message
}

// messageWriter is the produce side of a replay. *kafka.Writer satisfies it.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Replay reads a DLQ topic oldest-first within each partition and produces
// the messages to the original topic (name with the DLQ suffix stripped).
// Offsets are committed on the DLQ after each successful batch (at-least-once
// semantics: a crash may duplicate messages, mitigated by SkipExisting).
func Replay(ctx context.Context, opts ReplayOptions) (*ReplayResult, error) {
	if len(opts.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	original, ok := stripSuffix(opts.Topic, DefaultSuffixes)
	if !ok {
		return nil, fmt.Errorf("dlq: %q is not a DLQ topic (no known suffix)", opts.Topic)
	}

	group := opts.groupID
	if group == "" {
		group = DefaultReplayGroup
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultReplayBatchSize
	}

	admin := &kafka.Client{Transport: &kafka.Transport{Dial: kafka.DefaultDialer.DialFunc}}

	positions, err := replayPositions(ctx, opts.Brokers, opts.Topic, group, admin)
	if err != nil {
		return nil, fmt.Errorf("dlq: replay %q: %w", opts.Topic, err)
	}

	var filter replayFilter
	if opts.Filter != "" {
		key, value, err := ParseFilter(opts.Filter)
		if err != nil {
			return nil, fmt.Errorf("dlq: replay %q: %w", opts.Topic, err)
		}
		filter.key, filter.value = key, value
	}
	filter.olderThan = opts.OlderThan
	filter.newerThan = opts.NewerThan

	if !opts.DryRun {
		exists, err := originalTopicExists(ctx, opts.Brokers, original)
		if err != nil {
			return nil, fmt.Errorf("dlq: replay %q: %w", opts.Topic, err)
		}
		if !exists {
			return nil, fmt.Errorf("dlq: replay %q: original topic %q does not exist", opts.Topic, original)
		}
	}

	producer := opts.producerFactory
	if producer == nil {
		producer = func() messageWriter { return newReplayWriter(opts.Brokers) }
	}
	w := producer()
	defer w.Close()

	res := &ReplayResult{DryRun: opts.DryRun}

	var ranges []partitionRange
	for _, pp := range positions {
		msgs, err := readRange(ctx, opts.Brokers[0], opts.Topic, pp.partition, pp.start, pp.hw)
		if err != nil {
			return res, fmt.Errorf("dlq: replay %q: partition %d: %w", opts.Topic, pp.partition, err)
		}
		ranges = append(ranges, partitionRange{partition: pp.partition, msgs: msgs})
	}

	// With skip-existing, scan the DLQ for marker copies left by earlier
	// replay runs (x-streampulse-source-offset header) and skip their keys.
	markedKeys := make(map[string]struct{})
	if opts.SkipExisting {
		for _, r := range ranges {
			for _, m := range r.msgs {
				if hasHeader(m, SourceOffsetHeader) {
					markedKeys[string(m.Key)] = struct{}{}
				}
			}
		}
	}

	var batch []kafka.Message
	read := 0
	now := time.Now()

	for _, r := range ranges {
		for _, m := range r.msgs {
			if opts.Limit > 0 && read >= opts.Limit {
				break
			}
			read++
			res.Total++
			if filter.exclude(m, now) {
				res.Filtered++
				continue
			}
			if opts.SkipExisting {
				if hasHeader(m, SourceOffsetHeader) {
					res.Skipped++
					continue
				}
				if _, ok := markedKeys[string(m.Key)]; ok {
					res.Skipped++
					continue
				}
			}
			if opts.DryRun {
				if len(res.Sample) < 10 {
					res.Sample = append(res.Sample, toMessage(opts.Topic, m))
				}
				continue
			}
			if err := w.WriteMessages(ctx, replayMessage(original, m)); err != nil {
				res.Failed++
				if len(batch) > 0 {
					if ferr := flushBatch(ctx, opts, admin, group, batch); ferr != nil {
						return res, errors.Join(
							fmt.Errorf("dlq: replay %q: produce to %q: %w", opts.Topic, original, err),
							fmt.Errorf("dlq: replay %q: %w", opts.Topic, ferr),
						)
					}
					res.Batches++
				}
				return res, fmt.Errorf("dlq: replay %q: produce to %q: %w", opts.Topic, original, err)
			}
			res.Replayed++
			batch = append(batch, m)
			if len(batch) >= batchSize {
				if err := flushBatch(ctx, opts, admin, group, batch); err != nil {
					return res, fmt.Errorf("dlq: replay %q: %w", opts.Topic, err)
				}
				res.Batches++
				batch = nil
			}
		}
		if opts.Limit > 0 && read >= opts.Limit {
			break
		}
	}
	if len(batch) > 0 {
		if err := flushBatch(ctx, opts, admin, group, batch); err != nil {
			return res, fmt.Errorf("dlq: replay %q: %w", opts.Topic, err)
		}
		res.Batches++
	}
	return res, nil
}

// partitionRange is the messages read from one partition of the DLQ.
type partitionRange struct {
	partition int
	msgs      []kafka.Message
}

// flushBatch marks the produced messages on the DLQ when skip-existing is on,
// then commits their offsets to the replay group.
func flushBatch(ctx context.Context, opts ReplayOptions, admin *kafka.Client, group string, batch []kafka.Message) error {
	if opts.SkipExisting {
		if err := markReplayed(ctx, opts.Brokers, opts.Topic, batch); err != nil {
			return fmt.Errorf("mark replayed: %w", err)
		}
	}
	if err := commitBatch(ctx, admin, opts.Brokers, group, batch); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// markReplayed writes marker copies of the replayed messages back onto the
// DLQ (same partition, same key, replay headers added) so a later replay with
// SkipExisting can recognize and skip them.
func markReplayed(ctx context.Context, brokers []string, topic string, batch []kafka.Message) error {
	byPartition := make(map[int][]kafka.Message)
	for _, m := range batch {
		byPartition[m.Partition] = append(byPartition[m.Partition], replayMessage(topic, m))
	}
	var errs []error
	for p, msgs := range byPartition {
		conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, p)
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p, err))
			continue
		}
		_, werr := conn.WriteMessages(msgs...)
		conn.Close()
		if werr != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p, werr))
		}
	}
	return errors.Join(errs...)
}

// ParseFilter parses a header filter of the form key=value.
func ParseFilter(s string) (key, value string, err error) {
	key, value, ok := strings.Cut(s, "=")
	if !ok || key == "" {
		return "", "", fmt.Errorf("dlq: filter %q must be key=value", s)
	}
	return key, value, nil
}

// replayFilter combines the header and age filters of a replay run.
type replayFilter struct {
	key, value string
	olderThan  time.Duration
	newerThan  time.Duration
}

// exclude reports whether a message is filtered out of the replay.
func (f replayFilter) exclude(m kafka.Message, now time.Time) bool {
	if f.key != "" {
		if v, ok := headerValueOK(m, f.key); !ok || v != f.value {
			return true
		}
	}
	if f.olderThan > 0 && !m.Time.Before(now.Add(-f.olderThan)) {
		return true
	}
	if f.newerThan > 0 && m.Time.Before(now.Add(-f.newerThan)) {
		return true
	}
	return false
}

// hasHeader reports whether a message carries the given header key.
func hasHeader(m kafka.Message, key string) bool {
	_, ok := headerValueOK(m, key)
	return ok
}

// headerValue returns the value of the first header with the given key.
func headerValue(m kafka.Message, key string) string {
	v, _ := headerValueOK(m, key)
	return v
}

func headerValueOK(m kafka.Message, key string) (string, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// replayMessage copies a DLQ message for the original topic, preserving key,
// value and headers and adding the replay marker headers.
func replayMessage(original string, m kafka.Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(m.Headers)+2)
	headers = append(headers, m.Headers...)
	headers = append(headers,
		kafka.Header{Key: ReplayHeader, Value: []byte("true")},
		kafka.Header{Key: SourceOffsetHeader, Value: []byte(strconv.FormatInt(m.Offset, 10))},
	)
	return kafka.Message{
		Topic:   original,
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
	}
}

// partPosition is the read window of one partition: from start (committed
// offset or first offset) to the current high-watermark.
type partPosition struct {
	partition int
	start     int64
	hw        int64
}

// replayPositions resolves the partitions of a DLQ topic and the offset each
// one starts reading from (the group's committed offset, or the first offset
// when nothing is committed).
func replayPositions(ctx context.Context, brokers []string, topic, group string, admin *kafka.Client) ([]partPosition, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", brokers[0], err)
	}
	partitions, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })

	var ids []int
	for _, p := range partitions {
		if p.Error == nil {
			ids = append(ids, p.ID)
		}
	}
	committed, err := fetchCommitted(ctx, admin, brokers, group, topic, ids)
	if err != nil {
		return nil, fmt.Errorf("committed offsets: %w", err)
	}

	positions := make([]partPosition, 0, len(partitions))
	for _, p := range partitions {
		if p.Error != nil {
			continue
		}
		c, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", p.ID, err)
		}
		hw, err := c.ReadLastOffset()
		c.Close()
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", p.ID, err)
		}
		start := committed[p.ID]
		if start < 0 {
			start = 0
		}
		positions = append(positions, partPosition{partition: p.ID, start: start, hw: hw})
	}
	return positions, nil
}

// fetchCommitted returns the committed offsets of a consumer group for one
// topic, keyed by partition; partitions without a commit are absent.
func fetchCommitted(ctx context.Context, admin *kafka.Client, brokers []string, group, topic string, partitions []int) (map[int]int64, error) {
	if len(partitions) == 0 {
		return map[int]int64{}, nil
	}
	req := &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: partitions},
	}
	var errs []error
	for _, b := range brokers {
		resp, err := admin.OffsetFetch(ctx, &kafka.OffsetFetchRequest{Addr: kafka.TCP(b), GroupID: group, Topics: req.Topics})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		if resp.Error != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, resp.Error))
			continue
		}
		out := make(map[int]int64, len(resp.Topics[topic]))
		for _, p := range resp.Topics[topic] {
			if p.Error != nil || p.CommittedOffset < 0 {
				continue
			}
			out[p.Partition] = p.CommittedOffset
		}
		return out, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}

// commitBatch commits the offsets of the produced messages (offset+1 per
// partition) to the replay group, failing over across brokers.
func commitBatch(ctx context.Context, admin *kafka.Client, brokers []string, group string, msgs []kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	offsets := make(map[int]int64)
	topic := msgs[0].Topic
	for _, m := range msgs {
		if o := m.Offset + 1; o > offsets[m.Partition] {
			offsets[m.Partition] = o
		}
	}
	parts := make([]kafka.OffsetCommit, 0, len(offsets))
	for p, o := range offsets {
		parts = append(parts, kafka.OffsetCommit{Partition: p, Offset: o})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Partition < parts[j].Partition })

	var errs []error
	for _, b := range brokers {
		resp, err := admin.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
			Addr:         kafka.TCP(b),
			GroupID:      group,
			GenerationID: -1,
			Topics:       map[string][]kafka.OffsetCommit{topic: parts},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		for t, ps := range resp.Topics {
			for _, p := range ps {
				if p.Error != nil {
					errs = append(errs, fmt.Errorf("commit %s partition %d: %w", t, p.Partition, p.Error))
				}
			}
		}
		if len(errs) == 0 {
			return nil
		}
	}
	return fmt.Errorf("commit to %q: %w", topic, errors.Join(errs...))
}

// originalTopicExists reports whether the original topic exists and has at
// least one healthy partition.
func originalTopicExists(ctx context.Context, brokers []string, topic string) (bool, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", brokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return false, fmt.Errorf("original topic %q: %w", topic, err)
	}
	for _, p := range partitions {
		if p.Error == nil {
			return true, nil
		}
		if !errors.Is(p.Error, kafka.UnknownTopicOrPartition) {
			return false, fmt.Errorf("original topic %q partition %d: %w", topic, p.ID, p.Error)
		}
	}
	return false, nil
}

// newReplayWriter builds the default producer for a replay: required acks,
// auto topic creation disabled so a missing original topic fails per batch.
// Messages carry their own topic (the original topic of the DLQ).
func newReplayWriter(brokers []string) messageWriter {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		RequiredAcks: int(kafka.RequireAll),
	})
	w.AllowAutoTopicCreation = false
	return w
}
