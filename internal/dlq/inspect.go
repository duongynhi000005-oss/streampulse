// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/segmentio/kafka-go"
)

// DefaultInspectLimit is the number of messages Inspect reads when limit is
// zero or negative.
const DefaultInspectLimit = 10

// DefaultDisplayMaxBytes is the truncation budget DisplayValue applies when
// maxBytes is zero or negative.
const DefaultDisplayMaxBytes = 1000

// Message is one message read from a DLQ topic.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Inspect reads up to limit messages from the end of topic, distributing the
// reads round-robin across partitions. Messages are returned oldest-first per
// partition. An empty topic returns an empty slice with no error; partitions
// that fail to read are reported in the returned error (joined with any
// partial results).
func Inspect(ctx context.Context, brokers []string, topic string, limit int) ([]Message, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	if limit <= 0 {
		limit = DefaultInspectLimit
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dlq: dial %s: %w", brokers[0], err)
	}
	partitions, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return nil, fmt.Errorf("dlq: inspect %q: %w", topic, err)
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })

	var tails [][]Message
	var errs []error
	for _, p := range partitions {
		if p.Error != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, p.Error))
			continue
		}
		tail, err := readTail(ctx, brokers[0], topic, p.ID, limit)
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.ID, err))
			continue
		}
		tails = append(tails, tail)
	}

	out := interleave(tails, limit)
	if len(out) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("dlq: inspect %q: %w", topic, errors.Join(errs...))
	}
	return out, errors.Join(errs...)
}

// readTail reads the last limit messages of one partition, oldest-first.
func readTail(ctx context.Context, broker, topic string, partition, limit int) ([]Message, error) {
	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return nil, err
	}
	hw, err := conn.ReadLastOffset()
	conn.Close()
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
	raw, err := readRange(ctx, broker, topic, partition, start, hw)
	if err != nil {
		return nil, err
	}
	out := make([]Message, len(raw))
	for i, m := range raw {
		out[i] = toMessage(topic, m)
	}
	return out, nil
}

// readRange reads the messages of one partition in the offset range
// [start, end), oldest-first.
func readRange(ctx context.Context, broker, topic string, partition int, start, end int64) ([]kafka.Message, error) {
	conn, err := kafka.DefaultDialer.DialLeader(ctx, "tcp", broker, topic, partition)
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
				break
			}
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// interleave merges per-partition tails round-robin, capped at limit.
func interleave(tails [][]Message, limit int) []Message {
	out := make([]Message, 0, limit)
	idx := make([]int, len(tails))
	for len(out) < limit {
		advanced := false
		for i, tail := range tails {
			if idx[i] < len(tail) && len(out) < limit {
				out = append(out, tail[idx[i]])
				idx[i]++
				advanced = true
			}
		}
		if !advanced {
			break
		}
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

// DisplayValue renders a message payload for terminal output: plain text
// when it is valid UTF-8, lowercase hex otherwise, truncated to maxBytes
// with a marker for the omitted tail.
func DisplayValue(v []byte, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = DefaultDisplayMaxBytes
	}
	if len(v) == 0 {
		return ""
	}
	if utf8.Valid(v) && isPrintableText(v) {
		return displayText(string(v), maxBytes)
	}
	if len(v) > maxBytes {
		return hex.EncodeToString(v[:maxBytes]) + fmt.Sprintf("...(+%d more bytes)", len(v)-maxBytes)
	}
	return hex.EncodeToString(v)
}

// isPrintableText reports whether every rune is printable or a common
// whitespace character, i.e. the payload looks like human-readable text
// rather than arbitrary binary.
func isPrintableText(v []byte) bool {
	for _, r := range string(v) {
		if r == '\n' || r == '\t' || r == '\r' {
			continue
		}
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func displayText(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		if b.Len()+utf8.RuneLen(r) > maxBytes {
			break
		}
		b.WriteRune(r)
	}
	return b.String() + fmt.Sprintf("...(+%d more bytes)", len(s)-b.Len())
}
