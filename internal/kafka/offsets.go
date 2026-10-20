// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// TopicOffsets returns the high-watermark offset of every partition of every
// non-internal topic, keyed by topic then partition. Partitions whose metadata
// or offset lookup failed are skipped.
func (c *Client) TopicOffsets(ctx context.Context) (map[string]map[int]int64, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	req := &kafka.ListOffsetsRequest{Addr: conn.RemoteAddr(), Topics: make(map[string][]kafka.OffsetRequest)}
	offsets := make(map[string]map[int]int64)
	for _, p := range partitions {
		if isInternalTopic(p.Topic) || p.Error != nil {
			continue
		}
		if _, ok := offsets[p.Topic]; !ok {
			offsets[p.Topic] = make(map[int]int64)
		}
		req.Topics[p.Topic] = append(req.Topics[p.Topic], kafka.LastOffsetOf(p.ID))
	}
	if len(req.Topics) == 0 {
		return offsets, nil
	}

	resp, err := c.adminClient.ListOffsets(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	for topic, parts := range resp.Topics {
		m, ok := offsets[topic]
		if !ok {
			continue
		}
		for _, p := range parts {
			if p.Error != nil || p.LastOffset < 0 {
				continue
			}
			m[p.Partition] = p.LastOffset
		}
	}
	return offsets, nil
}

// GroupLag returns the per-group, per-topic lag for every consumer group in
// the cluster. Lag is the high-watermark minus the committed offset, floored
// at zero; partitions without a committed offset count as the full
// high-watermark.
func (c *Client) GroupLag(ctx context.Context) (map[string]map[string]int64, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	groups, err := c.ListConsumerGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	if len(groups) == 0 {
		return nil, nil
	}

	hw, err := c.TopicOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("topic offsets: %w", err)
	}

	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.Name
	}
	return c.groupLag(ctx, names, hw)
}

// groupLag fetches the committed offsets of each group and computes its lag
// against the given high-watermarks. Per-group failures are aggregated and
// partial results returned.
func (c *Client) groupLag(ctx context.Context, groups []string, hw map[string]map[int]int64) (map[string]map[string]int64, error) {
	lag := make(map[string]map[string]int64, len(groups))
	var errs []error
	for _, name := range groups {
		committed, err := c.committedOffsets(ctx, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", name, err))
			continue
		}
		lag[name] = lagFromOffsets(committed, hw)
	}
	return lag, errors.Join(errs...)
}

// committedOffsets returns the committed offsets of one consumer group, keyed
// by topic then partition.
func (c *Client) committedOffsets(ctx context.Context, group string) (map[string]map[int]int64, error) {
	var errs []error
	for _, b := range c.brokers {
		resp, err := c.adminClient.OffsetFetch(ctx, &kafka.OffsetFetchRequest{Addr: kafka.TCP(b), GroupID: group})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		if resp.Error != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, resp.Error))
			continue
		}
		committed := make(map[string]map[int]int64, len(resp.Topics))
		for topic, parts := range resp.Topics {
			m := make(map[int]int64, len(parts))
			for _, p := range parts {
				if p.Error != nil || p.CommittedOffset < 0 {
					continue
				}
				m[p.Partition] = p.CommittedOffset
			}
			committed[topic] = m
		}
		return committed, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}

// lagFromOffsets computes per-topic lag for one group: high-watermark minus
// committed offset per partition, floored at zero, summed per topic. Topics
// the group never committed for, and partitions missing from the
// high-watermarks, are excluded; a partition without a commit counts as the
// full high-watermark.
func lagFromOffsets(committed, hw map[string]map[int]int64) map[string]int64 {
	lag := make(map[string]int64)
	for topic, parts := range hw {
		cm, consumed := committed[topic]
		if !consumed {
			continue
		}
		for p, h := range parts {
			if h < 0 {
				continue
			}
			if d := h - cm[p]; d > 0 {
				lag[topic] += d
			}
		}
	}
	return lag
}
