// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"fmt"
)

// PartitionHealth reports the health of a topic's partitions: the total
// partition count and how many of them carried metadata errors (the
// partitions ListTopics would skip). A missing topic surfaces as an error
// from the metadata request.
func (c *Client) PartitionHealth(ctx context.Context, topic string) (partitions, errored int, err error) {
	if len(c.brokers) == 0 {
		return 0, 0, fmt.Errorf("no brokers configured")
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, 0, fmt.Errorf("read partitions for %q: %w", topic, err)
	}
	for _, p := range parts {
		partitions++
		if p.Error != nil {
			errored++
		}
	}
	return partitions, errored, nil
}
