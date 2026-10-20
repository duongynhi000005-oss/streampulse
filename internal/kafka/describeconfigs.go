// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// DescribeConfigResource identifies a single resource whose config entries
// should be described.
type DescribeConfigResource struct {
	// Type is the resource kind: "topic" for v0.1.
	Type string
	// Name is the resource name, e.g. the topic name.
	Name string
	// ConfigNames are the config keys to fetch (e.g. "retention.ms").
	ConfigNames []string
}

// kafkaResourceTypes maps the public resource type names onto kafka-go's
// protocol types.
var kafkaResourceTypes = map[string]kafka.ResourceType{
	"topic": kafka.ResourceTypeTopic,
}

// DescribeConfigs returns the config entries of each resource, keyed by
// resource name then config name. Resources the broker rejects (unknown name,
// ACL denied) are omitted from the result; a transport-level failure across
// all brokers is returned as an error.
func (c *Client) DescribeConfigs(ctx context.Context, resources []DescribeConfigResource) (map[string]map[string]string, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	if len(resources) == 0 {
		return map[string]map[string]string{}, nil
	}

	req := &kafka.DescribeConfigsRequest{Resources: make([]kafka.DescribeConfigRequestResource, len(resources))}
	for i, r := range resources {
		rt, ok := kafkaResourceTypes[r.Type]
		if !ok {
			return nil, fmt.Errorf("unknown resource type %q", r.Type)
		}
		req.Resources[i] = kafka.DescribeConfigRequestResource{
			ResourceType: rt,
			ResourceName: r.Name,
			ConfigNames:  r.ConfigNames,
		}
	}

	var errs []error
	for _, b := range c.brokers {
		req.Addr = kafka.TCP(b)
		resp, err := c.adminClient.DescribeConfigs(ctx, req)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		out := make(map[string]map[string]string, len(resp.Resources))
		for _, r := range resp.Resources {
			if r.Error != nil {
				continue
			}
			entries := make(map[string]string, len(r.ConfigEntries))
			for _, e := range r.ConfigEntries {
				entries[e.ConfigName] = e.ConfigValue
			}
			out[r.ResourceName] = entries
		}
		return out, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}
