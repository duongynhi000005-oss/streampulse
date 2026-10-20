// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package kafka provides the Kafka client wrapper using kafka-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describegroups"
)

// Client wraps the kafka-go connection for admin and consumer operations.
type Client struct {
	brokers []string

	dialer      *kafka.Dialer
	adminClient *kafka.Client
	transport   *kafka.Transport
}

// Options configures broker connectivity.
type Options struct {
	TLS  TLSOptions
	SASL SASLOptions
}

// NewClient creates a new Kafka client with plaintext connectivity.
func NewClient(brokers []string) *Client {
	c, err := NewClientWithOptions(brokers, Options{})
	if err != nil {
		// Default options never fail: no TLS files or SASL are configured.
		panic(fmt.Sprintf("kafka: NewClient with default options: %v", err))
	}
	return c
}

// NewClientWithOptions creates a new Kafka client with optional TLS and SASL
// authentication. Broker addresses may be bare host:port or carry a URL
// scheme (ssl://, sasl_ssl:// force TLS; plaintext:// is the default).
func NewClientWithOptions(brokers []string, opts Options) (*Client, error) {
	brokers, secure := normalizeBrokers(brokers)

	tlsOpts := opts.TLS
	if secure {
		tlsOpts.Enabled = true
	}
	tlsCfg, err := buildTLSConfig(tlsOpts)
	if err != nil {
		return nil, err
	}

	mech, err := buildSASL(opts.SASL, os.Getenv)
	if err != nil {
		return nil, err
	}

	dialer := &kafka.Dialer{
		Timeout:       5 * time.Second,
		TLS:           tlsCfg,
		SASLMechanism: mech,
	}
	transport := &kafka.Transport{
		Dial: dialer.DialFunc,
		TLS:  tlsCfg,
		SASL: mech,
	}
	return &Client{
		brokers:     brokers,
		dialer:      dialer,
		transport:   transport,
		adminClient: &kafka.Client{Transport: transport},
	}, nil
}

// normalizeBrokers strips an optional URL scheme from each broker address and
// reports whether any broker requires TLS (ssl:// or sasl_ssl:// form).
func normalizeBrokers(brokers []string) ([]string, bool) {
	normalized := make([]string, len(brokers))
	secure := false
	for i, b := range brokers {
		var s bool
		normalized[i], s = normalizeBroker(b)
		secure = secure || s
	}
	return normalized, secure
}

// normalizeBroker strips the scheme from a broker address and reports whether
// the scheme implies TLS.
func normalizeBroker(b string) (string, bool) {
	if rest, ok := strings.CutPrefix(b, "sasl_ssl://"); ok {
		return rest, true
	}
	if rest, ok := strings.CutPrefix(b, "ssl://"); ok {
		return rest, true
	}
	if rest, ok := strings.CutPrefix(b, "plaintext://"); ok {
		return rest, false
	}
	return b, false
}

// Close releases the transport's connection pool and background goroutines.
func (c *Client) Close() {
	c.transport.CloseIdleConnections()
}

// Brokers returns the broker addresses the client connects to, with any URL
// scheme already normalized away.
func (c *Client) Brokers() []string {
	return append([]string(nil), c.brokers...)
}

// Ping checks connectivity to the Kafka cluster.
func (c *Client) Ping(ctx context.Context) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Brokers()
	return err
}

// ClusterInfo returns basic cluster metadata.
type ClusterInfo struct {
	BrokerCount                 int
	ControllerID                int
	Brokers                     []BrokerInfo
	UnderReplicatedPartitions   int
}

// BrokerInfo holds metadata for a single Kafka broker.
type BrokerInfo struct {
	Host              string
	Port              int
	ID                int
	Rack              string
	LeaderPartitions  int
	ReplicaPartitions int
}

// DescribeCluster returns cluster metadata including the broker list with
// partition leader and replica counts derived from Metadata.
func (c *Client) DescribeCluster(ctx context.Context) (*ClusterInfo, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("controller: %w", err)
	}

	brokers, err := conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("brokers: %w", err)
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	leaderCount := make(map[int]int)
	replicaCount := make(map[int]int)
	for _, p := range partitions {
		if p.Error != nil {
			continue
		}
		leaderCount[p.Leader.ID]++
		for _, r := range p.Replicas {
			replicaCount[r.ID]++
		}
	}

	brokerInfos := make([]BrokerInfo, len(brokers))
	for i, b := range brokers {
		brokerInfos[i] = BrokerInfo{
			Host:              b.Host,
			Port:              b.Port,
			ID:                b.ID,
			Rack:              b.Rack,
			LeaderPartitions:  leaderCount[b.ID],
			ReplicaPartitions: replicaCount[b.ID],
		}
	}

	return &ClusterInfo{
		BrokerCount:               len(brokers),
		ControllerID:              controller.ID,
		Brokers:                   brokerInfos,
		UnderReplicatedPartitions: underReplicatedCount(partitions),
	}, nil
}

// underReplicatedCount counts partitions whose ISR is smaller than the full
// replica set: the leader is missing in-sync replicas, so the partition
// cannot serve its full redundancy.
func underReplicatedCount(partitions []kafka.Partition) int {
	n := 0
	for _, p := range partitions {
		if p.Error != nil {
			continue
		}
		if len(p.Isr) < len(p.Replicas) {
			n++
		}
	}
	return n
}

// GroupInfo holds metadata for a single consumer group.
type GroupInfo struct {
	Name    string
	State   string
	Members int
}

// ListConsumerGroups returns all consumer groups in the cluster.
// Uses kafka-go's Client.ListGroups and the protocol-level DescribeGroups
// (via transport.RoundTrip), with multi-broker failover.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]GroupInfo, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	var errs []error
	for _, b := range c.brokers {
		conn, err := c.dialer.DialContext(ctx, "tcp", b)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		conn.Close()

		groups, err := c.groupsFromBroker(ctx, kafka.TCP(b))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		return groups, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}

// groupsFromBroker lists and describes consumer groups through one broker.
// DescribeGroups uses the protocol package directly (not kafka.Client), because
// kafka-go's client wrapper fails to decode member metadata from modern consumers.
func (c *Client) groupsFromBroker(ctx context.Context, addr net.Addr) ([]GroupInfo, error) {
	listResp, err := c.adminClient.ListGroups(ctx, &kafka.ListGroupsRequest{Addr: addr})
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	if listResp.Error != nil {
		return nil, fmt.Errorf("list groups: %w", listResp.Error)
	}
	if len(listResp.Groups) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(listResp.Groups))
	for _, g := range listResp.Groups {
		ids = append(ids, g.GroupID)
	}

	raw, err := c.transport.RoundTrip(ctx, addr, &describegroups.Request{Groups: ids})
	if err != nil {
		return nil, fmt.Errorf("describe groups: %w", err)
	}
	resp, ok := raw.(*describegroups.Response)
	if !ok {
		return nil, fmt.Errorf("describe groups: unexpected response type %T", raw)
	}

	groups := make([]GroupInfo, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		if g.ErrorCode != 0 {
			continue
		}
		groups = append(groups, GroupInfo{Name: g.GroupID, State: g.GroupState, Members: len(g.Members)})
	}
	return groups, nil
}

// TopicInfo holds metadata for a single Kafka topic.
type TopicInfo struct {
	Name       string
	Partitions int
}

// ListTopics returns all non-internal topics in the cluster.
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	return partitionsToTopics(partitions), nil
}

// dial opens a connection to the first available broker.
func (c *Client) dial(ctx context.Context) (*kafka.Conn, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	var errs []error
	for _, b := range c.brokers {
		conn, err := c.dialer.DialContext(ctx, "tcp", b)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return nil, fmt.Errorf("dial all brokers: %w", errors.Join(errs...))
}

// partitionsToTopics groups kafka-go partitions by topic, filtering internal topics.
func partitionsToTopics(partitions []kafka.Partition) []TopicInfo {
	topicPartitions := make(map[string]int)
	for _, p := range partitions {
		if isInternalTopic(p.Topic) || p.Error != nil {
			continue
		}
		topicPartitions[p.Topic]++
	}

	topics := make([]TopicInfo, 0, len(topicPartitions))
	for name, count := range topicPartitions {
		topics = append(topics, TopicInfo{Name: name, Partitions: count})
	}

	sort.Slice(topics, func(i, j int) bool {
		return strings.Compare(topics[i].Name, topics[j].Name) < 0
	})

	return topics
}

// isInternalTopic reports whether a topic is an internal Kafka topic.
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "__")
}
