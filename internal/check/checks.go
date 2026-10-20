// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package check implements the CI health checks behind the check command:
// connectivity, topic, consumer-group, retention, and replication gates that
// exit 0 when healthy and 1 or 2 when not.
package check

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
)

// Status is the outcome of a single check.
type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
	StatusSkip Status = "skip"
)

// Result is the outcome of a single check.
type Result struct {
	Name    string
	Status  Status
	Message string
	Value   float64
}

// Check is a single health check. Run returns the check result, or an error
// which RunAll converts into a failing result (a monitoring gap is itself a
// health problem, never a skip).
type Check struct {
	Name string
	Run  func(ctx context.Context, env Env) (Result, error)
}

// Client is the subset of the kafka client the checks need.
type Client interface {
	Ping(ctx context.Context) error
	PartitionHealth(ctx context.Context, topic string) (partitions, errored int, err error)
	DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error)
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error)
	GroupLag(ctx context.Context) (map[string]map[string]int64, error)
}

// Compile-time check that the kafka client satisfies the check contract.
var _ Client = (*kafka.Client)(nil)

// Env carries the client and flags used by the checks.
type Env struct {
	Client Client
	Flags  Flags
}

// Flags holds the check options set from the command line.
type Flags struct {
	Topics            []string
	Groups            []string
	MinPartitions     int
	MaxLag            int64
	MinRetentionHours float64
	CheckReplication  bool
	Timeout           time.Duration
}

// Defaults applied when the corresponding flag is left at zero.
const (
	DefaultMinPartitions = 1
	DefaultMaxLag        = int64(1000)
)

// ConnectivityCheck is the name of the connectivity check result.
const ConnectivityCheck = "connectivity"

// RunAll runs every applicable check sequentially and returns all results,
// even after a failure. If the connectivity check fails nothing else can
// run, so the remaining checks are reported as skipped. A positive
// Flags.Timeout bounds the whole run.
func RunAll(ctx context.Context, env Env) []Result {
	if env.Flags.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, env.Flags.Timeout)
		defer cancel()
	}

	checks := []Check{{Name: ConnectivityCheck, Run: runConnectivity}}
	for _, topic := range env.Flags.Topics {
		checks = append(checks, checkTopic(topic))
	}
	for _, group := range env.Flags.Groups {
		checks = append(checks, checkGroup(group))
	}
	if env.Flags.MaxLag > 0 && len(env.Flags.Groups) == 0 {
		// With no explicit --group, the lag gate applies to every consumer
		// group in the cluster.
		checks = append(checks, checkAllGroupLag())
	}
	if env.Flags.MinRetentionHours > 0 {
		for _, topic := range env.Flags.Topics {
			checks = append(checks, checkRetention(topic, env.Flags.MinRetentionHours))
		}
	}
	if env.Flags.CheckReplication {
		checks = append(checks, Check{Name: "replication", Run: runReplication})
	}

	results := make([]Result, 0, len(checks))
	connected := true
	for _, c := range checks {
		if !connected {
			results = append(results, Result{Name: c.Name, Status: StatusSkip, Message: "skipped: connectivity check failed"})
			continue
		}
		result, err := c.Run(ctx, env)
		if err != nil {
			result = Result{Name: c.Name, Status: StatusFail, Message: err.Error()}
		} else {
			result.Name = c.Name
		}
		if c.Name == ConnectivityCheck && result.Status == StatusFail {
			connected = false
		}
		results = append(results, result)
	}
	return results
}

// Verdict maps check results to a process exit code: 0 all checks passed,
// 1 at least one check failed, 2 the connectivity check failed (cluster
// unreachable or auth error — a pipeline problem, not a health verdict).
func Verdict(results []Result) int {
	for _, r := range results {
		if r.Name == ConnectivityCheck && r.Status == StatusFail {
			return 2
		}
	}
	for _, r := range results {
		if r.Status == StatusFail {
			return 1
		}
	}
	return 0
}

// runConnectivity verifies the cluster is reachable.
func runConnectivity(ctx context.Context, env Env) (Result, error) {
	if err := env.Client.Ping(ctx); err != nil {
		return Result{}, fmt.Errorf("ping: %w", err)
	}
	return Result{Status: StatusPass, Message: "brokers reachable", Value: 1}, nil
}

// checkTopic builds the health check for one topic: it must exist, have at
// least the minimum partition count, and no partition may be in error.
func checkTopic(topic string) Check {
	return Check{
		Name: "topic " + topic,
		Run: func(ctx context.Context, env Env) (Result, error) {
			partitions, errored, err := env.Client.PartitionHealth(ctx, topic)
			if err != nil {
				return Result{}, fmt.Errorf("partitions: %w", err)
			}
			min := env.Flags.MinPartitions
			if min < 1 {
				min = DefaultMinPartitions
			}

			var problems []string
			if partitions == 0 {
				problems = append(problems, "no partitions")
			}
			if partitions < min {
				problems = append(problems, fmt.Sprintf("%d partitions, min %d", partitions, min))
			}
			if errored > 0 {
				problems = append(problems, fmt.Sprintf("%d partitions errored", errored))
			}
			if len(problems) > 0 {
				return Result{Status: StatusFail, Message: strings.Join(problems, "; ")}, nil
			}
			return Result{
				Status:  StatusPass,
				Message: fmt.Sprintf("%d partitions, min %d", partitions, min),
				Value:   float64(partitions),
			}, nil
		},
	}
}

// checkGroup builds the health check for one consumer group: it must exist,
// be Stable, have active members, and its total lag must not exceed the
// configured maximum.
func checkGroup(group string) Check {
	return Check{
		Name: "group " + group,
		Run: func(ctx context.Context, env Env) (Result, error) {
			groups, err := env.Client.ListConsumerGroups(ctx)
			if err != nil {
				return Result{}, fmt.Errorf("list consumer groups: %w", err)
			}
			var info *kafka.GroupInfo
			for i := range groups {
				if groups[i].Name == group {
					info = &groups[i]
					break
				}
			}
			if info == nil {
				return Result{Status: StatusFail, Message: "group not found"}, nil
			}

			var problems []string
			if info.State != "Stable" {
				problems = append(problems, fmt.Sprintf("state %s, want Stable", info.State))
			}
			if info.Members == 0 {
				problems = append(problems, "no members")
			}

			lag, err := env.Client.GroupLag(ctx)
			if err != nil {
				return Result{}, fmt.Errorf("group lag: %w", err)
			}
			var total int64
			for _, l := range lag[group] {
				total += l
			}
			maxLag := env.Flags.MaxLag
			if maxLag < 1 {
				maxLag = DefaultMaxLag
			}
			if total > maxLag {
				problems = append(problems, fmt.Sprintf("lag %d, max %d", total, maxLag))
			}
			if len(problems) > 0 {
				return Result{Status: StatusFail, Message: strings.Join(problems, "; "), Value: float64(total)}, nil
			}
			return Result{Status: StatusPass, Message: fmt.Sprintf("state Stable, lag %d", total), Value: float64(total)}, nil
		},
	}
}

// checkAllGroupLag builds a check that every consumer group in the cluster
// stays within the configured lag maximum.
func checkAllGroupLag() Check {
	return Check{
		Name: "group lag",
		Run: func(ctx context.Context, env Env) (Result, error) {
			groups, err := env.Client.ListConsumerGroups(ctx)
			if err != nil {
				return Result{}, fmt.Errorf("list consumer groups: %w", err)
			}
			lag, err := env.Client.GroupLag(ctx)
			if err != nil {
				return Result{}, fmt.Errorf("group lag: %w", err)
			}
			maxLag := env.Flags.MaxLag
			if maxLag < 1 {
				maxLag = DefaultMaxLag
			}
			var problems []string
			var worst int64
			for _, g := range groups {
				var total int64
				for _, l := range lag[g.Name] {
					total += l
				}
				if total > worst {
					worst = total
				}
				if total > maxLag {
					problems = append(problems, fmt.Sprintf("%s lag %d, max %d", g.Name, total, maxLag))
				}
			}
			if len(problems) > 0 {
				return Result{Status: StatusFail, Message: strings.Join(problems, "; "), Value: float64(worst)}, nil
			}
			return Result{Status: StatusPass, Message: fmt.Sprintf("max lag %d", worst), Value: float64(worst)}, nil
		},
	}
}

// topicConfigsClient is the optional client capability for reading topic
// configuration, satisfied by the kafka client once topic config support
// lands.
type topicConfigsClient interface {
	DescribeConfigs(ctx context.Context, topic string) (map[string]string, error)
}

// checkRetention builds the health check for one topic's retention: its
// retention.ms must be at least the requested minimum, unless retention is
// unlimited (negative value), which satisfies any threshold.
func checkRetention(topic string, minHours float64) Check {
	return Check{
		Name: "retention " + topic,
		Run: func(ctx context.Context, env Env) (Result, error) {
			dc, ok := env.Client.(topicConfigsClient)
			if !ok {
				return Result{}, fmt.Errorf("client does not support DescribeConfigs")
			}
			configs, err := dc.DescribeConfigs(ctx, topic)
			if err != nil {
				return Result{}, fmt.Errorf("describe configs: %w", err)
			}
			raw, ok := configs["retention.ms"]
			if !ok {
				return Result{}, fmt.Errorf("retention.ms missing from config response")
			}
			ms, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return Result{}, fmt.Errorf("retention.ms %q: %w", raw, err)
			}
			if ms < 0 {
				return Result{Status: StatusPass, Message: "retention unlimited", Value: math.MaxFloat64}, nil
			}
			hours := float64(ms) / hoursToMS
			if hours < minHours {
				return Result{Status: StatusFail, Message: fmt.Sprintf("retention %.1fh, min %.1fh", hours, minHours), Value: hours}, nil
			}
			return Result{Status: StatusPass, Message: fmt.Sprintf("retention %.1fh, min %.1fh", hours, minHours), Value: hours}, nil
		},
	}
}

const hoursToMS = float64(1000 * 60 * 60)

// runReplication verifies no partition is under-replicated: every replica
// must be in sync (ISR == full replica set).
func runReplication(ctx context.Context, env Env) (Result, error) {
	cluster, err := env.Client.DescribeCluster(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("describe cluster: %w", err)
	}
	if cluster.UnderReplicatedPartitions > 0 {
		return Result{
			Status:  StatusFail,
			Message: fmt.Sprintf("%d under-replicated partitions", cluster.UnderReplicatedPartitions),
			Value:   float64(cluster.UnderReplicatedPartitions),
		}, nil
	}
	return Result{Status: StatusPass, Message: "all partitions fully replicated"}, nil
}
