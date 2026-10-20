// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package check

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsedev/streampulse/internal/kafka"
)

// fakeClient implements the check Client contract with configurable results.
type fakeClient struct {
	pingErr error

	partitions int
	errored    int
	partErr    error

	cluster   *kafka.ClusterInfo
	clusterErr error
	groups    []kafka.GroupInfo
	groupsErr error
	lag       map[string]map[string]int64
	lagErr    error
}

func (f *fakeClient) Ping(context.Context) error { return f.pingErr }

func (f *fakeClient) PartitionHealth(context.Context, string) (int, int, error) {
	return f.partitions, f.errored, f.partErr
}

func (f *fakeClient) DescribeCluster(context.Context) (*kafka.ClusterInfo, error) {
	return f.cluster, f.clusterErr
}

func (f *fakeClient) ListConsumerGroups(context.Context) ([]kafka.GroupInfo, error) {
	return f.groups, f.groupsErr
}

func (f *fakeClient) GroupLag(context.Context) (map[string]map[string]int64, error) {
	return f.lag, f.lagErr
}

var _ Client = (*fakeClient)(nil)

// fakeConfigsClient extends fakeClient with topic config support.
type fakeConfigsClient struct {
	fakeClient
	configs map[string]string
	cfgErr  error
}

func (f *fakeConfigsClient) DescribeConfigs(context.Context, string) (map[string]string, error) {
	return f.configs, f.cfgErr
}

func TestConnectivityOnly(t *testing.T) {
	env := Env{Client: &fakeClient{}, Flags: Flags{}}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 1)
	assert.Equal(t, "connectivity", results[0].Name)
	assert.Equal(t, StatusPass, results[0].Status)
	assert.Equal(t, 0, Verdict(results))
}

func TestConnectivityFailSkipsRemaining(t *testing.T) {
	env := Env{
		Client: &fakeClient{pingErr: errors.New("dial tcp 127.0.0.1:9093: connection refused")},
		Flags:  Flags{Topics: []string{"orders"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[0].Status, "connectivity must fail")
	assert.Equal(t, StatusSkip, results[1].Status, "checks after connectivity failure are skipped")
	assert.Equal(t, 2, Verdict(results))
}

func TestTopicCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 6},
		Flags:  Flags{Topics: []string{"orders"}, MinPartitions: 6},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusPass, results[0].Status)
	assert.Equal(t, "topic orders", results[1].Name)
	assert.Equal(t, StatusPass, results[1].Status)
	assert.Equal(t, float64(6), results[1].Value)
	assert.Contains(t, results[1].Message, "6 partitions")
	assert.Equal(t, 0, Verdict(results))
}

func TestTopicCheckBelowMinPartitions(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 2},
		Flags:  Flags{Topics: []string{"orders"}, MinPartitions: 6},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "min 6")
	assert.Equal(t, 1, Verdict(results))
}

func TestTopicCheckErroredPartitions(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 6, errored: 2},
		Flags:  Flags{Topics: []string{"orders"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "errored")
	assert.Equal(t, 1, Verdict(results))
}

func TestTopicCheckMissingTopic(t *testing.T) {
	env := Env{
		Client: &fakeClient{partErr: errors.New("unknown topic or partition")},
		Flags:  Flags{Topics: []string{"nope"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "unknown topic or partition")
}

func TestGroupCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeClient{
			groups: []kafka.GroupInfo{{Name: "orders-processor", State: "Stable", Members: 1}},
			lag:    map[string]map[string]int64{"orders-processor": {"orders": 50}},
		},
		Flags: Flags{Groups: []string{"orders-processor"}, MaxLag: 1000},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, "group orders-processor", results[1].Name)
	assert.Equal(t, StatusPass, results[1].Status)
	assert.Equal(t, float64(50), results[1].Value)
	assert.Contains(t, results[1].Message, "lag 50")
	assert.Equal(t, 0, Verdict(results))
}

func TestGroupCheckDeadState(t *testing.T) {
	env := Env{
		Client: &fakeClient{groups: []kafka.GroupInfo{{Name: "g1", State: "Dead", Members: 0}}},
		Flags:  Flags{Groups: []string{"g1"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "Dead")
	assert.Equal(t, 1, Verdict(results))
}

func TestGroupCheckLagTooHigh(t *testing.T) {
	env := Env{
		Client: &fakeClient{
			groups: []kafka.GroupInfo{{Name: "g1", State: "Stable", Members: 2}},
			lag:    map[string]map[string]int64{"g1": {"orders": 2400}},
		},
		Flags: Flags{Groups: []string{"g1"}, MaxLag: 1000},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "lag 2400, max 1000")
	assert.Equal(t, 1, Verdict(results))
}

func TestGroupCheckNoMembers(t *testing.T) {
	env := Env{
		Client: &fakeClient{groups: []kafka.GroupInfo{{Name: "g1", State: "Stable", Members: 0}}},
		Flags:  Flags{Groups: []string{"g1"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "no members")
}

func TestGroupCheckMissingGroup(t *testing.T) {
	env := Env{
		Client: &fakeClient{},
		Flags:  Flags{Groups: []string{"ghost"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "not found")
}

func TestGroupCheckLagErrorIsFail(t *testing.T) {
	env := Env{
		Client: &fakeClient{
			groups: []kafka.GroupInfo{{Name: "g1", State: "Stable", Members: 1}},
			lagErr: errors.New("offset fetch failed"),
		},
		Flags: Flags{Groups: []string{"g1"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status, "a lag computation error must fail, not skip")
	assert.Contains(t, results[1].Message, "group lag")
}

func TestGroupCheckListErrorIsFail(t *testing.T) {
	env := Env{
		Client: &fakeClient{groupsErr: errors.New("list groups failed")},
		Flags:  Flags{Groups: []string{"g1"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status, "a group listing error must fail, not skip")
	assert.Contains(t, results[1].Message, "list consumer groups")
}

func TestRetentionCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeConfigsClient{
			fakeClient: fakeClient{partitions: 6},
			configs:    map[string]string{"retention.ms": "3600000000"}, // 1000h
		},
		Flags: Flags{Topics: []string{"orders"}, MinRetentionHours: 24},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 3)
	assert.Equal(t, "retention orders", results[2].Name)
	assert.Equal(t, StatusPass, results[2].Status)
	assert.Equal(t, 0, Verdict(results))
}

func TestRetentionCheckBelowMin(t *testing.T) {
	env := Env{
		Client: &fakeConfigsClient{
			fakeClient: fakeClient{partitions: 6},
			configs:    map[string]string{"retention.ms": "43200000"}, // 12h
		},
		Flags: Flags{Topics: []string{"orders"}, MinRetentionHours: 24},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 3)
	assert.Equal(t, StatusFail, results[2].Status)
	assert.Contains(t, results[2].Message, "min 24")
	assert.Equal(t, 1, Verdict(results))
}

func TestRetentionCheckUnlimited(t *testing.T) {
	env := Env{
		Client: &fakeConfigsClient{
			fakeClient: fakeClient{partitions: 6},
			configs:    map[string]string{"retention.ms": "-1"},
		},
		Flags: Flags{Topics: []string{"orders"}, MinRetentionHours: 24},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 3)
	assert.Equal(t, StatusPass, results[2].Status, "unlimited retention satisfies any minimum")
}

func TestRetentionCheckUnsupportedClient(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 6},
		Flags:  Flags{Topics: []string{"orders"}, MinRetentionHours: 24},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 3)
	assert.Equal(t, StatusFail, results[2].Status, "an unverifiable retention gate must fail, not pass")
	assert.Contains(t, results[2].Message, "DescribeConfigs")
}

func TestRetentionCheckConfigError(t *testing.T) {
	env := Env{
		Client: &fakeConfigsClient{
			fakeClient: fakeClient{partitions: 6},
			cfgErr:     errors.New("describe configs failed"),
		},
		Flags: Flags{Topics: []string{"orders"}, MinRetentionHours: 24},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 3)
	assert.Equal(t, StatusFail, results[2].Status)
	assert.Contains(t, results[2].Message, "describe configs")
}

func TestAllGroupLagCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeClient{
			groups: []kafka.GroupInfo{
				{Name: "g1", State: "Stable", Members: 1},
				{Name: "g2", State: "Stable", Members: 2},
			},
			lag: map[string]map[string]int64{"g1": {"orders": 50}, "g2": {"payments": 900}},
		},
		Flags: Flags{MaxLag: 1000},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, "group lag", results[1].Name)
	assert.Equal(t, StatusPass, results[1].Status)
	assert.Contains(t, results[1].Message, "max lag 900")
	assert.Equal(t, 0, Verdict(results))
}

func TestAllGroupLagCheckFailsOnAnyGroup(t *testing.T) {
	env := Env{
		Client: &fakeClient{
			groups: []kafka.GroupInfo{
				{Name: "g1", State: "Stable", Members: 1},
				{Name: "g2", State: "Stable", Members: 2},
			},
			lag: map[string]map[string]int64{"g1": {"orders": 2400}, "g2": {"payments": 5}},
		},
		Flags: Flags{MaxLag: 1000},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, "group lag", results[1].Name)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "g1 lag 2400, max 1000")
	assert.Equal(t, 1, Verdict(results))
}

func TestAllGroupLagCheckNoGroups(t *testing.T) {
	env := Env{
		Client: &fakeClient{groups: nil, lag: map[string]map[string]int64{}},
		Flags:  Flags{MaxLag: 1000},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusPass, results[1].Status)
}

func TestReplicationCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeClient{cluster: &kafka.ClusterInfo{
			UnderReplicatedPartitions: 0,
			Brokers: []kafka.BrokerInfo{{ID: 0, LeaderPartitions: 6, ReplicaPartitions: 6}},
		}},
		Flags: Flags{CheckReplication: true},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, "replication", results[1].Name)
	assert.Equal(t, StatusPass, results[1].Status)
	assert.Equal(t, 0, Verdict(results))
}

func TestReplicationCheckUnderReplicated(t *testing.T) {
	env := Env{
		Client: &fakeClient{cluster: &kafka.ClusterInfo{
			UnderReplicatedPartitions: 2,
			Brokers: []kafka.BrokerInfo{
				{ID: 0, LeaderPartitions: 6, ReplicaPartitions: 6},
				{ID: 1, LeaderPartitions: 2, ReplicaPartitions: 8},
			},
		}},
		Flags: Flags{CheckReplication: true},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "2 under-replicated partitions")
	assert.Equal(t, 1, Verdict(results))
}

func TestReplicationCheckHealthyMultiBrokerCluster(t *testing.T) {
	// A healthy RF>=2 cluster: brokers host more replicas than they lead, but
	// every partition is fully replicated — the check must pass.
	env := Env{
		Client: &fakeClient{cluster: &kafka.ClusterInfo{
			UnderReplicatedPartitions: 0,
			Brokers: []kafka.BrokerInfo{
				{ID: 0, LeaderPartitions: 4, ReplicaPartitions: 12},
				{ID: 1, LeaderPartitions: 4, ReplicaPartitions: 12},
				{ID: 2, LeaderPartitions: 4, ReplicaPartitions: 12},
			},
		}},
		Flags: Flags{CheckReplication: true},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusPass, results[1].Status, "replica>leader per broker is normal on multi-broker clusters")
	assert.Equal(t, 0, Verdict(results))
}

func TestReplicationCheckClusterError(t *testing.T) {
	env := Env{
		Client: &fakeClient{clusterErr: errors.New("describe cluster failed")},
		Flags:  Flags{CheckReplication: true},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status, "a describe cluster error must fail, not skip")
	assert.Contains(t, results[1].Message, "describe cluster")
}

func TestVerdict(t *testing.T) {
	pass := Result{Name: "connectivity", Status: StatusPass}
	fail := Result{Name: "topic orders", Status: StatusFail}
	skip := Result{Name: "topic orders", Status: StatusSkip}
	connFail := Result{Name: "connectivity", Status: StatusFail}

	cases := []struct {
		name    string
		results []Result
		want    int
	}{
		{"all pass", []Result{pass, pass}, 0},
		{"one fail among passes", []Result{pass, fail}, 1},
		{"connectivity fail", []Result{connFail, skip}, 2},
		{"empty results", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Verdict(tc.results))
		})
	}
}
