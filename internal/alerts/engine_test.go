// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package alerts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testTime() time.Time {
	return time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
}

func lagMetrics(v float64) []storage.Metric {
	return []storage.Metric{{
		EntityType: "consumer_group", EntityName: "orders-processor",
		Metric: "kafka.group.lag", Value: v,
	}}
}

func brokerMetrics() []storage.Metric {
	return []storage.Metric{{
		EntityType: "broker", EntityName: "b1",
		Metric: "kafka.broker.leader_partitions", Value: 10,
	}}
}

func consumerLagRule() Rule {
	return Rule{
		Name: "consumer-lag", Severity: "warning",
		Condition: mustCondition("lag > 1000"), EntityType: "consumer_group",
		For: 2 * time.Minute, RepeatInterval: time.Hour,
	}
}

func brokerDownRule() Rule {
	return Rule{
		Name: "broker-down", Severity: "critical",
		Condition: mustCondition("broker.up == 0"), EntityType: "broker",
		For: 2 * time.Minute, RepeatInterval: time.Hour,
	}
}

func underReplicatedRule() Rule {
	return Rule{
		Name: "under-replicated", Severity: "critical",
		Condition: mustCondition("under_replicated > 0"), EntityType: "cluster",
		For: 2 * time.Minute, RepeatInterval: time.Hour,
	}
}

func scrapeFailingRule() Rule {
	return Rule{
		Name: "scrape-failing", Severity: "critical",
		Condition: mustCondition("scrape_errors_total > 0"), EntityType: "cluster",
		For: 0, RepeatInterval: time.Hour,
	}
}

func TestBuiltinRules(t *testing.T) {
	rules := BuiltinRules()
	require.Len(t, rules, 6, "six built-in rules per alerts.md")

	seen := map[string]bool{}
	for _, r := range rules {
		assert.False(t, seen[r.Name], "duplicate rule %q", r.Name)
		seen[r.Name] = true
		assert.NotEmpty(t, r.Severity)
		assert.NotEmpty(t, r.EntityType)
		assert.True(t, r.RepeatInterval > 0)
		require.NotNil(t, r.Condition, "rule %q must have a parsed condition", r.Name)
	}
}

func TestEngineLagPendingThenFiring(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{consumerLagRule()}, store)
	ctx := context.Background()
	now := testTime()

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(1500), now))
	s, ok := eng.State("consumer-lag")
	require.True(t, ok)
	assert.Equal(t, "pending", s.Status)
	assert.Equal(t, 1500.0, s.LastValue)

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(1500), now.Add(2*time.Minute)))
	s, _ = eng.State("consumer-lag")
	assert.Equal(t, "firing", s.Status)
	assert.Equal(t, 1, s.NotifyCount)

	// Below threshold → resolved.
	require.NoError(t, eng.Evaluate(ctx, lagMetrics(500), now.Add(3*time.Minute)))
	s, _ = eng.State("consumer-lag")
	assert.Equal(t, "ok", s.Status)
}

func TestEngineScrapeFailingAfterThreeBrokerlessCycles(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{scrapeFailingRule()}, store)
	ctx := context.Background()
	now := testTime()
	// Non-broker metrics: the batch looks healthy but the broker collector
	// produced nothing.
	noBrokers := []storage.Metric{{EntityType: "topic", EntityName: "orders", Metric: "kafka.topic.partition_count", Value: 3}}

	for i := 0; i < 2; i++ {
		require.NoError(t, eng.Evaluate(ctx, noBrokers, now.Add(time.Duration(i)*10*time.Second)))
	}
	s, _ := eng.State("scrape-failing")
	assert.Equal(t, "ok", s.Status, "two brokerless cycles is not yet a failure")

	require.NoError(t, eng.Evaluate(ctx, noBrokers, now.Add(30*time.Second)))
	s, _ = eng.State("scrape-failing")
	assert.Equal(t, "firing", s.Status, "three consecutive brokerless cycles fire scrape-failing")

	// Broker metrics return → resolved.
	require.NoError(t, eng.Evaluate(ctx, brokerMetrics(), now.Add(40*time.Second)))
	s, _ = eng.State("scrape-failing")
	assert.Equal(t, "ok", s.Status)
}

func TestEngineBrokerDown(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{brokerDownRule()}, store)
	ctx := context.Background()
	now := testTime()
	noBrokers := lagMetrics(10)

	require.NoError(t, eng.Evaluate(ctx, noBrokers, now))
	s, _ := eng.State("broker-down")
	assert.Equal(t, "pending", s.Status)

	require.NoError(t, eng.Evaluate(ctx, noBrokers, now.Add(2*time.Minute)))
	s, _ = eng.State("broker-down")
	assert.Equal(t, "firing", s.Status)

	require.NoError(t, eng.Evaluate(ctx, brokerMetrics(), now.Add(3*time.Minute)))
	s, _ = eng.State("broker-down")
	assert.Equal(t, "ok", s.Status)
}

func TestEngineUnderReplicated(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{underReplicatedRule()}, store)
	ctx := context.Background()
	now := testTime()

	// A healthy RF>=2 cluster: every broker hosts more replicas than it leads
	// (the old heuristic) but the cluster reports no under-replicated
	// partitions — the rule must not fire.
	healthy := []storage.Metric{
		{EntityType: "broker", EntityName: "b1", Metric: "kafka.broker.replica_partitions", Value: 5},
		{EntityType: "broker", EntityName: "b1", Metric: "kafka.broker.leader_partitions", Value: 3},
		{EntityType: "broker", EntityName: "b2", Metric: "kafka.broker.replica_partitions", Value: 5},
		{EntityType: "broker", EntityName: "b2", Metric: "kafka.broker.leader_partitions", Value: 3},
		{EntityType: "cluster", EntityName: "cluster", Metric: "kafka.cluster.under_replicated_partitions", Value: 0},
	}
	require.NoError(t, eng.Evaluate(ctx, healthy, now))
	s, _ := eng.State("under-replicated")
	assert.Equal(t, "ok", s.Status, "replica>leader per broker is normal on multi-broker clusters")

	under := []storage.Metric{
		{EntityType: "cluster", EntityName: "cluster", Metric: "kafka.cluster.under_replicated_partitions", Value: 2},
	}
	require.NoError(t, eng.Evaluate(ctx, under, now))
	s, _ = eng.State("under-replicated")
	assert.Equal(t, "pending", s.Status, "2 under-replicated partitions")

	require.NoError(t, eng.Evaluate(ctx, under, now.Add(2*time.Minute)))
	s, _ = eng.State("under-replicated")
	assert.Equal(t, "firing", s.Status)

	require.NoError(t, eng.Evaluate(ctx, healthy, now.Add(3*time.Minute)))
	s, _ = eng.State("under-replicated")
	assert.Equal(t, "ok", s.Status)
}

func TestEnginePartitionSkew(t *testing.T) {
	rule := Rule{
		Name: "partition-skew", Severity: "warning",
		Condition: mustCondition("skew > 1.5"), EntityType: "cluster",
		For: 2 * time.Minute, RepeatInterval: time.Hour,
	}
	store := newMemStore(t)
	eng := New([]Rule{rule}, store)
	ctx := context.Background()
	now := testTime()

	skewed := []storage.Metric{
		{EntityType: "cluster", EntityName: "cluster", Metric: "kafka.cluster.partition_skew", Value: 1.667},
	}
	require.NoError(t, eng.Evaluate(ctx, skewed, now))
	s, _ := eng.State("partition-skew")
	assert.Equal(t, "pending", s.Status)

	require.NoError(t, eng.Evaluate(ctx, skewed, now.Add(2*time.Minute)))
	s, _ = eng.State("partition-skew")
	assert.Equal(t, "firing", s.Status)

	balanced := []storage.Metric{
		{EntityType: "cluster", EntityName: "cluster", Metric: "kafka.cluster.partition_skew", Value: 1.0},
	}
	require.NoError(t, eng.Evaluate(ctx, balanced, now.Add(3*time.Minute)))
	s, _ = eng.State("partition-skew")
	assert.Equal(t, "ok", s.Status)
}

func TestMergeRulesUnknownName(t *testing.T) {
	_, err := MergeRules(BuiltinRules(), []config.AlertRule{{Name: "nope"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")
}

func TestMergeRulesOverridesByName(t *testing.T) {
	rules, err := MergeRules(BuiltinRules(), []config.AlertRule{
		{Name: "consumer-lag", Condition: "lag > 500", Severity: "critical", For: "30s"},
	})
	require.NoError(t, err)

	var got *Rule
	for i := range rules {
		if rules[i].Name == "consumer-lag" {
			got = &rules[i]
		}
	}
	require.NotNil(t, got)
	require.NotNil(t, got.Condition)
	assert.Equal(t, 500.0, got.Condition.Threshold, "override changes the threshold")
	assert.Equal(t, "critical", got.Severity)
	assert.Equal(t, 30*time.Second, got.For)

	// Unmentioned rules keep their builtin values.
	var lag *Rule
	for i := range rules {
		if rules[i].Name == "dlq-growth" {
			lag = &rules[i]
		}
	}
	require.NotNil(t, lag)
	require.NotNil(t, lag.Condition)
	assert.Equal(t, 10.0, lag.Condition.Threshold)
	assert.Equal(t, "warning", lag.Severity)
}

func TestMergeRulesBadOverride(t *testing.T) {
	_, err := MergeRules(BuiltinRules(), []config.AlertRule{{Name: "consumer-lag", Condition: "lag >!"}})
	require.Error(t, err)
	_, err = MergeRules(BuiltinRules(), []config.AlertRule{{Name: "consumer-lag", For: "banana"}})
	require.Error(t, err)
}

func TestEngineRehydratesPersistedState(t *testing.T) {
	store := newMemStore(t)
	rules := []Rule{consumerLagRule()}
	ctx := context.Background()
	now := testTime()

	e1 := New(rules, store)
	require.NoError(t, e1.Evaluate(ctx, lagMetrics(1500), now))
	require.NoError(t, e1.Evaluate(ctx, lagMetrics(1500), now.Add(2*time.Minute)))

	// A second engine on the same store rehydrates the firing state.
	e2 := New(rules, store)
	s, ok := e2.State("consumer-lag")
	require.True(t, ok)
	assert.Equal(t, "firing", s.Status)
	assert.Equal(t, 1, s.NotifyCount)

	require.NoError(t, e2.Evaluate(ctx, lagMetrics(500), now.Add(3*time.Minute)))
	s, _ = e2.State("consumer-lag")
	assert.Equal(t, "ok", s.Status)
}

type recordingNotifier struct {
	got []Notification
}

func (r *recordingNotifier) Notify(ctx context.Context, n Notification) error {
	r.got = append(r.got, n)
	return nil
}

func TestEvaluateNotifiesOnFireAndResolve(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{consumerLagRule()}, store)
	ctx := context.Background()
	now := testTime()

	rn := &recordingNotifier{}
	eng.SetNotifiers([]Notifier{rn})

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(1500), now))
	assert.Empty(t, rn.got, "pending state must not notify")

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(1500), now.Add(2*time.Minute)))
	require.Len(t, rn.got, 1)
	assert.Equal(t, "firing", rn.got[0].Status)
	assert.Equal(t, "consumer-lag", rn.got[0].Rule)
	assert.Equal(t, "warning", rn.got[0].Severity)
	assert.Equal(t, 1500.0, rn.got[0].Value)
	assert.NotEmpty(t, rn.got[0].Message)
	assert.Equal(t, now.Add(2*time.Minute), rn.got[0].Timestamp)

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(500), now.Add(3*time.Minute)))
	require.Len(t, rn.got, 2)
	assert.Equal(t, "resolved", rn.got[1].Status)
}

type failNotifier struct {
	err   error
	calls int
}

func (f *failNotifier) Notify(ctx context.Context, n Notification) error {
	f.calls++
	return f.err
}

func TestEvaluateNotifierErrorDoesNotFail(t *testing.T) {
	store := newMemStore(t)
	eng := New([]Rule{consumerLagRule()}, store)
	ctx := context.Background()
	now := testTime()

	fn := &failNotifier{err: errors.New("boom")}
	eng.SetNotifiers([]Notifier{fn})

	require.NoError(t, eng.Evaluate(ctx, lagMetrics(1500), now))
	err := eng.Evaluate(ctx, lagMetrics(1500), now.Add(2*time.Minute))
	require.NoError(t, err, "a failing notifier must not fail Evaluate")
	assert.Equal(t, 1, fn.calls)
	assert.Equal(t, "firing", eng.mustState(t, "consumer-lag").Status, "state still advances")
}

func (e *Engine) mustState(t *testing.T, name string) State {
	t.Helper()
	s, ok := e.State(name)
	require.True(t, ok, "rule %q not found", name)
	return s
}
