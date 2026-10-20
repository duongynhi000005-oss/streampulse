// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package alerts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCondition(t *testing.T) {
	cases := []struct {
		in      string
		metric  string
		op      string
		value   float64
		wantErr bool
	}{
		{"lag > 1000", "kafka.group.lag", ">", 1000, false},
		{"under_replicated > 0", "kafka.cluster.under_replicated_partitions", ">", 0, false},
		{"replica == leader", "replica", "==", 0, true}, // two identifiers → unsupported
		{"lag >", "", "", 0, true},
		{"growth_rate >= 10.5", "dlq.topic.growth_rate", ">=", 10.5, false},
		{"bogus < 1", "", "", 0, true},
	}
	for _, tc := range cases {
		c, err := ParseCondition(tc.in)
		if tc.wantErr {
			require.Error(t, err, tc.in)
			continue
		}
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.metric, c.Metric)
		assert.Equal(t, tc.op, c.Op)
		assert.Equal(t, tc.value, c.Threshold)
	}
}

func TestConditionEval(t *testing.T) {
	c := mustCondition("lag > 1000")
	assert.True(t, c.Evaluate("kafka.group.lag", 1500))
	assert.False(t, c.Evaluate("kafka.group.lag", 500))
	assert.False(t, c.Evaluate("kafka.broker.up", 1)) // wrong metric → false
}
