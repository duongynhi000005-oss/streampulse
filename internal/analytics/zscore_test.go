// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollingZScore(t *testing.T) {
	// last value far from the previous window mean → large z
	vals := []float64{10, 11, 9, 10.5, 9.5, 10, 40}
	z, mean, err := RollingZScore(vals)
	require.NoError(t, err)
	assert.InDelta(t, 10, mean, 0.01)
	assert.Greater(t, z, 3.0)

	// flat series → z 0, no divide-by-zero
	z, _, err = RollingZScore([]float64{5, 5, 5, 5})
	require.NoError(t, err)
	assert.Zero(t, z)

	// insufficient data
	_, _, err = RollingZScore([]float64{1})
	require.Error(t, err)
}

func TestSeasonalZScore(t *testing.T) {
	// 4 weeks of values at the same hour-of-week slot = 90/110 alternating
	// (mean 100, non-zero std), latest = 300
	vals := make([]float64, 0, 29)
	for w := 0; w < 4; w++ {
		for i := 0; i < 7; i++ {
			if (w+i)%2 == 0 {
				vals = append(vals, 90)
			} else {
				vals = append(vals, 110)
			}
		}
	}
	vals = append(vals, 300) // current point last → 29 total, 28 history
	z, err := SeasonalZScore(vals)
	require.NoError(t, err)
	assert.Greater(t, z, 3.0)

	// insufficient baseline (< 3 history samples)
	_, err = SeasonalZScore([]float64{100, 100, 100})
	require.Error(t, err)
}
