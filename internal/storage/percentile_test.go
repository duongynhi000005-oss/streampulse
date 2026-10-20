// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package storage

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nearestRank is the reference nearest-rank implementation used to cross-check
// percentiles against randomized input.
func nearestRank(values []float64, qs []float64) map[float64]float64 {
	vs := append([]float64(nil), values...)
	sort.Float64s(vs)
	out := make(map[float64]float64, len(qs))
	for _, q := range qs {
		idx := int(math.Ceil(q*float64(len(vs)))) - 1
		if idx < 0 {
			idx = 0
		}
		out[q] = vs[idx]
	}
	return out
}

func TestPercentilesGolden(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		want   map[float64]float64
	}{
		{"odd n", []float64{1, 2, 3, 4, 5}, map[float64]float64{0.5: 3, 0.95: 5, 0.99: 5}},
		{"even n", []float64{10, 20, 30, 40, 50, 60}, map[float64]float64{0.5: 30, 0.95: 60, 0.99: 60}},
		{"all equal", []float64{7, 7, 7, 7}, map[float64]float64{0.5: 7, 0.95: 7, 0.99: 7}},
		{"single element", []float64{42}, map[float64]float64{0.5: 42, 0.95: 42, 0.99: 42}},
		{"unsorted input", []float64{5, 1, 3, 2, 4}, map[float64]float64{0.5: 3, 0.95: 5, 0.99: 5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := percentiles(tc.values, []float64{0.5, 0.95, 0.99})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPercentilesRandomVsReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for n := 1; n <= 1000; n += 7 {
		values := make([]float64, n)
		for i := range values {
			values[i] = rng.Float64() * 10000
		}
		qs := []float64{0.01, 0.05, 0.5, 0.95, 0.99}
		got, err := percentiles(values, qs)
		require.NoError(t, err)
		assert.Equal(t, nearestRank(values, qs), got, "n=%d", n)
	}
}

func TestPercentilesRejectsBadInput(t *testing.T) {
	_, err := percentiles(nil, []float64{0.5})
	require.Error(t, err, "empty values must error")

	_, err = percentiles([]float64{1, math.NaN(), 3}, []float64{0.5})
	require.Error(t, err, "NaN value must error")

	_, err = percentiles([]float64{1, math.Inf(1)}, []float64{0.5})
	require.Error(t, err, "Inf value must error")

	_, err = percentiles([]float64{1, 2, 3}, []float64{0.5, math.NaN()})
	require.Error(t, err, "NaN quantile must error")

	_, err = percentiles([]float64{1, 2, 3}, []float64{1.5})
	require.Error(t, err, "quantile out of range must error")
}
