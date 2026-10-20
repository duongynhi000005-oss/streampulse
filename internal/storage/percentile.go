// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package storage

import (
	"fmt"
	"math"
	"sort"
)

// percentiles computes the requested quantiles of values using the
// nearest-rank method: for quantile q the result is the element at
// index ceil(q*n)-1 of the sorted values. It errors on empty input,
// non-finite values, or quantiles outside [0, 1].
func percentiles(values []float64, qs []float64) (map[float64]float64, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("percentiles: empty values")
	}

	vs := make([]float64, len(values))
	copy(vs, values)
	for _, v := range vs {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("percentiles: non-finite value %v", v)
		}
	}
	sort.Float64s(vs)

	out := make(map[float64]float64, len(qs))
	for _, q := range qs {
		if math.IsNaN(q) || math.IsInf(q, 0) || q < 0 || q > 1 {
			return nil, fmt.Errorf("percentiles: invalid quantile %v", q)
		}
		idx := int(math.Ceil(q*float64(len(vs)))) - 1
		if idx < 0 {
			idx = 0
		}
		out[q] = vs[idx]
	}
	return out, nil
}
