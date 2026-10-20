// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"fmt"
	"math"
	"time"
)

// meanStd returns the mean and sample standard deviation of values.
func meanStd(values []float64) (mean, std float64) {
	if len(values) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))
	if len(values) < 2 {
		return mean, 0
	}
	var ss float64
	for _, v := range values {
		d := v - mean
		ss += d * d
	}
	return mean, math.Sqrt(ss / float64(len(values)-1))
}

// RollingZScore returns the z-score of the last value against the mean/std of
// the preceding values, plus that mean. Errors on fewer than 2 preceding
// samples. A zero standard deviation yields z = 0 (no anomaly signal).
func RollingZScore(values []float64) (z, mean float64, err error) {
	if len(values) < 2 {
		return 0, 0, fmt.Errorf("rolling z-score needs at least 2 samples, got %d", len(values))
	}
	v := values[len(values)-1]
	mean, std := meanStd(values[:len(values)-1])
	if std == 0 {
		return 0, mean, nil
	}
	return (v - mean) / std, mean, nil
}

// SeasonalZScore scores the last value of slotValues against its seasonal
// baseline: the preceding values of the same bucket. slotValues must contain
// the current point LAST with at least 3 history samples (4 total). A zero
// baseline standard deviation yields z = 0 (no anomaly signal).
func SeasonalZScore(slotValues []float64) (z float64, err error) {
	if len(slotValues) < 4 {
		return 0, fmt.Errorf("seasonal baseline needs at least 3 history samples, got %d", len(slotValues)-1)
	}
	history := slotValues[:len(slotValues)-1]
	mean, std := meanStd(history)
	if std == 0 {
		return 0, nil
	}
	return (slotValues[len(slotValues)-1] - mean) / std, nil
}

// ProfileIndex maps a timestamp to a slot in a cyclic profile of cycleLen
// slots. Cycle 168 = hour-of-week; cycle 24 = hour-of-day.
func ProfileIndex(t time.Time, cycleLen int) int {
	return (int(t.Weekday())*24 + t.Hour()) % cycleLen
}
