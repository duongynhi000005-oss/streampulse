// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparkline(t *testing.T) {
	cases := []struct {
		name   string
		points []float64
		want   string
	}{
		{name: "empty input", points: nil, want: ""},
		{name: "single point", points: []float64{42}, want: "▁"},
		{name: "flat", points: []float64{5, 5, 5}, want: "▁▁▁"},
		{name: "monotonic", points: []float64{0, 1, 2, 3, 4, 5, 6}, want: "▁▂▃▅▆▇█"},
		{name: "spike", points: []float64{1, 10, 1}, want: "▁█▁"},
		{name: "negative values", points: []float64{-10, 0, 10}, want: "▁▅█"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Sparkline(tc.points))
		})
	}
}

func TestBars(t *testing.T) {
	t.Run("labels and proportional widths", func(t *testing.T) {
		got := Bars([]string{"1", "2", "3"}, []float64{3, 1, 2}, 30)
		want := "1 | ██████████████████████████\n2 | █████████\n3 | █████████████████"
		require.Equal(t, want, got)
	})

	t.Run("empty input", func(t *testing.T) {
		assert.Equal(t, "", Bars(nil, nil, 30))
	})

	t.Run("zero values render empty bars", func(t *testing.T) {
		got := Bars([]string{"a", "b"}, []float64{0, 0}, 20)
		assert.Equal(t, "a | \nb | ", got)
	})

	t.Run("width below minimum does not panic", func(t *testing.T) {
		got := Bars([]string{"a", "b"}, []float64{1, 2}, 4)
		assert.NotPanics(t, func() { Bars([]string{"a", "b"}, []float64{1, 2}, 4) })
		lines := strings.Split(got, "\n")
		require.Len(t, lines, 2)
		assert.Equal(t, "a | ███", lines[0])
		assert.Equal(t, "b | ██████", lines[1])
	})

	t.Run("short value list treated as zero", func(t *testing.T) {
		got := Bars([]string{"a", "b"}, []float64{2}, 20)
		assert.Equal(t, "a | ████████████████\nb | ", got)
	})
}
