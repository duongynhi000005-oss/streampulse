// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

// sparkGlyphs are the eight sparkline buckets, from lowest to highest.
const sparkGlyphs = "▁▂▃▄▅▆▇█"

// Sparkline renders a single-line ASCII sparkline of the given values using
// eight glyph buckets (▁▂▃▄▅▆▇█) normalized to the minimum and maximum of the
// input. Flat or empty inputs render the lowest glyph (or nothing).
func Sparkline(points []float64) string {
	if len(points) == 0 {
		return ""
	}
	min, max := points[0], points[0]
	for _, p := range points {
		if p < min {
			min = p
		}
		if p > max {
			max = p
		}
	}

	span := max - min
	glyphSet := []rune(sparkGlyphs)
	glyphs := make([]rune, len(points))
	for i, p := range points {
		idx := 0
		if span > 0 {
			idx = int(math.Round((p - min) / span * float64(len(glyphSet)-1)))
			if idx < 0 {
				idx = 0
			}
			if idx >= len(glyphSet) {
				idx = len(glyphSet) - 1
			}
		}
		glyphs[i] = glyphSet[idx]
	}
	return string(glyphs)
}

// minBarWidth is the narrowest renderable barchart; narrower widths are
// clamped so the renderer never panics.
const minBarWidth = 10

// maxLabelWidth caps the label column so a single long label cannot consume
// the whole chart width.
const maxLabelWidth = 20

// Bars renders a horizontal barchart, one line per label, as
// "<label> | ███…" where bar length is proportional to the value relative to
// the maximum. width bounds the total line width; values missing from a
// shorter values slice count as zero.
func Bars(labels []string, values []float64, width int) string {
	if len(labels) == 0 {
		return ""
	}
	if width < minBarWidth {
		width = minBarWidth
	}

	labelW := 0
	for _, l := range labels {
		if n := utf8.RuneCountInString(l); n > labelW {
			labelW = n
		}
	}
	if labelW > maxLabelWidth {
		labelW = maxLabelWidth
	}

	barArea := width - labelW - 3 // label column, " | "
	if barArea < 1 {
		barArea = 1
	}

	max := 0.0
	for _, v := range values {
		if v > max {
			max = v
		}
	}

	var b strings.Builder
	for i, label := range labels {
		v := 0.0
		if i < len(values) {
			v = values[i]
		}
		n := 0
		if max > 0 {
			n = int(math.Round(v / max * float64(barArea)))
			if n > barArea {
				n = barArea
			}
		}
		fmt.Fprintf(&b, "%-*s | %s", labelW, label, strings.Repeat("█", n))
		if i < len(labels)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}
