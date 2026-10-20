// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import "time"

// Point is one daily aggregation point of a growth report.
type Point struct {
	Time time.Time `json:"time"`
	Rate float64   `json:"rate"`
}

// GrowthReport describes the message growth of one topic over a window.
type GrowthReport struct {
	Topic     string        `json:"topic"`
	Window    time.Duration `json:"window"`
	Points    []Point       `json:"points"`
	Delta     float64       `json:"delta"` // msgs/sec, first → last point
	Sparkline string        `json:"sparkline"`
}

// SkewReport describes the cluster-wide partition leadership distribution.
// Leaders maps broker id → partition count; a topic of "" means the report
// covers the whole cluster.
type SkewReport struct {
	Topic    string         `json:"topic"`
	Leaders  map[string]int `json:"leaders"`
	Ratio    float64        `json:"ratio"` // max / avg leaders per broker
	Balanced bool           `json:"balanced"`
}

// Anomaly flags one point that deviates from its baseline.
type Anomaly struct {
	Metric    string    `json:"metric"`
	Entity    string    `json:"entity"`
	Time      time.Time `json:"time"`
	Value     float64   `json:"value"`
	Expected  float64   `json:"expected"`
	ZScore    float64   `json:"z_score"`
	Direction string    `json:"direction"` // high | low
	Severity  string    `json:"severity"`  // warning (single threshold: |z| >= 2)
}

// ThroughputReport describes the time-of-day/day-of-week profile and trend of
// one topic's rate metric over the window.
type ThroughputReport struct {
	Topic         string        `json:"topic"`
	Metric        string        `json:"metric"`
	Window        time.Duration `json:"window"`
	HourlyProfile [24]float64   `json:"hourly_profile"` // mean per hour-of-day
	DailyProfile  [7]float64    `json:"daily_profile"`  // mean per weekday
	PeakHour      int           `json:"peak_hour"`
	PeakDay       int           `json:"peak_day"`
	Slope         float64       `json:"slope"`       // linear fit, per second
	Forecast7d    float64       `json:"forecast_7d"` // projected rate in 7 days
}

// RetentionReport describes the retention posture of one topic.
type RetentionReport struct {
	Topic string `json:"topic"`
	// RetentionMS is the configured retention.ms; 0 means unknown or unset.
	RetentionMS time.Duration `json:"retention_ms"`
	// EstimateFillDays is how many days of data fit within retention.bytes
	// at the current byte rate; 0 when either side is unknown.
	EstimateFillDays float64 `json:"estimate_fill_days"`
	// OldestOffsetAge is the age of the oldest persisted data point.
	OldestOffsetAge time.Duration `json:"oldest_offset_age"`
	// AtRisk reports whether the byte-based fill estimate is shorter than
	// the time-based retention.
	AtRisk bool `json:"at_risk"`
}

// RebalanceReport counts rebalance events (transitions into PreparingRebalance)
// for one consumer group on one UTC day.
type RebalanceReport struct {
	Group string    `json:"group"`
	Day   time.Time `json:"day"` // UTC midnight of the day
	Count int       `json:"count"`
}
