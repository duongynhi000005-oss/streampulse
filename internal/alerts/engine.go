// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/storage"
)

// Rule is a compiled alert rule: a condition evaluated against scraped
// metrics, with a pending window (For) before firing and a re-notification
// interval (RepeatInterval) while firing.
type Rule struct {
	Name           string
	Group          string
	Severity       string
	Condition      *Condition
	For            time.Duration
	RepeatInterval time.Duration
	EntityType     string
}

// BuiltinRules returns the six default v0.1 rules (alerts.md rule table).
// Rules are overridable by config entries matched on Name (see MergeRules).
func BuiltinRules() []Rule {
	return []Rule{
		{
			Name: "broker-down", Severity: "critical",
			Condition: mustCondition("broker.up == 0"), EntityType: "broker",
			For: 2 * time.Minute, RepeatInterval: time.Hour,
		},
		{
			Name: "under-replicated", Severity: "critical",
			Condition: mustCondition("under_replicated > 0"), EntityType: "cluster",
			For: 2 * time.Minute, RepeatInterval: time.Hour,
		},
		{
			Name: "consumer-lag", Severity: "warning",
			Condition: mustCondition("lag > 1000"), EntityType: "consumer_group",
			For: 2 * time.Minute, RepeatInterval: time.Hour,
		},
		{
			Name: "dlq-growth", Severity: "warning",
			Condition: mustCondition("growth_rate > 10"), EntityType: "topic",
			For: 2 * time.Minute, RepeatInterval: time.Hour,
		},
		{
			Name: "partition-skew", Severity: "warning",
			Condition: mustCondition("skew > 1.5"), EntityType: "cluster",
			For: 2 * time.Minute, RepeatInterval: time.Hour,
		},
		{
			// Condition: scrape_errors_total > 0. The counter is not part
			// of the metric batch; the engine evaluates it as "the scrape
			// produced no broker metrics" and the 3-cycle requirement is
			// encoded in the engine, so For is 0 (fire as soon as met).
			Name: "scrape-failing", Severity: "critical",
			Condition: mustCondition("scrape_errors_total > 0"), EntityType: "cluster",
			For: 0, RepeatInterval: time.Hour,
		},
	}
}

// MergeRules applies config overrides to a builtin rule set. Overrides are
// matched by Name; an unknown name is an error. Empty override fields keep
// the builtin values.
func MergeRules(builtins []Rule, overrides []config.AlertRule) ([]Rule, error) {
	rules := make([]Rule, len(builtins))
	copy(rules, builtins)

	byName := make(map[string]int, len(rules))
	for i := range rules {
		byName[rules[i].Name] = i
	}

	for _, o := range overrides {
		i, ok := byName[o.Name]
		if !ok {
			return nil, fmt.Errorf("unknown alert rule %q", o.Name)
		}
		r := &rules[i]
		if o.Group != "" {
			r.Group = o.Group
		}
		if o.Severity != "" {
			r.Severity = o.Severity
		}
		if o.Condition != "" {
			c, err := ParseCondition(o.Condition)
			if err != nil {
				return nil, fmt.Errorf("alert rule %q: %w", o.Name, err)
			}
			r.Condition = c
		}
		if o.For != "" {
			d, err := time.ParseDuration(o.For)
			if err != nil {
				return nil, fmt.Errorf("alert rule %q: for %q: %w", o.Name, o.For, err)
			}
			r.For = d
		}
	}
	return rules, nil
}

// Notification is delivered to Notifiers on firing and resolution events.
type Notification struct {
	Rule      string
	Severity  string
	Status    string // "firing" | "resolved"
	Value     float64
	Entity    string
	Message   string
	Timestamp time.Time
}

// Notifier delivers an alert notification. Implementations live in
// internal/alerts/notify (slack, email, pagerduty). A failing notifier never
// fails an Evaluate cycle: the error is logged and the next notification
// happens on the repeat interval.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

type engineRule struct {
	Rule
	state State
}

// Engine evaluates rules against scraped metrics every cycle. Rule state is
// persisted through the store (alert_state) and rehydrated at construction,
// so notifications stay idempotent across daemon restarts.
type Engine struct {
	rules          []engineRule
	store          storage.MetricsStore
	notifiers      []Notifier
	noBrokerCycles int
}

// New compiles the rules and rehydrates their persisted state from the
// store (nil store runs in-memory only). Duplicate rule names panic as a
// programming error.
func New(rules []Rule, store storage.MetricsStore) *Engine {
	e := &Engine{store: store}
	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if seen[r.Name] {
			panic(fmt.Sprintf("duplicate alert rule %q", r.Name))
		}
		seen[r.Name] = true
		e.rules = append(e.rules, engineRule{Rule: r, state: State{RuleName: r.Name}})
	}
	e.rehydrate()
	return e
}

// rehydrate loads persisted alert states so a restarted engine continues
// from where the previous one stopped. A failing store keeps the engine
// in-memory (the daemon logs and retries).
func (e *Engine) rehydrate() {
	if e.store == nil {
		return
	}
	rows, err := e.store.QueryAlertState(context.Background())
	if err != nil {
		slog.Warn("alert engine: rehydrate state", "error", err)
		return
	}
	byName := make(map[string]State, len(rows))
	for _, r := range rows {
		byName[r.RuleName] = State{
			RuleName:    r.RuleName,
			Status:      r.Status,
			LastFired:   r.LastFired,
			LastValue:   r.LastValue,
			NotifyCount: r.NotifyCount,
		}
	}
	for i := range e.rules {
		if s, ok := byName[e.rules[i].Name]; ok {
			e.rules[i].state = s
		}
	}
}

// Evaluate runs one evaluation cycle over the latest scrape batch, advancing
// each rule's state machine and dispatching notifications. now is the daemon
// wall clock. Notification and persistence errors are logged and never fail
// the cycle; the state machine always advances in memory.
func (e *Engine) Evaluate(ctx context.Context, metrics []storage.Metric, now time.Time) error {
	hasBroker := false
	for _, m := range metrics {
		if m.EntityType == "broker" {
			hasBroker = true
			break
		}
	}
	if hasBroker {
		e.noBrokerCycles = 0
	} else {
		e.noBrokerCycles++
	}

	for i := range e.rules {
		rule := &e.rules[i]
		condTrue, value, entity := rule.evalMetrics(metrics, e.noBrokerCycles)

		event, err := rule.state.Transition(ctx, e.store, condTrue, value, now, rule.For, rule.RepeatInterval)
		if err != nil {
			slog.Error("alert engine: transition failed", "rule", rule.Name, "error", err)
			continue
		}
		switch event {
		case EventFired, EventRepeated:
			e.notify(ctx, &rule.Rule, "firing", value, entity, now)
		case EventResolved:
			e.notify(ctx, &rule.Rule, "resolved", value, entity, now)
		}
	}
	return nil
}

// evalMetrics decides whether the rule's condition held for this cycle over
// the metrics batch, returning the triggering value and entity name.
func (r *engineRule) evalMetrics(metrics []storage.Metric, noBrokerCycles int) (bool, float64, string) {
	switch r.Name {
	case "broker-down":
		// Fires when the broker collector produced no metrics this cycle.
		return noBrokerCycles > 0, 0, ""
	case "scrape-failing":
		// Fires once the scrape has failed for 3 consecutive cycles.
		return noBrokerCycles >= 3, float64(noBrokerCycles), ""
	default:
		if r.Condition == nil {
			return false, 0, ""
		}
		found := false
		var maxVal float64
		var entity string
		for _, m := range metrics {
			if m.Metric != r.Condition.Metric {
				continue
			}
			if r.Condition.Evaluate(m.Metric, m.Value) {
				found = true
				if m.Value > maxVal {
					maxVal = m.Value
					entity = m.EntityName
				}
			}
		}
		return found, maxVal, entity
	}
}

// notify dispatches a notification to all notifiers; errors are logged.
func (e *Engine) notify(ctx context.Context, r *Rule, status string, value float64, entity string, now time.Time) {
	n := Notification{
		Rule:      r.Name,
		Severity:  r.Severity,
		Status:    status,
		Value:     value,
		Entity:    entity,
		Message:   r.message(status, value, entity),
		Timestamp: now,
	}
	for _, not := range e.notifiers {
		if err := not.Notify(ctx, n); err != nil {
			slog.Error("alert engine: notify failed", "rule", r.Name, "status", status, "error", err)
		}
	}
}

// message builds the human-readable notification text.
func (r *Rule) message(status string, value float64, entity string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", r.Name, status)
	if entity != "" {
		fmt.Fprintf(&b, " [%s]", entity)
	}
	if r.Condition != nil {
		fmt.Fprintf(&b, ": value %.2f (threshold %s %.2f)", value, r.Condition.Op, r.Condition.Threshold)
	} else {
		fmt.Fprintf(&b, ": value %.2f", value)
	}
	return b.String()
}

// State returns the current state for the named rule.
func (e *Engine) State(ruleName string) (State, bool) {
	for i := range e.rules {
		if e.rules[i].Name == ruleName {
			return e.rules[i].state, true
		}
	}
	return State{}, false
}

// Rules returns the compiled rules in evaluation order.
func (e *Engine) Rules() []Rule {
	out := make([]Rule, len(e.rules))
	for i := range e.rules {
		out[i] = e.rules[i].Rule
	}
	return out
}

// SetNotifiers installs the notification targets used on firing and
// resolution events.
func (e *Engine) SetNotifiers(ns []Notifier) {
	e.notifiers = ns
}
