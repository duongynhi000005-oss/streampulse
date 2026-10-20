// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
)

// Alert states. State machine: ok → pending → firing → (resolved) → ok.
const (
	StateOK      = "ok"
	StatePending = "pending"
	StateFiring  = "firing"
)

// Event is the outcome of one state transition. Only transitions that
// require a notification carry a non-none event.
type Event string

const (
	// EventNone is a transition that needs no notification (state changes
	// without a fire, or a firing that stays within its repeat interval).
	EventNone Event = "none"
	// EventFired is the entry into firing (first notification).
	EventFired Event = "fired"
	// EventResolved is the return to ok after having fired.
	EventResolved Event = "resolved"
	// EventRepeated is a re-notification of a still-firing rule whose
	// repeat interval has elapsed since the last notification.
	EventRepeated Event = "repeated"
)

// State is the alert state machine state for one rule. It is persisted to
// the alert_state table (status, last_fired, last_value, notify_count) so a
// daemon restart rehydrates and stays idempotent; PendingSince is in-memory
// only (the persistence schema has no pending column).
type State struct {
	RuleName     string
	Status       string // ok | pending | firing
	LastFired    time.Time
	LastValue    float64
	NotifyCount  int
	PendingSince time.Time // in-memory; when the pending window started
}

// Transition advances the state machine for one evaluation cycle and
// persists the new state through store (nil store skips persistence, e.g.
// store-unavailable cycles that keep state in memory).
//
// condTrue reports whether the rule condition held this cycle; value is the
// observed metric value (0 when the metric was absent). now is the daemon
// wall clock. forDur is the pending window before firing (<= 0 fires
// immediately); repeat is the minimum interval between firing
// notifications (<= 0 disables repeats).
//
// Transition table:
//
//	ok + false       → ok       (none)
//	ok + true        → pending  (fired immediately when forDur <= 0)
//	pending + false  → ok       (none — never fired)
//	pending + true   → firing   (fired) when now-pendingStart >= forDur
//	firing + false   → ok       (resolved)
//	firing + true    → firing   (repeated) when now-lastFired >= repeat
func (s *State) Transition(ctx context.Context, store storage.MetricsStore, condTrue bool, value float64, now time.Time, forDur, repeat time.Duration) (Event, error) {
	s.LastValue = value
	event := EventNone

	switch s.Status {
	case "", StateOK:
		s.Status = StateOK
		if condTrue {
			s.Status = StatePending
			s.PendingSince = now
			if forDur <= 0 {
				s.fire(now)
				event = EventFired
			}
		}
	case StatePending:
		if !condTrue {
			s.Status = StateOK
		} else {
			// A rehydrated state has no PendingSince; restart the window.
			if s.PendingSince.IsZero() {
				s.PendingSince = now
			}
			if now.Sub(s.PendingSince) >= forDur {
				s.fire(now)
				event = EventFired
			}
		}
	case StateFiring:
		if !condTrue {
			s.Status = StateOK
			event = EventResolved
		} else if repeat > 0 && !s.LastFired.IsZero() && now.Sub(s.LastFired) >= repeat {
			s.fire(now)
			event = EventRepeated
		}
	default:
		return event, fmt.Errorf("state %q for rule %s: unknown state", s.Status, s.RuleName)
	}

	if store != nil {
		if err := store.SaveAlertState(ctx, s.toRow()); err != nil {
			return event, fmt.Errorf("persist alert state for %s: %w", s.RuleName, err)
		}
	}
	return event, nil
}

// fire moves the state into firing and records the notification.
func (s *State) fire(now time.Time) {
	s.Status = StateFiring
	s.LastFired = now
	s.NotifyCount++
}

func (s *State) toRow() storage.AlertStateRow {
	return storage.AlertStateRow{
		RuleName:    s.RuleName,
		Status:      s.Status,
		LastFired:   s.LastFired,
		LastValue:   s.LastValue,
		NotifyCount: s.NotifyCount,
	}
}
