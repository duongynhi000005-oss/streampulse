// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package alerts

import (
	"context"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemStore(t *testing.T) storage.MetricsStore {
	t.Helper()
	s, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStateTransitionTable(t *testing.T) {
	store := newMemStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	const forDur = 2 * time.Minute
	const repeat = time.Hour

	s := State{RuleName: "consumer-lag"}

	t.Run("ok and false stays ok", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, false, 0, now, forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventNone, ev)
		assert.Equal(t, "ok", s.Status)
	})

	t.Run("ok and true enters pending", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, true, 1500, now, forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventNone, ev)
		assert.Equal(t, "pending", s.Status)
	})

	t.Run("pending and true before for stays pending", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, true, 1500, now.Add(time.Minute), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventNone, ev)
		assert.Equal(t, "pending", s.Status)
	})

	t.Run("pending and true after for fires", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, true, 1500, now.Add(forDur), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventFired, ev)
		assert.Equal(t, "firing", s.Status)
		assert.Equal(t, now.Add(forDur), s.LastFired)
		assert.Equal(t, 1, s.NotifyCount)
	})

	t.Run("pending and false returns to ok", func(t *testing.T) {
		p := State{RuleName: "consumer-lag"}
		_, err := p.Transition(ctx, store, true, 1500, now, forDur, repeat)
		require.NoError(t, err)
		ev, err := p.Transition(ctx, store, false, 0, now.Add(time.Minute), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventNone, ev, "never fired → no resolution notice")
		assert.Equal(t, "ok", p.Status)
	})

	t.Run("firing and true before repeat stays quiet", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, true, 1600, now.Add(forDur).Add(30*time.Minute), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventNone, ev)
		assert.Equal(t, "firing", s.Status)
		assert.Equal(t, 1, s.NotifyCount)
	})

	t.Run("firing and true after repeat re-notifies", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, true, 1600, now.Add(forDur).Add(repeat), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventRepeated, ev)
		assert.Equal(t, "firing", s.Status)
		assert.Equal(t, now.Add(forDur).Add(repeat), s.LastFired)
		assert.Equal(t, 2, s.NotifyCount)
	})

	t.Run("firing and false resolves", func(t *testing.T) {
		ev, err := s.Transition(ctx, store, false, 0, now.Add(forDur).Add(repeat).Add(time.Minute), forDur, repeat)
		require.NoError(t, err)
		assert.Equal(t, EventResolved, ev)
		assert.Equal(t, "ok", s.Status)
		assert.Equal(t, 2, s.NotifyCount, "resolution does not notify count")
	})
}

func TestTransitionFiresImmediatelyWithoutForDuration(t *testing.T) {
	store := newMemStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	s := State{RuleName: "scrape-failing"}
	ev, err := s.Transition(ctx, store, true, 3, now, 0, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, EventFired, ev)
	assert.Equal(t, "firing", s.Status)
	assert.Equal(t, 1, s.NotifyCount)
}

func TestStatePersistsAndRehydrates(t *testing.T) {
	store := newMemStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	const forDur = 2 * time.Minute

	s := State{RuleName: "consumer-lag"}
	_, err := s.Transition(ctx, store, true, 1500, now, forDur, time.Hour)
	require.NoError(t, err)
	_, err = s.Transition(ctx, store, true, 1500, now.Add(forDur), forDur, time.Hour)
	require.NoError(t, err)

	rows, err := store.QueryAlertState(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "consumer-lag", rows[0].RuleName)
	assert.Equal(t, "firing", rows[0].Status)
	assert.Equal(t, now.Add(forDur), rows[0].LastFired)
	assert.Equal(t, 1500.0, rows[0].LastValue)
	assert.Equal(t, 1, rows[0].NotifyCount)

	// Rehydrate a fresh state from the store; the machine continues from
	// where it was: firing + false → resolved.
	rehydrated := State{}
	for _, r := range rows {
		if r.RuleName == "consumer-lag" {
			rehydrated = State{
				RuleName:    r.RuleName,
				Status:      r.Status,
				LastFired:   r.LastFired,
				LastValue:   r.LastValue,
				NotifyCount: r.NotifyCount,
			}
		}
	}
	assert.Equal(t, "firing", rehydrated.Status)

	ev, err := rehydrated.Transition(ctx, store, false, 0, now.Add(forDur).Add(time.Minute), forDur, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, EventResolved, ev)
	assert.Equal(t, "ok", rehydrated.Status)
}

func TestTransitionPersistsEveryCycle(t *testing.T) {
	store := newMemStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	s := State{RuleName: "consumer-lag"}
	_, err := s.Transition(ctx, store, true, 900, now, 2*time.Minute, time.Hour)
	require.NoError(t, err)

	rows, err := store.QueryAlertState(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "pending", rows[0].Status, "pending is persisted, not only firing")
	assert.Equal(t, 900.0, rows[0].LastValue)
}
