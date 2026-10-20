// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlertStateRoundTrip(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()
	ctx := context.Background()

	firedAt := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	err = s.SaveAlertState(ctx, AlertStateRow{RuleName: "consumer-lag", Status: "firing", LastFired: firedAt, LastValue: 1500, NotifyCount: 2})
	require.NoError(t, err)
	err = s.SaveAlertState(ctx, AlertStateRow{RuleName: "broker-down", Status: "ok"})
	require.NoError(t, err)

	rows, err := s.QueryAlertState(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2, "both saved rows must be returned")

	// Rows come back ordered by rule name.
	assert.Equal(t, "broker-down", rows[0].RuleName)
	assert.Equal(t, "ok", rows[0].Status)
	assert.True(t, rows[0].LastFired.IsZero(), "unfired rule has zero last_fired")
	assert.Equal(t, 0, rows[0].NotifyCount)

	assert.Equal(t, "consumer-lag", rows[1].RuleName)
	assert.Equal(t, "firing", rows[1].Status)
	assert.Equal(t, firedAt, rows[1].LastFired)
	assert.Equal(t, 1500.0, rows[1].LastValue)
	assert.Equal(t, 2, rows[1].NotifyCount)
}

func TestAlertStateUpsert(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()
	ctx := context.Background()

	err = s.SaveAlertState(ctx, AlertStateRow{RuleName: "consumer-lag", Status: "pending", LastValue: 100})
	require.NoError(t, err)
	err = s.SaveAlertState(ctx, AlertStateRow{RuleName: "consumer-lag", Status: "firing", LastFired: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC), LastValue: 1500, NotifyCount: 1})
	require.NoError(t, err)

	rows, err := s.QueryAlertState(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1, "upsert must not create a duplicate row")
	assert.Equal(t, "firing", rows[0].Status)
	assert.Equal(t, 1500.0, rows[0].LastValue)
	assert.Equal(t, 1, rows[0].NotifyCount)
}
