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

func TestNewStoreUnsupportedBackends(t *testing.T) {
	for _, typ := range []string{"postgres", "clickhouse", "postgress"} {
		store, err := NewStore(typ, "")
		require.Error(t, err, typ)
		assert.Nil(t, store, typ)
	}
}

func TestNewStoreDefaultsToSQLite(t *testing.T) {
	store, err := NewStore("", ":memory:")
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()
}

func TestSQLiteStoreWriteBatch(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	ts := time.Now().Truncate(time.Millisecond)
	err = s.WriteBatch(context.Background(), []Metric{
		{TS: ts, ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 12.5},
		{TS: ts.Add(5 * time.Second), ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Tags: map[string]string{"a": "b"}, Value: 13.5},
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
	assert.Equal(t, 2, count)

	var storedTS int64
	require.NoError(t, s.db.QueryRow(`SELECT ts FROM raw_metrics WHERE value = 12.5`).Scan(&storedTS))
	assert.Equal(t, ts.UnixMilli(), storedTS)

	var tags string
	require.NoError(t, s.db.QueryRow(`SELECT tags FROM raw_metrics WHERE value = 13.5`).Scan(&tags))
	assert.JSONEq(t, `{"a":"b"}`, tags)
}

func TestSQLiteStorePurge(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	err = s.WriteBatch(context.Background(), []Metric{
		{TS: time.Now().Add(-48 * time.Hour), ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 1},
		{TS: time.Now(), ClusterID: "c1", Metric: "msg_rate", EntityType: "topic", EntityName: "orders", Value: 2},
	})
	require.NoError(t, err)

	require.NoError(t, s.Purge(context.Background(), Retention{Raw: 24 * time.Hour}))

	var count int
	require.NoError(t, s.db.QueryRow(`SELECT COUNT(*) FROM raw_metrics`).Scan(&count))
	assert.Equal(t, 1, count)
}

func TestValidTypes(t *testing.T) {
	assert.Equal(t, []string{"sqlite", "postgres", "clickhouse"}, ValidTypes())
}
