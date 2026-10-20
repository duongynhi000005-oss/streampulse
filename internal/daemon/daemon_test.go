// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig() *config.Config {
	return config.DefaultConfig()
}

func TestNewReturnsDaemon(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	d := New(testConfig(), store, kafka.NewClient([]string{"127.0.0.1:1"}))
	require.NotNil(t, d)
	assert.NotNil(t, d.store)
	assert.NotNil(t, d.client)
	assert.NotNil(t, d.scraper)
}

func TestRunBlocksUntilContextCancel(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	d := New(testConfig(), store, kafka.NewClient([]string{"127.0.0.1:1"}))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	// Run must block: nothing may return before the context is canceled.
	select {
	case err := <-errCh:
		t.Fatalf("Run returned before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within the grace period")
	}
}

func TestRunReturnsOnExternalShutdown(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	d := New(testConfig(), store, kafka.NewClient([]string{"127.0.0.1:1"}))

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(context.Background()) }()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, d.Shutdown())

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	d := New(testConfig(), store, kafka.NewClient([]string{"127.0.0.1:1"}))

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(context.Background()) }()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, d.Shutdown())
	require.NoError(t, d.Shutdown()) // second signal path: must be a no-op

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}
