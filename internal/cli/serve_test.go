// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package cli

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestServeExitsZeroOnShutdown runs the serve command with a context that is
// canceled shortly after startup (the CLI-level equivalent of SIGTERM) and
// asserts the command exits cleanly.
func TestServeExitsZeroOnShutdown(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "state.db"))

	root := NewRootCommand("test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	root.SetContext(ctx)
	root.SetArgs([]string{"serve"})
	require.NoError(t, root.Execute())
}
