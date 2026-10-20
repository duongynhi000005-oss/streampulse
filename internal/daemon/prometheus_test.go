// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPromServer(t *testing.T) *PromServer {
	t.Helper()
	s := NewPromServer(&config.PromConfig{Listen: "127.0.0.1:0", Path: "/metrics"},
		NewScrapeStats(), PromOptions{Version: "v0.1.0-dev"})
	require.NoError(t, s.Start())
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })
	return s
}

func getMetrics(t *testing.T, s *PromServer) string {
	t.Helper()
	resp, err := http.Get("http://" + s.Addr().String() + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func TestPromEndpointServesMetrics(t *testing.T) {
	s := testPromServer(t)

	body := getMetrics(t, s)
	assert.Contains(t, body, "streampulse_scrapes_total 0")
	assert.Contains(t, body, `streampulse_build_info{version="v0.1.0-dev"} 1`)
	assert.Contains(t, body, "streampulse_alerts_firing")
}

func TestPromUnknownPathReturns404(t *testing.T) {
	s := testPromServer(t)

	resp, err := http.Get("http://" + s.Addr().String() + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestPromCountersReflectScrape(t *testing.T) {
	stats := NewScrapeStats()
	s := NewPromServer(&config.PromConfig{Listen: "127.0.0.1:0", Path: "/metrics"},
		stats, PromOptions{Version: "v0.1.0-dev"})
	require.NoError(t, s.Start())
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	d := &Daemon{scraper: &fakeScraper{}, stats: stats}
	d.doScrape(context.Background())

	require.Eventually(t, func() bool {
		return strings.Contains(getMetrics(t, s), "streampulse_scrapes_total 1")
	}, time.Second, 20*time.Millisecond)
}

func TestPromShutdownClosesListener(t *testing.T) {
	s := testPromServer(t)
	addr := s.Addr().String()

	require.NoError(t, s.Shutdown(context.Background()))
	resp, err := http.Get("http://" + addr + "/metrics")
	if err == nil {
		resp.Body.Close()
	}
	assert.Error(t, err, "listener must be closed after Shutdown")
}
