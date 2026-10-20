// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pulsedev/streampulse/internal/config"
)

// ScrapeStats holds the Prometheus metrics describing the daemon's own
// scraping behavior (daemon.md). The alert loop (Phase 5) drives
// AlertFiring.
type ScrapeStats struct {
	ScrapesTotal      prometheus.Counter
	ScrapeErrorsTotal prometheus.Counter
	ScrapeDuration    prometheus.Histogram
	AlertFiring       *prometheus.GaugeVec
}

// NewScrapeStats creates the scrape statistics with one pre-registered,
// zero-valued alerts_firing series so the family renders before any alert
// rule exists (Phase 5 wires per-rule series).
func NewScrapeStats() *ScrapeStats {
	stats := &ScrapeStats{
		ScrapesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streampulse_scrapes_total",
			Help: "Total number of Kafka scrape cycles completed.",
		}),
		ScrapeErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streampulse_scrape_errors_total",
			Help: "Total number of Kafka scrape cycles that failed.",
		}),
		ScrapeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "streampulse_scrape_duration_seconds",
			Help:    "Duration of one Kafka scrape cycle.",
			Buckets: prometheus.DefBuckets,
		}),
		AlertFiring: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streampulse_alerts_firing",
			Help: "Alerts currently firing, by rule.",
		}, []string{"rule"}),
	}
	stats.AlertFiring.WithLabelValues("").Set(0)
	return stats
}

// PromOptions configures the Prometheus endpoint.
type PromOptions struct {
	// Version is the daemon build version (main.version ldflag); defaults
	// to v0.1.0-dev.
	Version string
}

// PromServer serves the daemon's metrics over HTTP.
type PromServer struct {
	server   *http.Server
	mu       sync.Mutex
	listener net.Listener
}

// NewPromServer creates the /metrics HTTP server described by cfg. The
// stats are registered in a fresh registry along with build_info.
func NewPromServer(cfg *config.PromConfig, stats *ScrapeStats, opts PromOptions) *PromServer {
	version := opts.Version
	if version == "" {
		version = "v0.1.0-dev"
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(stats.ScrapesTotal, stats.ScrapeErrorsTotal, stats.ScrapeDuration, stats.AlertFiring)

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streampulse_build_info",
		Help: "Build information for the streampulse daemon.",
	}, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)
	registry.MustRegister(buildInfo)

	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	return &PromServer{server: &http.Server{Addr: cfg.Listen, Handler: mux}}
}

// Start binds the configured listen address (":0" binds an ephemeral port)
// and serves metrics in the background. Serve errors are expected on
// Shutdown and are not fatal.
func (s *PromServer) Start() error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("prometheus listen %s: %w", s.server.Addr, err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("prometheus server", "err", err)
		}
	}()
	return nil
}

// Addr returns the bound listener address (useful when listening on :0),
// or nil before Start.
func (s *PromServer) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Shutdown gracefully stops the server and closes its listener.
func (s *PromServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
