// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pulsedev/streampulse/internal/alerts"
)

// pagerDutyEndpoint is the Events API v2 ingest URL.
const pagerDutyEndpoint = "https://events.pagerduty.com/v2/enqueue"

// severityRank orders PagerDuty severities so MinSeverity filters work.
var severityRank = map[string]int{"info": 1, "warning": 2, "error": 3, "critical": 4}

// PagerDuty posts Events API v2 payloads. Only notifications at or above
// MinSeverity are delivered (default "critical").
type PagerDuty struct {
	RoutingKey  string
	MinSeverity string
	Endpoint    string
	client      *http.Client
}

// NewPagerDuty creates a PagerDuty notifier. routingKey may be a literal key
// or the name of an env var holding it (webhook_env config field). An empty
// minSeverity defaults to "critical".
func NewPagerDuty(routingKey, minSeverity string) *PagerDuty {
	if minSeverity == "" {
		minSeverity = "critical"
	}
	return &PagerDuty{
		RoutingKey:  resolveSecret(routingKey),
		MinSeverity: minSeverity,
		Endpoint:    pagerDutyEndpoint,
		client:      &http.Client{Timeout: httpTimeout},
	}
}

// Notify sends a trigger or resolve event, retrying once on a 5xx response.
// Notifications below MinSeverity are skipped without error.
func (p *PagerDuty) Notify(ctx context.Context, n alerts.Notification) error {
	if severityRank[n.Severity] < severityRank[p.MinSeverity] {
		return nil
	}
	action := "trigger"
	if n.Status == "resolved" {
		action = "resolve"
	}
	payload := map[string]any{
		"routing_key":  p.RoutingKey,
		"event_action": action,
		"dedup_key":    n.Rule,
		"payload": map[string]any{
			"summary":   n.Message,
			"source":    "streampulse",
			"severity":  n.Severity,
			"timestamp": n.Timestamp.UTC().Format(time.RFC3339),
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pagerduty: marshal payload: %w", err)
	}
	if err := postJSON(ctx, p.client, p.Endpoint, "application/json", body); err != nil {
		return fmt.Errorf("pagerduty: %w", err)
	}
	return nil
}
