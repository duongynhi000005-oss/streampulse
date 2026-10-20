// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pulsedev/streampulse/internal/alerts"
)

// Slack posts notifications to a Slack incoming webhook with a {"text": ...}
// payload.
type Slack struct {
	WebhookURL string
	client     *http.Client
}

// NewSlack creates a Slack notifier. webhook may be a literal URL or the
// name of an env var holding the URL (webhook_env config field); env
// resolution happens at construction.
func NewSlack(webhook string) *Slack {
	return &Slack{
		WebhookURL: resolveSecret(webhook),
		client:     &http.Client{Timeout: httpTimeout},
	}
}

// Notify posts the notification message to the webhook, retrying once on a
// 5xx response.
func (s *Slack) Notify(ctx context.Context, n alerts.Notification) error {
	body, err := json.Marshal(map[string]string{"text": n.Message})
	if err != nil {
		return fmt.Errorf("slack: marshal payload: %w", err)
	}
	if err := postJSON(ctx, s.client, s.WebhookURL, "application/json", body); err != nil {
		return fmt.Errorf("slack: %w", err)
	}
	return nil
}
