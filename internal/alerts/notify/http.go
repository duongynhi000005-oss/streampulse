// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package notify

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// httpTimeout bounds every outbound notification request.
const httpTimeout = 5 * time.Second

// maxAttempts is the request budget for HTTP notifiers: one attempt plus one
// retry on failure.
const maxAttempts = 2

// resolveSecret returns the value of the env var named by s when set,
// falling back to s itself. Env var names cannot contain ':' or '/', so
// literal URLs never resolve; secrets referenced via webhook_env/password_env
// config fields are resolved at construction and never stored in yaml.
func resolveSecret(s string) string {
	if v := os.Getenv(s); v != "" {
		return v
	}
	return s
}

// postJSON sends body to url with the given content type. A 5xx response is
// retried once; any final non-2xx response is an error.
func postJSON(ctx context.Context, client *http.Client, url, contentType string, body []byte) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("post %s: %w", url, err)
		}
		req.Header.Set("Content-Type", contentType)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("post %s: %w", url, err)
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 500 && attempt < maxAttempts {
			lastErr = fmt.Errorf("post %s: status %d", url, resp.StatusCode)
			continue
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("post %s: status %d", url, resp.StatusCode)
		}
		return nil
	}
	return lastErr
}
