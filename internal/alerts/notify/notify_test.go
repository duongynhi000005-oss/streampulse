// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package notify

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/alerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testNotification() alerts.Notification {
	return alerts.Notification{
		Rule: "consumer-lag", Severity: "critical", Status: "firing",
		Value: 1500, Entity: "orders-processor",
		Message:   "consumer-lag firing: value 1500.00 (threshold > 1000.00)",
		Timestamp: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC),
	}
}

// ─── Slack ────────────────────────────────────────────────────────────────

func TestSlackPostsJSON(t *testing.T) {
	var got map[string]string
	var contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := NewSlack(srv.URL)
	require.NoError(t, n.Notify(context.Background(), testNotification()))
	assert.Equal(t, "application/json", contentType)
	assert.Equal(t, "consumer-lag firing: value 1500.00 (threshold > 1000.00)", got["text"])
}

func TestSlackRetriesOnceOn5xx(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := NewSlack(srv.URL)
	require.NoError(t, n.Notify(context.Background(), testNotification()))
	assert.Equal(t, 2, calls, "one retry after a 5xx")
}

func TestSlackFailsAfterRetriesExhausted(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	n := NewSlack(srv.URL)
	require.Error(t, n.Notify(context.Background(), testNotification()))
	assert.Equal(t, 2, calls, "two attempts total, then the error surfaces")
}

func TestSlackResolvesEnvAtConstruction(t *testing.T) {
	t.Setenv("STREAMPULSE_TEST_WEBHOOK", "https://hooks.slack.example/abc")
	n := NewSlack("STREAMPULSE_TEST_WEBHOOK")
	assert.Equal(t, "https://hooks.slack.example/abc", n.WebhookURL)
}

// ─── PagerDuty ────────────────────────────────────────────────────────────

func TestPagerDutyPostsEventV2(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	n := NewPagerDuty("routing-key", "critical")
	n.Endpoint = srv.URL
	require.NoError(t, n.Notify(context.Background(), testNotification()))
	assert.Equal(t, "routing-key", got["routing_key"])
	assert.Equal(t, "trigger", got["event_action"])
	assert.Equal(t, "consumer-lag", got["dedup_key"])
	payload, ok := got["payload"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "consumer-lag firing: value 1500.00 (threshold > 1000.00)", payload["summary"])
	assert.Equal(t, "critical", payload["severity"])
	assert.Equal(t, "streampulse", payload["source"])
	assert.Equal(t, "2026-08-12T10:00:00Z", payload["timestamp"])
}

func TestPagerDutyResolveAction(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	n := NewPagerDuty("rk", "critical")
	n.Endpoint = srv.URL
	resolved := testNotification()
	resolved.Status = "resolved"
	require.NoError(t, n.Notify(context.Background(), resolved))
	assert.Equal(t, "resolve", got["event_action"])
}

func TestPagerDutySkipsBelowMinSeverity(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	n := NewPagerDuty("rk", "critical")
	n.Endpoint = srv.URL
	below := testNotification()
	below.Severity = "warning"
	require.NoError(t, n.Notify(context.Background(), below))
	assert.Equal(t, 0, calls, "warning must not reach a critical-only notifier")
}

func TestPagerDutyDefaultsToCriticalMinSeverity(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	n := NewPagerDuty("rk", "")
	n.Endpoint = srv.URL
	below := testNotification()
	below.Severity = "warning"
	require.NoError(t, n.Notify(context.Background(), below))
	assert.Equal(t, 0, calls)
}

// ─── Email ────────────────────────────────────────────────────────────────

// startFakeSMTP runs a minimal SMTP server on 127.0.0.1 that captures the
// message data of the first conversation and answers each conversation with
// 250/354/221 responses.
func startFakeSMTP(t *testing.T) (addr string, received chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	received = make(chan string, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSMTP(conn, received)
		}
	}()
	return ln.Addr().String(), received
}

func serveSMTP(conn net.Conn, received chan<- string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	reply := func(s string) {
		_, _ = conn.Write([]byte(s + "\r\n"))
	}
	reply("220 fake ESMTP ready")
	var data strings.Builder
	inData := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if inData {
			if line == "." {
				inData = false
				reply("250 ok")
				received <- data.String()
				continue
			}
			data.WriteString(line + "\n")
			continue
		}
		switch {
		case strings.HasPrefix(line, "EHLO"):
			reply("250-fake")
			reply("250-AUTH PLAIN")
			reply("250 SIZE 10485760")
		case strings.HasPrefix(line, "AUTH"):
			reply("235 ok")
		case strings.HasPrefix(line, "MAIL"):
			reply("250 ok")
		case strings.HasPrefix(line, "RCPT"):
			reply("250 ok")
		case strings.HasPrefix(line, "DATA"):
			reply("354 end with <CRLF>.<CRLF>")
			inData = true
			data.Reset()
		case strings.HasPrefix(line, "QUIT"):
			reply("221 bye")
			return
		default:
			reply("250 ok")
		}
	}
}

func TestEmailSendsPlainText(t *testing.T) {
	addr, received := startFakeSMTP(t)
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	n := NewEmail(SMTPConfig{
		Host: host, Port: port,
		User: "svc", Password: "pw",
		From: "alerts@streampulse.dev",
		To:   []string{"ops@example.com", "oncall@example.com"},
	})
	require.NoError(t, n.Notify(context.Background(), testNotification()))

	select {
	case msg := <-received:
		assert.Contains(t, msg, "To: ops@example.com, oncall@example.com")
		assert.Contains(t, msg, "From: alerts@streampulse.dev")
		assert.Contains(t, msg, "Subject: [streampulse] firing: consumer-lag")
		assert.Contains(t, msg, "consumer-lag firing: value 1500.00 (threshold > 1000.00)")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the SMTP message")
	}
}

func TestEmailResolvesPasswordEnvAtConstruction(t *testing.T) {
	t.Setenv("STREAMPULSE_TEST_SMTP_PW", "hunter2")
	e := NewEmail(SMTPConfig{Password: "STREAMPULSE_TEST_SMTP_PW"})
	assert.Equal(t, "hunter2", e.cfg.Password)
}
