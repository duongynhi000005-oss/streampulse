// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package notify

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/pulsedev/streampulse/internal/alerts"
)

// SMTPConfig configures the email notifier. Password is resolved from
// password_env at construction; To recipients come from the alert channel's
// comma-separated "to" field.
type SMTPConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	From     string
	To       []string
}

// Email sends plain-text alert emails via SMTP (v0.1: no HTML).
type Email struct {
	cfg SMTPConfig
}

// NewEmail creates an email notifier.
func NewEmail(cfg SMTPConfig) *Email {
	cfg.Password = resolveSecret(cfg.Password)
	return &Email{cfg: cfg}
}

// Notify sends the notification as a plain-text email to all recipients.
func (e *Email) Notify(ctx context.Context, n alerts.Notification) error {
	addr := fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port)
	var auth smtp.Auth
	if e.cfg.User != "" {
		auth = smtp.PlainAuth("", e.cfg.User, e.cfg.Password, e.cfg.Host)
	}
	if err := smtp.SendMail(addr, auth, e.cfg.From, e.cfg.To, e.message(n)); err != nil {
		return fmt.Errorf("email: %w", err)
	}
	return nil
}

// message builds the RFC-5322 message (headers + blank line + body).
func (e *Email) message(n alerts.Notification) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(e.cfg.To, ", "))
	fmt.Fprintf(&b, "From: %s\r\n", e.cfg.From)
	fmt.Fprintf(&b, "Subject: [streampulse] %s: %s\r\n", n.Status, n.Rule)
	b.WriteString("\r\n")
	b.WriteString(n.Message)
	return []byte(b.String())
}
