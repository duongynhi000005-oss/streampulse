// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/pulsedev/streampulse/internal/dlq"
)

// ─── DLQ inspect / replay view ─────────────────────────────────────────────

// dlqInspectMsg carries the result of an async DLQ message inspection.
type dlqInspectMsg struct {
	topic    string
	messages []dlq.Message
	err      error
}

// dlqReplayMsg carries the result of an async DLQ replay run.
type dlqReplayMsg struct {
	topic  string
	result *dlq.ReplayResult
	err    error
}

// fetchDLQ discovers dead-letter topics through the kafka client and maps
// them into dashboard rows. Growth renders as "-" when the discovery did not
// produce a rate.
func (m *Model) fetchDLQ(ctx context.Context) ([]DLQRow, error) {
	if m.kafkaClient == nil {
		return nil, nil
	}
	discover := m.discoverDLQ
	if discover == nil {
		discover = dlq.Discover
	}
	topics, err := discover(ctx, m.kafkaClient, nil)
	if err != nil {
		return nil, err
	}
	rows := make([]DLQRow, 0, len(topics))
	for _, t := range topics {
		row := DLQRow{Topic: t.Name, MessageCount: strconv.FormatInt(t.MessageCount, 10)}
		if t.GrowthRate == 0 {
			row.Growth = "-"
		} else {
			row.Growth = fmt.Sprintf("%.1f", t.GrowthRate)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// openDLQView opens the inspect viewport for topic and starts loading its
// last messages.
func (m *Model) openDLQView(topic string) tea.Cmd {
	m.dlqTopic = topic
	m.dlqConfirm = false
	width, height := m.width, m.height-8
	if width < 40 {
		width = 80
	}
	if height < 10 {
		height = 10
	}
	vp := viewport.New(width, height)
	m.dlqView = &vp
	return m.dlqInspectCmd()
}

// dlqInspectCmd loads the last messages of the current DLQ topic.
func (m *Model) dlqInspectCmd() tea.Cmd {
	topic, brokers := m.dlqTopic, m.brokerAddrs
	fn := m.dlqInspectFn
	if fn == nil {
		fn = dlq.Inspect
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		msgs, err := fn(ctx, brokers, topic, dlq.DefaultInspectLimit)
		return dlqInspectMsg{topic: topic, messages: msgs, err: err}
	}
}

// dlqReplayCmd replays the current DLQ topic without dry-run.
func (m *Model) dlqReplayCmd() tea.Cmd {
	topic, brokers := m.dlqTopic, m.brokerAddrs
	fn := m.dlqReplayFn
	if fn == nil {
		fn = dlq.Replay
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, err := fn(ctx, dlq.ReplayOptions{
			Brokers: brokers,
			Topic:   topic,
			Limit:   dlq.DefaultInspectLimit,
		})
		return dlqReplayMsg{topic: topic, result: result, err: err}
	}
}

// handleDLQInspectMsg renders the inspected messages into the viewport.
func (m *Model) handleDLQInspectMsg(msg dlqInspectMsg) {
	if m.dlqView == nil || msg.topic != m.dlqTopic {
		return // view closed or stale topic while loading
	}
	if msg.err != nil {
		m.dlqView.SetContent(fmt.Sprintf("INSPECT %s FAILED: %v", msg.topic, msg.err))
		return
	}
	m.dlqView.SetContent(dlqInspectContent(msg.topic, msg.messages))
}

// handleDLQReplayMsg renders the replay result into the viewport.
func (m *Model) handleDLQReplayMsg(msg dlqReplayMsg) {
	if m.dlqView == nil || msg.topic != m.dlqTopic {
		return
	}
	m.dlqConfirm = false
	if msg.err != nil {
		m.dlqView.SetContent(fmt.Sprintf("REPLAY %s FAILED: %v", msg.topic, msg.err))
		return
	}
	m.dlqView.SetContent(dlqReplayContent(msg.topic, msg.result))
}

// dlqInspectContent renders the inspected messages as text lines.
func dlqInspectContent(topic string, msgs []dlq.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSPECT %s (last %d):", topic, len(msgs))
	if len(msgs) == 0 {
		b.WriteString("\nno messages")
		return b.String()
	}
	for _, m := range msgs {
		fmt.Fprintf(&b, "\n\n[p%d @ off %d] %s\n", m.Partition, m.Offset, m.Timestamp.Format("15:04:05"))
		if len(m.Key) > 0 {
			fmt.Fprintf(&b, "key: %s\n", dlq.DisplayValue(m.Key, 200))
		}
		fmt.Fprintf(&b, "%s", dlq.DisplayValue(m.Value, 1000))
	}
	return b.String()
}

// dlqReplayContent renders the summary of a completed replay run.
func dlqReplayContent(topic string, res *dlq.ReplayResult) string {
	return fmt.Sprintf("REPLAY %s: replayed %d of %d messages (%d batches)",
		topic, res.Replayed, res.Total, res.Batches)
}
