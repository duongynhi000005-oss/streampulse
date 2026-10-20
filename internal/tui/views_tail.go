// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pulsedev/streampulse/internal/tail"
)

// ─── Topic tail view (Topics tab, Enter) ────────────────────────────────────

const (
	// tailSnapshotLimit is how many messages the tail view loads on open.
	tailSnapshotLimit = 50
	// tailPerPartition bounds messages read per partition per follow tick.
	tailPerPartition = 10
	// tailBufferCap is the rolling message buffer cap.
	tailBufferCap = 200
	// tailFollowInterval is the live-follow poll cadence.
	tailFollowInterval = time.Second
)

// tailSnapshotMsg carries the result of the initial snapshot read.
type tailSnapshotMsg struct {
	topic string
	msgs  []tail.Message
	err   error
}

// tailFollowMsg carries one follow tick's new messages and updated offsets.
type tailFollowMsg struct {
	topic string
	msgs  []tail.Message
	offs  map[int]int64
	err   error
}

// tailTickMsg drives the live follow loop.
type tailTickMsg time.Time

// tailRetryMsg re-runs the snapshot after a failure.
type tailRetryMsg struct{ topic string }

// openTailView opens the tail viewport for topic and starts the snapshot.
func (m *Model) openTailView(topic string) tea.Cmd {
	m.tailTopic = topic
	m.tailMessages = nil
	m.tailOffsets = nil
	m.tailPaused = false
	m.tailPinned = true
	m.tailErr = ""

	width, height := m.width, m.height-8
	if width < 40 {
		width = 80
	}
	if height < 10 {
		height = 10
	}
	vp := viewport.New(width, height)
	m.tailView = &vp
	return m.tailSnapshotCmd()
}

// tailBroker returns the broker used for tail reads (injectable in tests).
func (m *Model) tailBroker() tail.Broker {
	if m.tailBrokerFn != nil {
		return m.tailBrokerFn()
	}
	return tail.NewBroker(m.brokerAddrs)
}

// tailSnapshotCmd reads the last tailSnapshotLimit messages of the topic.
func (m *Model) tailSnapshotCmd() tea.Cmd {
	topic, b := m.tailTopic, m.tailBroker()
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		msgs, err := tail.Snapshot(ctx, b, topic, tailSnapshotLimit)
		return tailSnapshotMsg{topic: topic, msgs: msgs, err: err}
	}
}

// tailFollowCmd performs one follow poll past the current offsets.
func (m *Model) tailFollowCmd() tea.Cmd {
	topic, offs, b := m.tailTopic, m.tailOffsets, m.tailBroker()
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		msgs, next, err := tail.ReadNew(ctx, b, topic, offs, tailPerPartition)
		return tailFollowMsg{topic: topic, msgs: msgs, offs: next, err: err}
	}
}

// handleTailSnapshotMsg renders the snapshot and seeds the follow offsets.
// On error the view shows the problem and a retry is scheduled.
func (m *Model) handleTailSnapshotMsg(msg tailSnapshotMsg) tea.Cmd {
	if m.tailView == nil || msg.topic != m.tailTopic {
		return nil // view closed or stale topic while loading
	}
	if msg.err != nil {
		m.tailErr = fmt.Sprintf("tail %s: %v", msg.topic, msg.err)
		m.tailView.SetContent(m.tailErr)
		return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
			return tailRetryMsg{topic: msg.topic}
		})
	}
	m.tailErr = ""
	m.tailMessages = msg.msgs
	if len(m.tailMessages) > tailBufferCap {
		m.tailMessages = m.tailMessages[len(m.tailMessages)-tailBufferCap:]
	}

	offs := make(map[int]int64)
	for _, msg := range msg.msgs {
		if o, ok := offs[msg.Partition]; !ok || msg.Offset+1 > o {
			offs[msg.Partition] = msg.Offset + 1
		}
	}
	if len(offs) == 0 {
		offs = nil // empty topic: follow from current high-watermarks
	}
	m.tailOffsets = offs
	m.renderTailMessages()
	// start the live follow loop
	return tea.Tick(tailFollowInterval, func(t time.Time) tea.Msg {
		return tailTickMsg(t)
	})
}

// handleTailFollowMsg appends new messages to the buffer and re-renders.
func (m *Model) handleTailFollowMsg(msg tailFollowMsg) {
	if m.tailView == nil || msg.topic != m.tailTopic {
		return
	}
	if msg.err != nil {
		m.tailErr = msg.err.Error()
	} else {
		m.tailErr = ""
	}
	if msg.offs != nil {
		m.tailOffsets = msg.offs
	}
	m.tailMessages = append(m.tailMessages, msg.msgs...)
	if len(m.tailMessages) > tailBufferCap {
		m.tailMessages = m.tailMessages[len(m.tailMessages)-tailBufferCap:]
	}
	m.renderTailMessages()
}

// renderTailMessages writes the rolling buffer into the viewport, auto-
// scrolling to the newest while pinned.
func (m *Model) renderTailMessages() {
	if m.tailView == nil {
		return
	}
	var b strings.Builder
	for i, msg := range m.tailMessages {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(tail.FormatMessage(msg))
	}
	if m.tailErr != "" {
		b.WriteString("\n\n[" + m.tailErr + "]")
	}
	if b.Len() == 0 {
		b.WriteString("no messages")
	}
	m.tailView.SetContent(b.String())
	if m.tailPinned {
		m.tailView.GotoBottom()
	}
}

// handleTailViewKey handles keys while the tail view is open.
func (m *Model) handleTailViewKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.closeTailView()
	case "q", "ctrl+c":
		return tea.Quit
	case "p":
		m.tailPaused = !m.tailPaused
	case "g":
		m.tailPinned = true
		if m.tailView != nil {
			m.tailView.GotoBottom()
		}
	case "j", "down":
		if m.tailView != nil {
			m.tailView.LineDown(1)
		}
		m.tailPinned = false
	case "k", "up":
		if m.tailView != nil {
			m.tailView.LineUp(1)
		}
		m.tailPinned = false
	}
	return nil
}

// closeTailView returns to the Topics table.
func (m *Model) closeTailView() {
	m.tailView = nil
	m.tailTopic = ""
	m.tailMessages = nil
	m.tailOffsets = nil
	m.tailPaused = false
	m.tailPinned = false
	m.tailErr = ""
}

// renderTailView is the Topics-tab content while the tail view is open.
func (m *Model) renderTailView() string {
	status := "following"
	if m.tailPaused {
		status = "paused"
	}

	summary := ""
	if n := len(m.tailMessages); n > 0 {
		summary = fmt.Sprintf(" │ %d msgs │ last %s", n, m.tailMessages[n-1].Timestamp.UTC().Format("15:04:05.000"))
	}
	if offs := m.tailOffsets; len(offs) > 0 {
		parts := make([]string, 0, len(offs))
		for p, o := range offs {
			parts = append(parts, fmt.Sprintf("p%d→%d", p, o))
		}
		sort.Strings(parts)
		summary += " │ " + strings.Join(parts, " ")
	}

	title := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render(fmt.Sprintf("▬ TAIL %s — %s%s", m.tailTopic, status, summary))

	body := "no messages"
	if m.tailView != nil {
		body = m.tailView.View()
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		body,
		helpStyle.Render("  esc: close  │  p: pause/resume  │  j/k: scroll  │  g: bottom"),
	)
}
