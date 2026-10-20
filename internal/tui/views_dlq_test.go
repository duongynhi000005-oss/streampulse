// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"context"
	"errors"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pulsedev/streampulse/internal/dlq"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDiscover returns a canned discovery result regardless of the client.
func fakeDiscover(topics []dlq.Topic, err error) func(context.Context, dlq.Client, []string) ([]dlq.Topic, error) {
	return func(context.Context, dlq.Client, []string) ([]dlq.Topic, error) {
		return topics, err
	}
}

func TestFetchDLQPopulatesRows(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.discoverDLQ = fakeDiscover([]dlq.Topic{
		{Name: "payments.dlq", OriginalTopic: "payments", OriginalExists: true, MessageCount: 42},
		{Name: "orders.error", OriginalTopic: "orders", OriginalExists: true, MessageCount: 7, GrowthRate: 3.5},
	}, nil)

	rows, err := m.fetchDLQ(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 2)

	assert.Equal(t, "payments.dlq", rows[0].Topic)
	assert.Equal(t, "42", rows[0].MessageCount)
	assert.Equal(t, "-", rows[0].Growth, "zero growth rate renders as dash")

	assert.Equal(t, "orders.error", rows[1].Topic)
	assert.Equal(t, "7", rows[1].MessageCount)
	assert.Equal(t, "3.5", rows[1].Growth, "non-zero growth rate is shown")
}

func TestDLQTableHasNoErrorPatternColumn(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.ready = true
	m.dlqTopics = []DLQRow{{Topic: "payments.dlq", MessageCount: "42", Growth: "3.5"}}
	m.buildTables()
	m.activeTab = 4

	view := m.renderContent()
	assert.Contains(t, view, "DLQ TOPIC")
	assert.Contains(t, view, "MESSAGES")
	assert.Contains(t, view, "GROWTH")
	assert.NotContains(t, view, "ERROR PATTERN", "dead ERROR PATTERN column is removed")
}

func TestFetchDLQDiscoveryError(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.discoverDLQ = fakeDiscover(nil, errors.New("boom"))

	rows, err := m.fetchDLQ(context.Background())
	require.Error(t, err)
	assert.Nil(t, rows)
}

func TestNewModelWithKafkaStoresBrokers(t *testing.T) {
	c := kafka.NewClient([]string{"kafka:9092", "kafka:9093"})
	m := NewModelWithKafka(c)
	assert.Equal(t, []string{"kafka:9092", "kafka:9093"}, m.brokerAddrs)
}

// openDLQView opens the inspect view for the given topic through Update,
// dispatching and consuming the inspect command.
func openDLQView(t *testing.T, m *Model, topic string, msgs []dlq.Message, err error) {
	t.Helper()
	m.dlqTopics = []DLQRow{{Topic: topic, MessageCount: "42"}}
	m.ready = true
	m.width = 120
	m.height = 40
	m.buildTables()
	m.activeTab = 4
	m.dlqInspectFn = func(ctx context.Context, brokers []string, topic string, limit int) ([]dlq.Message, error) {
		assert.Equal(t, m.brokerAddrs, brokers)
		assert.Equal(t, 10, limit, "inspect view requests the default limit")
		return msgs, err
	}

	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = tm.(*Model)
	require.NotNil(t, cmd, "ENTER must dispatch the inspect command")
	require.NotNil(t, m.dlqView, "ENTER must open the inspect view")
	require.Equal(t, topic, m.dlqTopic)

	msg := cmd()
	require.IsType(t, dlqInspectMsg{}, msg)
	tm, _ = m.Update(msg)
	m = tm.(*Model)
}

func TestDLQEnterOpensInspectViewWithMessages(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	ts := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	openDLQView(t, m, "payments.dlq", []dlq.Message{
		{Topic: "payments.dlq", Partition: 0, Offset: 5, Key: []byte("k1"), Value: []byte(`{"error":"DB_TIMEOUT"}`), Timestamp: ts},
	}, nil)

	content := m.dlqView.View()
	assert.Contains(t, content, "payments.dlq")
	assert.Contains(t, content, "p0 @ off 5")
	assert.Contains(t, content, `{"error":"DB_TIMEOUT"}`, "value renders via DisplayValue")
	assert.Contains(t, content, "k1", "key renders via DisplayValue")
}

func TestDLQEnterInspectErrorShownInView(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	openDLQView(t, m, "payments.dlq", nil, errors.New("dial timeout"))

	assert.Contains(t, m.dlqView.View(), "dial timeout")
}

func TestDLQEnterOnEmptyTableDoesNothing(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.ready = true
	m.buildTables()
	m.activeTab = 4

	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = tm.(*Model)
	assert.Nil(t, m.dlqView, "no DLQ rows -> ENTER must not open the inspect view")
	assert.Nil(t, cmd)
}

func TestDLQEscClosesInspectView(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	openDLQView(t, m, "payments.dlq", nil, nil)

	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = tm.(*Model)
	assert.Nil(t, m.dlqView)
	assert.Empty(t, m.dlqTopic)
	assert.False(t, m.dlqConfirm)
}

func TestDLQQQuitsInsteadOfClosing(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	openDLQView(t, m, "payments.dlq", nil, nil)

	tm, cmd := m.Update(key("q"))
	m = tm.(*Model)
	assert.NotNil(t, m.dlqView, "q must not close the inspect view")
	assert.Equal(t, "payments.dlq", m.dlqTopic, "q must not clear the DLQ topic")
	require.NotNil(t, cmd, "q must quit the app")
	assert.IsType(t, tea.QuitMsg{}, cmd())
}

func TestDLQReplayConfirmAndCancel(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	openDLQView(t, m, "payments.dlq", nil, nil)

	// R asks for confirmation; n cancels without dispatching anything.
	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = tm.(*Model)
	assert.True(t, m.dlqConfirm)
	assert.Nil(t, cmd, "confirm state must not dispatch yet")

	tm, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	m = tm.(*Model)
	assert.False(t, m.dlqConfirm)
	assert.Nil(t, cmd)

	// R again, y confirms and dispatches the replay command.
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = tm.(*Model)
	require.True(t, m.dlqConfirm)

	m.dlqReplayFn = func(ctx context.Context, opts dlq.ReplayOptions) (*dlq.ReplayResult, error) {
		assert.Equal(t, m.brokerAddrs, opts.Brokers)
		assert.Equal(t, "payments.dlq", opts.Topic)
		assert.False(t, opts.DryRun)
		assert.Equal(t, 10, opts.Limit)
		return &dlq.ReplayResult{DryRun: false, Total: 42, Replayed: 40, Failed: 2, Batches: 1}, nil
	}

	tm, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = tm.(*Model)
	require.NotNil(t, cmd, "y must dispatch the replay command")
	assert.False(t, m.dlqConfirm, "confirm resets once replay is dispatched")

	msg := cmd()
	require.IsType(t, dlqReplayMsg{}, msg)
	tm, _ = m.Update(msg)
	m = tm.(*Model)

	assert.Contains(t, m.dlqView.View(), "replayed 40 of 42")
	assert.True(t, m.dlqView != nil, "replay result renders inside the inspect view")
}

func TestDLQReplayErrorShownInView(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	openDLQView(t, m, "payments.dlq", nil, nil)

	m.dlqReplayFn = func(ctx context.Context, opts dlq.ReplayOptions) (*dlq.ReplayResult, error) {
		return nil, errors.New("original topic does not exist")
	}
	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = tm.(*Model)
	tm, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = tm.(*Model)
	require.NotNil(t, cmd)

	msg := cmd()
	tm, _ = m.Update(msg)
	m = tm.(*Model)

	assert.Contains(t, m.dlqView.View(), "original topic does not exist")
	assert.False(t, m.dlqConfirm)
}

func TestDLQViewJKMovesViewport(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	msgs := make([]dlq.Message, 50)
	for i := range msgs {
		msgs[i] = dlq.Message{Topic: "t.dlq", Partition: 0, Offset: int64(i), Value: []byte("x")}
	}
	openDLQView(t, m, "t.dlq", msgs, nil)
	require.Equal(t, 0, m.dlqView.YOffset)

	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Greater(t, m.dlqView.YOffset, 0, "j scrolls the inspect viewport down")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	m = tm.(*Model)
	assert.Less(t, m.dlqView.YOffset, 50, "k scrolls the inspect viewport up")
}
