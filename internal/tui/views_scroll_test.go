// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sizedModel returns a model sized to the 80x24 acceptance-criteria terminal
// with n seeded topics.
func sizedTopicsModel(t *testing.T, n int) *Model {
	t.Helper()
	m := NewModelWithStore(nil)
	topics := make([]TopicRow, n)
	for i := range topics {
		topics[i] = TopicRow{Name: topicName(i), Partitions: 3}
	}
	m.topics = topics
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = tm.(*Model)
	m.activeTab = 1
	return m
}

func topicName(i int) string {
	return "topic-" + strings.Repeat("x", 2) + string(rune('a'+i%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestTableHeightsBoundedAtSmallTerminal(t *testing.T) {
	m := sizedTopicsModel(t, 40)

	// Topics budget at 80x24: header+tabs+help (3) + title (3) + hint (1).
	assert.LessOrEqual(t, m.topicsTable.Height()+1, m.height-7,
		"topics table must fit the content region")

	m.activeTab = 2
	m.consumerGroups = make([]ConsumerGroupRow, 40)
	for i := range m.consumerGroups {
		m.consumerGroups[i] = ConsumerGroupRow{Group: topicName(i)}
	}
	m.buildTables()
	assert.LessOrEqual(t, m.groupsTable.Height()+1, m.height-6,
		"consumers table must fit the content region")
}

func TestJKScrollsBoundedTableToLastRow(t *testing.T) {
	const n = 40
	m := sizedTopicsModel(t, n)
	require.Less(t, m.topicsTable.Height(), n, "precondition: table scrolls")

	last := topicName(n - 1)
	for i := 0; i < n-1; i++ {
		tm, _ := m.Update(key("j"))
		m = tm.(*Model)
	}
	assert.Equal(t, n-1, m.topicsTable.Cursor(), "j reaches the last row")

	view := m.renderContent()
	assert.Contains(t, view, last, "last row scrolled into view")

	for i := 0; i < n-1; i++ {
		tm, _ := m.Update(key("k"))
		m = tm.(*Model)
	}
	assert.Equal(t, 0, m.topicsTable.Cursor(), "k returns to the first row")
	assert.Contains(t, m.renderContent(), topicName(0), "first row back in view")
}

func TestBuildTablesPreservesCursorAcrossRefresh(t *testing.T) {
	m := sizedTopicsModel(t, 6)
	tm, _ := m.Update(key("j"))
	tm, _ = tm.Update(key("j"))
	m = tm.(*Model)
	require.Equal(t, 2, m.topicsTable.Cursor())

	// The 2s tick delivers fresh data; tables are rebuilt.
	tm, _ = m.Update(DataUpdated{Topics: m.topics})
	m = tm.(*Model)

	assert.Equal(t, 2, m.topicsTable.Cursor(),
		"refresh must not reset the row selection")
}

func TestNoFooterOverlapAt80x24(t *testing.T) {
	cases := []struct {
		tab   int
		seed  func(m *Model)
		label string
	}{
		{1, func(m *Model) { m.topics = manyTopics(30) }, "topics"},
		{2, func(m *Model) {
			m.consumerGroups = make([]ConsumerGroupRow, 30)
			for i := range m.consumerGroups {
				m.consumerGroups[i] = ConsumerGroupRow{Group: topicName(i)}
			}
		}, "consumers"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			m := NewModelWithStore(nil)
			tc.seed(m)
			tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
			m = tm.(*Model)
			m.activeTab = tc.tab

			view := m.View()
			lines := strings.Split(view, "\n")
			assert.Len(t, lines, 24, "view must fill exactly the terminal height")
			assert.True(t, strings.HasSuffix(strings.TrimRight(lines[len(lines)-1], " "), "q: quit"),
				"footer help bar must be the last visible line")
		})
	}
}

// TestSingleAutoRefreshIndicator asserts SP-09 acceptance: the "Auto-refresh"
// cadence appears exactly once and only in the footer, never in the header.
func TestSingleAutoRefreshIndicator(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 80

	for tab := 0; tab < 6; tab++ {
		m.activeTab = tab
		header := m.renderHeader()
		footer := m.renderHelp()
		assert.NotContains(t, header, "Auto-refresh", "tab %d header must not repeat the cadence", tab)
		assert.Contains(t, footer, "Auto-refresh: "+autoRefreshInterval.String(), "tab %d footer must show the cadence", tab)
		assert.Equal(t, 1, strings.Count(header+footer, "Auto-refresh"),
			"tab %d must expose exactly one Auto-refresh indicator", tab)
	}
}

func TestContextualHelpBar(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 80

	m.activeTab = 5
	help := m.renderHelp()
	assert.Contains(t, help, "a: analyze")
	assert.Contains(t, help, "w: window")

	m.activeTab = 1
	assert.Contains(t, m.renderHelp(), "enter: tail")
	assert.Contains(t, m.renderHelp(), "/: search")

	m.activeTab = 4
	assert.Contains(t, m.renderHelp(), "enter: inspect")

	m.activeTab = 0
	assert.NotContains(t, m.renderHelp(), "\n", "help stays a single line")
}

// TestHelpBarAlwaysShowsGlobalKeysAndNoStaleKeys asserts the footer for every
// table tab keeps the global keys (1-6 jump, ? help, q quit) and the single
// auto-refresh indicator, and never advertises the Analytics-only "a" key
// (SP-07 acceptance criteria 3; SP-09 moved the refresh indicator to the
// footer and dropped the now-redundant "r: refresh" hint).
func TestHelpBarAlwaysShowsGlobalKeysAndNoStaleKeys(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 80

	for tab := 0; tab < 5; tab++ {
		m.activeTab = tab
		help := m.renderHelp()
		assert.Contains(t, help, "1-6: jump", "tab %d must keep the 1-6 global key", tab)
		assert.Contains(t, help, "Auto-refresh: 2s", "tab %d must show the auto-refresh cadence", tab)
		assert.Contains(t, help, "?: help", "tab %d must advertise the ? help modal", tab)
		assert.Contains(t, help, "q: quit", "tab %d must keep the q global key", tab)
		assert.NotContains(t, help, "a: analyze", "tab %d must not advertise the analytics-only a key", tab)
		assert.NotContains(t, help, "\n", "tab %d help stays a single line", tab)
	}
}

// TestHelpBarFollowsOverlay asserts the footer switches to the active overlay's
// keys (tail, DLQ inspect) and drops the tab-list keys (SP-07 criterion 1).
func TestHelpBarFollowsOverlay(t *testing.T) {
	m := NewModelWithStore(nil)
	m.activeTab = 1

	m.openTailView("orders")
	help := m.renderHelp()
	assert.Contains(t, help, "p: pause/resume")
	assert.Contains(t, help, "esc: back")
	assert.NotContains(t, help, "/: search", "tail overlay must not advertise the topics table search")
	m.closeTailView()

	m.dlqView = &viewport.Model{}
	help = m.renderHelp()
	assert.Contains(t, help, "r: replay")
	assert.NotContains(t, help, "enter: inspect", "DLQ inspect overlay must not advertise the DLQ list enter key")
}

// ─── Pagination (Showing N of M + PgUp/PgDn) ───────────────────────────────

func TestPgUpPgDownPagesThroughLargeList(t *testing.T) {
	const n = 40
	m := sizedTopicsModel(t, n)
	page := m.topicsTable.Height()
	require.Greater(t, page, 0, "precondition: table has a visible page size")
	require.Less(t, page, n, "precondition: table scrolls")

	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	m = tm.(*Model)
	assert.Equal(t, page, m.topicsTable.Cursor(), "pgdown pages one screen down")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	m = tm.(*Model)
	assert.Equal(t, 2*page, m.topicsTable.Cursor(), "second pgdown pages again")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = tm.(*Model)
	assert.Equal(t, page, m.topicsTable.Cursor(), "pgup pages one screen up")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = tm.(*Model)
	assert.Equal(t, 0, m.topicsTable.Cursor(), "pgup returns to the top")
}

func TestPgDownClampsAtLastRow(t *testing.T) {
	m := sizedTopicsModel(t, 40)
	m.topicsTable.GotoBottom()
	last := m.topicsTable.Cursor()

	for i := 0; i < 5; i++ {
		tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
		m = tm.(*Model)
	}
	assert.Equal(t, last, m.topicsTable.Cursor(), "pgdown clamps at the last row")

	m.topicsTable.GotoTop()
	for i := 0; i < 5; i++ {
		tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
		m = tm.(*Model)
	}
	assert.Equal(t, 0, m.topicsTable.Cursor(), "pgup clamps at the first row")
}

func TestTopicsViewShowsPaginationIndicator(t *testing.T) {
	m := sizedTopicsModel(t, 30)

	view := m.renderContent()
	assert.Contains(t, view, "Showing 30 of 30")
}

func TestConsumersViewShowsPaginationIndicator(t *testing.T) {
	m := NewModelWithStore(nil)
	m.consumerGroups = make([]ConsumerGroupRow, 30)
	for i := range m.consumerGroups {
		m.consumerGroups[i] = ConsumerGroupRow{Group: topicName(i)}
	}
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = tm.(*Model)
	m.activeTab = 2

	view := m.renderContent()
	assert.Contains(t, view, "Showing 30 of 30")
}

func TestPaginationIndicatorCountsFilteredTopics(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}, {Name: "orders.dlq"}, {Name: "audit"}}
	m.searchQuery = "order"
	m.activeTab = 1
	m.buildTables()

	view := m.renderContent()
	assert.Contains(t, view, "Showing 2 of 4", "filtered count must update after search")
}

func TestSearchFooterShowsMatchCount(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}, {Name: "orders.dlq"}, {Name: "audit"}}
	m.searching = true
	m.searchQuery = "orders"

	help := m.renderHelp()
	assert.Contains(t, help, "orders — 2 of 4", "footer must show query and match count while searching")
	assert.Contains(t, help, "case-insensitive", "help text must communicate case-insensitivity")
}

func TestSearchNoMatchesShowsDistinctState(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}}
	m.searchQuery = "zzz"
	m.activeTab = 1
	m.buildTables()

	view := m.renderContent()
	assert.Contains(t, view, "No match /zzz/", "zero matches must show a distinct state")
	assert.NotContains(t, view, "No data")
}

func manyTopics(n int) []TopicRow {
	rows := make([]TopicRow, n)
	for i := range rows {
		rows[i] = TopicRow{Name: topicName(i), Partitions: 1}
	}
	return rows
}
func TestAnalyticsScrollsToBottomPanesAt80x24(t *testing.T) {
	m := NewModelWithStore(nil)
	m.analytics = []analytics.GrowthReport{
		{Topic: "orders", Delta: 10, Sparkline: "▁▃▅▇█"},
		{Topic: "payments", Delta: 8, Sparkline: "▁▃▅▇█"},
	}
	m.anomalies = []analytics.Anomaly{
		{Entity: "g1", Metric: "lag", Value: 500, Expected: 100, ZScore: 4.1, Direction: "high", Severity: "critical"},
	}
	m.rebalances = make([]analytics.RebalanceReport, 30)
	for i := range m.rebalances {
		m.rebalances[i] = analytics.RebalanceReport{Group: topicName(i)}
	}
	m.patterns = []analytics.ThroughputReport{{Topic: "orders", Metric: "msg_rate", PeakHour: 9}}

	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = tm.(*Model)
	m.activeTab = 5

	require.NotNil(t, m.analyticsView, "window size creates the analytics viewport")
	m.renderAnalyticsView() // prime the viewport with pane content
	require.False(t, m.analyticsView.AtBottom(), "precondition: content overflows")
	assert.NotContains(t, m.analyticsView.View(), "REBALANCES",
		"bottom panes hidden before scrolling")
	assert.NotContains(t, m.analyticsView.View(), "PATTERNS",
		"bottom panes hidden before scrolling")

	// Scroll partway: REBALANCES and PATTERNS come into view.
	for i := 0; i < 30; i++ {
		tm, _ = m.Update(key("j"))
		m = tm.(*Model)
	}
	assert.Greater(t, m.analyticsView.YOffset, 0, "j scrolls the analytics view")
	scrolled := m.analyticsView.View()
	assert.Contains(t, scrolled, "REBALANCES", "rebalances pane reachable via scroll")
	assert.Contains(t, scrolled, "PATTERNS", "patterns pane reachable via scroll")

	for i := 0; i < 200; i++ {
		tm, _ = m.Update(key("k"))
		m = tm.(*Model)
	}
	assert.Equal(t, 0, m.analyticsView.YOffset, "k returns to the top")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = tm.(*Model)
	assert.Greater(t, m.analyticsView.YOffset, 0, "down arrow scrolls too")
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
	m = tm.(*Model)
	assert.Equal(t, 0, m.analyticsView.YOffset, "up arrow scrolls back")
}

func TestBracketKeysCycleAnalyticsTopicSelection(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.activeTab = 5
	m.patterns = []analytics.ThroughputReport{{Topic: "orders"}, {Topic: "payments"}, {Topic: "audit"}}

	tm, _ := m.Update(key("]"))
	m = tm.(*Model)
	assert.Equal(t, 1, m.patternIdx, "] selects the next pattern topic")

	tm, _ = m.Update(key("]"))
	m = tm.(*Model)
	assert.Equal(t, 2, m.patternIdx)

	tm, _ = m.Update(key("]"))
	m = tm.(*Model)
	assert.Equal(t, 0, m.patternIdx, "] wraps around")

	tm, _ = m.Update(key("["))
	m = tm.(*Model)
	assert.Equal(t, 2, m.patternIdx, "[ cycles backwards")

	m.patterns = nil
	m.analytics = []analytics.GrowthReport{{Topic: "a", Delta: 1}, {Topic: "b", Delta: 2}}
	tm, _ = m.Update(key("]"))
	m = tm.(*Model)
	assert.Equal(t, 1, m.selectedTopic, "] falls back to the growth selection")
	tm, _ = m.Update(key("["))
	m = tm.(*Model)
	assert.Equal(t, 0, m.selectedTopic)
}

func TestAnalyticsViewportResizesOnWindowSizeMsg(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = tm.(*Model)
	require.NotNil(t, m.analyticsView)

	tm, _ = m.Update(tea.WindowSizeMsg{Width: 120, Height: 50})
	m = tm.(*Model)
	assert.Equal(t, 120, m.analyticsView.Width, "resize updates viewport width")
	assert.Equal(t, 47, m.analyticsView.Height, "resize updates viewport height")
}
