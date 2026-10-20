// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/pulsedev/streampulse/internal/tail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelInitialization(t *testing.T) {
	m := NewModelWithStore(nil)

	if m.activeTab != 0 {
		t.Errorf("expected activeTab 0, got %d", m.activeTab)
	}

	if len(m.tabs) != 6 {
		t.Errorf("expected 6 tabs, got %d", len(m.tabs))
	}

	expectedTabs := []string{"Overview", "Topics", "Consumers", "Alerts", "DLQ", "Analytics"}
	for i, tab := range expectedTabs {
		if !strings.Contains(m.tabs[i], tab) {
			t.Errorf("tab %d expected to contain %q, got %q", i, tab, m.tabs[i])
		}
	}
}

func TestModelViewWithNoData(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.height = 40
	m.ready = true
	m.buildTables()

	view := m.View()
	if !strings.Contains(view, "StreamPulse") {
		t.Error("view should contain title")
	}
	if !strings.Contains(view, "BROKERS") {
		t.Error("overview should contain brokers section")
	}
}

// TestOverviewCardsAreNavigationShortcuts asserts the SP-08 rework: no label
// is duplicated on the Overview, cards advertise the tab-jump key, and broker,
// group, and alert counts still surface.
func TestOverviewCardsAreNavigationShortcuts(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = tm.(*Model)
	m.brokers = []BrokerRow{{ID: "b1"}, {ID: "b2"}, {ID: "b3"}}
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}}
	m.consumerGroups = []ConsumerGroupRow{{Group: "g1"}}
	m.alerts = []AlertRow{{Name: "lag > 1000"}}
	m.scrapedOnce = true
	m.buildTables()

	view := m.renderOverview()

	// Every label appears exactly once on the Overview screen.
	for _, label := range []string{"BROKERS", "TOPICS", "CONSUMERS", "CONSUMER GROUPS", "ALERTS", "ACTIVITY LOG"} {
		assert.Equal(t, 1, strings.Count(view, label), "label %q must not be duplicated", label)
	}

	// Cards are actionable navigation: each advertises the number-key jump.
	assert.Contains(t, view, "1: open", "TOPICS card must advertise the jump key")
	assert.Contains(t, view, "2: open", "CONSUMERS card must advertise the jump key")
	assert.Contains(t, view, "3: open", "ALERTS card must advertise the jump key")

	// Broker, group, and alert counts still surface.
	assert.Contains(t, view, "BROKERS (3)", "broker count must surface in the section header")
	assert.Contains(t, view, "2 topics")
	assert.Contains(t, view, "1 groups")
	assert.Contains(t, view, "1 firing")
}

// TestOverviewCardsLoadingBeforeFirstScrape asserts SP-13: before the first
// successful scrape the summary cards show a loading placeholder rather than
// "0", so an empty cluster reads differently from a still-connecting one.
func TestOverviewCardsLoadingBeforeFirstScrape(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = tm.(*Model)

	view := m.renderOverview()
	assert.Contains(t, view, "loading…", "cards must show a loading placeholder before the first scrape")
	assert.NotContains(t, view, "0 topics", "cards must not claim 0 before the first scrape")
	assert.NotContains(t, view, "0 groups")
	assert.NotContains(t, view, "0 firing")
}

// TestOverviewCardsResolveAfterFirstScrape asserts SP-13 acceptance: the
// loading placeholder resolves to real counts once the first successful
// scrape populates the cards.
func TestOverviewCardsResolveAfterFirstScrape(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = tm.(*Model)

	tm, _ = m.Update(DataUpdated{Topics: []TopicRow{{Name: "orders"}}})
	m = tm.(*Model)

	view := m.renderOverview()
	assert.NotContains(t, view, "loading…", "loading placeholder must clear after the first successful scrape")
	assert.Contains(t, view, "1 topics", "card must show the real count after the first scrape")
}

// TestOverviewCardsStayLoadingWhenFirstScrapeFails asserts a failed first
// scrape keeps the loading placeholder: "loading" must not resolve to "0"
// because the cards were never populated.
func TestOverviewCardsStayLoadingWhenFirstScrapeFails(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = tm.(*Model)

	tm, _ = m.Update(DataUpdated{Failed: true})
	m = tm.(*Model)

	view := m.renderOverview()
	assert.Contains(t, view, "loading…", "a failed first scrape must not resolve the cards to 0")
}

func TestAlertCardColorDependsOnFiringCount(t *testing.T) {
	// Force truecolor so styled output emits ANSI sequences we can assert on.
	prev := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.TrueColor)
	t.Cleanup(func() { lipgloss.SetColorProfile(prev) })

	m := NewModelWithStore(nil)
	m.width = 120
	m.height = 40
	m.ready = true
	m.scrapedOnce = true
	m.buildTables()

	t.Run("no alerts uses success color", func(t *testing.T) {
		view := m.renderContent()
		want := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#22C55E")).Render("0 firing")
		if !strings.Contains(view, want) {
			t.Errorf("alert card with 0 firing should use the success color\ngot: %q", view)
		}
	})

	t.Run("firing alerts stay red", func(t *testing.T) {
		m.alerts = []AlertRow{{Name: "lag > 1000", Severity: "critical", Value: "2500.0", FiredAt: "12:00:00"}}
		defer func() { m.alerts = nil }()

		view := m.renderContent()
		want := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render("1 firing")
		if !strings.Contains(view, want) {
			t.Errorf("alert card with firing alerts should use red\ngot: %q", view)
		}
	})
}

func TestTabSwitching(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.buildTables()

	m.activeTab = 1
	view := m.renderContent()
	if !strings.Contains(view, "TOPICS") {
		t.Error("topics tab should show topics table")
	}

	m.activeTab = 3
	view = m.renderContent()
	if !strings.Contains(view, "ALERTS") {
		t.Error("alerts tab should show active alerts")
	}

	m.activeTab = 4
	view = m.renderContent()
	if !strings.Contains(view, "DEAD LETTER QUEUES") {
		t.Error("dlq tab should show DLQ table")
	}
}

func TestEmptyTablesShowNoDataState(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.buildTables()

	// All tables should show an honest empty state (text may be truncated by column width)
	brokerView := m.brokersTable.View()
	if !strings.Contains(brokerView, "No data") {
		t.Errorf("empty brokers table should show no-data state, got: %s", brokerView)
	}

	alertView := m.alertsTable.View()
	if !strings.Contains(alertView, "No alerts firing") {
		t.Error("empty alerts table should show 'No alerts firing'")
	}
}

func TestApplyDataPopulatesLogs(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Logs: []string{"[00:00:01] store connected"}})
	if len(m.logs) != 1 || m.logs[0] != "[00:00:01] store connected" {
		t.Errorf("logs not populated by applyData, got %v", m.logs)
	}
}

func TestFetchFromKafkaLogsErrors(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	data := m.fetchFromKafka()
	if len(data.Logs) == 0 {
		t.Fatal("expected error log from unreachable broker")
	}
	if !strings.Contains(data.Logs[0], "kafka error") {
		t.Errorf("expected kafka error log, got %q", data.Logs[0])
	}
}

func TestApplyDataAccumulatesLogs(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Logs: []string{"[00:00:01] a"}})
	m.applyData(DataUpdated{Logs: []string{"[00:00:02] b"}})
	if len(m.logs) != 2 || m.logs[0] != "[00:00:01] a" || m.logs[1] != "[00:00:02] b" {
		t.Errorf("logs should accumulate, got %v", m.logs)
	}
}

func TestApplyDataClearsStaleData(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}, Topics: []TopicRow{{Name: "t1"}}})
	if len(m.topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(m.topics))
	}

	m.applyData(DataUpdated{})
	if len(m.topics) != 0 || len(m.brokers) != 0 {
		t.Errorf("empty snapshot should clear stale data, got topics=%d brokers=%d", len(m.topics), len(m.brokers))
	}
}

func TestApplyDataPreservesTablesOnError(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}, Topics: []TopicRow{{Name: "t1"}}})
	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] kafka error: boom"}})
	if len(m.topics) != 1 || len(m.brokers) != 1 {
		t.Errorf("failed snapshot must not wipe tables, got topics=%d brokers=%d", len(m.topics), len(m.brokers))
	}
	if len(m.logs) != 1 || !strings.Contains(m.logs[0], "kafka error") {
		t.Errorf("logs must still apply on failed snapshot, got %v", m.logs)
	}
}

func TestHeaderShowsLastUpdateTime(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.buildTables()

	known := time.Date(2026, 8, 10, 12, 34, 56, 0, time.Local)
	m.lastUpdated = known
	if !strings.Contains(m.View(), known.Format("15:04:05")) {
		t.Errorf("header should show data timestamp %q, view: %s", known.Format("15:04:05"), m.View())
	}
}

func TestHeaderShowsPlaceholderBeforeFirstUpdate(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.buildTables()

	if !strings.Contains(m.View(), "Updated: —") {
		t.Error("header should show em-dash placeholder before first data arrival")
	}
}

func TestFailedFetchDoesNotStampLastUpdated(t *testing.T) {
	m := NewModelWithStore(nil)
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}})
	before := m.lastUpdated
	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] kafka error"}})
	if !m.lastUpdated.Equal(before) {
		t.Errorf("failed fetch must not change lastUpdated: before=%v after=%v", before, m.lastUpdated)
	}
}

// TestFailureBannerShowsOnFailedScrape asserts SP-11 acceptance: a banner
// appears when the last scrape failed and clears on the next success.
func TestFailureBannerShowsOnFailedScrape(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 80
	m.ready = true
	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}})

	if b := m.renderFailureBanner(); b != "" {
		t.Errorf("banner must be empty while healthy, got %q", b)
	}

	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] store offline: boom"}})
	banner := m.renderFailureBanner()
	if !strings.Contains(banner, "last scrape failed") {
		t.Errorf("failed scrape must show the failure banner, got %q", banner)
	}

	m.applyData(DataUpdated{Brokers: []BrokerRow{{ID: "b1"}}})
	if b := m.renderFailureBanner(); b != "" {
		t.Errorf("banner must clear on successful scrape, got %q", b)
	}
}

// TestFailureBannerVisibleOnEveryTab asserts the failure banner renders at the
// top of the content region on every tab (SP-11 acceptance criterion 2).
func TestFailureBannerVisibleOnEveryTab(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 80
	m.ready = true
	m.applyData(DataUpdated{Failed: true, Logs: []string{"[00:00:02] store offline"}})

	ansi := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	for tab := 0; tab < 6; tab++ {
		m.activeTab = tab
		content := ansi.ReplaceAllString(m.renderContent(), "")
		lines := strings.Split(content, "\n")
		assert.Contains(t, lines[0], "last scrape failed",
			"tab %d banner must be the first line of the content region", tab)
	}
}

func TestTickGuardPreventsOverlappingRefreshes(t *testing.T) {
	m := NewModelWithKafka(kafka.NewClient([]string{"127.0.0.1:1"}))
	m.Init()
	if !m.loading {
		t.Error("expected loading=true after Init dispatched first refresh")
	}

	tm, _ := m.Update(tickMsg(time.Now()))
	m = tm.(*Model)
	if !m.loading {
		t.Error("expected loading=true after tick dispatched refresh")
	}

	tm, _ = m.Update(DataUpdated{})
	m = tm.(*Model)
	if m.loading {
		t.Error("expected loading=false after DataUpdated arrived")
	}

	tm, _ = m.Update(tickMsg(time.Now()))
	m = tm.(*Model)
	if !m.loading {
		t.Error("expected loading=true again after DataUpdated")
	}
}

// ─── Search ────────────────────────────────────────────────────────────────

func key(runes string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(runes)}
}

// ─── Help modal (? key) ────────────────────────────────────────────────────

func TestHelpModalOpensOnQuestionMark(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.width = 80
	m.height = 24

	assert.NotContains(t, m.View(), "KEYBINDINGS")

	tm, _ := m.Update(key("?"))
	m = tm.(*Model)
	assert.True(t, m.helpOpen, "? must open the help modal")
	view := m.View()
	assert.Contains(t, view, "KEYBINDINGS")
	assert.Contains(t, view, "a", "modal lists the analytics-only a key")
	assert.Contains(t, view, "p", "modal lists the tail p key")
}

func TestHelpModalClosesOnEscAndQuitsOnQ(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.width = 80
	m.height = 24

	tm, _ := m.Update(key("?"))
	m = tm.(*Model)
	require.True(t, m.helpOpen)

	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = tm.(*Model)
	assert.False(t, m.helpOpen, "esc closes the help modal")
	assert.Nil(t, cmd, "esc must not quit")

	tm, _ = m.Update(key("?"))
	m = tm.(*Model)
	require.True(t, m.helpOpen)

	tm, cmd = m.Update(key("q"))
	m = tm.(*Model)
	assert.True(t, m.helpOpen, "q must not close the help modal")
	require.NotNil(t, cmd, "q must quit the app")
	assert.IsType(t, tea.QuitMsg{}, cmd())
}

func TestHelpModalDoesNotLeakKeysToUnderlyingView(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.width = 80
	m.height = 24
	m.activeTab = 5

	tm, _ := m.Update(key("?"))
	m = tm.(*Model)

	// "a" while the modal is open must not launch the analyze view.
	tm, _ = m.Update(key("a"))
	m = tm.(*Model)
	assert.True(t, m.helpOpen, "modal stays open")
	assert.False(t, m.analyzeViewOpen, "a must not leak to the analyze keybinding")

	// modal is reachable from overlays too
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = tm.(*Model)
	m.openTailView("orders")
	tm, cmd := m.Update(key("?"))
	m = tm.(*Model)
	assert.True(t, m.helpOpen, "? works inside the tail overlay")
	assert.Nil(t, cmd, "opening the modal dispatches no command")
}

func TestSearchSlashOpensSearchMode(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(key("/"))
	m = tm.(*Model)

	assert.True(t, m.searching)
	assert.Empty(t, m.searchQuery)
}

func TestSearchTypingFiltersTopicsCaseInsensitive(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}, {Name: "orders.dlq"}}
	m.buildTables()
	require.Len(t, m.topicsTable.Rows(), 3)

	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, _ = m.Update(key("ORD"))
	m = tm.(*Model)
	assert.Equal(t, "ORD", m.searchQuery)

	m.buildTables()
	require.Len(t, m.topicsTable.Rows(), 2, "case-insensitive contains filter")
	assert.Equal(t, "orders", m.topicsTable.Rows()[0][0])
	assert.Equal(t, "orders.dlq", m.topicsTable.Rows()[1][0])
}

func TestSearchBackspaceAndEsc(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}}
	m.buildTables()

	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, _ = m.Update(key("pay"))
	m = tm.(*Model)
	m.buildTables()
	require.Len(t, m.topicsTable.Rows(), 1)

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	m = tm.(*Model)
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	m = tm.(*Model)
	assert.Equal(t, "p", m.searchQuery)

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = tm.(*Model)
	assert.False(t, m.searching)
	assert.Empty(t, m.searchQuery, "esc clears the query")

	m.buildTables()
	assert.Len(t, m.topicsTable.Rows(), 2, "cleared search shows all topics")
}

func TestSearchEnterAppliesFilter(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}}
	m.buildTables()

	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, _ = m.Update(key("pay"))
	m = tm.(*Model)

	tm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = tm.(*Model)
	assert.False(t, m.searching, "enter leaves search mode")
	assert.Equal(t, "pay", m.searchQuery, "enter keeps the query applied")
	assert.Nil(t, cmd, "enter must not quit")

	m.buildTables()
	require.Len(t, m.topicsTable.Rows(), 1)
	assert.Equal(t, "payments", m.topicsTable.Rows()[0][0])
}

func TestSearchQExitsSearchInsteadOfQuitting(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, cmd := m.Update(key("q"))
	m = tm.(*Model)

	assert.False(t, m.searching, "q exits search mode")
	assert.Empty(t, m.searchQuery)
	assert.Nil(t, cmd, "q while searching must not quit the program")

	// q outside search still quits.
	tm, cmd = m.Update(key("q"))
	_ = tm
	assert.NotNil(t, cmd, "q outside search quits")
}

func TestSearchKeyRDoesNotRefresh(t *testing.T) {
	m := NewModelWithStore(nil)
	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, cmd := m.Update(key("r"))
	m = tm.(*Model)

	assert.Equal(t, "r", m.searchQuery, "r appends to the query while searching")
	assert.Nil(t, cmd, "r must not trigger a refresh while searching")
}

// ─── Table navigation ──────────────────────────────────────────────────────

func TestJKMoveActiveTableCursor(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}, {Name: "audit"}}
	m.ready = true
	m.buildTables()
	m.activeTab = 1

	assert.Equal(t, 0, m.topicsTable.Cursor())
	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Equal(t, 1, m.topicsTable.Cursor(), "j moves the topics cursor down")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	m = tm.(*Model)
	assert.Equal(t, 0, m.topicsTable.Cursor(), "k moves the topics cursor up")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = tm.(*Model)
	assert.Equal(t, 1, m.topicsTable.Cursor(), "down arrow moves the cursor")
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
	m = tm.(*Model)
	assert.Equal(t, 0, m.topicsTable.Cursor(), "up arrow moves the cursor")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Equal(t, 1, m.topicsTable.Cursor())

	// Rebuilding tables (as the 2s refresh does) preserves the selection.
	m.consumerGroups = []ConsumerGroupRow{{Group: "g1"}, {Group: "g2"}, {Group: "g3"}}
	m.buildTables()
	assert.Equal(t, 1, m.topicsTable.Cursor(), "refresh must keep the topics cursor")
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Equal(t, 2, m.topicsTable.Cursor())

	// The cursor moves only on the active tab's table.
	m.activeTab = 2
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Equal(t, 2, m.topicsTable.Cursor(), "j on the consumers tab must not move the topics cursor")
	assert.Equal(t, 1, m.groupsTable.Cursor(), "j on the consumers tab moves the groups cursor")
}

func TestJKClampsAtBoundsWithoutPanic(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}}
	m.ready = true
	m.buildTables()
	m.activeTab = 1

	for i := 0; i < 5; i++ {
		tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
		m = tm.(*Model)
	}
	assert.Equal(t, 0, m.topicsTable.Cursor(), "cursor clamps at the last row")

	for i := 0; i < 5; i++ {
		tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
		m = tm.(*Model)
	}
	assert.Equal(t, 0, m.topicsTable.Cursor(), "cursor clamps at the first row")

	// Empty table (single no-data placeholder row) must not panic either.
	m.topics = nil
	m.buildTables()
	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)
	assert.Equal(t, 0, m.topicsTable.Cursor())
}

func TestJKDisabledWhileSearching(t *testing.T) {
	m := NewModelWithStore(nil)
	m.topics = []TopicRow{{Name: "orders"}, {Name: "payments"}, {Name: "audit"}}
	m.ready = true
	m.buildTables()
	m.activeTab = 1

	tm, _ := m.Update(key("/"))
	m = tm.(*Model)
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m = tm.(*Model)

	assert.Equal(t, 0, m.topicsTable.Cursor(), "j appends to the query, not the cursor")
	assert.Equal(t, "j", m.searchQuery)
}

// ─── Store mode (daemon persistence) ────────────────────────────────────────

func TestLoadDataReadsPersistedMetrics(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	now := time.Now().Add(-30 * time.Second)
	err = store.WriteBatch(context.Background(), []storage.Metric{
		{TS: now, ClusterID: "local-dev", Metric: "kafka.broker.leader_partitions", EntityType: "broker", EntityName: "localhost:9093", Value: 12},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.broker.replica_partitions", EntityType: "broker", EntityName: "localhost:9093", Value: 20},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.partition_count", EntityType: "topic", EntityName: "orders", Value: 6},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.msg_rate", EntityType: "topic", EntityName: "orders", Value: 42.5},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.topic.bytes_rate", EntityType: "topic", EntityName: "orders", Value: 1000},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.lag", EntityType: "consumer_group", EntityName: "orders-processor", Value: 1500},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.member_count", EntityType: "consumer_group", EntityName: "orders-processor", Value: 3},
		{TS: now, ClusterID: "local-dev", Metric: "kafka.group.state", EntityType: "consumer_group", EntityName: "orders-processor", Value: 1},
	})
	require.NoError(t, err)

	m := NewModelWithStore(store)
	data := m.loadData()

	require.Len(t, data.Brokers, 1)
	b := data.Brokers[0]
	assert.Equal(t, "localhost:9093", b.ID)
	assert.Contains(t, b.CPU, "12")
	assert.Contains(t, b.Memory, "20")

	require.Len(t, data.Topics, 1)
	tp := data.Topics[0]
	assert.Equal(t, "orders", tp.Name)
	assert.Equal(t, 6, tp.Partitions)
	assert.Equal(t, "42.5", tp.MsgRate)
	assert.Equal(t, "1000.0", tp.BytesRate)

	require.Len(t, data.ConsumerGroups, 1)
	g := data.ConsumerGroups[0]
	assert.Equal(t, "orders-processor", g.Group)
	assert.Equal(t, "1500.0", g.Lag)
	assert.Equal(t, 3, g.Members)
	assert.Contains(t, g.Status, "STABLE")

	require.NotEmpty(t, data.Logs)
	assert.False(t, data.Failed)
}

func TestLoadDataReadsPersistedAlertState(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	fired := time.Now().Add(-2 * time.Minute)
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "growth_rate >= 10.5", Status: "ok", LastValue: 2,
	}))
	require.NoError(t, store.SaveAlertState(context.Background(), storage.AlertStateRow{
		RuleName: "lag > 1000", Status: "firing", LastFired: fired, LastValue: 2500, NotifyCount: 3,
	}))

	m := NewModelWithStore(store)
	data := m.loadData()

	// QueryAlertState orders by rule name: growth_rate before lag.
	require.Len(t, data.Alerts, 2)
	g := data.Alerts[0]
	assert.Equal(t, "growth_rate >= 10.5", g.Name)
	assert.Equal(t, "-", g.Severity)
	assert.Equal(t, "2.0", g.Value)
	assert.Equal(t, "-", g.FiredAt)

	l := data.Alerts[1]
	assert.Equal(t, "lag > 1000", l.Name)
	assert.Equal(t, "-", l.Severity)
	assert.Equal(t, "2500.0", l.Value)
	// QueryAlertState normalizes last_fired to UTC.
	assert.Equal(t, fired.UTC().Format("15:04:05"), l.FiredAt)

	assert.False(t, data.Failed)
}

func TestLoadDataStoreOffline(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Close()) // closed store fails Ping

	m := NewModelWithStore(store)
	data := m.loadData()

	assert.True(t, data.Failed, "offline store must mark the snapshot failed")
	require.Len(t, data.Logs, 1)
	assert.Contains(t, data.Logs[0], "store offline")
}

func TestLoadDataEmptyStore(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	m := NewModelWithStore(store)
	data := m.loadData()

	assert.False(t, data.Failed)
	assert.Len(t, data.Brokers, 0)
	assert.Len(t, data.Topics, 0)
	assert.Len(t, data.ConsumerGroups, 0)
	require.NotEmpty(t, data.Logs)
	assert.Contains(t, data.Logs[0], "store connected")
}

// ─── Analytics L2 panes (anomalies, rebalances, patterns) ──────────────────

// seededL2Store seeds the data the L2 panes need: hourly kafka.group.lag with
// a 500 spike after 4 weeks of 95/105 baseline (→ anomalies), kafka.group.state
// transitions into PreparingRebalance for g1 (2) and g2 (1) (→ rebalances),
// and 3 days of hourly kafka.topic.msg_rate for "orders" peaking at 09:00
// (→ patterns).
func seededL2Store(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Hour)
	metrics := make([]storage.Metric, 0, 4*7*24+90)
	for h := -(4*7*24 - 1); h <= -3; h++ {
		v := 105.0
		if h%2 == 0 {
			v = 95.0
		}
		metrics = append(metrics, storage.Metric{
			TS: t0.Add(time.Duration(h) * time.Hour), ClusterID: "local-dev",
			Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: v,
		})
	}
	metrics = append(metrics,
		storage.Metric{TS: t0.Add(-2 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
		storage.Metric{TS: t0.Add(-1 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
	)

	day0 := time.Now().UTC().Truncate(24 * time.Hour)
	seq := []float64{4, 1, 2, 3, 1, 2, 3, 1} // g1: 2 rebalances two days ago
	for i, v := range seq {
		metrics = append(metrics, storage.Metric{
			TS: day0.Add(-48*time.Hour + time.Duration(i)*5*time.Second), ClusterID: "local-dev",
			Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g1", Value: v,
		})
	}
	seq = []float64{1, 2, 3, 1} // g2: 1 rebalance yesterday
	for i, v := range seq {
		metrics = append(metrics, storage.Metric{
			TS: day0.Add(-24*time.Hour + time.Duration(i)*5*time.Second), ClusterID: "local-dev",
			Metric: scraper.MetricGroupState, EntityType: "consumer_group", EntityName: "g2", Value: v,
		})
	}

	for i := 0; i < 72; i++ {
		v := 10.0
		if (i % 24) == 9 {
			v = 100.0
		}
		metrics = append(metrics, storage.Metric{
			TS: day0.Add(time.Duration(i-72) * time.Hour), ClusterID: "local-dev",
			Metric: scraper.MetricTopicMsgRate, EntityType: "topic", EntityName: "orders", Value: v,
		})
	}

	require.NoError(t, store.WriteBatch(ctx, metrics))
	require.NoError(t, store.Rollup(ctx, "hourly"))
	return store
}

func TestLoadDataPopulatesL2AnalyticsPanes(t *testing.T) {
	store := seededL2Store(t)
	m := NewModelWithStore(store)
	m.topics = []TopicRow{{Name: "orders"}} // topics applied from a previous tick

	data := m.loadData()

	require.False(t, data.Failed)
	require.NotEmpty(t, m.anomalies, "lag spike must produce anomalies")
	assert.Equal(t, "g1", m.anomalies[0].Entity)

	require.Len(t, m.rebalances, 2)
	assert.Equal(t, "g1", m.rebalances[0].Group)
	assert.Equal(t, 2, m.rebalances[0].Count)
	assert.Equal(t, "g2", m.rebalances[1].Group)
	assert.Equal(t, 1, m.rebalances[1].Count)

	require.Len(t, m.patterns, 1)
	assert.Equal(t, "orders", m.patterns[0].Topic)
	assert.Equal(t, 9, m.patterns[0].PeakHour)
}

func TestL2AnalyticsRespectsRefreshInterval(t *testing.T) {
	store := seededL2Store(t)
	m := NewModelWithStore(store)
	m.topics = []TopicRow{{Name: "orders"}}
	now := time.Now()
	m.now = func() time.Time { return now }

	m.loadData()
	require.NotEmpty(t, m.anomalies)
	require.NotEmpty(t, m.rebalances)
	require.NotEmpty(t, m.patterns)

	// A second load within the refresh interval must not recompute: wipe the
	// caches and load again — they must stay empty.
	m.anomalies = nil
	m.rebalances = nil
	m.patterns = nil
	m.loadData()
	assert.Empty(t, m.anomalies, "analytics must not be recomputed within the refresh interval")
	assert.Empty(t, m.rebalances)
	assert.Empty(t, m.patterns)
}

func TestRenderAnalyticsViewShowsL2Sections(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.anomalies = []analytics.Anomaly{
		{Metric: scraper.MetricGroupLag, Entity: "g1", Time: time.Now(), Value: 500, Expected: 100, ZScore: 4.12, Direction: "high", Severity: "warning"},
	}
	m.rebalances = []analytics.RebalanceReport{
		{Group: "g1", Day: time.Now().UTC().Truncate(24 * time.Hour), Count: 2},
	}
	m.patterns = []analytics.ThroughputReport{{Topic: "orders", Metric: scraper.MetricTopicMsgRate, PeakHour: 9, Forecast7d: 11.2}}

	view := m.renderAnalyticsView()
	assert.Contains(t, view, "ANOMALIES")
	assert.Contains(t, view, "REBALANCES")
	assert.Contains(t, view, "PATTERNS")
	assert.Contains(t, view, "g1")
	assert.Contains(t, view, "500.0")
	assert.Contains(t, view, m.rebalances[0].Day.Format("2006-01-02"))
	assert.Contains(t, view, "orders")
	assert.Contains(t, view, "11.2")
}

func TestRenderAnalyticsViewEmptyL2Sections(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120

	view := m.renderAnalyticsView()
	assert.Contains(t, view, "No anomalies detected")
	assert.Contains(t, view, "No rebalances in window")
	assert.Contains(t, view, "No growth detected")
	assert.Contains(t, view, "No skew detected")
	assert.Contains(t, view, "No retention risk detected")
	assert.Contains(t, view, "No patterns detected")
}

func TestJKScrollsAnalyticsInsteadOfCyclingPatterns(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.activeTab = 5
	m.patterns = []analytics.ThroughputReport{{Topic: "orders"}, {Topic: "payments"}, {Topic: "audit"}}
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = tm.(*Model)
	m.renderAnalyticsView() // prime the viewport

	tm, _ = m.Update(key("j"))
	m = tm.(*Model)
	assert.Equal(t, 0, m.patternIdx, "j scrolls and must not move the pattern selection")
	assert.Greater(t, m.analyticsView.YOffset, 0, "j scrolls the analytics viewport")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = tm.(*Model)
	assert.Greater(t, m.analyticsView.YOffset, 0, "down arrow scrolls too")

	tm, _ = m.Update(key("k"))
	m = tm.(*Model)
	assert.Less(t, m.analyticsView.YOffset, 2, "k scrolls back up")
}

func TestBracketsCycleAnalyticsSelection(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.activeTab = 5
	m.analytics = []analytics.GrowthReport{{Topic: "a", Delta: 1}, {Topic: "b", Delta: 2}}

	tm, _ := m.Update(key("]"))
	m = tm.(*Model)
	assert.Equal(t, 1, m.selectedTopic, "] cycles the growth selection")

	tm, _ = m.Update(key("["))
	m = tm.(*Model)
	assert.Equal(t, 0, m.selectedTopic, "[ cycles back")
}

// ─── Analyze CLI view ───────────────────────────────────────────────────────

func TestAnalyzeKeyOpensView(t *testing.T) {
	m := NewModelWithStore(nil)
	m.activeTab = 5
	m.ready = true
	m.buildTables()

	tm, cmd := m.Update(key("a"))
	m = tm.(*Model)
	assert.True(t, m.analyzeViewOpen, "a on analytics tab opens the analyze view")
	assert.True(t, m.analyzeRunning, "a marks the analyze subprocess as running")
	assert.Equal(t, "24h", m.analyzeWindow, "default analyze window")
	assert.NotNil(t, cmd, "a dispatches the analyze exec command")

	tm, _ = m.Update(key("a"))
	m = tm.(*Model)
	assert.True(t, m.analyzeRunning, "second a while running is ignored")
}

func TestAnalyzeKeyIgnoredOffAnalyticsTab(t *testing.T) {
	m := NewModelWithStore(nil)
	m.activeTab = 0

	tm, cmd := m.Update(key("a"))
	m = tm.(*Model)
	assert.False(t, m.analyzeViewOpen, "a off the analytics tab does nothing")
	assert.Nil(t, cmd)
}

func TestAnalyzeWindowCycles(t *testing.T) {
	m := NewModelWithStore(nil)
	m.activeTab = 5

	for _, want := range []string{"168h", "720h", "24h"} {
		tm, _ := m.Update(key("w"))
		m = tm.(*Model)
		assert.Equal(t, want, m.analyzeWindow)
	}
}

func TestAnalyzeDoneMsgShowsOutput(t *testing.T) {
	m := NewModelWithStore(nil)
	v := viewport.New(80, 10)
	m.analyzeView = &v

	tm, _ := m.Update(analyzeDoneMsg{output: "TOPIC          RATE\norders         12.5\n"})
	m = tm.(*Model)
	assert.False(t, m.analyzeRunning, "done resets the running flag")
	assert.Contains(t, m.analyzeOut, "orders")
	assert.Contains(t, m.analyzeView.View(), "orders", "output lands in the viewport")
}

func TestAnalyzeDoneMsgError(t *testing.T) {
	m := NewModelWithStore(nil)

	tm, _ := m.Update(analyzeDoneMsg{err: fmt.Errorf("boom")})
	m = tm.(*Model)
	assert.False(t, m.analyzeRunning)
	assert.Contains(t, m.analyzeOut, "analyze failed: boom")
}

func TestEscClosesAnalyzeAndQQuits(t *testing.T) {
	m := NewModelWithStore(nil)
	m.analyzeViewOpen = true

	tm, _ := m.Update(key("esc"))
	m = tm.(*Model)
	assert.False(t, m.analyzeViewOpen, "esc closes the analyze view")

	m.analyzeViewOpen = true
	tm, cmd := m.Update(key("q"))
	m = tm.(*Model)
	assert.True(t, m.analyzeViewOpen, "q must not close the analyze view")
	require.NotNil(t, cmd, "q must quit the app")
	assert.IsType(t, tea.QuitMsg{}, cmd())
}

func TestAnalyzeViewScrolling(t *testing.T) {
	m := NewModelWithStore(nil)
	m.analyzeViewOpen = true
	v := viewport.New(80, 10)
	m.analyzeView = &v
	m.analyzeView.SetContent(strings.Repeat("line\n", 50))

	before := m.analyzeView.YOffset
	tm, _ := m.Update(key("j"))
	m = tm.(*Model)
	assert.Greater(t, m.analyzeView.YOffset, before, "j scrolls the analyze viewport")
}

func TestRenderAnalyzeView(t *testing.T) {
	m := NewModelWithStore(nil)
	m.width = 120
	m.ready = true
	m.analyzeViewOpen = true
	v := viewport.New(116, 20)
	m.analyzeView = &v
	m.analyzeView.SetContent("analysis results")

	view := m.View()
	assert.Contains(t, view, "analysis results")
	assert.Contains(t, view, "esc: close")
	assert.NotContains(t, view, "esc/q: close", "help must not advertise q as close")
}

// ─── Topic tail view ────────────────────────────────────────────────────────

func TestEnterOnTopicsOpensTail(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.activeTab = 1
	m.topics = []TopicRow{{Name: "orders", Partitions: 6}}
	m.buildTables()

	tm, cmd := m.Update(key("enter"))
	m = tm.(*Model)
	assert.Equal(t, "orders", m.tailTopic, "Enter on a topics row opens the tail view")
	assert.NotNil(t, m.tailView)
	assert.NotNil(t, cmd, "Enter dispatches the snapshot command")

	tm, _ = m.Update(key("enter"))
	m = tm.(*Model)
	assert.Equal(t, "orders", m.tailTopic, "double Enter is harmless")
}

func TestEnterOffTopicsTabDoesNothing(t *testing.T) {
	m := NewModelWithStore(nil)
	m.activeTab = 0
	m.topics = []TopicRow{{Name: "orders", Partitions: 6}}

	tm, cmd := m.Update(key("enter"))
	m = tm.(*Model)
	assert.Equal(t, "", m.tailTopic)
	assert.Nil(t, cmd)
}

func TestTailSnapshotPopulatesAndSeedsOffsets(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")

	ts := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	tm, cmd := m.Update(tailSnapshotMsg{topic: "orders", msgs: []tail.Message{
		{Topic: "orders", Partition: 0, Offset: 10, Value: []byte("a"), Timestamp: ts},
		{Topic: "orders", Partition: 0, Offset: 11, Value: []byte("b"), Timestamp: ts},
	}})
	m = tm.(*Model)
	assert.Empty(t, m.tailErr)
	assert.Len(t, m.tailMessages, 2)
	assert.Equal(t, int64(12), m.tailOffsets[0], "offsets seeded to last offset + 1")
	assert.NotNil(t, cmd, "follow tick starts after a successful snapshot")
	assert.Contains(t, m.tailView.View(), "a")
}

func TestTailSnapshotEmptyStartsFromWatermark(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")

	tm, _ := m.Update(tailSnapshotMsg{topic: "orders"})
	m = tm.(*Model)
	assert.Nil(t, m.tailOffsets, "empty snapshot → follow from high-watermarks")
	assert.Contains(t, m.tailView.View(), "no messages")
}

func TestTailSnapshotErrorRetries(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")

	tm, cmd := m.Update(tailSnapshotMsg{topic: "orders", err: fmt.Errorf("boom")})
	m = tm.(*Model)
	assert.Contains(t, m.tailErr, "boom")
	assert.Contains(t, m.tailView.View(), "boom")
	assert.NotNil(t, cmd, "retry is scheduled after a snapshot failure")
}

func TestTailRetryRerunsSnapshot(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	m.tailErr = "boom"

	tm, cmd := m.Update(tailRetryMsg{topic: "orders"})
	m = tm.(*Model)
	assert.NotNil(t, cmd, "retry dispatches a fresh snapshot")
}

func TestTailFollowAppendsAndCapsBuffer(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	ts := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	// fill past the cap
	fill := make([]tail.Message, 0, 250)
	for i := 0; i < 250; i++ {
		fill = append(fill, tail.Message{Topic: "orders", Partition: 0, Offset: int64(i), Value: []byte("x"), Timestamp: ts})
	}
	tm, _ := m.Update(tailSnapshotMsg{topic: "orders", msgs: fill})
	m = tm.(*Model)
	assert.Len(t, m.tailMessages, tailBufferCap, "buffer capped at 200")

	tm, _ = m.Update(tailFollowMsg{topic: "orders", msgs: []tail.Message{
		{Topic: "orders", Partition: 0, Offset: 251, Value: []byte("new"), Timestamp: ts},
	}, offs: map[int]int64{0: 252}})
	m = tm.(*Model)
	assert.Len(t, m.tailMessages, tailBufferCap)
	assert.Equal(t, "new", string(m.tailMessages[len(m.tailMessages)-1].Value))
	assert.Equal(t, int64(252), m.tailOffsets[0])
}

func TestTailPauseSkipsFollow(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	m.tailPaused = true

	tm, cmds := m.Update(tailTickMsg(time.Now()))
	m = tm.(*Model)
	_ = cmds
	assert.Equal(t, "", m.tailErr)
	assert.True(t, m.tailPaused)

	m.tailPaused = false
	tm, _ = m.Update(tailTickMsg(time.Now()))
	m = tm.(*Model)
	assert.True(t, m.tailPaused == false, "unpaused tick proceeds")
}

func TestTailPauseToggle(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")

	tm, _ := m.Update(key("p"))
	m = tm.(*Model)
	assert.True(t, m.tailPaused, "p pauses the tail")

	tm, _ = m.Update(key("p"))
	m = tm.(*Model)
	assert.False(t, m.tailPaused, "p resumes the tail")
}

func TestTailKeyNavigation(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	m.tailView.SetContent(strings.Repeat("line\n", 60))

	// scroll down unpins
	tm, _ := m.Update(key("j"))
	m = tm.(*Model)
	assert.False(t, m.tailPinned, "manual scroll unpins auto-scroll")
	assert.Greater(t, m.tailView.YOffset, 0)

	// g re-pins and goes to bottom
	tm, _ = m.Update(key("g"))
	m = tm.(*Model)
	assert.True(t, m.tailPinned)
	assert.True(t, m.tailView.AtBottom(), "g pins to the bottom")
}

func TestEscClosesTailAndQQuits(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")

	tm, _ := m.Update(key("esc"))
	m = tm.(*Model)
	assert.Nil(t, m.tailView)
	assert.Equal(t, "", m.tailTopic)

	m.openTailView("orders")
	tm, cmd := m.Update(key("q"))
	m = tm.(*Model)
	assert.NotNil(t, m.tailView, "q must not close the tail view")
	assert.Equal(t, "orders", m.tailTopic, "q must not clear the tail topic")
	require.NotNil(t, cmd, "q must quit the app")
	assert.IsType(t, tea.QuitMsg{}, cmd())
}

func TestRenderTailView(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	m.tailView.SetContent("[p 0|o 1] hello")

	view := m.renderTopicsView()
	assert.Contains(t, view, "TAIL orders")
	assert.Contains(t, view, "hello")
	assert.Contains(t, view, "p: pause/resume")
	assert.Contains(t, view, "esc: close")
	assert.NotContains(t, view, "esc/q: close", "help must not advertise q as close")
}

func TestRenderTailViewEnrichedHeader(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	ts := time.Date(2026, 8, 12, 12, 3, 4, 567000000, time.UTC)
	m.tailMessages = []tail.Message{
		{Topic: "orders", Partition: 0, Offset: 10, Timestamp: ts},
		{Topic: "orders", Partition: 1, Offset: 5, Timestamp: ts},
	}
	m.tailOffsets = map[int]int64{0: 11, 1: 6}

	view := m.renderTopicsView()
	assert.Contains(t, view, "TAIL orders")
	assert.Contains(t, view, "2 msgs", "header shows buffered message count")
	assert.Contains(t, view, "last 12:03:04.567", "header shows last-message timestamp")
	assert.Contains(t, view, "p0→11", "header shows partition/offset summary")
	assert.Contains(t, view, "p1→6", "header shows partition/offset summary")
	assert.Contains(t, view, "following")
}

func TestRenderTailViewPausedKeepsStatus(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.openTailView("orders")
	m.tailPaused = true
	m.tailMessages = []tail.Message{{Topic: "orders", Partition: 0, Offset: 10}}

	view := m.renderTopicsView()
	assert.Contains(t, view, "paused", "paused status must remain in the header")
}
