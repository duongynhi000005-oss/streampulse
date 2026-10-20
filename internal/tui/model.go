// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package tui provides the bubbletea terminal UI for StreamPulse.
// Real-time k9s-style dashboard — auto-refreshes every 2 seconds.
// Reads from the daemon's SQLite state.db. No manual reload needed.
package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/pulsedev/streampulse/internal/dlq"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/pulsedev/streampulse/internal/tail"
)

// ─── Styles ────────────────────────────────────────────────────────────────

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7C3AED")).
			Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#A78BFA")).
			Background(lipgloss.Color("#1F1A2E")).
			Padding(0, 1)

	statusOK = lipgloss.NewStyle().Foreground(lipgloss.Color("#22C55E")).Render("●")

	tabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#9CA3AF"))

	activeTabStyle = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Bold(true)

	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF"))

	cardStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#4C1D95")).
			Padding(1, 2).
			Width(28)

	failureBannerStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(lipgloss.Color("#DC2626")).
				Padding(0, 1)
)

// ─── Messages ──────────────────────────────────────────────────────────────

type tickMsg time.Time

// analyzeDoneMsg carries the captured output of the analyze subprocess.
type analyzeDoneMsg struct {
	output string
	err    error
}

// analyzeWindows are the selectable --window values for the analyze view.
var analyzeWindows = []string{"24h", "168h", "720h"}

// DataUpdated is sent when the daemon writes new data (simulated via tick for now).
type DataUpdated struct {
	Brokers        []BrokerRow
	Topics         []TopicRow
	ConsumerGroups []ConsumerGroupRow
	Alerts         []AlertRow
	DLQTopics      []DLQRow
	Logs           []string
	Failed         bool
}

// ─── Data Transfer Types ────────────────────────────────────────────────────

type BrokerRow struct {
	ID     string
	Status string
	CPU    string
	Memory string
	Rate   string
}

type TopicRow struct {
	Name       string
	Partitions int
	MsgRate    string
	BytesRate  string
	Retention  string
}

type ConsumerGroupRow struct {
	Group   string
	Lag     string
	Status  string
	Members int
	Topic   string
}

type AlertRow struct {
	Name     string
	Severity string
	Value    string
	FiredAt  string
}

type DLQRow struct {
	Topic        string
	MessageCount string
	Growth       string
}

// ─── Model ─────────────────────────────────────────────────────────────────

// Model is the bubbletea model. Connects to the daemon's SQLite store
// or directly to Kafka via --brokers flag. All views refresh every 2 seconds.
type Model struct {
	width  int
	height int
	ready  bool

	activeTab int
	tabs      []string

	store       storage.MetricsStore
	kafkaClient *kafka.Client
	brokerAddrs []string // kafka mode: broker addresses for dlq inspect/replay

	// Live data (updated every tick from store)
	brokers        []BrokerRow
	topics         []TopicRow
	consumerGroups []ConsumerGroupRow
	alerts         []AlertRow
	dlqTopics      []DLQRow
	logs           []string

	lastUpdated  time.Time
	loading      bool
	scrapedOnce  bool
	scrapeFailed bool

	// Topic search (active while searching)
	searching   bool
	searchQuery string

	// Help modal (? opens, esc closes)
	helpOpen bool

	// Tables (rebuilt on data change)
	brokersTable table.Model
	topicsTable  table.Model
	groupsTable  table.Model
	alertsTable  table.Model
	dlqTable     table.Model

	// Sub-views
	logView viewport.Model

	// DLQ inspect view (open while dlqView != nil)
	dlqView    *viewport.Model
	dlqTopic   string
	dlqConfirm bool

	// Analyze CLI view (Analytics tab, key "a")
	analyzeRunning  bool
	analyzeWindow   string
	analyzeOut      string
	analyzeViewOpen bool
	analyzeView     *viewport.Model

	// Analytics tab content viewport (scrollable pane stack)
	analyticsView *viewport.Model

	// Topic tail view (Topics tab, Enter)
	tailTopic    string
	tailMessages []tail.Message
	tailOffsets  map[int]int64
	tailPaused   bool
	tailPinned   bool
	tailErr      string
	tailView     *viewport.Model
	tailBrokerFn func() tail.Broker // test injection

	// DLQ module hooks (injectable for tests)
	discoverDLQ  func(ctx context.Context, client dlq.Client, suffixes []string) ([]dlq.Topic, error)
	dlqInspectFn func(ctx context.Context, brokers []string, topic string, limit int) ([]dlq.Message, error)
	dlqReplayFn  func(ctx context.Context, opts dlq.ReplayOptions) (*dlq.ReplayResult, error)

	// Analytics cache (recomputed at most every 30s)
	analytics        []analytics.GrowthReport
	skew             []analytics.SkewReport
	retention        []analytics.RetentionReport
	anomalies        []analytics.Anomaly
	rebalances       []analytics.RebalanceReport
	patterns         []analytics.ThroughputReport
	patternIdx       int
	analyticsErr     error
	analyticsUpdated time.Time
	selectedTopic    int

	// now is the clock used for analytics staleness (injectable for tests).
	now func() time.Time
}

// NewModel creates a TUI model connected to the daemon's store.
// If storePath is empty, uses the default ~/.streampulse/state.db.
func NewModel(storePath string) (*Model, error) {
	if storePath == "" {
		storePath = "~/.streampulse/state.db"
	}

	store, err := storage.NewStore("sqlite", storePath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	m := &Model{
		store: store,
		tabs:  []string{"▦ Overview", "▤ Topics", "▣ Consumers", "▲ Alerts", "▬ DLQ", "▨ Analytics"},
	}

	return m, nil
}

// NewModelWithStore creates a TUI model with an existing store (for testing).
func NewModelWithStore(store storage.MetricsStore) *Model {
	return &Model{
		store: store,
		tabs:  []string{"▦ Overview", "▤ Topics", "▣ Consumers", "▲ Alerts", "▬ DLQ", "▨ Analytics"},
	}
}

// NewModelWithKafka creates a TUI model that fetches data directly from Kafka.
func NewModelWithKafka(client *kafka.Client) *Model {
	return &Model{
		kafkaClient: client,
		brokerAddrs: client.Brokers(),
		tabs:        []string{"▦ Overview", "▤ Topics", "▣ Consumers", "▲ Alerts", "▬ DLQ", "▨ Analytics"},
	}
}

// ─── Init ──────────────────────────────────────────────────────────────────

func (m *Model) Init() tea.Cmd {
	m.loading = true
	return tea.Batch(
		m.refreshCmd(), // immediate first load
		tickCmd(),      // start 2-second tick
		tea.EnterAltScreen,
	)
}

// autoRefreshInterval is the single source of truth for the refresh cadence:
// it drives both the tick and the footer indicator (SP-09).
const autoRefreshInterval = 2 * time.Second

func tickCmd() tea.Cmd {
	return tea.Tick(autoRefreshInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *Model) refreshCmd() tea.Cmd {
	return func() tea.Msg {
		return m.loadData()
	}
}

// ─── Data Loading (reads from daemon's SQLite) ──────────────────────────────

func (m *Model) loadData() DataUpdated {
	if m.kafkaClient != nil {
		return m.fetchFromKafka()
	}

	if m.store == nil {
		return DataUpdated{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := DataUpdated{}
	logs := make([]string, 0, 2)
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
	}

	if err := m.store.Ping(ctx); err != nil {
		logf("store offline: %v", err)
		data.Failed = true
		data.Logs = logs
		return data
	}
	logf("store connected")

	// Query the last minute of persisted metrics and map the latest value
	// per entity into the dashboard rows. QueryRaw orders by timestamp
	// ascending, so later rows overwrite earlier ones per entity.
	rows, err := m.store.QueryRaw(ctx, storage.QueryParams{
		From:  time.Now().Add(-time.Minute),
		Limit: 10000,
	})
	if err != nil {
		logf("query store: %v", err)
		data.Failed = true
		data.Logs = logs
		return data
	}

	latest := make(map[string]map[string]float64) // metric → entity → value
	for _, r := range rows {
		if latest[r.Metric] == nil {
			latest[r.Metric] = make(map[string]float64)
		}
		latest[r.Metric][r.EntityName] = r.Avg
	}

	data.Brokers = brokerRowsFromStore(latest)
	data.Topics = topicRowsFromStore(latest)
	data.ConsumerGroups = groupRowsFromStore(latest)

	state, err := m.store.QueryAlertState(ctx)
	if err != nil {
		logf("alerts: %v", err)
	} else {
		data.Alerts = alertRowsFromStore(state)
	}

	if err := m.refreshAnalytics(ctx); err != nil {
		logf("analytics: %v", err)
	}
	data.Logs = logs
	return data
}

// alertRowsFromStore maps persisted alert states into dashboard rows. The
// rule severity is not part of the persisted state, so it renders as "-".
func alertRowsFromStore(state []storage.AlertStateRow) []AlertRow {
	rows := make([]AlertRow, 0, len(state))
	for _, s := range state {
		row := AlertRow{Name: s.RuleName, Severity: "-", Value: fmt.Sprintf("%.1f", s.LastValue)}
		if !s.LastFired.IsZero() {
			row.FiredAt = s.LastFired.Format("15:04:05")
		} else {
			row.FiredAt = "-"
		}
		rows = append(rows, row)
	}
	return rows
}

// brokerRowsFromStore maps latest broker metrics into dashboard rows.
func brokerRowsFromStore(latest map[string]map[string]float64) []BrokerRow {
	rows := make([]BrokerRow, 0, len(latest["kafka.broker.leader_partitions"]))
	for name, leaders := range latest["kafka.broker.leader_partitions"] {
		row := BrokerRow{
			ID:     name,
			Status: statusOK + " UP",
			CPU:    fmt.Sprintf("%.0f leaders", leaders),
			Memory: "-",
			Rate:   "-",
		}
		if replicas, ok := latest["kafka.broker.replica_partitions"][name]; ok {
			row.Memory = fmt.Sprintf("%.0f replicas", replicas)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// topicRowsFromStore maps latest topic metrics into dashboard rows.
func topicRowsFromStore(latest map[string]map[string]float64) []TopicRow {
	rows := make([]TopicRow, 0, len(latest["kafka.topic.partition_count"]))
	for name, parts := range latest["kafka.topic.partition_count"] {
		row := TopicRow{
			Name:       name,
			Partitions: int(parts),
			MsgRate:    "-",
			BytesRate:  "-",
			Retention:  "-",
		}
		if v, ok := latest["kafka.topic.msg_rate"][name]; ok {
			row.MsgRate = fmt.Sprintf("%.1f", v)
		}
		if v, ok := latest["kafka.topic.bytes_rate"][name]; ok {
			row.BytesRate = fmt.Sprintf("%.1f", v)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

// groupRowsFromStore maps latest consumer-group metrics into dashboard rows.
func groupRowsFromStore(latest map[string]map[string]float64) []ConsumerGroupRow {
	rows := make([]ConsumerGroupRow, 0, len(latest["kafka.group.lag"]))
	for name, lag := range latest["kafka.group.lag"] {
		row := ConsumerGroupRow{
			Group:   name,
			Lag:     fmt.Sprintf("%.1f", lag),
			Status:  "-",
			Members: 0,
			Topic:   "-",
		}
		if v, ok := latest["kafka.group.member_count"][name]; ok {
			row.Members = int(v)
		}
		if v, ok := latest["kafka.group.state"][name]; ok {
			row.Status = statusOK + " " + groupStateName(v)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Group < rows[j].Group })
	return rows
}

// groupStateName maps the persisted numeric group state (scraper.md: 0=Empty,
// 1=Stable, 2=PreparingRebalance, 3=CompletingRebalance, 4=Dead) to a label.
func groupStateName(state float64) string {
	switch int(state) {
	case 1:
		return "STABLE"
	case 2:
		return "PREPARING REBALANCE"
	case 3:
		return "COMPLETING REBALANCE"
	case 4:
		return "DEAD"
	default:
		return "EMPTY"
	}
}

// ─── Analytics ─────────────────────────────────────────────────────────────

const (
	// analyticsRefreshInterval bounds how often the analytics cache is
	// recomputed (the 2s tick only recomputes when it is stale).
	analyticsRefreshInterval = 30 * time.Second
	// analyticsWindow is the growth report window (daily buckets).
	analyticsWindow = 7 * 24 * time.Hour
	// analyticsTopN caps the growth list shown on the Analytics tab.
	analyticsTopN = 5
	// Uniform empty-state phrasing for the analytics panes (SP-14).
	emptyGrowth     = "No growth detected"
	emptySkew       = "No skew detected"
	emptyRetention  = "No retention risk detected"
	emptyAnomalies  = "No anomalies detected"
	emptyRebalances = "No rebalances in window"
	emptyPatterns   = "No patterns detected"
)

// refreshAnalytics recomputes the analytics cache when it is older than
// analyticsRefreshInterval. Growth needs the store, skew needs the live
// cluster, and retention needs both; reports whose inputs are missing are
// skipped. Report errors are isolated: a failing report never discards the
// cached data of the others.
func (m *Model) refreshAnalytics(ctx context.Context) error {
	if m.store == nil && m.kafkaClient == nil {
		return nil
	}
	now := m.now
	if now == nil {
		now = time.Now
	}
	if now().Sub(m.analyticsUpdated) <= analyticsRefreshInterval {
		return nil
	}

	var errs []error
	if m.store != nil {
		a := analytics.NewAnalyzer(m.store, m.kafkaClient)
		reports, err := a.Growth(ctx, nil, analyticsWindow)
		if err != nil {
			errs = append(errs, fmt.Errorf("growth: %w", err))
		} else {
			m.analytics = topGrowth(reports, analyticsTopN)
			m.selectedTopic = 0
		}

		// L2 reports: anomalies, rebalance history, and throughput patterns
		// (topics from the live topics table; skipped without any).
		anoms, err := a.Anomalies(ctx, nil, analyticsWindow)
		if err != nil {
			errs = append(errs, fmt.Errorf("anomalies: %w", err))
		} else {
			m.anomalies = anoms
		}
		rebs, err := a.Rebalances(ctx, nil, analyticsWindow)
		if err != nil {
			errs = append(errs, fmt.Errorf("rebalances: %w", err))
		} else {
			m.rebalances = rebs
		}
		topics := topicNamesOf(m.topics)
		if len(topics) > 0 {
			pats, err := a.Patterns(ctx, topics, scraper.MetricTopicMsgRate, analyticsWindow)
			if err != nil {
				errs = append(errs, fmt.Errorf("patterns: %w", err))
			} else {
				m.patterns = pats
				if m.patternIdx >= len(m.patterns) {
					m.patternIdx = 0
				}
			}
		}
	}
	if m.kafkaClient != nil {
		a := analytics.NewAnalyzer(m.store, m.kafkaClient)
		skew, err := a.Skew(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("skew: %w", err))
		} else {
			m.skew = skew
		}
	}
	if m.store != nil && m.kafkaClient != nil {
		a := analytics.NewAnalyzer(m.store, m.kafkaClient)
		ret, err := a.Retention(ctx, growthTopicNames(m.analytics))
		if err != nil {
			errs = append(errs, fmt.Errorf("retention: %w", err))
		} else {
			m.retention = ret
		}
	}
	m.analyticsErr = errors.Join(errs...)
	m.analyticsUpdated = now()
	return m.analyticsErr
}

// topGrowth keeps the n fastest-growing reports, ordered by delta desc.
func topGrowth(reports []analytics.GrowthReport, n int) []analytics.GrowthReport {
	sorted := append([]analytics.GrowthReport(nil), reports...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Delta > sorted[j].Delta })
	if len(sorted) > n {
		sorted = sorted[:n]
	}
	return sorted
}

// growthTopicNames returns the topic names of the cached growth reports.
func growthTopicNames(reports []analytics.GrowthReport) []string {
	names := make([]string, 0, len(reports))
	for _, r := range reports {
		names = append(names, r.Topic)
	}
	return names
}

// topicNamesOf returns the names of the given topic rows.
func topicNamesOf(rows []TopicRow) []string {
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Name)
	}
	return names
}

// cycleAnalyticsSelection moves the highlighted growth topic, wrapping at
// both ends of the cached list.
func (m *Model) cycleAnalyticsSelection(delta int) {
	n := len(m.analytics)
	if n == 0 {
		m.selectedTopic = 0
		return
	}
	m.selectedTopic = (m.selectedTopic + delta + n) % n
}

// cyclePatternSelection moves the highlighted pattern topic, wrapping at both
// ends. Without cached patterns it falls back to the growth selection.
func (m *Model) cyclePatternSelection(delta int) {
	n := len(m.patterns)
	if n == 0 {
		m.cycleAnalyticsSelection(delta)
		return
	}
	m.patternIdx = (m.patternIdx + delta + n) % n
}

func (m *Model) fetchFromKafka() DataUpdated {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := DataUpdated{}
	logs := make([]string, 0, 2)
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
	}

	cluster, err := m.kafkaClient.DescribeCluster(ctx)
	if err != nil {
		logf("kafka error: %v", err)
		data.Failed = true
		data.Logs = logs
		return data
	}

	topics, topicErr := m.kafkaClient.ListTopics(ctx)
	groups, groupErr := m.kafkaClient.ListConsumerGroups(ctx)

	topicCount := len(topics)
	partCount := 0
	for _, t := range topics {
		partCount += t.Partitions
	}

	if topicErr != nil {
		logf("topics error: %v", topicErr)
	}
	if groupErr != nil {
		logf("groups error: %v", groupErr)
	}
	if topicErr != nil || groupErr != nil {
		data.Failed = true
	}
	logf("scrape: %d brokers, %d topics (%d partitions), %d groups",
		cluster.BrokerCount, topicCount, partCount, len(groups))

	for _, b := range cluster.Brokers {
		status := statusOK + " UP"
		if b.ID == cluster.ControllerID {
			status = statusOK + " CONTROLLER"
		}
		data.Brokers = append(data.Brokers, BrokerRow{
			ID:     fmt.Sprintf("%s:%d", b.Host, b.Port),
			Status: status,
			CPU:    fmt.Sprintf("%d leaders", b.LeaderPartitions),
			Memory: fmt.Sprintf("%d replicas", b.ReplicaPartitions),
			Rate:   b.Rack,
		})
	}

	for _, t := range topics {
		data.Topics = append(data.Topics, TopicRow{
			Name:       t.Name,
			Partitions: t.Partitions,
			MsgRate:    "-",
			BytesRate:  "-",
			Retention:  "-",
		})
	}

	for _, g := range groups {
		data.ConsumerGroups = append(data.ConsumerGroups, ConsumerGroupRow{
			Group:   g.Name,
			Status:  statusOK + " " + g.State,
			Members: g.Members,
			Lag:     "-",
			Topic:   "-",
		})
	}

	dlqs, dlqErr := m.fetchDLQ(ctx)
	if dlqErr != nil {
		logf("dlq discovery error: %v", dlqErr)
		data.Failed = true
	} else {
		data.DLQTopics = dlqs
	}

	if err := m.refreshAnalytics(ctx); err != nil {
		logf("analytics: %v", err)
	}
	data.Logs = logs
	return data
}

// ─── Layout ─────────────────────────────────────────────────────────────────

const (
	// chromeHeight is the fixed vertical chrome outside the content region:
	// header bar, tab bar, and footer help bar (one line each).
	chromeHeight = 3
	// sectionTitleLines is the rendered height of a padded section heading
	// (Padding(1, 0) → blank + text + blank).
	sectionTitleLines = 3
)

// contentHeight returns the number of lines available for the content region
// between the tab bar and the footer help bar.
func (m *Model) contentHeight() int {
	h := m.height - chromeHeight
	if h < 1 {
		h = 1
	}
	return h
}

// tableMaxHeight bounds a table to the space left after the fixed chrome and
// the per-view decorations (section headings, hints) of the tab it renders on.
// Unbounded (math.MaxInt) while the terminal size is unknown.
func (m *Model) tableMaxHeight(decor int) int {
	if m.height <= 0 {
		return math.MaxInt
	}
	h := m.contentHeight() - decor
	if h < 2 { // header row + at least one data row
		h = 2
	}
	return h
}

// ─── Update ────────────────────────────────────────────────────────────────

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if !m.ready {
			m.ready = true
			m.logView = viewport.New(msg.Width, 6)
			m.analyzeView = &viewport.Model{}
			*m.analyzeView = viewport.New(msg.Width-4, msg.Height-6)
		}
		if m.analyticsView == nil {
			vp := viewport.New(msg.Width, m.contentHeight())
			m.analyticsView = &vp
		} else {
			m.analyticsView.Width = msg.Width
			m.analyticsView.Height = m.contentHeight()
		}
		m.buildTables()

	case tickMsg:
		if !m.loading {
			m.loading = true
			cmds = append(cmds, m.refreshCmd())
		}
		cmds = append(cmds, tickCmd())

	case DataUpdated:
		m.loading = false
		m.applyData(msg)
		m.buildTables()

	case dlqInspectMsg:
		m.handleDLQInspectMsg(msg)

	case dlqReplayMsg:
		m.handleDLQReplayMsg(msg)

	case tailSnapshotMsg:
		cmds = append(cmds, m.handleTailSnapshotMsg(msg))

	case tailFollowMsg:
		m.handleTailFollowMsg(msg)

	case tailRetryMsg:
		if msg.topic == m.tailTopic && m.tailView != nil {
			cmds = append(cmds, m.tailSnapshotCmd())
		}

	case tailTickMsg:
		if m.tailTopic != "" && m.tailView != nil && !m.tailPaused {
			cmds = append(cmds, m.tailFollowCmd())
		}
		cmds = append(cmds, tea.Tick(tailFollowInterval, func(t time.Time) tea.Msg {
			return tailTickMsg(t)
		}))

	case analyzeDoneMsg:
		m.analyzeRunning = false
		if msg.err != nil {
			m.analyzeOut = fmt.Sprintf("analyze failed: %v\n%s", msg.err, msg.output)
		} else {
			m.analyzeOut = msg.output
		}
		if m.analyzeView != nil {
			m.analyzeView.SetContent(m.analyzeOut)
			m.analyzeView.GotoTop()
		}

	case tea.KeyMsg:
		// "?" opens the help modal from any view or overlay; while the modal
		// is open only esc (close) and q/ctrl+c (quit) are live.
		if msg.String() == "?" {
			m.helpOpen = true
			break
		}
		if m.helpOpen {
			cmds = append(cmds, m.handleHelpKey(msg))
			break
		}
		if m.dlqView != nil {
			cmds = append(cmds, m.handleDLQViewKey(msg))
			break
		}
		if m.tailView != nil {
			cmds = append(cmds, m.handleTailViewKey(msg))
			break
		}
		if m.analyzeViewOpen {
			cmds = append(cmds, m.handleAnalyzeViewKey(msg))
			break
		}
		if m.searching {
			m.handleSearchKey(msg)
			break
		}
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "/":
			m.searching = true
		case "tab", "l", "right":
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
		case "shift+tab", "h", "left":
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
		case "j", "down":
			if m.activeTab == 5 {
				if m.analyticsView != nil {
					m.analyticsView.LineDown(1)
				}
			} else if t := m.activeTable(); t != nil {
				t.MoveDown(1)
			}
		case "k", "up":
			if m.activeTab == 5 {
				if m.analyticsView != nil {
					m.analyticsView.LineUp(1)
				}
			} else if t := m.activeTable(); t != nil {
				t.MoveUp(1)
			}
		case "pgup":
			if t := m.activeTable(); t != nil {
				t.MoveUp(pageSize(t))
			}
		case "pgdown":
			if t := m.activeTable(); t != nil {
				t.MoveDown(pageSize(t))
			}
		case "]":
			if m.activeTab == 5 {
				m.cyclePatternSelection(1)
			}
		case "[":
			if m.activeTab == 5 {
				m.cyclePatternSelection(-1)
			}
		case "1", "2", "3", "4", "5", "6":
			if idx := int(msg.Runes[0] - '1'); idx < len(m.tabs) {
				m.activeTab = idx
			}
		case "enter":
			if m.activeTab == 4 && len(m.dlqTopics) > 0 {
				topic := m.dlqTable.SelectedRow()[0]
				cmds = append(cmds, m.openDLQView(topic))
			}
			if m.activeTab == 1 && len(m.topics) > 0 {
				topic := m.topicsTable.SelectedRow()[0]
				cmds = append(cmds, m.openTailView(topic))
			}
		case "r":
			if !m.loading {
				m.loading = true
				cmds = append(cmds, m.refreshCmd())
			}
		case "a":
			if m.activeTab == 5 && !m.analyzeRunning {
				if m.analyzeWindow == "" {
					m.analyzeWindow = analyzeWindows[0]
				}
				m.analyzeRunning = true
				m.analyzeViewOpen = true
				if m.analyzeView != nil {
					m.analyzeView.SetContent("running analyze...")
				}
				cmds = append(cmds, m.analyzeCmd())
			}
		case "w":
			if m.activeTab == 5 && !m.analyzeRunning {
				m.cycleAnalyzeWindow()
			}
		}
	}

	return m, tea.Batch(cmds...)
}

// handleDLQViewKey handles keys while the DLQ inspect view is open.
func (m *Model) handleDLQViewKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.closeDLQView()
	case "q", "ctrl+c":
		return tea.Quit
	case "r":
		m.dlqConfirm = true
	case "y":
		if m.dlqConfirm {
			m.dlqConfirm = false
			return m.dlqReplayCmd()
		}
	case "n":
		m.dlqConfirm = false
	case "j", "down":
		m.dlqView.LineDown(1)
	case "k", "up":
		m.dlqView.LineUp(1)
	}
	return nil
}

// closeDLQView returns to the DLQ table.
func (m *Model) closeDLQView() {
	m.dlqView = nil
	m.dlqTopic = ""
	m.dlqConfirm = false
}

// handleHelpKey handles keys while the ? help modal is open.
func (m *Model) handleHelpKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.helpOpen = false
	case "q", "ctrl+c":
		return tea.Quit
	}
	return nil
}

// analyzeCmd shells out to the analyze CLI, capturing its output.
func (m *Model) analyzeCmd() tea.Cmd {
	exe, err := os.Executable()
	if err != nil {
		return func() tea.Msg {
			return analyzeDoneMsg{err: fmt.Errorf("resolve executable: %w", err)}
		}
	}
	c := exec.Command(exe, "analyze", "--window", m.analyzeWindow)
	var out bytes.Buffer
	c.Stdout = &out
	c.Stderr = &out
	return tea.ExecProcess(c, func(err error) tea.Msg {
		return analyzeDoneMsg{output: out.String(), err: err}
	})
}

// handleAnalyzeViewKey handles keys while the analyze CLI view is open.
func (m *Model) handleAnalyzeViewKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.analyzeViewOpen = false
		m.analyzeOut = ""
	case "q", "ctrl+c":
		return tea.Quit
	case "j", "down":
		if m.analyzeView != nil {
			m.analyzeView.LineDown(1)
		}
	case "k", "up":
		if m.analyzeView != nil {
			m.analyzeView.LineUp(1)
		}
	}
	return nil
}

// cycleAnalyzeWindow rotates the analyze --window value (24h → 168h → 720h).
func (m *Model) cycleAnalyzeWindow() {
	if m.analyzeWindow == "" {
		m.analyzeWindow = analyzeWindows[0]
	}
	for i, w := range analyzeWindows {
		if w == m.analyzeWindow {
			m.analyzeWindow = analyzeWindows[(i+1)%len(analyzeWindows)]
			return
		}
	}
	m.analyzeWindow = analyzeWindows[0]
}

// handleSearchKey handles keys while topic search is active: printable runes
// append to the query, backspace removes, enter applies (leaving the filter
// in place), esc and q close the search.
func (m *Model) handleSearchKey(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "q":
		m.searching = false
		m.searchQuery = ""
	case "enter":
		m.searching = false
	case "backspace":
		if r := []rune(m.searchQuery); len(r) > 0 {
			m.searchQuery = string(r[:len(r)-1])
		}
	default:
		if msg.Type == tea.KeyRunes && len(msg.Runes) > 0 {
			m.searchQuery += string(msg.Runes)
		}
	}
}

// activeTable returns the table of the active tab (nil for the Analytics tab,
// which has no table).
func (m *Model) activeTable() *table.Model {
	switch m.activeTab {
	case 0:
		return &m.brokersTable
	case 1:
		return &m.topicsTable
	case 2:
		return &m.groupsTable
	case 3:
		return &m.alertsTable
	case 4:
		return &m.dlqTable
	}
	return nil
}

// pageSize returns the number of rows a PgUp/PgDn page spans: one full screen
// of the table's visible viewport, never fewer than one row.
func pageSize(t *table.Model) int {
	h := t.Height() // viewport height = rendered height minus the header row
	if h < 1 {
		h = 1
	}
	return h
}

func (m *Model) applyData(d DataUpdated) {
	m.scrapeFailed = d.Failed
	if !d.Failed {
		m.scrapedOnce = true
		m.brokers = d.Brokers
		m.topics = d.Topics
		m.consumerGroups = d.ConsumerGroups
		m.alerts = d.Alerts
		m.dlqTopics = d.DLQTopics
		m.lastUpdated = time.Now()
	}
	m.logs = append(m.logs, d.Logs...)
	if len(m.logs) > 50 {
		m.logs = m.logs[len(m.logs)-50:]
	}
}

func (m *Model) buildTables() {
	// Per-tab vertical budget for the table itself: content region minus the
	// decorations rendered around it on that tab (headings, hints, blanks).
	// The groups table renders on both Overview and its own tab; it takes the
	// dedicated-tab budget so Consumers stays fully reachable.
	brokersMax := m.tableMaxHeight(2*sectionTitleLines + 19)
	topicsMax := m.tableMaxHeight(sectionTitleLines + 2)    // title + tail hint + pagination
	consumersMax := m.tableMaxHeight(sectionTitleLines + 1) // title + pagination
	alertsMax := m.tableMaxHeight(sectionTitleLines)        // title
	dlqMax := m.tableMaxHeight(sectionTitleLines + 2)       // title + blank + hint

	m.brokersTable = buildTable(
		[]table.Column{
			{Title: "BROKER", Width: 16},
			{Title: "STATUS", Width: 8},
			{Title: "CPU", Width: 8},
			{Title: "MEM", Width: 8},
			{Title: "MSGS/S", Width: 12},
		},
		rowsFromBrokers(m.brokers),
		m.brokersTable.Cursor(), brokersMax,
	)

	m.topicsTable = buildTable(
		[]table.Column{
			{Title: "TOPIC", Width: 20},
			{Title: "PARTITIONS", Width: 12},
			{Title: "MSG/S", Width: 10},
			{Title: "BYTES/S", Width: 12},
			{Title: "RETENTION", Width: 12},
		},
		rowsFromTopics(filteredTopics(m.topics, m.searchQuery), m.searchQuery),
		m.topicsTable.Cursor(), topicsMax,
	)

	m.groupsTable = buildTable(
		[]table.Column{
			{Title: "GROUP", Width: 22},
			{Title: "LAG", Width: 10},
			{Title: "STATUS", Width: 10},
			{Title: "MEMBERS", Width: 10},
			{Title: "TOPIC", Width: 14},
		},
		rowsFromConsumerGroups(m.consumerGroups),
		m.groupsTable.Cursor(), consumersMax,
	)

	m.alertsTable = buildTable(
		[]table.Column{
			{Title: "ALERT", Width: 30},
			{Title: "SEVERITY", Width: 10},
			{Title: "VALUE", Width: 12},
			{Title: "FIRED", Width: 10},
		},
		rowsFromAlerts(m.alerts),
		m.alertsTable.Cursor(), alertsMax,
	)

	m.dlqTable = buildTable(
		[]table.Column{
			{Title: "DLQ TOPIC", Width: 44},
			{Title: "MESSAGES", Width: 16},
			{Title: "GROWTH", Width: 14},
		},
		rowsFromDLQ(m.dlqTopics),
		m.dlqTable.Cursor(), dlqMax,
	)
}

// ─── Table Builders ────────────────────────────────────────────────────────

// buildTable creates a table whose height is compact (rows + header) but
// capped at maxHeight; beyond that the table scrolls internally while j/k
// move the selection. cursor is restored after the rebuild so the live
// 2-second refresh never resets the operator's selection.
func buildTable(cols []table.Column, rows []table.Row, cursor, maxHeight int) table.Model {
	height := len(rows) + 1 // header + data rows
	if height > maxHeight {
		height = maxHeight
	}
	if height < 1 {
		height = 1
	}
	t := table.New(
		table.WithColumns(cols),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(height),
	)
	s := table.DefaultStyles()
	s.Header = headerStyle
	s.Selected = lipgloss.NewStyle().
		Background(lipgloss.Color("#4C1D95")).
		Foreground(lipgloss.Color("#FFFFFF"))
	t.SetStyles(s)
	t.SetCursor(cursor)
	return t
}

func rowsFromBrokers(b []BrokerRow) []table.Row {
	if len(b) == 0 {
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(b))
	for i, br := range b {
		rows[i] = table.Row{br.ID, br.Status, br.CPU, br.Memory, br.Rate}
	}
	return rows
}

// rowsFromTopics renders topic rows, or a distinct "no match" state when a
// search query filters everything out. The full query + count live in the
// footer, so the row stays short enough for the 20-wide TOPIC column.
func rowsFromTopics(t []TopicRow, query string) []table.Row {
	if len(t) == 0 {
		if query != "" {
			return []table.Row{{fmt.Sprintf("No match /%s/", query), "-", "-", "-", "-"}}
		}
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(t))
	for i, tp := range t {
		rows[i] = table.Row{tp.Name, fmt.Sprintf("%d", tp.Partitions), tp.MsgRate, tp.BytesRate, tp.Retention}
	}
	return rows
}

func rowsFromConsumerGroups(g []ConsumerGroupRow) []table.Row {
	if len(g) == 0 {
		return []table.Row{{"No data", "-", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(g))
	for i, cg := range g {
		rows[i] = table.Row{cg.Group, cg.Lag, cg.Status, fmt.Sprintf("%d", cg.Members), cg.Topic}
	}
	return rows
}

func rowsFromAlerts(a []AlertRow) []table.Row {
	if len(a) == 0 {
		return []table.Row{{"No alerts firing", "-", "-", "-"}}
	}
	rows := make([]table.Row, len(a))
	for i, al := range a {
		rows[i] = table.Row{al.Name, al.Severity, al.Value, al.FiredAt}
	}
	return rows
}

func rowsFromDLQ(d []DLQRow) []table.Row {
	if len(d) == 0 {
		return []table.Row{{"No DLQ topics detected", "-", "-"}}
	}
	rows := make([]table.Row, len(d))
	for i, dlq := range d {
		rows[i] = table.Row{dlq.Topic, dlq.MessageCount, dlq.Growth}
	}
	return rows
}

// filteredTopics returns the topics matching the search query (case-insensitive
// contains on the topic name), or all topics when no query is set. The filter
// stays applied after Enter closes the search; Esc clears the query.
func filteredTopics(topics []TopicRow, query string) []TopicRow {
	if query == "" {
		return topics
	}
	q := strings.ToLower(query)
	out := make([]TopicRow, 0, len(topics))
	for _, t := range topics {
		if strings.Contains(strings.ToLower(t.Name), q) {
			out = append(out, t)
		}
	}
	return out
}

// ─── View ──────────────────────────────────────────────────────────────────

func (m *Model) View() string {
	if !m.ready {
		return "Initializing StreamPulse..."
	}

	if m.analyzeViewOpen {
		return m.renderAnalyzeView()
	}

	if m.helpOpen {
		return m.renderHelpModal()
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		m.renderHeader(),
		m.renderTabs(),
		m.renderContent(),
		m.renderHelp(),
	)
}

// renderAnalyzeView is the full-screen overlay showing the analyze CLI output.
func (m *Model) renderAnalyzeView() string {
	title := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 2).
		Render(fmt.Sprintf("▶ analyze --window %s", m.analyzeWindow))

	body := "running..."
	if m.analyzeView != nil {
		body = m.analyzeView.View()
	}
	if body == "" {
		body = "no output"
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		body,
		helpStyle.Render("  esc: close  │  j/k: scroll"),
	)
}

func (m *Model) renderHeader() string {
	left := titleStyle.Render("▶ StreamPulse v0.1.0")

	brokerCount := len(m.brokers)
	if brokerCount == 0 {
		brokerCount = 3 // default display
	}
	updated := "—"
	if !m.lastUpdated.IsZero() {
		updated = m.lastUpdated.Format("15:04:05")
	}
	status := fmt.Sprintf("Brokers: %d  │  Updated: %s",
		brokerCount, updated)

	right := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#9CA3AF")).
		Render(status)

	pad := m.width - lipgloss.Width(left) - lipgloss.Width(right) - 4
	if pad < 0 {
		pad = 0 // narrow terminals must not panic (strings.Repeat with negative count)
	}
	bar := lipgloss.NewStyle().
		Background(lipgloss.Color("#1F1A2E")).
		Width(m.width).
		Padding(0, 2).
		Render(lipgloss.JoinHorizontal(lipgloss.Left,
			left,
			strings.Repeat(" ", pad),
			right))

	return bar
}

func (m *Model) renderTabs() string {
	var tabs []string
	for i, t := range m.tabs {
		if i == m.activeTab {
			tabs = append(tabs, activeTabStyle.Render(t))
		} else {
			tabs = append(tabs, tabStyle.Render(t))
		}
	}
	return lipgloss.NewStyle().
		Background(lipgloss.Color("#181425")).
		Width(m.width).
		MaxHeight(1). // narrow terminals: truncate instead of wrapping into the content region
		Render(lipgloss.JoinHorizontal(lipgloss.Left, tabs...))
}

// renderFailureBanner renders the red "last scrape failed" warning at the top
// of the content region. It shows whenever the last refresh failed and clears
// on the next successful refresh (SP-11).
func (m *Model) renderFailureBanner() string {
	if !m.scrapeFailed {
		return ""
	}
	return failureBannerStyle.Width(m.width).
		Render("!  last scrape failed — data may be stale (details in activity log)")
}

func (m *Model) renderContent() string {
	var body string
	switch m.activeTab {
	case 0:
		body = m.renderOverview()
	case 1:
		body = m.renderTopicsView()
	case 2:
		body = m.renderConsumersView()
	case 3:
		body = m.renderAlertsView()
	case 4:
		body = m.renderDLQView()
	case 5:
		body = m.renderAnalyticsView()
	}

	if banner := m.renderFailureBanner(); banner != "" {
		return lipgloss.JoinVertical(lipgloss.Left, banner, body)
	}
	return body
}

// alertCardValueStyle returns the ALERTS summary-card value style. A healthy
// cluster (nothing firing) reads as calm; only actual firings appear alarming.
func alertCardValueStyle(healthy bool) lipgloss.Style {
	if healthy {
		return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#22C55E"))
	}
	return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444"))
}

// cardValueStyle is the default summary-card value style (white bold).
var cardValueStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF"))

func (m *Model) renderOverview() string {
	// Summary cards double as navigation shortcuts: each advertises the number
	// key that jumps to the tab owning that resource. Brokers have no dedicated
	// tab, so their count lives in the section header instead of a card.
	cards := lipgloss.JoinHorizontal(lipgloss.Top,
		m.overviewCard("TOPICS", m.summaryValue(len(m.topics), "topics"), 1, cardValueStyle),
		m.overviewCard("CONSUMERS", m.summaryValue(len(m.consumerGroups), "groups"), 2, cardValueStyle),
		m.overviewCard("ALERTS", m.summaryValue(len(m.alerts), "firing"), 3, alertCardValueStyle(len(m.alerts) == 0)),
	)

	logSection := ""
	if m.logView.Width > 0 {
		m.logView.SetContent(strings.Join(m.logs, "\n"))
		m.logView.GotoBottom()
		logSection = lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVITY LOG"),
			m.logView.View(),
		)
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		cards,
		"",
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render(fmt.Sprintf("BROKERS (%d)", len(m.brokers))),
		m.brokersTable.View(),
		"",
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
		"",
		logSection,
	)
}

// summaryValue renders a resource count for a summary card, showing a loading
// placeholder until the first successful scrape distinguishes an empty cluster
// from one still connecting (SP-13).
func (m *Model) summaryValue(n int, unit string) string {
	if !m.scrapedOnce {
		return "loading…"
	}
	return fmt.Sprintf("%d %s", n, unit)
}

// overviewCard renders one summary card: label, value, and the number-key
// shortcut to the tab where that resource's full table lives.
func (m *Model) overviewCard(label, value string, key int, valueStyle lipgloss.Style) string {
	return cardStyle.Render(lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF")).Render(label),
		valueStyle.Render(value),
		helpStyle.Render(fmt.Sprintf("%d: open", key)),
	))
}

func (m *Model) renderTopicsView() string {
	if m.tailView != nil {
		return m.renderTailView()
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("TOPICS"),
		m.topicsTable.View(),
		m.renderPagination(len(filteredTopics(m.topics, m.searchQuery)), len(m.topics)),
		helpStyle.Render("  ENTER: tail topic"),
	)
}

func (m *Model) renderConsumersView() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("CONSUMER GROUPS"),
		m.groupsTable.View(),
		m.renderPagination(len(m.consumerGroups), len(m.consumerGroups)),
	)
}

// renderPagination renders the "Showing N of M" row-count indicator under a
// table, where N is the visible (post-filter) row count and M the total.
func (m *Model) renderPagination(visible, total int) string {
	return helpStyle.Render(fmt.Sprintf("  Showing %d of %d", visible, total))
}

func (m *Model) renderAlertsView() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ACTIVE ALERTS"),
		m.alertsTable.View(),
	)
}

func (m *Model) renderDLQView() string {
	if m.dlqView != nil {
		prompt := ""
		if m.dlqConfirm {
			prompt = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FBBF24")).
				Render(fmt.Sprintf("Replay %s to %s? (y/n)", m.dlqTopic, strings.TrimSuffix(m.dlqTopic, ".dlq"))) + "\n"
		}
		return lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("DEAD LETTER QUEUES — "+m.dlqTopic),
			prompt,
			m.dlqView.View(),
			"",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF")).Render("  esc: back  │  r: replay  │  y/n: confirm  │  j/k: scroll"),
		)
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("DEAD LETTER QUEUES"),
		m.dlqTable.View(),
		"",
		lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF")).Render("  ENTER: inspect  │  r: replay"),
	)
}

func (m *Model) renderAnalyticsView() string {
	content := m.analyticsContent()
	if m.analyticsView == nil {
		return content // no window size yet (tests): render unbounded
	}
	m.analyticsView.SetContent(content)
	return m.analyticsView.View()
}

// analyticsContent renders the six analytics panes as one scrollable body.
func (m *Model) analyticsContent() string {
	parts := []string{
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#A78BFA")).Padding(1, 0).Render("ANALYTICS"),
	}
	if m.analyticsErr != nil {
		parts = append(parts, lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EF4444")).
			Render("  analytics: "+m.analyticsErr.Error()))
	}
	parts = append(parts,
		m.renderGrowthPane(),
		"",
		m.renderSkewPane(),
		"",
		m.renderRetentionPane(),
		"",
		m.renderAnomaliesPane(),
		"",
		m.renderRebalancesPane(),
		"",
		m.renderPatternsPane(),
	)
	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

// renderGrowthPane renders the top-N topic growth sparklines, highlighting
// the selected topic.
func (m *Model) renderGrowthPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render(fmt.Sprintf("▇ TOPIC GROWTH (%dh)", int(analyticsWindow/time.Hour)))

	width := m.width - 4
	if width < 10 {
		width = 80
	}
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#4C1D95")).
		Padding(1, 2).
		Width(width)

	if len(m.analytics) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left,
			title,
			box.Render(helpStyle.Render(emptyGrowth)),
		)
	}

	sel := m.selectedTopic
	if sel >= len(m.analytics) {
		sel = 0
	}
	lines := make([]string, 0, len(m.analytics))
	for i, g := range m.analytics {
		marker := "  "
		name := lipgloss.NewStyle().Foreground(lipgloss.Color("#9CA3AF")).Render(g.Topic)
		if i == sel {
			marker = "▸ "
			name = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render(g.Topic)
		}
		lines = append(lines, fmt.Sprintf("%s%s  %s  %s %.1f msgs/s",
			marker, name, g.Sparkline, deltaSign(g.Delta), g.Delta))
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		box.Render(lipgloss.JoinVertical(lipgloss.Left, lines...)),
	)
}

// deltaSign prefixes a growth delta with its direction.
func deltaSign(d float64) string {
	if d < 0 {
		return fmt.Sprintf("%.1f", d)
	}
	return fmt.Sprintf("+%.1f", d)
}

// renderSkewPane renders the cluster partition leadership distribution.
func (m *Model) renderSkewPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render("PARTITION SKEW")

	if len(m.skew) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, helpStyle.Render("  "+emptySkew))
	}

	var lines []string
	for _, s := range m.skew {
		ids := make([]string, 0, len(s.Leaders))
		for id := range s.Leaders {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			lines = append(lines, fmt.Sprintf("  broker %s: %d leaders", id, s.Leaders[id]))
		}
		status := lipgloss.NewStyle().Foreground(lipgloss.Color("#22C55E")).Render("balanced")
		if !s.Balanced {
			status = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render("UNBALANCED")
		}
		lines = append(lines, fmt.Sprintf("  max/avg ratio %.2f — %s", s.Ratio, status))
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// renderRetentionPane renders topics whose byte-based retention estimate is
// at risk of filling before the time-based retention.
func (m *Model) renderRetentionPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render("RETENTION")

	if len(m.retention) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, helpStyle.Render("  "+emptyRetention))
	}
	lines := make([]string, 0, len(m.retention))
	for _, r := range m.retention {
		if !r.AtRisk {
			continue
		}
		lines = append(lines, lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FBBF24")).
			Render(fmt.Sprintf("  ! %s: retention %s, fill estimate %.1f days — at risk",
				r.Topic, r.RetentionMS, r.EstimateFillDays)))
	}
	if len(lines) == 0 {
		lines = append(lines, helpStyle.Render("  "+emptyRetention))
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// renderAnomaliesPane renders the most severe anomaly points, ordered by
// |z-score| descending, with the severity colored (warning → yellow,
// critical → red).
func (m *Model) renderAnomaliesPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render("ANOMALIES")

	if len(m.anomalies) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, helpStyle.Render("  "+emptyAnomalies))
	}

	sorted := append([]analytics.Anomaly(nil), m.anomalies...)
	sort.Slice(sorted, func(i, j int) bool {
		return math.Abs(sorted[i].ZScore) > math.Abs(sorted[j].ZScore)
	})
	if len(sorted) > analyticsTopN {
		sorted = sorted[:analyticsTopN]
	}

	lines := make([]string, 0, len(sorted)+1)
	lines = append(lines, fmt.Sprintf("  %-22s %-24s %9s %9s %6s %-4s %s",
		"ENTITY", "METRIC", "VALUE", "EXPECTED", "Z", "DIR", "SEVERITY"))
	for _, an := range sorted {
		sev := lipgloss.NewStyle().Foreground(lipgloss.Color("#FBBF24")).Render(an.Severity)
		if an.Severity == "critical" {
			sev = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EF4444")).Render(an.Severity)
		}
		lines = append(lines, fmt.Sprintf("  %-22s %-24s %9.1f %9.1f %6.2f %-4s %s",
			an.Entity, an.Metric, an.Value, an.Expected, an.ZScore, an.Direction, sev))
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// renderRebalancesPane renders the per-group per-day rebalance counts, most
// recent last (up to 10 rows).
func (m *Model) renderRebalancesPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render("REBALANCES")

	if len(m.rebalances) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, helpStyle.Render("  "+emptyRebalances))
	}

	rows := m.rebalances
	start := 0
	if len(rows) > 10 {
		start = len(rows) - 10
	}
	lines := make([]string, 0, len(rows)-start+1)
	lines = append(lines, fmt.Sprintf("  %-22s %-12s %s", "GROUP", "DAY", "REBALANCES"))
	for _, r := range rows[start:] {
		lines = append(lines, fmt.Sprintf("  %-22s %-12s %d",
			r.Group, r.Day.Format("2006-01-02"), r.Count))
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// renderPatternsPane renders the hourly throughput profile of the selected
// topic (j/k to cycle), its peak hour, and the 7-day forecast.
func (m *Model) renderPatternsPane() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#A78BFA")).
		Padding(1, 0).
		Render("PATTERNS")

	if len(m.patterns) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, helpStyle.Render("  "+emptyPatterns))
	}

	p := m.patterns[m.patternIdx%len(m.patterns)]
	labels := make([]string, len(p.HourlyProfile))
	for h := range p.HourlyProfile {
		labels[h] = fmt.Sprintf("%02d", h)
	}
	width := m.width - 4
	if width < 10 {
		width = 80
	}
	chart := analytics.Bars(labels, p.HourlyProfile[:], width)

	lines := []string{
		fmt.Sprintf("  %s — %s (7d)  peak %02d:00  forecast 7d: %.1f",
			p.Topic, p.Metric, p.PeakHour, p.Forecast7d),
		"",
		chart,
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// renderHelp renders the one-line footer: the keys relevant to the current
// view/overlay plus the auto-refresh cadence and the global keys (1-6 jump,
// ? help, q quit). The r refresh key and the full legend live in the ? modal,
// so the bar carries a single refresh indicator (SP-09) and stays short enough
// not to truncate at the 80-column acceptance width. Context keys are chosen
// so the bar never advertises a key that does nothing in the current view.
func (m *Model) renderHelp() string {
	context := ""
	switch {
	case m.searching:
		return helpStyle.Render(
			lipgloss.NewStyle().
				Background(lipgloss.Color("#1F1A2E")).
				Width(m.width).
				MaxHeight(1).
				Padding(0, 1).
				Render(fmt.Sprintf("/ search: %s — %d of %d (case-insensitive) │ esc: close",
					m.searchQuery, len(filteredTopics(m.topics, m.searchQuery)), len(m.topics))),
		)
	case m.dlqView != nil:
		context = "r: replay │ esc: back │ "
	case m.tailView != nil:
		context = "p: pause/resume │ esc: back │ "
	case m.activeTab == 1:
		context = "/: search │ enter: tail │ "
	case m.activeTab == 2:
		context = "pgup/pgdn: page │ "
	case m.activeTab == 4:
		context = "enter: inspect │ "
	case m.activeTab == 5:
		context = "w: window │ a: analyze │ "
	case m.activeTab == 0:
		context = "j/k: move │ "
	}
	help := context + "Auto-refresh: " + autoRefreshInterval.String() +
		" │ 1-6: jump │ ?: help │ q: quit"
	return helpStyle.Render(
		lipgloss.NewStyle().
			Background(lipgloss.Color("#1F1A2E")).
			Width(m.width).
			MaxHeight(1).
			Padding(0, 1).
			Render(help),
	)
}

// renderHelpModal renders the full keybinding legend overlay, shown when ? is
// pressed and dismissed with esc. It documents every global and per-view key
// so the footer itself can stay short.
func (m *Model) renderHelpModal() string {
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Render("KEYBINDINGS"),
		"",
		" GLOBAL",
		"   1-6       jump to tab",
		"   tab/l/h   switch tab",
		"   r         refresh",
		"   ?         this help",
		"   q         quit",
		"",
		" TABLES (Overview, Consumers, Alerts, DLQ)",
		"   j/k       move selection",
		"   pgup/dn   page",
		"   enter     open (tail topic / DLQ topic)",
		"",
		" TOPICS",
		"   /         search",
		"",
		" ANALYTICS",
		"   j/k       scroll",
		"   [ / ]     cycle pattern topic",
		"   w         analyze window",
		"   a         run analyze",
		"",
		" TAIL (Enter on a topic)",
		"   p         pause/resume",
		"   g         jump to bottom",
		"   esc       back",
		"",
		" DLQ INSPECT (Enter on a DLQ topic)",
		"   r         replay",
		"   y/n       confirm replay",
		"   esc       back",
	}

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#4C1D95")).
		Padding(1, 2).
		Render(lipgloss.JoinVertical(lipgloss.Left, lines...))
	box = lipgloss.NewStyle().Width(m.width).Align(lipgloss.Center).Render(box)

	vpad := m.height - lipgloss.Height(box)
	if vpad > 0 {
		box = strings.Repeat("\n", vpad/2) + box
	}
	return box + "\n" + helpStyle.Render("  esc: close  │  q: quit")
}

// Run starts the bubbletea program with live data.
// If client is non-nil, data is fetched directly from Kafka.
// Otherwise, the daemon's SQLite store is used.
// The client is borrowed, not closed: callers own it (see cli.runTUI).
func Run(client *kafka.Client) error {
	if client != nil {
		return runWithKafka(client)
	}
	return runWithStore("") // default store path
}

func runWithKafka(client *kafka.Client) error {
	m := NewModelWithKafka(client)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func runWithStore(storePath string) error {
	if storePath == "" {
		storePath = "~/.streampulse/state.db"
	}
	m, err := NewModel(storePath)
	if err != nil {
		return fmt.Errorf("create model: %w", err)
	}
	defer m.store.Close() // runWithStore owns the store; closes once on every exit path
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
