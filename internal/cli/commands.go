package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/pulsedev/streampulse/internal/alerts"
	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/pulsedev/streampulse/internal/check"
	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/daemon"
	"github.com/pulsedev/streampulse/internal/dlq"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/pulsedev/streampulse/internal/tui"
	"github.com/spf13/cobra"
)

// validateAlertConditions checks that every configured alert rule condition
// parses, so a typo fails at startup instead of silently keeping the builtin
// rule or erroring mid-run. An empty condition is allowed: it keeps the
// builtin condition (see alerts.MergeRules).
func validateAlertConditions(cfg *config.Config) error {
	for _, r := range cfg.Alerts {
		if r.Condition == "" {
			continue
		}
		if _, err := alerts.ParseCondition(r.Condition); err != nil {
			return fmt.Errorf("alert rule %q: %w", r.Name, err)
		}
	}
	return nil
}

// newKafkaClient builds the kafka client for cfg, applying the TLS and SASL
// settings from the kafka config section.
func newKafkaClient(cfg *config.Config) (*kafka.Client, error) {
	return kafka.NewClientWithOptions(cfg.Brokers, kafka.Options{
		TLS: kafka.TLSOptions{
			Enabled:            cfg.Kafka.TLS.Enabled,
			CAFile:             cfg.Kafka.TLS.CAFile,
			CertFile:           cfg.Kafka.TLS.CertFile,
			KeyFile:            cfg.Kafka.TLS.KeyFile,
			InsecureSkipVerify: cfg.Kafka.TLS.InsecureSkipVerify,
		},
		SASL: kafka.SASLOptions{
			Mechanism:   cfg.Kafka.SASL.Mechanism,
			Username:    cfg.Kafka.SASL.Username,
			PasswordEnv: cfg.Kafka.SASL.PasswordEnv,
		},
	})
}

func runTUI(cfg *config.Config) error {
	var client *kafka.Client
	if len(cfg.Brokers) > 0 {
		c, err := newKafkaClient(cfg)
		if err != nil {
			return err
		}
		client = c
		defer client.Close()
	}
	return tui.Run(client)
}

func newServeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start daemon mode — continuous metrics collection and alerting",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}

			store, err := storage.NewStore(cfg.Storage.Type, cfg.Storage.SQLite.Path)
			if err != nil {
				return err
			}
			defer store.Close()

			client, err := kafka.NewClientWithOptions(cfg.Brokers, kafka.Options{
				TLS: kafka.TLSOptions{
					Enabled:            cfg.Kafka.TLS.Enabled,
					CAFile:             cfg.Kafka.TLS.CAFile,
					CertFile:           cfg.Kafka.TLS.CertFile,
					KeyFile:            cfg.Kafka.TLS.KeyFile,
					InsecureSkipVerify: cfg.Kafka.TLS.InsecureSkipVerify,
				},
				SASL: kafka.SASLOptions{
					Mechanism:   cfg.Kafka.SASL.Mechanism,
					Username:    cfg.Kafka.SASL.Username,
					PasswordEnv: cfg.Kafka.SASL.PasswordEnv,
				},
			})
			if err != nil {
				return err
			}
			defer client.Close()

			d := daemon.NewWithOptions(cfg, store, client, daemon.Options{Version: cmd.Root().Version})

			// SIGTERM/SIGINT cancel the daemon context; a second signal
			// takes the immediate-exit path (signal.NotifyContext stops
			// catching, so the default handler kills the process).
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return d.Run(ctx)
		},
	}
}

// checkResultView is the JSON shape of one check result.
type checkResultView struct {
	Name    string  `json:"name"`
	Status  string  `json:"status"`
	Message string  `json:"message"`
	Value   float64 `json:"value,omitempty"`
}

// newCheckResults runs the health checks for cfg and returns the results and
// the process verdict (0 pass, 1 failed check, 2 connectivity/usage). The
// command wrapper maps the verdict to os.Exit; tests call this directly.
func newCheckResults(ctx context.Context, cfg *config.Config, flags check.Flags) ([]check.Result, int, error) {
	client, err := newKafkaClient(cfg)
	if err != nil {
		return nil, 2, err
	}
	defer client.Close()

	results := check.RunAll(ctx, check.Env{Client: client, Flags: flags})
	return results, check.Verdict(results), nil
}

// printCheckResults renders one line per check plus the verdict line.
func printCheckResults(w io.Writer, results []check.Result) {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", r.Name, r.Status, r.Message)
	}
	tw.Flush()
}

// verdictLabel maps the numeric exit code to its human label.
func verdictLabel(verdict int) string {
	switch verdict {
	case 1:
		return "FAIL"
	case 2:
		return "ERROR"
	default:
		return "PASS"
	}
}

// printCheckJSON renders the results plus the verdict as JSON.
func printCheckJSON(w io.Writer, results []check.Result, verdict int) error {
	views := make([]checkResultView, len(results))
	for i, r := range results {
		views[i] = checkResultView{Name: r.Name, Status: string(r.Status), Message: r.Message, Value: r.Value}
	}
	return json.NewEncoder(w).Encode(struct {
		Results  []checkResultView `json:"results"`
		Verdict  int               `json:"verdict"`
		ExitCode int               `json:"exit_code"`
	}{Results: views, Verdict: verdict, ExitCode: verdict})
}

func newCheckCommand() *cobra.Command {
	var (
		topics            []string
		groups            []string
		minPartitions     int
		maxLag            int64
		minRetentionHours float64
		checkReplication  bool
		timeout           time.Duration
		jsonOut           bool
	)
	cmd := &cobra.Command{
		Use:   "check",
		Short: "One-shot health check for CI/CD pipelines",
		Long: `One-shot health check for CI/CD pipelines.

Exit codes:
  0  all checks passed
  1  a check failed (threshold exceeded, resource unhealthy)
  2  usage, config, or connectivity error (pipeline problem)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			results, verdict, err := newCheckResults(cmd.Context(), cfg, check.Flags{
				Topics:            topics,
				Groups:            groups,
				MinPartitions:     minPartitions,
				MaxLag:            maxLag,
				MinRetentionHours: minRetentionHours,
				CheckReplication:  checkReplication,
				Timeout:           timeout,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printCheckJSON(cmd.OutOrStdout(), results, verdict); err != nil {
					return err
				}
			} else {
				printCheckResults(cmd.OutOrStdout(), results)
				fmt.Fprintf(cmd.OutOrStdout(), "verdict: %s (exit %d)\n", verdictLabel(verdict), verdict)
			}
			os.Exit(verdict)
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&topics, "topic", nil, "Topic to check for existence and partition health (repeatable)")
	cmd.Flags().StringArrayVar(&groups, "group", nil, "Consumer group to check for state and lag (repeatable)")
	cmd.Flags().IntVar(&minPartitions, "min-partitions", check.DefaultMinPartitions, "Minimum partitions per topic")
	cmd.Flags().Int64Var(&maxLag, "max-lag", check.DefaultMaxLag, "Maximum total consumer lag per group")
	cmd.Flags().Float64Var(&minRetentionHours, "min-retention-hours", 0, "Minimum topic retention in hours (checked per topic)")
	cmd.Flags().BoolVar(&checkReplication, "check-replication", false, "Fail when any partition is under-replicated (ISR smaller than replica set)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Overall deadline for the check run")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output results and verdict as JSON")
	return cmd
}

func newDLQCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Dead letter queue management",
	}
	cmd.AddCommand(newDLQListCommand())
	cmd.AddCommand(newDLQInspectCommand())
	cmd.AddCommand(newDLQReplayCommand())
	return cmd
}

// dlqTopicView is the JSON shape of a discovered DLQ topic.
type dlqTopicView struct {
	Name           string  `json:"name"`
	OriginalTopic  string  `json:"original_topic"`
	OriginalExists bool    `json:"original_exists"`
	MessageCount   int64   `json:"message_count"`
	GrowthRate     float64 `json:"growth_rate,omitempty"`
}

func newDLQListCommand() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "Auto-discover DLQ topics",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			client, err := newKafkaClient(cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			topics, err := dlq.Discover(cmd.Context(), client, nil)
			if err != nil {
				return err
			}
			if len(topics) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no DLQ topics found")
				return nil
			}
			if jsonOut {
				views := make([]dlqTopicView, len(topics))
				for i, t := range topics {
					views[i] = dlqTopicView{
						Name: t.Name, OriginalTopic: t.OriginalTopic,
						OriginalExists: t.OriginalExists, MessageCount: t.MessageCount,
						GrowthRate: t.GrowthRate,
					}
				}
				return json.NewEncoder(cmd.OutOrStdout()).Encode(views)
			}
			printDLQTable(cmd.OutOrStdout(), topics)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output as JSON")
	return cmd
}

// printDLQTable renders the discovered DLQ topics as an aligned table.
func printDLQTable(w io.Writer, topics []dlq.Topic) {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tORIGINAL TOPIC\tMESSAGES\tGROWTH")
	for _, t := range topics {
		original := t.OriginalTopic
		if !t.OriginalExists {
			original += " (missing)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", t.Name, original, t.MessageCount, "-")
	}
	tw.Flush()
}

// dlqMessageView is the JSON shape of one inspected message, with key and
// value rendered via the dlq display rules (text or hex, truncated).
type dlqMessageView struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func newDLQInspectCommand() *cobra.Command {
	var (
		topic    string
		limit    int
		maxBytes int
		jsonOut  bool
	)
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect messages in a DLQ topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return fmt.Errorf("dlq inspect: --topic is required")
			}
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			msgs, err := dlq.Inspect(cmd.Context(), cfg.Brokers, topic, limit)
			if err != nil {
				return err
			}
			if len(msgs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no messages")
				return nil
			}
			views := make([]dlqMessageView, len(msgs))
			for i, m := range msgs {
				views[i] = dlqMessageView{
					Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Timestamp: m.Timestamp,
					Key: dlq.DisplayValue(m.Key, maxBytes), Value: dlq.DisplayValue(m.Value, maxBytes),
					Headers: m.Headers,
				}
			}
			if jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(views)
			}
			printDLQMessages(cmd.OutOrStdout(), views)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "DLQ topic to inspect (required)")
	cmd.Flags().IntVar(&limit, "limit", dlq.DefaultInspectLimit, "Maximum number of messages to read")
	cmd.Flags().IntVar(&maxBytes, "max-bytes", dlq.DefaultDisplayMaxBytes, "Truncate key/value payloads to this many bytes")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output as JSON")
	return cmd
}

// printDLQMessages renders one block per inspected message.
func printDLQMessages(w io.Writer, views []dlqMessageView) {
	for i, m := range views {
		if i > 0 {
			fmt.Fprintln(w, "---")
		}
		fmt.Fprintf(w, "partition=%d offset=%d time=%s key=%q\n", m.Partition, m.Offset, m.Timestamp.Format(time.RFC3339), m.Key)
		fmt.Fprintf(w, "  value: %s\n", m.Value)
		if len(m.Headers) > 0 {
			keys := make([]string, 0, len(m.Headers))
			for k := range m.Headers {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var parts []string
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%s", k, m.Headers[k]))
			}
			fmt.Fprintf(w, "  headers: %s\n", strings.Join(parts, " "))
		}
	}
}

// dlqReplayView is the JSON shape of a replay run summary.
type dlqReplayView struct {
	DryRun   bool  `json:"dry_run"`
	Total    int64 `json:"total"`
	Replayed int64 `json:"replayed"`
	Filtered int64 `json:"filtered"`
	Skipped  int64 `json:"skipped"`
	Failed   int64 `json:"failed"`
	Batches  int   `json:"batches"`
}

// replayDryRunLine summarizes a dry run. Candidates are the messages that
// would be produced, i.e. every read message minus filtered and skipped ones;
// ReplayResult.Replayed stays zero because nothing was produced.
// dlqReplay is a test seam so the command output can be exercised without a
// broker; it defaults to the real replay.
var dlqReplay = dlq.Replay

func replayDryRunLine(res *dlq.ReplayResult) string {
	return fmt.Sprintf("would replay %d of %d messages (%d filtered, %d skipped)\n",
		res.Total-res.Filtered-res.Skipped, res.Total, res.Filtered, res.Skipped)
}

func newDLQReplayCommand() *cobra.Command {
	var (
		topic        string
		dryRun       bool
		limit        int
		olderThan    string
		filter       string
		skipExisting bool
		jsonOut      bool
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay DLQ messages to original topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return fmt.Errorf("dlq replay: --topic is required")
			}
			var older time.Duration
			if olderThan != "" {
				d, err := time.ParseDuration(olderThan)
				if err != nil {
					return fmt.Errorf("dlq replay: --older-than %q: %w", olderThan, err)
				}
				older = d
			}
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			res, err := dlqReplay(cmd.Context(), dlq.ReplayOptions{
				Brokers: cfg.Brokers, Topic: topic, DryRun: dryRun,
				Limit: limit, OlderThan: older, Filter: filter, SkipExisting: skipExisting,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(dlqReplayView{
					DryRun: res.DryRun, Total: res.Total, Replayed: res.Replayed,
					Filtered: res.Filtered, Skipped: res.Skipped, Failed: res.Failed, Batches: res.Batches,
				})
			}
			if dryRun {
				fmt.Fprint(cmd.OutOrStdout(), replayDryRunLine(res))
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "replayed %d of %d messages (%d filtered, %d skipped, %d failed) in %d batches\n",
					res.Replayed, res.Total, res.Filtered, res.Skipped, res.Failed, res.Batches)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "DLQ topic to replay (required)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Count and sample messages without producing")
	cmd.Flags().IntVar(&limit, "limit", 0, "Maximum number of messages to read (0 = no limit)")
	cmd.Flags().StringVar(&olderThan, "older-than", "", "Only replay messages older than this duration (e.g. 1h)")
	cmd.Flags().StringVar(&filter, "filter", "", "Only replay messages with a header key=value match")
	cmd.Flags().BoolVar(&skipExisting, "skip-existing", false, "Skip messages already replayed (marker header present)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output summary as JSON")
	return cmd
}

func newAnalyzeCommand() *cobra.Command {
	var (
		windowStr       string
		topicsStr       string
		skew            bool
		retention       bool
		anomalyMetrics  []string
		rebalanceGroups []string
		patternsMetric  string
		jsonOut         bool
	)
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analytics — trends, growth, partition skew",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			window, err := time.ParseDuration(windowStr)
			if err != nil || window <= 0 {
				return fmt.Errorf("analyze: invalid window %q (e.g. 24h, 168h)", windowStr)
			}
			var topics []string
			for _, t := range strings.Split(topicsStr, ",") {
				if t = strings.TrimSpace(t); t != "" {
					topics = append(topics, t)
				}
			}

			runAnomalies := cmd.Flags().Changed("anomalies")
			var anomalyMetricNames []string
			if runAnomalies {
				anomalyMetricNames, err = resolveAnomalyMetrics(anomalyMetrics)
				if err != nil {
					return err
				}
			}
			var patternMetric string
			if patternsMetric != "" {
				patternMetric, err = resolvePatternMetric(patternsMetric)
				if err != nil {
					return err
				}
			}
			runRebalances := cmd.Flags().Changed("rebalances")

			store, err := storage.NewStore(cfg.Storage.Type, cfg.Storage.SQLite.Path)
			if err != nil {
				return err
			}
			defer store.Close()
			client, err := newKafkaClient(cfg)
			if err != nil {
				return err
			}
			defer client.Close()

			analyzer := analytics.NewAnalyzer(store, client)

			growth, err := analyzer.Growth(cmd.Context(), topics, window)
			if err != nil {
				return fmt.Errorf("analyze: growth: %w", err)
			}
			growth = topGrowth(growth, 10)

			var patternReports []analytics.ThroughputReport
			if patternMetric != "" {
				patternReports, err = analyzer.Patterns(cmd.Context(), topics, patternMetric, window)
				if err != nil {
					return fmt.Errorf("analyze: patterns: %w", err)
				}
			}

			var skewReports []analytics.SkewReport
			if skew {
				skewReports, err = analyzer.Skew(cmd.Context())
				if err != nil {
					return fmt.Errorf("analyze: skew: %w", err)
				}
			}
			var retentionReports []analytics.RetentionReport
			if retention {
				reportTopics := topics
				if len(reportTopics) == 0 {
					for _, g := range growth {
						reportTopics = append(reportTopics, g.Topic)
					}
				}
				retentionReports, err = analyzer.Retention(cmd.Context(), reportTopics)
				if err != nil {
					return fmt.Errorf("analyze: retention: %w", err)
				}
			}

			var anomalyReports []analytics.Anomaly
			if runAnomalies {
				anomalyReports, err = analyzer.Anomalies(cmd.Context(), anomalyMetricNames, window)
				if err != nil {
					return fmt.Errorf("analyze: anomalies: %w", err)
				}
			}
			var rebalanceReports []analytics.RebalanceReport
			if runRebalances {
				rebalanceReports, err = analyzer.Rebalances(cmd.Context(), rebalanceGroups, window)
				if err != nil {
					return fmt.Errorf("analyze: rebalances: %w", err)
				}
			}

			if len(growth) == 0 && len(patternReports) == 0 && len(skewReports) == 0 &&
				len(retentionReports) == 0 && len(anomalyReports) == 0 && len(rebalanceReports) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no data")
				return nil
			}
			if jsonOut {
				sections := map[string]any{"growth": emptySlice(growth)}
				if patternMetric != "" {
					sections["patterns"] = emptySlice(patternReports)
				}
				if skew {
					sections["skew"] = emptySlice(skewReports)
				}
				if retention {
					sections["retention"] = emptySlice(retentionReports)
				}
				if runAnomalies {
					sections["anomalies"] = emptySlice(anomalyReports)
				}
				if runRebalances {
					sections["rebalances"] = emptySlice(rebalanceReports)
				}
				return json.NewEncoder(cmd.OutOrStdout()).Encode(sections)
			}
			printGrowthReport(cmd.OutOrStdout(), growth)
			printPatternReport(cmd.OutOrStdout(), patternReports)
			printSkewReport(cmd.OutOrStdout(), skewReports)
			printRetentionReport(cmd.OutOrStdout(), retentionReports)
			printAnomalyReport(cmd.OutOrStdout(), anomalyReports)
			printRebalanceReport(cmd.OutOrStdout(), rebalanceReports)
			return nil
		},
	}
	cmd.Flags().StringVar(&windowStr, "window", "24h", "Growth window (e.g. 1h, 24h, 168h)")
	cmd.Flags().StringVar(&topicsStr, "topics", "", "Comma-separated topic filter (default: top 10 by message volume)")
	cmd.Flags().BoolVar(&skew, "skew", false, "Include the partition skew report")
	cmd.Flags().BoolVar(&retention, "retention", false, "Include the retention report")
	cmd.Flags().StringSliceVar(&anomalyMetrics, "anomalies", nil, "Detect anomalies for these metrics (lag, msg_rate, bytes_rate; default all)")
	cmd.Flags().StringSliceVar(&rebalanceGroups, "rebalances", nil, "Show rebalance history (optional: comma-separated group filters)")
	cmd.Flags().StringVar(&patternsMetric, "patterns", "", "Show throughput patterns for a metric (msg_rate, bytes_rate)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output full report structs as JSON")
	return cmd
}

// resolveAnomalyMetrics maps --anomalies values (full metric names or their
// last-segment aliases, e.g. "lag") to full metric names. An empty selection
// means all anomaly metrics.
func resolveAnomalyMetrics(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		full, ok := anomalyMetric(n)
		if !ok {
			return nil, fmt.Errorf("analyze: --anomalies: unknown metric %q (use lag, msg_rate, or bytes_rate)", n)
		}
		out = append(out, full)
	}
	return out, nil
}

// anomalyMetric resolves one --anomalies value to a full metric name.
func anomalyMetric(name string) (string, bool) {
	for _, m := range analytics.AnomalyMetrics {
		if m == name {
			return m, true
		}
		if i := strings.LastIndexByte(m, '.'); i >= 0 && m[i+1:] == name {
			return m, true
		}
	}
	return "", false
}

// resolvePatternMetric maps the --patterns value to the topic rate metric.
func resolvePatternMetric(name string) (string, error) {
	switch name {
	case "msg_rate":
		return scraper.MetricTopicMsgRate, nil
	case "bytes_rate":
		return scraper.MetricTopicBytesRate, nil
	}
	return "", fmt.Errorf("analyze: --patterns: unknown metric %q (use msg_rate or bytes_rate)", name)
}

// emptySlice returns a non-nil empty slice for nil input so JSON sections
// marshal as [] instead of null.
func emptySlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// topGrowth keeps the n topics with the highest total message rate in the
// window, preserving the analyzer's name ordering.
func topGrowth(reports []analytics.GrowthReport, n int) []analytics.GrowthReport {
	if len(reports) <= n {
		return reports
	}
	total := make(map[string]float64, len(reports))
	for _, r := range reports {
		for _, p := range r.Points {
			total[r.Topic] += p.Rate
		}
	}
	order := append([]analytics.GrowthReport(nil), reports...)
	sort.SliceStable(order, func(i, j int) bool { return total[order[i].Topic] > total[order[j].Topic] })
	return order[:n]
}

// printGrowthReport renders the growth section with per-topic sparklines.
func printGrowthReport(w io.Writer, reports []analytics.GrowthReport) {
	if len(reports) == 0 {
		return
	}
	fmt.Fprintf(w, "Growth (%s window)\n", reports[0].Window)
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	for _, r := range reports {
		fmt.Fprintf(tw, "  %s\t%s\t%+.2f msgs/s\n", r.Topic, r.Sparkline, r.Delta)
	}
	tw.Flush()
}

// printPatternReport renders the per-topic throughput profiles with the
// hourly barchart.
func printPatternReport(w io.Writer, reports []analytics.ThroughputReport) {
	if len(reports) == 0 {
		return
	}
	labels := make([]string, 24)
	for h := range labels {
		labels[h] = fmt.Sprintf("%02d:00", h)
	}
	fmt.Fprintln(w, "PATTERNS")
	for _, r := range reports {
		fmt.Fprintf(w, "  %s (%s) peak %s %02d:00, slope %+.4g/s, forecast 7d %.2f\n",
			r.Topic, r.Metric, time.Weekday(r.PeakDay), r.PeakHour, r.Slope, r.Forecast7d)
		fmt.Fprintln(w, analytics.Bars(labels, r.HourlyProfile[:], 60))
	}
}

// printAnomalyReport renders the flagged anomaly rows.
func printAnomalyReport(w io.Writer, anomalies []analytics.Anomaly) {
	if len(anomalies) == 0 {
		return
	}
	fmt.Fprintln(w, "ANOMALIES")
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tENTITY\tTIME\tVALUE\tEXPECTED\tZ\tDIR\tSEVERITY")
	for _, a := range anomalies {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f\t%.2f\t%.2f\t%s\t%s\n",
			a.Metric, a.Entity, a.Time.UTC().Format("2006-01-02 15:04:05 UTC"),
			a.Value, a.Expected, a.ZScore, a.Direction, a.Severity)
	}
	tw.Flush()
}

// printRebalanceReport renders per-group per-day rebalance counts.
func printRebalanceReport(w io.Writer, reports []analytics.RebalanceReport) {
	if len(reports) == 0 {
		return
	}
	fmt.Fprintln(w, "REBALANCES")
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "GROUP\tDAY\tCOUNT")
	for _, r := range reports {
		fmt.Fprintf(tw, "%s\t%s\t%d\n", r.Group, r.Day.Format("2006-01-02"), r.Count)
	}
	tw.Flush()
}

// printSkewReport renders the cluster leadership distribution.
func printSkewReport(w io.Writer, reports []analytics.SkewReport) {
	if len(reports) == 0 {
		return
	}
	fmt.Fprintln(w, "Skew")
	for _, r := range reports {
		status := "balanced"
		if !r.Balanced {
			status = "SKEWED"
		}
		ids := make([]string, 0, len(r.Leaders))
		for id := range r.Leaders {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var parts []string
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf("broker %s=%d", id, r.Leaders[id]))
		}
		fmt.Fprintf(w, "  ratio %.2f (%s)  %s\n", r.Ratio, status, strings.Join(parts, ", "))
	}
}

// printRetentionReport renders the per-topic retention posture.
func printRetentionReport(w io.Writer, reports []analytics.RetentionReport) {
	if len(reports) == 0 {
		return
	}
	fmt.Fprintln(w, "Retention")
	for _, r := range reports {
		atRisk := "no"
		if r.AtRisk {
			atRisk = "YES"
		}
		retention := "unknown"
		if r.RetentionMS > 0 {
			retention = fmt.Sprintf("%.1fh", r.RetentionMS.Hours())
		}
		fmt.Fprintf(w, "  %s: retention %s, oldest %s, fill estimate %.1fd, at risk: %s\n",
			r.Topic, retention, r.OldestOffsetAge.Round(time.Minute), r.EstimateFillDays, atRisk)
	}
}

func newAlertsCommand() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "alerts",
		Short: "View current alert status",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}

			store, err := storage.NewStore(cfg.Storage.Type, cfg.Storage.SQLite.Path)
			if err != nil {
				return err
			}
			defer store.Close()

			rows, err := store.QueryAlertState(cmd.Context())
			if err != nil {
				return fmt.Errorf("query alert state: %w", err)
			}
			if len(rows) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no alerts")
				return nil
			}
			if jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(rows)
			}
			printAlertTable(cmd.OutOrStdout(), rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output alert state as JSON")
	return cmd
}

// printAlertTable renders the persisted alert states as an aligned table
// (rule, status, last fired, last value, notify count).
func printAlertTable(w io.Writer, rows []storage.AlertStateRow) {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "RULE\tSTATUS\tLAST FIRED\tVALUE\tNOTIFY COUNT")
	for _, r := range rows {
		lastFired := "-"
		if !r.LastFired.IsZero() {
			lastFired = r.LastFired.Format(time.RFC3339)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f\t%d\n", r.RuleName, r.Status, lastFired, r.LastValue, r.NotifyCount)
	}
	tw.Flush()
}
