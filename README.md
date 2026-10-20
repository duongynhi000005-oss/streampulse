# StreamPulse — The k9s for Apache Kafka

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**Single binary. Zero CGo. `brew install streampulse`.**

Real-time Kafka observability in your terminal: consumer lag, broker health, DLQ management, analytics, and a production alert engine — all in a k9s-style TUI backed by a 24/7 daemon.

## Quick Start

```bash
# Build
make build                       # → bin/streampulse

# Start the daemon (scrape every 5s, SQLite persistence, Prometheus :9090)
bin/streampulse serve --brokers localhost:9093

# Open the TUI (another terminal) — connects to Kafka directly
bin/streampulse                  # default brokers: localhost:9093

# CI/CD health gate (exit 0 healthy / 1 failed / 2 error)
bin/streampulse check --topic orders --group orders-processor --max-lag 1000 --json

# DLQ management
bin/streampulse dlq list
bin/streampulse dlq inspect --topic payments.dlq --limit 10
bin/streampulse dlq replay --topic payments.dlq --dry-run

# Alerts & analytics
bin/streampulse alerts --json
bin/streampulse analyze --topics orders,payments --window 24h
```

Local dev Kafka: `docker compose up -d` (KRaft Kafka 3.9 + producer/consumer).

## Features

### Implemented (v0.1)

| Feature | Description |
|---------|-------------|
| 🖥️ **TUI Dashboard** | 6 tabs — Overview, Topics, Consumers, Alerts, DLQ, Analytics. Auto-refresh 2s, `/` search, table navigation, activity log |
| ⚙️ **Daemon** | `serve` — scraper loop (5s), hourly/daily rollup, per-resolution retention purge, graceful shutdown, startup backoff |
| 📊 **Prometheus /metrics** | `:9090/metrics` — scrape stats, alert state, build info |
| 🔔 **Alert Engine** | 6 rules (broker down, under-replication, consumer lag, DLQ growth, partition skew, scrape failing) with ok→pending→firing state machine + Slack/Email/PagerDuty |
| 📂 **DLQ Module** | Convention-based discovery (`.dlq/.dead/.error/.failed`), inspect, replay with dry-run, filters, skip-existing |
| 📈 **Analytics L1** | Topic growth sparklines, partition skew, retention analysis (CLI + TUI) |
| ✅ **CI/CD Health Gate** | `check` — connectivity, topics, group lag, retention, replication; exits 0/1/2 |
| 🔐 **Kafka Auth** | TLS/mTLS, SASL PLAIN/SCRAM, AWS MSK IAM (SigV4, stdlib-only) |
| 💾 **Storage** | SQLite (WAL), raw(5s,24h)→hourly(90d)→daily(365d), JSON tags, percentile aggregates |

### v0.2 (Planned)

- REST API on daemon (`:9090/api`)
- PostgreSQL backend
- Embedded local web dashboard
- Kafka Connect module

*(Analytics L2 — anomaly detection, rebalance history, throughput patterns — is implemented; see below.)*

### Analytics L2 (Implemented)

| Feature | Description |
|---------|-------------|
| 🔎 **Anomaly Detection** | Seasonal hour-of-week baselines + rolling Z-score on lag/msg-rate/bytes-rate; `analyze --anomalies` |
| 🔁 **Rebalance History** | Per-group per-day rebalance counts from persisted group-state samples; `analyze --rebalances` |
| 📊 **Throughput Patterns** | Hour-of-day/day-of-week profiles, peak hour/day, linear trend forecast; `analyze --patterns` |
| 🖥️ **TUI Panes** | Anomaly list, rebalance table, and selected-topic pattern bars on the Analytics tab |

### v0.3 (Planned)

- PulseDev Cloud (hosted dashboards, team access, SSO)
- ClickHouse analytics backend
- Kafka Streams + Flink monitoring
- Schema Registry compatibility

## Configuration

Precedence: **defaults < YAML file < env (`STREAMPULSE_*`) < flags**.

Config file: `--config <path>`, `$STREAMPULSE_CONFIG`, `~/.config/streampulse/streampulse.yaml`, or `/etc/streampulse/streampulse.yaml`.

```yaml
# ~/.config/streampulse/streampulse.yaml
brokers: ["localhost:9093"]
cluster_id: local-dev
scrape_interval: 5s

storage:
  type: sqlite            # sqlite | postgres (v0.2) | clickhouse (v0.3)
  sqlite:
    path: ~/.streampulse/state.db

kafka:
  tls:
    enabled: false
    ca_file: ""
    cert_file: ""         # mTLS
    key_file: ""
  sasl:
    mechanism: ""         # plain | scram-sha-256 | scram-sha-512 | aws-iam
    username: ""
    password_env: STREAMPULSE_SASL_PASSWORD

prometheus:
  listen: ":9090"
  path: /metrics

alerts:
  - name: consumer-lag
    condition: "lag > 1000"
    for: 2m
    severity: warning
    notify:
      - type: slack
        webhook_env: SLACK_WEBHOOK_URL
```

Secrets (passwords, webhook URLs) are referenced by `*_env` names — never stored in the YAML.

## `check` exit codes

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | A check failed (threshold exceeded) |
| 2 | Usage/config/connectivity error |

## Building from Source

```bash
git clone https://github.com/pulsedev/streampulse.git
cd streampulse
make build        # → bin/streampulse
make run          # → build + run TUI
make test         # → go test -race ./...
make lint         # → golangci-lint
```

## Tech Stack

| Layer | Choice |
|-------|--------|
| Language | Go 1.25 |
| TUI | [bubbletea](https://github.com/charmbracelet/bubbletea) |
| Kafka | [kafka-go](https://github.com/segmentio/kafka-go) |
| Storage | SQLite ([modernc](https://modernc.org/sqlite)) |
| Metrics | [Prometheus client](https://github.com/prometheus/client_golang) |
| Scheduling | [robfig/cron](https://github.com/robfig/cron) |

## License

Apache 2.0 — see [LICENSE](LICENSE). Third-party component licenses are listed in
[NOTICE](NOTICE). Contributions are accepted under the same license with a DCO
sign-off; see [CONTRIBUTING.md](CONTRIBUTING.md).

