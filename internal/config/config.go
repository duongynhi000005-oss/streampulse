// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package config provides configuration loading via viper.
package config

// Config holds all StreamPulse configuration.
type Config struct {
	Brokers        []string      `mapstructure:"brokers"`
	ClusterID      string        `mapstructure:"cluster_id"`
	ScrapeInterval string        `mapstructure:"scrape_interval"`
	Storage        StorageConfig `mapstructure:"storage"`
	Kafka          KafkaConfig   `mapstructure:"kafka"`
	Alerts         []AlertRule   `mapstructure:"alerts"`
	Prometheus     PromConfig    `mapstructure:"prometheus"`
	Serve          ServeConfig   `mapstructure:"serve"`
}

// KafkaConfig holds Kafka connection security settings.
type KafkaConfig struct {
	TLS  TLSConfig  `mapstructure:"tls"`
	SASL SASLConfig `mapstructure:"sasl"`
}

// TLSConfig holds TLS/mTLS settings for the Kafka connection.
type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// SASLConfig holds SASL authentication settings.
type SASLConfig struct {
	Mechanism   string `mapstructure:"mechanism"`
	Username    string `mapstructure:"username"`
	PasswordEnv string `mapstructure:"password_env"`
}

// PromConfig holds the Prometheus metrics endpoint settings.
type PromConfig struct {
	Listen string `mapstructure:"listen"`
	Path   string `mapstructure:"path"`
}

// ServeConfig holds daemon mode settings.
type ServeConfig struct {
	LogLevel string `mapstructure:"log_level"`
}

// StorageConfig holds the storage backend configuration.
type StorageConfig struct {
	Type     string       `mapstructure:"type"`
	SQLite   SQLiteConfig `mapstructure:"sqlite"`
	Postgres DBConfig     `mapstructure:"postgres"`
}

// SQLiteConfig holds SQLite-specific settings.
type SQLiteConfig struct {
	Path string `mapstructure:"path"`
}

// DBConfig holds database connection settings.
type DBConfig struct {
	DSN      string `mapstructure:"dsn"`
	MaxConns int    `mapstructure:"max_connections"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

// AlertRule defines an alert condition.
type AlertRule struct {
	Name      string         `mapstructure:"name"`
	Group     string         `mapstructure:"group"`
	Condition string         `mapstructure:"condition"`
	For       string         `mapstructure:"for"`
	Severity  string         `mapstructure:"severity"`
	Notify    []AlertChannel `mapstructure:"notify"`
}

// AlertChannel defines a notification target.
type AlertChannel struct {
	Type    string `mapstructure:"type"`
	Webhook string `mapstructure:"webhook"`
	Channel string `mapstructure:"channel"`
	To      string `mapstructure:"to"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Brokers:        []string{"localhost:9093"},
		ClusterID:      "local-dev",
		ScrapeInterval: "5s",
		Storage: StorageConfig{
			Type: "sqlite",
			SQLite: SQLiteConfig{
				Path: "~/.streampulse/state.db",
			},
		},
		Prometheus: PromConfig{
			Listen: ":9090",
			Path:   "/metrics",
		},
		Serve: ServeConfig{
			LogLevel: "info",
		},
	}
}
