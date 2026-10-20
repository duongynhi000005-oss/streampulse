// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/pulsedev/streampulse/internal/kafka/sasliam"
)

// TLSOptions configures TLS and mutual TLS for broker connections. An empty
// (zero-value) TLSOptions leaves the client in plaintext mode.
type TLSOptions struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// buildTLSConfig loads the *tls.Config described by opts, or nil when TLS is
// not enabled. CAFile populates RootCAs; CertFile+KeyFile enable mTLS client
// authentication. Error messages never include key material.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	if !opts.Enabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}

	if opts.CAFile != "" {
		pemBytes, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("CA file %s: no valid certificates found", opts.CAFile)
		}
		cfg.RootCAs = pool
	}

	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("mTLS requires both cert_file and key_file")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if opts.InsecureSkipVerify {
		slog.Warn("kafka: TLS InsecureSkipVerify is enabled; broker certificates will not be verified")
	}
	return cfg, nil
}

// SASLOptions configures SASL authentication for broker connections. An
// empty Mechanism disables SASL. Password is resolved from the environment
// variable named by PasswordEnv — it is never passed inline.
type SASLOptions struct {
	Mechanism   string // plain | scram-sha-256 | scram-sha-512 | aws-iam
	Username    string
	PasswordEnv string
}

// buildSASL constructs the sasl.Mechanism described by opts, or nil when SASL
// is not configured. getenv resolves the password environment variable and is
// injected for testability; callers pass os.Getenv. Error messages never
// include credentials.
func buildSASL(opts SASLOptions, getenv func(string) string) (sasl.Mechanism, error) {
	switch opts.Mechanism {
	case "":
		return nil, nil
	case "plain", "scram-sha-256", "scram-sha-512":
		if opts.Username == "" {
			return nil, fmt.Errorf("sasl %s: username is required", opts.Mechanism)
		}
		password, err := saslPassword(opts, getenv)
		if err != nil {
			return nil, err
		}
		switch opts.Mechanism {
		case "plain":
			return plain.Mechanism{Username: opts.Username, Password: password}, nil
		case "scram-sha-256":
			return scram.Mechanism(scram.SHA256, opts.Username, password)
		default:
			return scram.Mechanism(scram.SHA512, opts.Username, password)
		}
	case "aws-iam":
		return &sasliam.Mechanism{Provider: sasliam.EnvProvider{}}, nil
	default:
		return nil, fmt.Errorf("unknown sasl mechanism %q", opts.Mechanism)
	}
}

// saslPassword resolves the password from the named environment variable,
// erroring when it is unconfigured or unset.
func saslPassword(opts SASLOptions, getenv func(string) string) (string, error) {
	if opts.PasswordEnv == "" {
		return "", fmt.Errorf("sasl %s: password_env is not configured", opts.Mechanism)
	}
	password := getenv(opts.PasswordEnv)
	if password == "" {
		return "", fmt.Errorf("sasl %s: environment variable %s is not set or empty", opts.Mechanism, opts.PasswordEnv)
	}
	return password, nil
}
