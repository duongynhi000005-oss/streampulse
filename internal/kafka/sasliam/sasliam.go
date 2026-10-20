// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package sasliam implements the AWS MSK IAM SASL mechanism for kafka-go.
//
// The mechanism performs the AWS MSK IAM handshake: the client sends an
// AWS SigV4-signed token request (version byte 0x01, mechanism name
// "AWS_MSK_IAM", and a JSON payload) to the broker, which authenticates it
// against AWS IAM and replies with a JSON status. Stdlib only — no AWS SDK.
package sasliam

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go/sasl"
)

const (
	// service is the AWS SigV4 service name used by MSK IAM.
	service = "kafka-cluster"
	// tokenVersion is the version field of the token payload.
	tokenVersion = "2020_10_22"
	// tokenTTLSeconds is the validity period of the signed token.
	tokenTTLSeconds = 900
	// defaultRegion is used when AWS_REGION / AWS_DEFAULT_REGION are unset.
	defaultRegion = "us-east-1"
)

// CredentialsProvider supplies AWS credentials for token signing.
type CredentialsProvider interface {
	// Credentials returns the access key, secret key, and optional session
	// token used to sign the MSK IAM token request.
	Credentials(ctx context.Context) (AccessKey, SecretKey, Token string, Err error)
}

// EnvProvider reads credentials from the AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN environment variables.
type EnvProvider struct{}

// Credentials implements CredentialsProvider.
func (EnvProvider) Credentials(ctx context.Context) (string, string, string, error) {
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if ak == "" || sk == "" {
		return "", "", "", fmt.Errorf("sasliam: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}
	return ak, sk, os.Getenv("AWS_SESSION_TOKEN"), nil
}

// Mechanism implements the AWS MSK IAM SASL mechanism. It is safe for
// concurrent use: state is immutable after construction.
type Mechanism struct {
	// Provider supplies AWS credentials. Defaults to EnvProvider.
	Provider CredentialsProvider
	// Region overrides AWS_REGION / AWS_DEFAULT_REGION when signing tokens.
	Region string
}

var _ sasl.Mechanism = (*Mechanism)(nil)

// Name returns the SASL mechanism identifier.
func (m *Mechanism) Name() string { return "AWS_MSK_IAM" }

// Start builds the MSK IAM token request: version byte, mechanism name, and
// the SigV4-signed JSON payload. The broker hostname is taken from the SASL
// metadata attached to ctx by kafka-go.
func (m *Mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	provider := m.Provider
	if provider == nil {
		provider = EnvProvider{}
	}
	ak, sk, token, err := provider.Credentials(ctx)
	if err != nil {
		return nil, nil, err
	}

	host := "localhost"
	if meta := sasl.MetadataFromContext(ctx); meta != nil && meta.Host != "" {
		host = net.JoinHostPort(meta.Host, strconv.Itoa(meta.Port))
	}

	payload, err := tokenRequest(ak, sk, token, host, m.Region, time.Now())
	if err != nil {
		return nil, nil, err
	}

	msg := append([]byte{0x01}, []byte("AWS_MSK_IAM")...)
	msg = append(msg, payload...)
	return stateMachine{}, msg, nil
}

// stateMachine completes the single round-trip MSK IAM exchange.
type stateMachine struct{}

// Next validates the broker response; status 200 completes authentication.
func (stateMachine) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if len(bytes.TrimSpace(challenge)) == 0 {
		return true, nil, nil
	}
	var resp struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(challenge, &resp); err != nil {
		return false, nil, fmt.Errorf("sasliam: invalid server response: %w", err)
	}
	if resp.Status != 200 {
		return false, nil, fmt.Errorf("sasliam: authentication failed: broker returned status %d", resp.Status)
	}
	return true, nil, nil
}

// tokenRequest builds the signed MSK IAM token payload.
func tokenRequest(ak, sk, sessionToken, host, region string, now time.Time) ([]byte, error) {
	region = resolveRegion(region)
	amzDate := now.UTC().Format("20060102T150405Z")

	params := map[string]string{
		"Action":  "KafkaCluster.SaslAuthenticate",
		"Version": "2018-11-14",
	}
	headers := map[string]string{
		"host":       host,
		"user-agent": "streampulse",
		"x-amz-date": amzDate,
	}

	canonical, err := canonicalRequest("GET", "/", params, headers, nil)
	if err != nil {
		return nil, err
	}
	sig, err := sign(sk, region, service, now, canonical)
	if err != nil {
		return nil, err
	}

	token := map[string]any{
		"version":           tokenVersion,
		"access_key":        ak,
		"secret_key":        sk,
		"session_token":     sessionToken,
		"signed_date":       amzDate,
		"signature_version": "4",
		"token_ttl_seconds": tokenTTLSeconds,
		"signature":         sig,
		"request_uri":       "/",
		"request_method":    "GET",
		"request_headers":   headers,
		"request_params":    params,
	}
	return json.Marshal(token)
}

// resolveRegion returns the region used for signing, preferring the explicit
// mechanism region over the AWS_REGION and AWS_DEFAULT_REGION environment
// variables, falling back to the default.
func resolveRegion(region string) string {
	if region != "" {
		return region
	}
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}
	return defaultRegion
}

// canonicalRequest builds the AWS SigV4 canonical request: sorted
// RFC 3986-encoded query parameters, lowercased and sorted header names, and
// the SHA-256 hex digest of the payload.
func canonicalRequest(method, uri string, params, headers map[string]string, payload []byte) (string, error) {
	canonicalHeaders := make(map[string]string, len(headers))
	for name, value := range headers {
		canonicalHeaders[strings.ToLower(name)] = strings.TrimSpace(value)
	}

	names := make([]string, 0, len(canonicalHeaders))
	for name := range canonicalHeaders {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString(method)
	b.WriteByte('\n')
	b.WriteString(uri)
	b.WriteByte('\n')

	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(uriEncode(k))
		b.WriteByte('=')
		b.WriteString(uriEncode(params[k]))
	}
	b.WriteByte('\n')

	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(canonicalHeaders[name])
		b.WriteByte('\n')
	}
	b.WriteByte('\n')

	b.WriteString(strings.Join(names, ";"))
	b.WriteByte('\n')

	sum := sha256.Sum256(payload)
	b.WriteString(hex.EncodeToString(sum[:]))
	return b.String(), nil
}

// sign computes the AWS SigV4 signature for a canonical request using the
// secret access key, region, service, and signing time.
func sign(secret, region, service string, now time.Time, canonicalRequest string) (string, error) {
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	scope := strings.Join([]string{now.Format("20060102"), region, service, "aws4_request"}, "/")

	sum := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(sum[:]),
	}, "\n")

	key := signingKey(secret, now, region, service)
	return hex.EncodeToString(hmacSHA256(key, stringToSign)), nil
}

// signingKey derives the SigV4 signing key from the secret access key.
func signingKey(secret string, now time.Time, region, service string) []byte {
	date := now.Format("20060102")
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

// hmacSHA256 computes the HMAC-SHA256 of data with key.
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// uriEncode percent-encodes s per RFC 3986 (unreserved characters only).
func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
