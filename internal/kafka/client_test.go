// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionsToTopics(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0},
		{Topic: "orders", ID: 1},
		{Topic: "orders", ID: 2},
		{Topic: "payments", ID: 0},
		{Topic: "__consumer_offsets", ID: 0},
		{Topic: "audit", ID: 0},
	}

	topics := partitionsToTopics(partitions)

	assert.Len(t, topics, 3)
	assert.Equal(t, "audit", topics[0].Name)
	assert.Equal(t, 1, topics[0].Partitions)
	assert.Equal(t, "orders", topics[1].Name)
	assert.Equal(t, 3, topics[1].Partitions)
	assert.Equal(t, "payments", topics[2].Name)
	assert.Equal(t, 1, topics[2].Partitions)
}

func TestPartitionsToTopicsEmpty(t *testing.T) {
	topics := partitionsToTopics(nil)
	assert.Empty(t, topics)
}

func TestIsInternalTopic(t *testing.T) {
	assert.True(t, isInternalTopic("__consumer_offsets"))
	assert.True(t, isInternalTopic("__transaction_state"))
	assert.False(t, isInternalTopic("orders"))
	assert.False(t, isInternalTopic("_schemas"))
}

func TestNewClient(t *testing.T) {
	c := NewClient([]string{"localhost:9092"})
	assert.NotNil(t, c)
}

func TestBrokersAccessor(t *testing.T) {
	c := NewClient([]string{"localhost:9092", "kafka:9093"})
	assert.Equal(t, []string{"localhost:9092", "kafka:9093"}, c.Brokers())
}

// generateTestCert writes a self-signed CA and a client certificate/key pair
// (1h validity) into dir and returns their paths.
func generateTestCert(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "streampulse-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(caFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return caFile, certFile, keyFile
}

func TestNewClientWithOptionsPlain(t *testing.T) {
	c, err := NewClientWithOptions([]string{"localhost:9092"}, Options{})
	require.NoError(t, err)
	defer c.Close()
	assert.Nil(t, c.dialer.TLS)
}

func TestNewClientWithOptionsTLSFiles(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := generateTestCert(t, dir)
	opts := Options{TLS: TLSOptions{Enabled: true, CAFile: ca, CertFile: cert, KeyFile: key}}
	c, err := NewClientWithOptions([]string{"localhost:9092"}, opts)
	require.NoError(t, err)
	defer c.Close()
	require.NotNil(t, c.dialer.TLS)
	assert.Len(t, c.dialer.TLS.Certificates, 1)
}

func TestNewClientWithOptionsBadCA(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not a cert"), 0o644))
	_, err := NewClientWithOptions([]string{"x:1"}, Options{TLS: TLSOptions{Enabled: true, CAFile: bad}})
	require.Error(t, err)
}

func TestNewClientWithOptionsMTLSRequiresBothFiles(t *testing.T) {
	dir := t.TempDir()
	ca, cert, _ := generateTestCert(t, dir)
	_, err := NewClientWithOptions([]string{"x:1"}, Options{TLS: TLSOptions{Enabled: true, CAFile: ca, CertFile: cert}})
	require.Error(t, err)
}

func TestNewClientWithOptionsScheme(t *testing.T) {
	t.Run("ssl scheme forces TLS", func(t *testing.T) {
		c, err := NewClientWithOptions([]string{"ssl://localhost:9093"}, Options{})
		require.NoError(t, err)
		defer c.Close()
		require.NotNil(t, c.dialer.TLS)
		assert.Equal(t, []string{"localhost:9093"}, c.brokers)
	})
	t.Run("sasl_ssl scheme forces TLS", func(t *testing.T) {
		c, err := NewClientWithOptions([]string{"sasl_ssl://localhost:9093"}, Options{})
		require.NoError(t, err)
		defer c.Close()
		require.NotNil(t, c.dialer.TLS)
		assert.Equal(t, []string{"localhost:9093"}, c.brokers)
	})
	t.Run("plaintext scheme stays plain", func(t *testing.T) {
		c, err := NewClientWithOptions([]string{"plaintext://localhost:9092"}, Options{})
		require.NoError(t, err)
		defer c.Close()
		assert.Nil(t, c.dialer.TLS)
		assert.Equal(t, []string{"localhost:9092"}, c.brokers)
	})
}

func TestNewClientWithOptionsSameBehaviorAsNewClient(t *testing.T) {
	a := NewClient([]string{"localhost:9092"})
	defer a.Close()
	b, err := NewClientWithOptions([]string{"localhost:9092"}, Options{})
	require.NoError(t, err)
	defer b.Close()
	assert.Equal(t, a.brokers, b.brokers)
	assert.Equal(t, a.dialer.Timeout, b.dialer.Timeout)
}

func TestNewClientWithOptionsSASLPlain(t *testing.T) {
	t.Setenv("TEST_SASL_PW", "secret")
	c, err := NewClientWithOptions([]string{"localhost:9092"}, Options{
		SASL: SASLOptions{Mechanism: "plain", Username: "alice", PasswordEnv: "TEST_SASL_PW"},
	})
	require.NoError(t, err)
	defer c.Close()
	require.NotNil(t, c.dialer.SASLMechanism)
	assert.Equal(t, "PLAIN", c.dialer.SASLMechanism.Name())
	require.NotNil(t, c.transport.SASL)
}

func TestNewClientWithOptionsSASLScram(t *testing.T) {
	cases := []struct{ mech, want string }{
		{"scram-sha-256", "SCRAM-SHA-256"},
		{"scram-sha-512", "SCRAM-SHA-512"},
	}
	for _, tc := range cases {
		t.Run(tc.mech, func(t *testing.T) {
			t.Setenv("TEST_SASL_PW", "secret")
			c, err := NewClientWithOptions([]string{"localhost:9092"}, Options{
				SASL: SASLOptions{Mechanism: tc.mech, Username: "alice", PasswordEnv: "TEST_SASL_PW"},
			})
			require.NoError(t, err)
			defer c.Close()
			require.NotNil(t, c.dialer.SASLMechanism)
			assert.Equal(t, tc.want, c.dialer.SASLMechanism.Name())
		})
	}
}

func TestNewClientWithOptionsSASLComposesWithTLS(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := generateTestCert(t, dir)
	t.Setenv("TEST_SASL_PW", "secret")
	c, err := NewClientWithOptions([]string{"localhost:9093"}, Options{
		TLS:  TLSOptions{Enabled: true, CAFile: ca, CertFile: cert, KeyFile: key},
		SASL: SASLOptions{Mechanism: "scram-sha-512", Username: "alice", PasswordEnv: "TEST_SASL_PW"},
	})
	require.NoError(t, err)
	defer c.Close()
	require.NotNil(t, c.dialer.TLS)
	require.NotNil(t, c.dialer.SASLMechanism)
	assert.Equal(t, "SCRAM-SHA-512", c.dialer.SASLMechanism.Name())
}

func TestNewClientWithOptionsSASLErrors(t *testing.T) {
	cases := []struct {
		name string
		opts SASLOptions
		want string
	}{
		{"unknown mechanism", SASLOptions{Mechanism: "kerberos", Username: "u", PasswordEnv: "PW"}, "sasl"},
		{"plain no username", SASLOptions{Mechanism: "plain", PasswordEnv: "PW"}, "username"},
		{"scram no username", SASLOptions{Mechanism: "scram-sha-256", PasswordEnv: "PW"}, "username"},
		{"plain no password env", SASLOptions{Mechanism: "plain", Username: "u"}, "password_env"},
		{"plain empty password env", SASLOptions{Mechanism: "plain", Username: "u", PasswordEnv: "MISSING_PW_VAR"}, "MISSING_PW_VAR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClientWithOptions([]string{"localhost:9092"}, Options{SASL: tc.opts})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestNewClientWithOptionsSASLAWSIAM(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	c, err := NewClientWithOptions([]string{"localhost:9098"}, Options{
		SASL: SASLOptions{Mechanism: "aws-iam"},
	})
	require.NoError(t, err)
	defer c.Close()
	require.NotNil(t, c.dialer.SASLMechanism)
	assert.Equal(t, "AWS_MSK_IAM", c.dialer.SASLMechanism.Name())
	require.NotNil(t, c.transport.SASL)
}

func TestListTopicsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topics, err := client.ListTopics(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	assert.NotNil(t, topics)
	t.Logf("discovered topics: %+v", topics)
}

func TestListConsumerGroupsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	groups, err := client.ListConsumerGroups(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	t.Logf("discovered groups: %+v", groups)
}

func TestDialFailoverTriesAllBrokers(t *testing.T) {
	c := NewClient([]string{"127.0.0.1:1", "127.0.0.1:2"})
	err := c.Ping(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.1:1")
	assert.Contains(t, err.Error(), "127.0.0.1:2")
}

func TestListConsumerGroupsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.ListConsumerGroups(context.Background())
	require.Error(t, err)
}

func TestUnderReplicatedCount(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0, Replicas: []kafka.Broker{{ID: 0}, {ID: 1}, {ID: 2}}, Isr: []kafka.Broker{{ID: 0}, {ID: 1}, {ID: 2}}},
		{Topic: "orders", ID: 1, Replicas: []kafka.Broker{{ID: 0}, {ID: 1}, {ID: 2}}, Isr: []kafka.Broker{{ID: 0}}},
		{Topic: "payments", ID: 0, Replicas: []kafka.Broker{{ID: 0}, {ID: 1}}, Isr: []kafka.Broker{{ID: 0}, {ID: 1}}},
		{Topic: "audit", ID: 0, Replicas: []kafka.Broker{{ID: 0}, {ID: 1}}, Isr: []kafka.Broker{{ID: 0}}},
		// A partition whose metadata fetch errored is not counted.
		{Topic: "errored", ID: 0, Replicas: []kafka.Broker{{ID: 0}, {ID: 1}}, Isr: []kafka.Broker{{ID: 0}}, Error: fmt.Errorf("leader not available")},
	}
	assert.Equal(t, 2, underReplicatedCount(partitions))
}

func TestUnderReplicatedCountEmpty(t *testing.T) {
	assert.Equal(t, 0, underReplicatedCount(nil))
}

func TestPartitionsToTopicsSkipsErroredPartitions(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0, Error: fmt.Errorf("leader not available")},
		{Topic: "orders", ID: 1},
		{Topic: "__consumer_offsets", ID: 0},
	}
	topics := partitionsToTopics(partitions)
	require.Len(t, topics, 1)
	assert.Equal(t, "orders", topics[0].Name)
	assert.Equal(t, 1, topics[0].Partitions)
}

func TestTransportGoroutinesReleasedOnClose(t *testing.T) {
	// Dummy broker: accepts TCP connections so the transport creates its pool
	// and metadata-discover goroutine (an unreachable address never reaches
	// RoundTrip and would make this test vacuous).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	before := runtime.NumGoroutine()
	for i := 0; i < 2; i++ {
		c := NewClient([]string{l.Addr().String()})
		_, _ = c.ListConsumerGroups(context.Background())
		c.Close()
	}
	// Allow background goroutines to unwind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if runtime.NumGoroutine() > before+1 {
		t.Fatalf("goroutines not released after Close: before=%d after=%d", before, runtime.NumGoroutine())
	}
}
