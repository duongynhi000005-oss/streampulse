// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package sasliam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignAWSTestVector(t *testing.T) {
	// AWS documentation example, "Signature Calculations for the Authorization
	// Header: Transferring Payload in a Single Chunk": GET /test.txt on
	// examplebucket.s3.amazonaws.com with region us-east-1 and service s3.
	now, err := time.Parse("20060102T150405Z", "20130524T000000Z")
	require.NoError(t, err)

	canonical := strings.Join([]string{
		"GET",
		"/test.txt",
		"",
		"host:examplebucket.s3.amazonaws.com",
		"range:bytes=0-9",
		"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"x-amz-date:20130524T000000Z",
		"",
		"host;range;x-amz-content-sha256;x-amz-date",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}, "\n")

	sig, err := sign("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "s3", now, canonical)
	require.NoError(t, err)
	assert.Equal(t, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", sig)
}

func TestCanonicalRequest(t *testing.T) {
	canonical, err := canonicalRequest("GET", "/", map[string]string{
		"Version": "2018-11-14",
		"Action":  "KafkaCluster.SaslAuthenticate",
	}, map[string]string{
		"X-Amz-Date": "20130524T000000Z",
		"Host":       "b-1.example.kafka.us-east-1.amazonaws.com:9098",
	}, nil)
	require.NoError(t, err)
	want := "GET\n/\n" +
		"Action=KafkaCluster.SaslAuthenticate&Version=2018-11-14\n" +
		"host:b-1.example.kafka.us-east-1.amazonaws.com:9098\n" +
		"x-amz-date:20130524T000000Z\n" +
		"\n" +
		"host;x-amz-date\n" +
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	assert.Equal(t, want, canonical)
}

type staticProvider struct {
	ak, sk, token string
	err           error
}

func (p staticProvider) Credentials(context.Context) (string, string, string, error) {
	return p.ak, p.sk, p.token, p.err
}

func TestEnvProvider(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SESSION_TOKEN", "session-tok")

	ak, sk, tok, err := EnvProvider{}.Credentials(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "AKIDEXAMPLE", ak)
	assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", sk)
	assert.Equal(t, "session-tok", tok)
}

func TestEnvProviderMissingCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, _, _, err := EnvProvider{}.Credentials(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AWS_ACCESS_KEY_ID")
}

func TestResolveRegion(t *testing.T) {
	t.Run("explicit region wins", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		assert.Equal(t, "eu-west-1", resolveRegion("eu-west-1"))
	})
	t.Run("AWS_REGION", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		t.Setenv("AWS_DEFAULT_REGION", "us-east-2")
		assert.Equal(t, "us-west-2", resolveRegion(""))
	})
	t.Run("AWS_DEFAULT_REGION", func(t *testing.T) {
		t.Setenv("AWS_REGION", "")
		t.Setenv("AWS_DEFAULT_REGION", "us-east-2")
		assert.Equal(t, "us-east-2", resolveRegion(""))
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv("AWS_REGION", "")
		t.Setenv("AWS_DEFAULT_REGION", "")
		assert.Equal(t, "us-east-1", resolveRegion(""))
	})
}

func TestMechanismName(t *testing.T) {
	m := &Mechanism{Provider: staticProvider{}}
	assert.Equal(t, "AWS_MSK_IAM", m.Name())
}

func TestMechanismStartToken(t *testing.T) {
	m := &Mechanism{
		Provider: staticProvider{ak: "AKIDEXAMPLE", sk: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", token: "tok"},
		Region:   "us-east-1",
	}
	sess, ir, err := m.Start(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.NotNil(t, ir)

	assert.Equal(t, byte(0x01), ir[0], "first byte must be the MSK IAM version")
	assert.True(t, bytes.HasPrefix(ir[1:], []byte("AWS_MSK_IAM")), "request must carry the mechanism name")

	var token map[string]any
	require.NoError(t, json.Unmarshal(ir[1+len("AWS_MSK_IAM"):], &token))
	assert.Equal(t, "2020_10_22", token["version"])
	assert.Equal(t, "AKIDEXAMPLE", token["access_key"])
	assert.Equal(t, "tok", token["session_token"])
	assert.Equal(t, "4", token["signature_version"])
	assert.Equal(t, float64(900), token["token_ttl_seconds"])
	assert.Equal(t, "GET", token["request_method"])
	assert.Equal(t, "/", token["request_uri"])
	sig, ok := token["signature"].(string)
	require.True(t, ok, "signature field must be a string")
	assert.Regexp(t, `^[0-9a-f]{64}$`, sig)

	done, resp, err := sess.Next(context.Background(), []byte(`{"version":1,"status":200}`))
	require.NoError(t, err)
	assert.True(t, done)
	assert.Nil(t, resp)
}

func TestMechanismStartRejectsNon200(t *testing.T) {
	m := &Mechanism{Provider: staticProvider{ak: "AK", sk: "SK"}}
	sess, _, err := m.Start(context.Background())
	require.NoError(t, err)

	_, _, err = sess.Next(context.Background(), []byte(`{"version":1,"status":500,"error":"unauthorized"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestMechanismStartProviderError(t *testing.T) {
	m := &Mechanism{Provider: staticProvider{err: fmt.Errorf("no credentials available")}}
	_, _, err := m.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no credentials available")
}

func TestMechanismStartDefaultsToEnvProvider(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	m := &Mechanism{}
	_, _, err := m.Start(context.Background())
	require.Error(t, err)
}
