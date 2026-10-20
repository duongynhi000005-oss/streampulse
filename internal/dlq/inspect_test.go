// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package dlq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectNoBrokers(t *testing.T) {
	_, err := Inspect(context.Background(), nil, "payments.dlq", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestDisplayValue(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		max  int
		want string
	}{
		{"empty", nil, 100, ""},
		{"plain text", []byte("hello"), 100, "hello"},
		{"utf8 short", []byte("héllo"), 100, "héllo"},
		{"text truncated", []byte("abcdefghij"), 5, "abcde...(+5 more bytes)"},
		{"utf8 truncated at rune boundary", []byte("héllo"), 3, "hé...(+3 more bytes)"},
		{"binary hex", []byte{0x00, 0x01, 0xff, 0xfe}, 100, "0001fffe"},
		{"binary truncated", []byte{0x00, 0x01, 0x02, 0x03, 0x04}, 3, "000102...(+2 more bytes)"},
		{"zero max uses default", []byte("x"), 0, "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DisplayValue(tc.in, tc.max))
		})
	}
}

func TestDefaultInspectLimit(t *testing.T) {
	assert.Equal(t, 10, DefaultInspectLimit)
}
