// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tail

import (
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// FormatMessage renders one message as a single terminal line:
//
//	[p 2|o 1243|12:34:56.789] key="order-42" value=... (2 headers)
func FormatMessage(m Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[p %d|o %d|%s] ", m.Partition, m.Offset, m.Timestamp.UTC().Format("15:04:05.000"))
	if len(m.Key) > 0 {
		fmt.Fprintf(&b, "key=%q ", string(m.Key))
	}
	b.WriteString(DisplayValue(m.Value, DefaultDisplayMaxBytes))
	if len(m.Headers) > 0 {
		fmt.Fprintf(&b, " (%d headers)", len(m.Headers))
	}
	return b.String()
}

// DisplayValue renders a message payload for terminal output: plain text
// when it is valid printable UTF-8, lowercase hex otherwise, truncated to
// maxBytes with a marker for the omitted tail. Same convention as the dlq
// module's DisplayValue.
func DisplayValue(v []byte, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = DefaultDisplayMaxBytes
	}
	if len(v) == 0 {
		return ""
	}
	if utf8.Valid(v) && isPrintableText(v) {
		return displayText(string(v), maxBytes)
	}
	if len(v) > maxBytes {
		return hex.EncodeToString(v[:maxBytes]) + fmt.Sprintf("...(+%d more bytes)", len(v)-maxBytes)
	}
	return hex.EncodeToString(v)
}

// isPrintableText reports whether every rune is printable or a common
// whitespace character.
func isPrintableText(v []byte) bool {
	for _, r := range string(v) {
		if r == '\n' || r == '\t' || r == '\r' {
			continue
		}
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func displayText(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		if b.Len()+utf8.RuneLen(r) > maxBytes {
			break
		}
		b.WriteRune(r)
	}
	return b.String() + fmt.Sprintf("...(+%d more bytes)", len(s)-b.Len())
}
