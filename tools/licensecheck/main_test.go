// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheck(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"headered.go":         "// SPDX-License-Identifier: Apache-2.0\npackage a\n",
		"bare.go":             "package a\n",
		"late.go":             "package a\n\n// SPDX-License-Identifier: Apache-2.0\n",
		"notgo.txt":           "no header needed\n",
		"testdata/fixture.go": "package fixture\n",
		"vendor/dep/dep.go":   "package dep\n",
	}
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got, err := check(root)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	want := map[string]bool{filepath.Join(root, "bare.go"): true, filepath.Join(root, "late.go"): true}
	if len(got) != len(want) {
		t.Fatalf("missing = %v, want bare.go and late.go", got)
	}
	for _, path := range got {
		if !want[path] {
			t.Errorf("unexpected missing header: %s", path)
		}
	}
}
