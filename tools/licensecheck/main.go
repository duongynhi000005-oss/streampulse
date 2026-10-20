// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Command licensecheck fails when a Go source file lacks an Apache-2.0 SPDX
// header. CI runs it through `make license-check`.
package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	spdx = "SPDX-License-Identifier: Apache-2.0"
	// A header must sit above the package clause, so only the first lines count.
	scanLines = 10
)

// skipDirs holds directories that never ship project source.
var skipDirs = map[string]bool{
	".git":         true,
	"bin":          true,
	"vendor":       true,
	"node_modules": true,
	"testdata":     true,
}

func main() {
	missing, err := check(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, "licensecheck:", err)
		os.Exit(2)
	}
	if len(missing) > 0 {
		for _, path := range missing {
			fmt.Printf("%s: missing %q\n", path, spdx)
		}
		fmt.Printf("\n%d file(s) missing an SPDX header. Add above the package clause:\n\n"+
			"  // %s\n  // Copyright 2026 StreamPulse contributors\n\n", len(missing), spdx)
		os.Exit(1)
	}
	fmt.Println("licensecheck: all Go files carry an Apache-2.0 SPDX header")
}

// check returns the Go files below root that lack the SPDX header.
func check(root string) ([]string, error) {
	var missing []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		ok, err := hasHeader(path)
		if err != nil {
			return err
		}
		if !ok {
			missing = append(missing, path)
		}
		return nil
	})
	return missing, err
}

func hasHeader(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for i := 0; i < scanLines && scanner.Scan(); i++ {
		line := scanner.Text()
		if strings.Contains(line, spdx) {
			return true, nil
		}
		// Stop at the package clause: a header below it is not a header.
		if strings.HasPrefix(strings.TrimSpace(line), "package ") {
			break
		}
	}
	return false, scanner.Err()
}
