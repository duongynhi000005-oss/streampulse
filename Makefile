.PHONY: build run serve test test-cover lint fmt tidy deps license-check license-deps \
	license-deps-report e2e e2e-watch e2e-verify install clean release help

# Ensure go/vhs resolve even when the invoking shell omits Homebrew's bin dir
# (non-interactive shells often do). A missing dir in PATH is harmless.
PATH := $(PATH):/opt/homebrew/bin
export PATH

APP_NAME    := streampulse
BUILD_DIR   := bin
CMD_DIR     := cmd/streampulse
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "v0.1.0-dev")
LDFLAGS     := -ldflags="-s -w -X main.version=$(VERSION)"
# Pinned: the license report feeds NOTICE, so its output must be reproducible.
GO_LICENSES_VERSION := v2.0.1

## build: Build the binary
build:
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./$(CMD_DIR)

## run: Build and run the TUI (uses mock data if no Kafka available)
#   make run ARGS="--brokers localhost:9093"
run: build
	$(BUILD_DIR)/$(APP_NAME) $(ARGS)

## serve: Build and run daemon mode
#   make serve ARGS="--brokers localhost:9093"
serve: build
	$(BUILD_DIR)/$(APP_NAME) serve $(ARGS)

## test: Run all tests
test:
	go test -v -race -count=1 ./...

## e2e: Run VHS TUI end-to-end tests (requires vhs + docker Kafka)
e2e: build
	@command -v vhs >/dev/null 2>&1 || { echo "vhs not found — run: brew install vhs"; exit 1; }
	@docker exec streampulse-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 >/dev/null 2>&1 || { echo "Kafka not reachable — run: docker compose up -d"; exit 1; }
	@for tape in tests/e2e/vhs/*.tape; do \
		echo "== $$tape =="; \
		vhs "$$tape" || exit 1; \
	done
	@echo "Screenshots written to tests/e2e/screenshots/"

## e2e-watch: Replay a tape live (TAPE=02-topics-search.tape)
e2e-watch:
	@test -n "$(TAPE)" || { echo "Usage: make e2e-watch TAPE=02-topics-search.tape"; exit 1; }
	vhs play tests/e2e/vhs/$(TAPE)

## e2e-verify: OCR one frame per second of each GIF and check expected labels
## (requires ffmpeg + tesseract; frame extraction via ffmpeg — magick's
## per-frame extraction yields blank disposal layers for animated GIFs)
e2e-verify:
	@mkdir -p tests/e2e/.verify
	@for gif in tests/e2e/screenshots/*.gif; do \
		echo "== $$gif =="; \
		ffmpeg -y -loglevel error -i "$$gif" -vf fps=1 tests/e2e/.verify/f-%02d.png; \
		labels=""; \
		for f in tests/e2e/.verify/f-*.png; do \
			labels="$$labels $$(tesseract "$$f" stdout 2>/dev/null | grep -aoE 'StreamPulse|BROKERS|TOPICS|ANALYTICS|DEAD LETTER QUEUES|TAIL|payments\.dlq|orders|REBALANCES|PATTERNS|Q_QUIT_OK|Showing|bulk|PAGINATION_OK|No match|case-insensitive|NO_RESULTS_OK|KEYBINDINGS|msgs|paused|following|loading')"; \
		done; \
		echo "$$labels" | tr ' ' '\n' | grep -v '^$$' | sort | uniq -c; \
		rm -f tests/e2e/.verify/f-*.png; \
	done

## test-cover: Run tests with coverage
test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage: file://$(PWD)/coverage.html"

## lint: Run linter
lint:
	golangci-lint run ./...

## license-check: Verify every Go file carries an Apache-2.0 SPDX header
license-check:
	go run ./tools/licensecheck

# Paste the result into NOTICE when dependencies change.
## license-deps: Verify module checksums, then print the third-party license report
license-deps:
	go mod verify
	@$(MAKE) --no-print-directory license-deps-report

## license-deps-report: Print license, version and source URL for every dependency
license-deps-report:
	@report=$$(mktemp) && \
		go run github.com/google/go-licenses/v2@$(GO_LICENSES_VERSION) report \
			--ignore github.com/pulsedev/streampulse ./... > $$report && \
		sort -u $$report; \
	status=$$?; rm -f $$report; exit $$status

## fmt: Format code
fmt:
	go fmt ./...
	goimports -w .

## tidy: Tidy module dependencies
tidy:
	go mod tidy

## deps: Install development tools
deps:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

## install: Install to /usr/local/bin
install: build
	cp $(BUILD_DIR)/$(APP_NAME) /usr/local/bin/

## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

## release: Build release binaries for all platforms (requires goreleaser)
release:
	goreleaser build --snapshot --clean

## help: Show this help
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/^## //' -e 's/:.*$$//' | column -t -s ':'
