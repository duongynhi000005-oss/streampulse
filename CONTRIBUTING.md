# Contributing to StreamPulse

Thanks for helping make Kafka observability better. This document covers the
mechanics: local setup, commit rules, and the contributor terms we enforce in CI.

## Prerequisites

- Go 1.25 or newer
- `golangci-lint` and `goimports` (`make deps`)
- Docker, for the local Kafka in `docker-compose.yaml`
- Optional, for end-to-end tests: `vhs`, `ffmpeg`, `tesseract`

## Development loop

```bash
docker compose up -d       # Kafka 3.9 (KRaft) + demo producer on localhost:9093
make build                 # → bin/streampulse
make test                  # unit tests, race detector on
make lint                  # golangci-lint
make license-check         # SPDX headers on every Go file
```

See `AGENTS.md` for architecture, package layout, and coding conventions.

Before opening a pull request, run at least `make test` and `make lint`. If your
change touches the TUI, run `make e2e && make e2e-verify` with Kafka up.

## Commit messages

Use Conventional Commits, matching the existing history:

```
feat(tui): add partition skew column
fix(scraper): retry broker metadata on timeout
docs: describe DCO sign-off
```

Keep the subject under 72 characters, imperative mood, no trailing period.

## Developer Certificate of Origin (DCO)

StreamPulse uses the [Developer Certificate of Origin 1.1](https://developercertificate.org)
instead of a CLA. There is nothing to sign and no account to create. You certify
the DCO by adding a `Signed-off-by` trailer to every commit you author.

Add it automatically:

```bash
git commit -s -m "feat(tui): add partition skew column"
```

That produces:

```
feat(tui): add partition skew column

Signed-off-by: Your Name <you@example.com>
```

The email must match the commit author email; the name is not compared. If you
already committed without the trailer, fix the last commit:

```bash
git commit --amend -s --no-edit
```

Fix several commits in a branch:

```bash
git rebase --signoff main
git push --force-with-lease
```

The `dco` job in `.github/workflows/ci.yml` fails a pull request when any commit
in the branch lacks a matching trailer. Merge commits are ignored.

Maintainers merge with **Rebase and merge** so every sign-off survives on `main`.
A squash merge discards commit messages, so the squashed commit needs its own
`Signed-off-by` trailer from the maintainer who merges.

By signing off you certify the DCO and license your contribution to the project
under the [Apache License 2.0](LICENSE), the same terms as the project itself.

## License headers

Every Go file starts with:

```go
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors
```

Put these lines above the package clause and above any `//go:build` constraint.
`make license-check` fails on files without them. Do not add headers to files
under `testdata/`, `vendor/`, or `bin/`; the checker skips those directories.

## Third-party dependencies

New dependencies need a reason: the project ships one static binary, so prefer
the standard library or an existing module. Do not introduce CGo.

After adding or removing a module, refresh the notice file:

```bash
make license-deps                    # go mod verify + license report
# update the component list in NOTICE with the new output
make license-check
```

`NOTICE` must list every module bundled into the binary.

## Reporting bugs and proposing features

Open an issue at <https://github.com/vitdo82/streampulse/issues>. Include the
StreamPulse version (`streampulse --version`), Kafka and Go versions, the exact
command you ran, and the full error output. For TUI problems, add a screenshot
or the text of the frame.

Security issues: do not open a public issue. Use GitHub private vulnerability
reporting at <https://github.com/vitdo82/streampulse/security/advisories/new>.
