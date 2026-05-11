# CLAUDE.md

Notes for future Claude sessions. Read `README.md` for what dropline
does; track feature status in `progress.md` and update as you ship.

## Build / run

```powershell
go build ./...      # all packages compile
go test ./...       # full suite (alias: `make test`)
make all            # cross-compile + dist/SHA256SUMS
make e2e            # loopback smoke; needs raw-ICMP privilege
                    # (elevated PowerShell on Windows, setcap on Linux)
```

## Environment quirks (Windows host)

- **Go on `PATH` after fresh MSI install.** Current shell may not have
  Go until restart. Workaround at session start:
  ```powershell
  $env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User')
  ```
- **Make 4.4.1** via `winget install ezwinports.make`. `$(SHELL)` is
  sh.exe, so cmd-style recipes are wrapped in `cmd /C "..."`.
- **PowerShell 5.1:** redirecting a native exe's stderr with `2>&1`
  wraps each line in an ErrorRecord and flips `$?` to `$false` even
  on exit 0. Don't add `2>&1` to `go`/`make` calls.

## Active landmines

- **Raw ICMP needs Administrator on Windows for `dropline trace`.**
  Privcheck exits loud; no fallback. `serve` runs unprivileged but
  advertises `reverse_trace=off` without CAP_NET_RAW.
- **CGO is forbidden** (`CGO_ENABLED=0` is enforced for static
  binaries). Avoid libraries that need it. The TUI clipboard
  (`[c]opy json`) uses OSC 52 instead of a CGO-bound clipboard lib
  for this reason — modern terminals honor it, legacy `cmd.exe` /
  `conhost` silently no-op.
- **Server hosting needs raw UDP.** Azure App Service / Container Apps
  are HTTP-only. Use VM, ACI, or AKS with `LoadBalancer protocol: UDP`.
- **Don't roll a new release pipeline.** GitHub Actions workflows
  already exist (see "CI / releases" below). The Makefile is the
  single source of truth for cross-compile + checksums; both
  workflows just call `make all` / `make e2e`.

## CI / releases

- `.github/workflows/ci.yml` — runs `go vet` + `go test ./...` on
  `ubuntu-latest` + `windows-latest` matrix on every push/PR, plus
  a `build` job that cross-compiles via `make all` and runs the
  loopback e2e on Linux (`setcap cap_net_raw+ep` before `make e2e`).
  Windows matrix coverage exists *because* there's Windows-only code
  under `internal/privcheck`, `internal/svc`, and
  `internal/stream/kerneldrops_windows.go` — removing it would let
  Windows-only regressions land silently.
- `.github/workflows/release.yml` — triggers on `v*` tags. Runs
  `make all VERSION=${ref_name}` (binary's `dropline version`
  reports the tag via `-X main.version=…`), uses `make e2e` as a
  release gate, then `gh release create … --generate-notes`
  publishes the three binaries + `SHA256SUMS` to
  github.com/PStorgaard/dropline/releases.
- **Cut a release:** `git tag -a vX.Y.Z -m 'vX.Y.Z' && git push
  origin vX.Y.Z`. If e2e fails the release doesn't ship; retract
  with `git push --delete origin vX.Y.Z` and re-tag once fixed.

## Layout

```
cmd/dropline/        CLI entry, subcommand dispatch
cmd/e2e/             black-box loopback smoke (driven by `make e2e`)
internal/probe/      ICMP TTL-walking prober (forward + reverse share this)
internal/stream/     UDP loss test: Hub demuxer, Receiver, Sender
internal/control/    TCP control channel + JSON message types
internal/agg/        per-second bucket aggregator + suspect-hop correlator
internal/privcheck/  raw-ICMP capability detection (Windows/Linux split)
internal/tui/        bubbletea Model/Update/View
internal/report/     text + JSON renderers
internal/svc/        Windows service wrapper (no-op on Linux via build tag)
deploy/              systemd unit (`dropline.service`)
.github/workflows/   ci.yml + release.yml
```

**Architectural rule:** business goroutines push immutable
`StateSnapshot` structs into a single channel; the TUI consumes via
a re-arming `tea.Cmd`. Business logic never touches the UI; UI
never touches sockets. Preserve this when implementing.
