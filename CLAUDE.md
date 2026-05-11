# CLAUDE.md

Notes for future Claude sessions. Read `README.md` for what dropline
does; track feature status in `progress.md` and update as you ship.

## Build / run

```powershell
go build ./...      # all packages compile
go test ./...       # full suite (alias: `make test`)
make all            # cross-compile + dist/SHA256SUMS
make e2e            # loopback smoke; on Linux needs raw-ICMP
                    # capability (setcap cap_net_raw+ep). Windows runs
                    # unprivileged — the prober goes through
                    # iphlpapi.dll!IcmpSendEcho2Ex, not raw sockets.
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

- **Linux still needs CAP_NET_RAW for ICMP.** `serve` and `trace` on
  Linux/macOS use a raw ICMPv4 socket (`internal/probe/probe_unix.go`).
  Without the capability, `dropline trace` exits loud and `dropline
  serve` advertises `reverse_trace=off`. Windows is different — see
  next bullet.
- **Windows prober is build-tagged separately** at
  `internal/probe/probe_windows.go`. It uses
  `iphlpapi.dll!IcmpSendEcho2Ex` (the API `tracert.exe` uses) instead
  of raw sockets, because Windows raw ICMP drops `TimeExceeded` from
  intermediate hops (issue #1). Consequences for future edits:
  - The Windows `Prober` struct has different fields from the Unix
    one (no `inflight`/`nextSeq` — `IcmpSendEcho2Ex` blocks until
    reply or timeout, so the demux/aging logic isn't needed).
  - Anything that touches `Prober` internals (e.g. probe_test.go's
    `TestPruneInflightLocked`) is build-tagged `!windows`. Shared
    helpers (HopStat/Snapshot/Config, `lossTimeout`, `buildSnapshot`)
    live in `probe.go`.
  - `privcheck.RawICMP()` on Windows validates iphlpapi.dll +
    procs, not token elevation. `dropline trace` therefore runs
    from a non-elevated PowerShell.
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
- **Server-side reverse Sender writes from the listening socket.**
  `stream.SenderConfig.WriteTarget` flips the Sender's inner write
  call from `conn.Write` (connected, e.g. `DialUDP`) to
  `conn.WriteToUDP(buf, target)` (listening, e.g. `ListenUDP`).
  The Hub exposes its conn via `Hub.Conn()` so the per-session
  reverse Sender shares the single listening socket — which is
  also the only path back through a NATted client's pinhole.
  Don't mix the two: `WriteTarget != nil` + a connected conn
  returns EISCONN; `WriteTarget == nil` + a listening conn
  returns EDESTADDRREQ. The Sender constructor doesn't currently
  enforce this — the recipe is in `cmd/dropline/serve.go` only.
- **Reverse stream survives only as long as the NAT pinhole.**
  Reverse-direction UDP rides the NAT mapping the client's
  forward stream established. If the path traverses a NAT with a
  short idle timeout (<60s) and the test pauses longer than
  that, the mapping evicts and reverse packets bounce. Pause
  intentionally doesn't extend test duration, so the worst case
  is bounded. No code-level workaround in v1.
- **TCP_INFO sampler degrades silently on old Windows.** The
  Windows `internal/tcpinfo/` path calls `WSAIoctl SIO_TCP_INFO`
  with `TCP_INFO_v0`. Pre-Win10-1703 returns `WSAEINVAL`; the
  sampler latches into a "disabled" state and returns zero
  stats with nil error for the rest of the session. The wire
  shape stays the same (`tcp_corroborate` section present with
  zeros), so renderers don't branch.

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
internal/probe/      ICMP TTL-walking prober (forward + reverse share this).
                     Unix: raw ICMP socket (probe_unix.go).
                     Windows: iphlpapi.dll!IcmpSendEcho2Ex (probe_windows.go).
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
