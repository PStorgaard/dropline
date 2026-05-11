# CLAUDE.md

Notes for future Claude sessions. Read `README.md` for what dropline
does; track feature status in `progress.md` and update as you ship.

## Build / run

```powershell
go build ./...      # all packages compile
go test ./...       # full suite
make all            # cross-compile + dist/SHA256SUMS
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
- **CGO clipboard.** `golang.design/x/clipboard` needs CGO on Linux;
  `CGO_ENABLED=0` is enforced. Decide build-tag-Windows vs OSC52
  before wiring the TUI's `[c]opy json` keybinding.
- **Server hosting needs raw UDP.** Azure App Service / Container Apps
  are HTTP-only. Use VM, ACI, or AKS with `LoadBalancer protocol: UDP`.

## Layout

```
cmd/dropline/        CLI entry, subcommand dispatch
internal/probe/      ICMP TTL-walking prober (forward + reverse share this)
internal/stream/     UDP loss test: Hub demuxer, Receiver, Sender
internal/control/    TCP control channel + JSON message types
internal/agg/        per-second bucket aggregator + suspect-hop correlator
internal/tui/        bubbletea Model/Update/View
internal/report/     text + JSON renderers
internal/svc/        Windows service wrapper (stub)
deploy/              systemd unit
```

**Architectural rule:** business goroutines push immutable
`StateSnapshot` structs into a single channel; the TUI consumes via
a re-arming `tea.Cmd`. Business logic never touches the UI; UI
never touches sockets. Preserve this when implementing.
