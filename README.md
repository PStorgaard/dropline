# dropline

Single-binary network diagnostic in Go. Runs ICMP path tracing
(mtr-style: TTL-walking echo requests) and a UDP packet-loss test
**concurrently** against the same target, with temporal correlation
between per-hop stats and per-second stream loss. Replaces the
"`mtr` in one terminal, `iperf3 -u` in another" workflow with one
tool, one deployment, and JSON output suited for automation.

v1 is IPv4-only and pure Go (`CGO_ENABLED=0`). Cross-compiles to
`linux/amd64`, `linux/arm64`, `windows/amd64`.

## Install

**Pre-built binaries** ship from the
[Releases page](https://github.com/PStorgaard/dropline/releases).
Each release attaches `dropline-linux-amd64`, `dropline-linux-arm64`,
`dropline-windows-amd64.exe`, and a `SHA256SUMS` file. On Linux:

```bash
curl -fsSLO https://github.com/PStorgaard/dropline/releases/latest/download/dropline-linux-amd64
sudo install -m 0755 dropline-linux-amd64 /usr/local/bin/dropline
dropline version
```

**Build locally.** Cross-compiles all three targets to `dist/`:

```bash
make all
# Produces dist/dropline-{linux-amd64,linux-arm64,windows-amd64.exe}
# plus dist/SHA256SUMS.
```

**From source.**

```bash
go install github.com/PStorgaard/dropline/cmd/dropline@latest
```

## Quick start

Server (Linux primary, Windows supported):

```bash
dropline serve --listen :5301
```

Client (anywhere):

```bash
dropline trace srv.lab --rate 10M --duration 60s
```

JSON for automation:

```bash
dropline trace srv.lab --duration 30s --json | jq '.stream.loss_pct, .correlation.suspect_hops'
```

## CLI

```
dropline serve [--listen :5301] [--max-sessions 4]

dropline trace TARGET
    [--rate 10M] [--duration 60s] [--packet-size 1200]
    [--mtr-interval 1s] [--max-hops 30]
    [--reverse-trace on|off|auto]      (default: auto)
    [--tui | --report | --json]        (default: --tui on a TTY,
                                        --report otherwise)
    [--save FILE]                      (always writes JSON form)

dropline service install               (Windows only — see below)
                [--listen :5301] [--max-sessions 4]
dropline service uninstall

dropline version
```

`TARGET` is `host[:port]`; the default port is 5301.
`--rate` accepts integer bps with optional `K`/`M`/`G` suffix.
`--max-sessions` caps concurrent client sessions on the server (over-cap
dials get a clean `server busy` rejection rather than a queue).

## Linux: systemd

A hardened `DynamicUser` unit ships at
[`deploy/dropline.service`](deploy/dropline.service). Install and
start:

```bash
sudo install -m 0644 deploy/dropline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dropline
journalctl -u dropline -f
```

The reverse-trace feature on the server side requires
`CAP_NET_RAW`; without it the server starts fine and advertises
`reverse_trace=off` to clients. Add `AmbientCapabilities=CAP_NET_RAW`
(and optionally `CapabilityBoundingSet=CAP_NET_RAW` to harden) to
the unit if you want reverse trace enabled.

## TUI live controls

While `dropline trace --tui` is running:

- `[q]` / `Ctrl+C` / `Esc` — quit
- `[p]` — pause / resume the test. The sender stops emitting packets
  and the server stops forwarding `Stats` (so the sparkline freezes
  cleanly); the elapsed-time counter freezes too. The test's
  duration deadline does **not** pause — pause is for inspecting the
  dashboard, not extending the test, so a 60s test paused for 30s
  ends at the originally-scheduled wall-clock time with 30s of data.
- `[r]` — reset the per-second loss timeline (test keeps running)
- `[s]` — save the JSON report to disk (uses `--save FILE` if set,
  else `dropline-<sanitized-target>-<unix>.json` in the cwd)
- `[c]` — copy the JSON report to the system clipboard via an OSC 52
  escape sequence

`[s]` and `[c]` are active only on the test-complete view; mid-test
presses show a "waiting for final" hint. `[c]` works in modern
terminals that honor OSC 52 — Windows Terminal, iTerm2, Alacritty,
Kitty, WezTerm, and tmux with `set-clipboard on`. Legacy consoles
(`cmd.exe`, `conhost`) silently drop the escape; the footer still
reads "copied: N bytes" because the escape was emitted, but the
clipboard is unchanged.

## Windows: trace requires Administrator

The client's forward path-trace opens a raw ICMPv4 socket, which
on Windows requires an elevated process. `dropline trace` detects
the missing privilege at startup and exits with a clear error
rather than falling back to a degraded mode.

`dropline serve` runs unprivileged on Windows; the server side
only needs raw ICMP if you want to enable reverse trace.

### Windows service

To run the server as a Windows service (auto-start under
`NT AUTHORITY\LocalService`), use the SCM-aware subcommand from an
elevated shell:

```powershell
dropline service install --listen :5301 --max-sessions 4
sc.exe start  dropline
sc.exe stop   dropline
dropline service uninstall
```

`service install` bakes the chosen flags into the SCM-registered
argv so subsequent starts honor them. `service run` is the SCM
entry point and isn't meant to be invoked directly.

## Network loss vs kernel drops

The JSON report carries `stream.loss` (network loss; packets that
never reached the receiver) and `stream.kernel_drops` (UDP
datagrams the kernel dropped after they arrived at the host but
before the receiver process consumed them) as **separate** fields.
This split is the central honesty rule: a packet the kernel
dropped because the receive buffer was full is *not* network loss.

When `kernel_drops > 0`, the text and TUI outputs flip the verdict
to `⚠ network loss is suspect` so you don't draw bad conclusions
from a number that's actually telling you to tune the host.

The receiver sets `SO_RCVBUF = 16 MiB` best-effort. On Linux the
effective cap is `/proc/sys/net/core/rmem_max`; the server warns
on stderr at startup when its target exceeds the cap and tells you
the `sysctl` line to fix it.

Linux samples kernel drops via `/proc/net/snmp` (the `Udp.InErrors`
column, looked up by name so a kernel that reorders columns still
works). Windows samples via `iphlpapi.GetUdpStatisticsEx(AF_INET).DwInErrors`.

## ECMP path-divergence caveat

Forward path traces use ICMP (echo request with escalating TTL).
The UDP loss stream rides a different 5-tuple, so on multipath
networks the two flows can hash to different paths under
equal-cost multipath (ECMP) routing. A clean ICMP path doesn't
prove the UDP path is clean.

The reverse-trace feature (server → client) covers part of this:
asymmetric routes are normal on the public internet, and the
reverse path's hop count or first-public-AS often differs from
the forward path's. Treat the two paths as independent
observations, not as halves of one object.

## JSON output

`--json` writes a single JSON document to stdout when the test
ends. The authoritative shape lives in `internal/report/json.go`.
Key fields:

- `target`, `started_at`, `duration_s`, `config`
- `hops[]` — forward path with rolling RTT and per-hop loss%; each
  entry carries the hop's `ip` address
- `reverse_path[]`, `reverse_path_status` — reverse trace from
  server to client (when enabled). `reverse_path[]` is structurally
  identical to `hops[]` modulo two intentional differences: `addr`
  replaces `ip`, and there's no `suspect` flag (the correlator only
  runs against forward-path stream loss)
- `stream` — sent / recv / loss_pct / jitter / kernel_drops /
  bursts / rate_tx_bps / rate_rx_bps
- `timeline[]` — per-second buckets with `stream_loss` and a
  `hops` snapshot of the forward path at that second
- `correlation.suspect_hops[]` — `{ttl, confidence, evidence}`,
  ranked descending by confidence; populated when a hop's
  RTT-vs-baseline or per-hop loss spikes co-occur with stream-loss
  seconds

```bash
dropline trace srv.lab --duration 30s --json \
  | jq '{
      loss_pct: .stream.loss_pct,
      kernel_drops: .stream.kernel_drops,
      suspects: .correlation.suspect_hops
    }'
```

## Status

Live feature checklist: [`progress.md`](progress.md).

Out of scope for v1: IPv6, TCP throughput, paris-traceroute,
iperf3 wire compatibility, web dashboard, Prometheus metrics,
multi-target tracing.

## Releases

Pre-built binaries live on the
[Releases page](https://github.com/PStorgaard/dropline/releases).
Each download URL has the form
`https://github.com/PStorgaard/dropline/releases/download/<TAG>/<ASSET>`
— e.g. `.../download/v1.0.0/dropline-linux-amd64`. The
`/latest/download/<ASSET>` alias resolves to the most recent
non-prerelease tag. Every release also ships `SHA256SUMS`; verify
with `sha256sum -c SHA256SUMS` on Linux.

`dropline version` prints the tag the binary was built from (e.g.
`v1.0.0`) — useful when triaging a host's installed version.

### Cutting a release (maintainers)

The `.github/workflows/release.yml` pipeline runs on any pushed tag
matching `v*`. It cross-compiles, runs the loopback e2e smoke test
as a release gate, and uploads the binaries + `SHA256SUMS` to a new
GitHub Release with auto-generated notes.

```bash
git tag -a v1.0.0 -m 'v1.0.0'
git push origin v1.0.0
```

If the e2e step fails the release is not published; delete the tag
(`git push --delete origin v1.0.0`) once the underlying issue is
fixed and re-tag.
