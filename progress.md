# progress.md

Living checklist of what's done and what's left in dropline. Update as
you ship slices — don't let it drift. Spec is authoritative for *what*
each item means; this file only tracks status.

Legend: `[x]` done · `[~]` partial / wired-but-incomplete · `[ ]` todo

## Overall

- [x] Bootstrap: module, deps pinned, all packages exist as stubs
- [x] `go build ./...`, `go vet ./...`, `make all` green
- [x] Cross-compile to linux-amd64, linux-arm64, windows-amd64 with SHA256SUMS
- [x] First runnable `serve`: accepts one session, runs `stream.Receiver`, emits periodic `Stats` and a `Final` (single-session; multi-session via UDP demux is a follow-up)
- [x] First end-to-end slice: `trace` against a `serve` peer — `--report`, `--json`, and `--tui` all run end-to-end; only `[p]ause` and `[c]opy json` keybindings remain in the TUI section (both blocked — see internal/tui)
- [x] Reverse-direction UDP loss (`--reverse-stream auto|on|off`): server captures the client's UDP source addr on first packet via `HubFlow.RemoteAddr(ctx)`, runs a Sender against it from the listening socket using `SenderConfig.WriteTarget`; client adds a `stream.Receiver` on its dialed socket filtering by `reverseFlowID`; aggregator surfaces a `ReverseStream *StreamView`; renderers emit a `reverse_stream` JSON section + text subsection + a TUI KPI card. e2e asserts `recv > 0, loss_pct < 1.0`. Auto resolution mirrors `--reverse-trace`
- [x] TCP retransmit corroboration (`--tcp-corroborate auto|on|off`, `--tcp-corroborate-rate`): dedicated TCP probe connection to the dropline port, dispatched via `control.Server.ProbeHandler` after the first `TCPProbe` message; client streams dummy bytes at the configured rate and samples TCP_INFO via a new `internal/tcpinfo/` package (Linux `getsockopt TCP_INFO`, Windows `WSAIoctl SIO_TCP_INFO v0`, nop on macOS); renderers emit a `tcp_corroborate` JSON section + text subsection + TUI KPI card. e2e asserts the section is present
- [ ] Definition-of-done per spec § DoD

## Per-package

### cmd/dropline
- [x] Subcommand dispatch (`serve`, `trace`, `service`) — all three run end-to-end
- [x] `trace` flag parsing (target, duration, intervals, ports, output mode)
- [x] `serve` flag parsing (`--listen`, defaults to `:5301`)
- [x] `serve` end-to-end: bind TCP control + UDP, validate Hello, mint session id, run receiver, forward Stats, send Final
- [x] `trace` end-to-end: dial control, send Hello, await Ready, run sender, ingest Stats into client-side aggregator, render `--report` / `--json` on Final; `--save FILE` writes the JSON form regardless of stdout mode
- [x] `--tui` runs the live dashboard via `internal/tui`; control recv loop runs in a background goroutine while bubbletea owns the main goroutine. `--save` still writes JSON regardless of output mode
- [x] Multi-session via UDP demultiplexing — `stream.Hub` runs a single drainer per UDP socket and dispatches packets to per-flow channels; `--max-sessions` (default 4) admits up to N concurrent clients via a semaphore in `newServeHandler`, over-cap dials get `Error{Reason: "server busy: max sessions reached"}`
- [x] `service install` / `service uninstall` / `service run` — `install` bakes the chosen `--listen` and `--max-sessions` into the SCM-registered argv; `run` is the SCM entry point and drives `serveRun` via context cancellation. Linux gets a clear `svc: Windows-only; use the systemd unit on Linux` error
- [x] Privilege detection at startup — Linux gates on `CAP_NET_RAW`; Windows gates on `iphlpapi.dll` + `IcmpSendEcho2Ex` proc availability (no Administrator required, since the Windows prober uses `iphlpapi.dll!IcmpSendEcho2Ex` instead of raw sockets — issue #1). `trace` fails loud when the capability is missing; `serve` calls `privcheck.RawICMP()` once at startup and threads the bool into the per-session reverse-trace decision so the server still starts unprivileged
- [x] `--reverse-trace` flag is honored: client sends preference in Hello, server resolves to "on"/"off" in Ready, client maps Ready+pref → `reverse_path_status` (`ok` / `disabled_by_client` / `disabled_by_server`)

### internal/control
- [x] JSON message types (Hello, Ready, Error, Stats, Final, ReverseHopUpdate, Pause). Hello + Ready carry `reverse_trace` (omitempty). `Pause{Paused bool}` is the first non-Hello client→server message; the server's per-session recv loop demuxes it (any other message or any error cancels the session, preserving the pre-pause strict-disconnect shape)
- [x] Length-prefixed framing (read + write)
- [x] Client dialer
- [x] Server listener + per-connection goroutine
- [x] `Conn.Send` is concurrency-safe (write mutex) so server stats forwarder + reverse forwarder don't tear frames; `Conn.RemoteAddr()` exposes the client IP for reverse-trace target
- [x] Unit tests against loopback (round-trip + concurrent-Send)

### internal/stream
- [x] UDP receiver (sequence tracking, per-second loss buckets) — drainer + stats split, ring channel, Snapshot emission. Two construction modes: `NewReceiver(conn)` for the legacy direct-conn path (loopback test, single-session-style use) and `NewReceiverFromHub(hf)` for multi-session deployments where one drainer fans out to N receivers
- [x] `stream.Hub` — single-drainer demuxer, per-flow `HubFlow` handle (Observations chan, PullLocalDrops, Release). RWMutex-guarded flow registry; per-flow ring overflow surfaces as local drops via the hub→receiver hand-off
- [x] UDP sender (sequence-stamped, fixed pps) — busy-wait < 1ms / timer-sleep otherwise, drift-free t0+n*interval
- [x] Loopback end-to-end test (sender + receiver, zero loss)
- [x] Kernel-drop sampler implementations: Linux parses `/proc/net/snmp` Udp.InErrors (column-name lookup, robust to reorder); Windows calls `iphlpapi.GetUdpStatisticsEx(AF_INET).DwInErrors` via `windows.NewLazySystemDLL` (pure Go). Surfaces `InErrors` (a superset of `RcvbufErrors`) — summing per spec literal would double-count
- [x] `SO_RCVBUF` rmem_max warning on Linux — `serveRun` calls `stream.WarnIfRmemMaxBelow(os.Stderr, stream.DefaultSocketBuf)` once at startup; silent on non-Linux and on missing/unreadable `/proc/sys/net/core/rmem_max`
- [x] Result reporting back over control channel — `cmd/dropline serve` forwards Snapshots as `Stats` and emits `Final` mapped from final Snapshot (`Sent` derived from `MaxSeq+1`)

### internal/agg
- [x] Per-second bucket struct (`Bucket`, keyed by floor(elapsed_seconds))
- [x] `StateSnapshot` construction (single immutable struct pushed to channel; non-blocking send)
- [x] `IngestStream(stream.Snapshot)` with cumulative→delta math — wired into `cmd/dropline trace`, fed by inbound `control.Stats` so the client owns the per-second timeline used in JSON output
- [x] `IngestForwardHop(probe.Snapshot)` — translates hop stats into `HopView`, emitted on every StateSnapshot. Now also writes a per-second hop snapshot into the bucket at floor(elapsed) so `Bucket.Hops` populates the JSON `timeline[].hops`
- [x] `Aggregator.Snapshot()` getter — used by the trace driver to pull final state at end-of-test
- [x] Correlator: hop RTT/loss ↔ per-second stream-loss windows — `agg.Correlate` runs at end-of-test (loss-event detection via `max(0.5%, p95)` with linear-interpolation percentile, RTT-vs-baseline + per-bucket loss signals, max-of-the-two confidence, ranked output). Uses `≥` rather than `>` so uniform-loss timelines still surface correlation
- [x] `IngestReverseHop(control.ReverseHopUpdate)` — upserts per-TTL `ReverseHopView` and emits a fresh `StateSnapshot`; `SetReversePathStatus` does the same for the status banner so the TUI sees it immediately on session start

### internal/probe
- [x] Forward TTL-walking ICMP prober (client side) — `Prober` sweeps TTL 1..MaxHops at `--mtr-interval` and emits per-sweep `Snapshot{Hops []HopStat}`. Linux/macOS implementation in `probe_unix.go` opens raw ICMPv4 and demuxes by global seq; Windows implementation in `probe_windows.go` issues `iphlpapi.dll!IcmpSendEcho2Ex` once per TTL (one synchronous syscall per hop, surfaces TimeExceeded correctly — issue #1)
- [x] Hop response parsing (TTL-exceeded vs echo-reply terminus) — `parse.go` classifies and extracts the embedded original echo's id+seq from TimeExceeded payloads
- [x] Per-hop rolling stats (sent, recv, last/best/worst, Welford avg+stddev, loss_pct, terminus)
- [x] Reverse TTL-walking ICMP prober (server side) — `cmd/dropline serve` reuses `probe.Prober` per-session against the client's TCP `RemoteAddr`, hardcoded 1s interval. NAT terminus tagging is deferred (see Reverse trace section)

### internal/tui
- [x] Bubbletea Model/Update/View skeleton — alt-screen, 200ms render tick, snapshotMsg channel reader
- [x] Forward hop table — three-tier column ladder by terminal width (Best/Worst → drop on <80, IP → drop on <50)
- [x] Reverse hop table — stacked layout below the forward table; status + resolved-hop count in the section header
- [x] Per-second loss timeline / sparkline — 60-bucket window, absolute thresholds (0% always renders flat)
- [x] Re-arming `tea.Cmd` consuming the snapshot channel
- [x] Keybindings: `[q]uit` / Ctrl+C / Esc
- [x] Keybindings: `[p]ause` — `tui.Options.PauseFn` toggles a `stream.PauseGate` (sender blocks while paused, drift-free target shifted by accumulated pause time on resume so no burst) and sends a `control.Pause{Paused bool}` wire message; the server's per-session recv loop demuxes it into an `atomic.Bool` that gates the stats forwarder so the client's sparkline freezes cleanly. Footer shows `[PAUSED] [p] resume` while paused; the elapsed-time counter freezes via accumulated `pausedFor` in the model. Mid-test only. Test duration deadline does NOT pause — pause is for inspecting, not extending
- [x] Keybindings: `[r]eset` — `Aggregator.Reset()` clears `buckets` and re-arms delta math by stashing the current cumulative snapshot in `last`. Hops/reverse hops intentionally preserved (test-long rolling stats, not per-window history). Active only while the test is running
- [x] Keybindings: `[c]opy json` — `tui.Options.CopyFn` closure renders `report.Data` via `report.RenderJSON`, base64-encodes the bytes, and writes an OSC 52 escape (`\x1b]52;c;<base64>\a`) to `os.Stdout`. Modern terminals (Windows Terminal, iTerm2, Alacritty, Kitty, WezTerm, tmux with `set-clipboard on`) interpret it as "set system clipboard"; legacy `cmd.exe` / `conhost` silently no-op. Active only on the test-complete view (mid-test press shows "copy: waiting for final"). Reuses the same `builtData` atomic.Pointer the `[s]ave` slice publishes — single source of truth
- [x] Keybindings: `[s]ave` — `tui.Options.SaveFn` closure resolves the path (`cfg.savePath` if set, else generated `dropline-<sanitized-target>-<unix>.json`) and writes the JSON via `writeJSONFile`. Active only on the test-complete view; mid-test [s] shows a brief "save: waiting for final" hint. `--save FILE` flag still works for non-TUI flows
- [x] ▲ rolling-baseline indicator + RTT-vs-baseline colors — `probe.HopStat.BaselineRTTMS` carries an EWMA (α=0.2) updated on every reply; cascades into `agg.HopView.BaselineRTTMS`. Renderer tiers `(LastRTTMS - BaselineRTTMS) / StdDevRTTMS`: `>3` → red `▲▲`, `>1.5` → amber ` ▲`, else plain. Cell-narrow coloring; loss% keeps its own color rule
- [x] Suspect-hop highlighting in the table — `Aggregator.MarkSuspects` (called by the trace driver after `agg.Correlate`) flips `HopView.Suspect=true`; the renderer prefixes the TTL with a red `! ` gutter. End-of-test only — runs inside the recvLoop goroutine in TUI mode so the marked StateSnapshot reaches the bubbletea model before the snaps channel closes
- [x] Dashboard polish slice — section frames (rounded borders with title embedded in the top border), KPI card consolidating elapsed/duration progress-bar (`charm.land/bubbles/v2/progress`), loss/jitter/recv/lost, verdict (`✓ network loss is real` / `⚠ network loss is suspect`), bursts, and kernel-drops counter; forward + reverse hop tables now render through `charm.land/lipgloss/v2/table` (width ladder preserved); sparkline runes are colored per-bucket by `lossColor`; `[?] help` opens a full-screen overlay backed by `charm.land/bubbles/v2/help` + `key.Binding` with `?` / `esc` toggling, other keys inert while visible. Architectural rule unchanged — pure rendering, no business-logic touches

### internal/report
- [x] Text renderer (`RenderText`) — header, forward hop table (omitted when empty), stream stats, burst histogram.
- [x] JSON renderer (`RenderJSON`) — emits the spec schema. `hops` array now populated per spec shape (`ttl`, `ip`, `sent`, `recv`, `loss_pct`, `rtt_ms{last,best,worst,avg,stddev}`, `suspect`). `correlation.suspect_hops` and per-bucket `timeline[].hops` stay empty arrays. Bursts collapsed to `{1,2,3-9,10+,max}` per the spec sample.
- [x] Reverse hop array (`reverse_path`) + `reverse_path_status` field — slim wire shape (`ttl, addr, rtt_ms, loss_pct, terminus?`); rich rolling-stats shape is a follow-up. Text renderer prints a "reverse path (status: …):" section
- [x] Per-bucket `timeline[].hops` populated — every bucket carries the hop snapshot in effect at that second (rich shape with rtt_ms{last,best,worst,avg,stddev}); empty array (not null) for seconds with no hop ingest yet
- [x] Suspect-hop correlation entries — `correlation.suspect_hops` carries `{ttl, confidence, evidence}` per `agg.Correlate` ranking; per-hop `Suspect` flag in `hops[]` flips for any flagged TTL. Text renderer prints a "suspect hops:" section

### internal/svc
- [x] Windows service wrapper — `Install`, `Uninstall`, `Run` (auto-start, `NT AUTHORITY\LocalService`) via `golang.org/x/sys/windows/svc` + `svc/mgr`. `handler.Execute` translates SCM `Stop` / `Shutdown` into context cancellation that drives `serveRun` to a clean exit
- [x] No-op stubs on Linux (build-tagged) — return `errNotWindows` so the cross-platform `cmd/dropline service …` path compiles unconditionally

### deploy/
- [x] systemd unit for `dropline serve` (`deploy/dropline.service`)

## Reverse trace (cross-cutting)

Designed in conversation 2026-05-09. Server runs an ICMP TTL-walk back
toward the client's public IP (from control TCP `RemoteAddr`) and
streams hops to the client live.

### Lifecycle & target
- [x] Starts when control channel established; runs concurrently with forward trace
- [x] Same cadence as forward trace — `Hello.MTRIntervalMS` carries the client's `--mtr-interval` to the server, which threads it into the reverse prober's `MTRInterval`. Zero/missing falls back to 1s for old-client backward compat
- [x] Stops on disconnect / test end (sessionCtx-gated)
- [x] Target = control TCP `RemoteAddr` only (never client-supplied)
- [x] Works for RFC1918 / loopback `RemoteAddr` too (LAN tests)

### Privileges & opt-in
- [x] Server detects raw-ICMP capability at startup; capability flag threads through `newServeHandler` into the per-session reverse-trace decision
- [x] Server still starts unprivileged — only reverse trace is disabled (Ready.ReverseTrace="off" in that case)
- [x] Client flag `--reverse-trace={on,off,auto}` (default `auto`)
- [x] Client preference sent in `Hello.ReverseTrace`; server resolves and echoes the decision in `Ready.ReverseTrace`

### Wire & streaming
- [x] `ReverseHopUpdate` message: `{ttl, addr, rtt_ms, loss_pct, terminus?, sent?, recv?, rtt_ms_best?, rtt_ms_worst?, rtt_ms_avg?, rtt_ms_stddev?, rtt_ms_baseline?}`. Rich rolling fields are omitempty so old (slim-shape) servers stay byte-for-byte identical on the wire; new clients reading old-server messages see zeros and degrade naturally
- [x] Incremental updates merged client-side by TTL (`agg.IngestReverseHop` upserts the per-TTL map; no end-of-test buffering)
- [x] Backpressure: reverse prober's `revSnaps` channel is buffered 4; non-blocking emit drops oldest if the forwarder + `c.Send` can't keep up

### NAT terminus
- [x] Final hop tagged `terminus: "host"` vs `"nat"` — `cmd/dropline.classifyTerminus` compares the EchoReply's source address to the client IP we walked toward; matching IP → `"host"`, mismatch → `"nat"`, intermediate hops emit empty (omitempty on the wire)
- [x] `reverse_path_status` ships all four states (`ok` / `disabled_by_server` / `disabled_by_client` / `terminated_at_nat`). `agg.IngestReverseHop` sticky-escalates `"ok"` → `"terminated_at_nat"` the first time a hop with `Terminus=="nat"` is upserted; disabled states are not overridden

### TUI
- [x] Reverse hop table renders below the forward table (stacked layout); status line is "reverse path (status: …, N hops)"
- [x] Hops fill in progressively as TTLs resolve (every `IngestReverseHop` re-emits a `StateSnapshot`)
- [x] Stable status line variants — header carries the resolved-hop count and a friendly form of the wire status (`terminated at NAT`, `disabled by server`, `disabled by client`). JSON keeps the snake_case wire form for machine consumers
- [x] Banner styling for reverse-trace status — `reverseStatusStyle` maps the wire status to one of two visual tiers: `ok` and `disabled_by_client` stay dim (steady state and intentional disable); `disabled_by_server`, `terminated_at_nat`, and the empty/unknown fallback render amber so an empty `reverse_path` table carries its own explanation. Applied to both the `(status: …, N hops)` annotation and the "(no reverse hops)" body line; the section title `reverse path` keeps its `headerStyle`
- [x] Reverse hop table mirrors the forward width-ladder (≥80 cols → Best/Worst, ≥50 → drop them, <50 → drop Addr) and carries the same ▲ EWMA baseline glyph via `rttBaselineGlyphFor`. No suspect gutter on reverse rows (correlator only flags forward-path hops)

### JSON report
- [x] `reverse_path: [...]` array + `hops: [...]` for the forward path. Spec sample's `forward_path` rename is a separate breaking-change decision — not done this slice
- [x] Identical per-hop shape — `reverse_path[]` now carries `sent`, `recv`, `rtt_ms{last,best,worst,avg,stddev}` matching `hops[]`. Differences are intentional: `addr` in place of `ip`, no `suspect` flag (correlator runs against forward-path stream loss only). `rtt_ms` is a JSON object now — was a flat float in v0
- [x] `reverse_path_status` field — defaults to `disabled_by_server` when the client has nothing to set it to

### Out of scope (resist creep)
- Reverse UDP loss stream (server→client) — v2 question
- Correlating reverse hops with the forward UDP loss stream — different paths, would mislead

## Build / packaging
- [x] `go.mod`, deps pinned
- [x] Makefile with 3 cross-compile targets + SHA256SUMS — Windows-branch recipes (`build_for`, `ensure_dist`, `clean_dist`, `hash_cmd`) wrapped in `cmd /C "..."` so `make` works whether `$(SHELL)` is sh.exe or cmd.exe
- [x] CGO_ENABLED=0 enforced everywhere
- [x] README per spec § DoD #6 — install, systemd unit, Windows admin requirement, network-loss vs kernel-drops distinction, ECMP caveat, JSON output, status pointer
- [x] CI — GitHub Actions: `.github/workflows/ci.yml` runs `go vet` + `go test ./...` on push/PR with a `ubuntu-latest`+`windows-latest` matrix (so Windows-only code under `internal/privcheck`, `internal/svc`, `internal/stream/kerneldrops_windows.go` gets exercised), plus a parallel `build` job that runs `make all` and `make e2e` (with `setcap cap_net_raw+ep` on the Linux binary)
- [x] `make e2e` harness (spec § DoD #4) — `cmd/e2e/main.go` is a black-box driver that spawns `dropline serve` on a free loopback port, runs `dropline trace --json`, and asserts `recv > 0`, `loss_pct < 1`, `kernel_drops == 0`, and a present `correlation` field. Requires raw-ICMP privilege for the trace subprocess (elevated PowerShell on Windows, `setcap cap_net_raw+ep` on Linux); CI grants the cap automatically
- [x] Release/distribution mechanism (spec § DoD #5) — `.github/workflows/release.yml` triggers on `v*` tags, runs `make all VERSION=${ref_name}` (stamps the tag into the binary via `-ldflags "-X main.version=$(VERSION)"`), runs `make e2e` as a release gate, then publishes the three binaries + `SHA256SUMS` to `github.com/PStorgaard/dropline/releases/tag/<TAG>` with auto-generated notes via `gh release create --generate-notes`
