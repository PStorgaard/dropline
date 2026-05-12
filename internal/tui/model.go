package tui

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/help"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/PStorgaard/dropline/internal/agg"
)

const renderInterval = 200 * time.Millisecond

// snapshotMsg carries one StateSnapshot pulled from the aggregator's output
// channel. ok=false signals the channel has been closed by the caller —
// the TUI then enters its "test complete, awaiting quit" state.
type snapshotMsg struct {
	snap agg.StateSnapshot
	ok   bool
}

// tickMsg fires the periodic re-render. Bubbletea only repaints in response
// to a Msg, so we keep a 200ms ticker even when no new snapshot has arrived
// (header elapsed counter, dimmed-row decay, etc.).
type tickMsg struct{}

type model struct {
	opts   Options
	snaps  <-chan agg.StateSnapshot
	latest agg.StateSnapshot
	width  int
	height int
	done   bool // true once the snapshot channel has closed
	// status is a transient footer line shown for statusDecay after a
	// keybinding fires (e.g. "wrote: foo.json"). Empty otherwise.
	status   string
	statusAt time.Time
	// paused tracks the sticky pause indicator surfaced in the footer.
	// pausedAt is the wall-clock time the current pause began, zero
	// when not paused. pausedFor accumulates the wall-clock time spent
	// in completed pause cycles; both feed View's elapsed-time math
	// so the header counter freezes during a pause.
	paused    bool
	pausedAt  time.Time
	pausedFor time.Duration
	// helpVisible toggles the `?` overlay. When true View() renders the
	// help model fullscreen and skips the dashboard entirely.
	helpVisible bool
	help        help.Model
	// barWidth is the visible width of the elapsed-vs-duration progress
	// bar in renderKPI. Computed from the terminal width on each
	// WindowSizeMsg; floored at 10 so the bar keeps a visible nub even
	// on narrow terminals.
	barWidth int
	keys     keyMap
}

// statusDecay is how long a transient footer status (save / reset /
// blocked-save hints) stays visible before the periodic tickMsg clears
// it. Long enough to read; short enough not to outlive its trigger.
const statusDecay = 3 * time.Second

func newModel(opts Options, snaps <-chan agg.StateSnapshot) model {
	h := help.New()
	h.ShowAll = true
	return model{
		opts:     opts,
		snaps:    snaps,
		width:    80,
		height:   24,
		barWidth: 45, // 80 - 35; refreshed on first WindowSizeMsg
		help:     h,
		keys:     newKeyMap(),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(waitSnapshot(m.snaps), tickRender())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.help.SetWidth(msg.Width)
		// Reserve room for "  elapsed mm:ss  " + "  duration mm:ss".
		// Floor at 10 so the bar keeps a visible nub even on narrow
		// terminals.
		barW := msg.Width - 35
		if barW < 10 {
			barW = 10
		}
		m.barWidth = barW
		return m, nil
	case tea.KeyPressMsg:
		// In bubbletea v2 keys split into Press / Release events; we
		// only act on press. The interface's String() form is
		// unchanged and matches the v1 keystroke names.
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "esc":
			if m.helpVisible {
				m.helpVisible = false
				return m, nil
			}
			return m, tea.Quit
		case "?":
			m.helpVisible = !m.helpVisible
			return m, nil
		case "r":
			if m.opts.ResetFn != nil && !m.done && !m.helpVisible {
				m.opts.ResetFn()
				m.setStatus("history reset")
			}
			return m, nil
		case "s":
			if m.helpVisible || m.opts.SaveFn == nil {
				return m, nil
			}
			if !m.done {
				m.setStatus("save: waiting for final")
				return m, nil
			}
			path, err := m.opts.SaveFn()
			if err != nil {
				m.setStatus("save failed: " + err.Error())
			} else {
				m.setStatus("wrote: " + path)
			}
			return m, nil
		case "c":
			if m.helpVisible || m.opts.CopyFn == nil {
				return m, nil
			}
			if !m.done {
				m.setStatus("copy: waiting for final")
				return m, nil
			}
			n, err := m.opts.CopyFn()
			if err != nil {
				m.setStatus("copy failed: " + err.Error())
			} else {
				m.setStatus(fmt.Sprintf("copied: %d bytes", n))
			}
			return m, nil
		case "p":
			if m.helpVisible || m.opts.PauseFn == nil || m.done {
				return m, nil
			}
			now := time.Now()
			paused, err := m.opts.PauseFn()
			if err != nil {
				m.setStatus("pause failed: " + err.Error())
				return m, nil
			}
			if paused && !m.paused {
				m.pausedAt = now
			} else if !paused && m.paused {
				m.pausedFor += now.Sub(m.pausedAt)
				m.pausedAt = time.Time{}
			}
			m.paused = paused
			return m, nil
		}
		return m, nil
	case snapshotMsg:
		if !msg.ok {
			m.done = true
			return m, nil
		}
		m.latest = msg.snap
		return m, waitSnapshot(m.snaps)
	case tickMsg:
		if m.status != "" && time.Since(m.statusAt) >= statusDecay {
			m.status = ""
		}
		return m, tickRender()
	}
	return m, nil
}

// setStatus stamps a transient footer line with the current time so the
// next tickMsg past statusDecay clears it.
func (m *model) setStatus(s string) {
	m.status = s
	m.statusAt = time.Now()
}

func (m model) View() tea.View {
	// Help overlay short-circuits the dashboard: full-screen,
	// alt-screen still on so the user gets the same exit behavior.
	if m.helpVisible {
		body := headerStyle.Render("dropline · keybindings") + "\n\n" +
			m.help.FullHelpView(m.keys.FullHelp()) + "\n\n" +
			dimStyle.Render("press ? or esc to return")
		v := tea.NewView(body)
		v.AltScreen = true
		return v
	}

	elapsed := time.Since(m.opts.StartedAt) - m.pausedFor
	if m.paused && !m.pausedAt.IsZero() {
		elapsed -= time.Since(m.pausedAt)
	}
	if elapsed < 0 {
		elapsed = 0
	}
	if elapsed > m.opts.Duration {
		elapsed = m.opts.Duration
	}

	pct := 0.0
	if m.opts.Duration > 0 {
		pct = float64(elapsed) / float64(m.opts.Duration)
	}
	if pct > 1 {
		pct = 1
	}
	bar := renderBar(pct, m.barWidth)

	// Assemble the dashboard zones (everything above the footer). Each
	// zone is one rule-headed block; consecutive zones get a blank line
	// between them so the eye has somewhere to rest.
	sections := []string{
		renderKPI(m.opts, m.latest.Stream, elapsed, m.paused, bar, m.width),
	}
	if rev := renderReverseKPI(m.latest.ReverseStream, m.width); rev != "" {
		sections = append(sections, rev)
	}
	if tcp := renderTCPCorroborateKPI(m.latest.TCPCorroborate, m.width); tcp != "" {
		sections = append(sections, tcp)
	}
	if m.width >= 30 {
		if chart := renderSparkline(m.latest.Buckets, m.width); chart != "" {
			sections = append(sections, chart)
		}
	}
	sections = append(sections,
		renderHopTable(m.latest.LatestForwardHops, m.width),
		renderReverseHopTable(m.latest.LatestReverseHops, m.latest.ReversePathStatus, m.width),
	)
	if m.latest.TCPHopProbePort > 0 {
		sections = append(sections, renderTCPHopTable(
			m.latest.LatestForwardTCPHops, m.latest.TCPHopProbePort,
			m.latest.TCPHopProbeSupported, m.width))
		if m.latest.TCPHopProbeSupported && len(m.latest.LatestReverseTCPHops) > 0 {
			sections = append(sections, renderReverseTCPHopTable(
				m.latest.LatestReverseTCPHops, m.latest.TCPHopProbePort, m.width))
		}
	}

	parts := make([]string, 0, 2*len(sections)+1)
	for i, s := range sections {
		if i > 0 {
			parts = append(parts, "")
		}
		parts = append(parts, s)
	}
	// Footer is flush against the last zone (no blank line before it)
	// to keep the keybindings strip from looking orphaned.
	parts = append(parts, dimStyle.Render("  "+m.footerText()))

	v := tea.NewView(lipgloss.JoinVertical(lipgloss.Left, parts...))
	v.AltScreen = true
	return v
}

// footerText composes the dim footer line. Includes the live-mode
// keybindings while the test is running, swaps to the test-complete
// hints once snaps has closed, and overlays the transient status (e.g.
// "wrote: foo.json") when one is active.
func (m model) footerText() string {
	if m.status != "" && !m.paused {
		return m.status
	}
	if m.done {
		parts := []string{"test complete"}
		if m.opts.CopyFn != nil {
			parts = append(parts, "[c] copy")
		}
		if m.opts.SaveFn != nil {
			parts = append(parts, "[s] save")
		}
		parts = append(parts, "[?] help", "[q] exit")
		return strings.Join(parts, " · ")
	}
	pauseLabel := "[p] pause"
	if m.paused {
		pauseLabel = "[p] resume"
	}
	parts := []string{"[?] help", "[q] quit"}
	if m.opts.ResetFn != nil {
		parts = append([]string{"[r] reset"}, parts...)
	}
	if m.opts.SaveFn != nil {
		parts = append([]string{"[s] save"}, parts...)
	}
	if m.opts.CopyFn != nil {
		parts = append([]string{"[c] copy"}, parts...)
	}
	if m.opts.PauseFn != nil {
		parts = append([]string{pauseLabel}, parts...)
	}
	hint := strings.Join(parts, " · ")
	if m.paused {
		return "[PAUSED] " + hint
	}
	return hint
}

// waitSnapshot returns a Cmd that blocks on one read from the aggregator's
// output channel, then yields its result as a snapshotMsg. The model
// re-arms by calling waitSnapshot again from Update; closure of the channel
// produces a single snapshotMsg{ok: false} which transitions the model into
// "done" state without re-arming.
func waitSnapshot(snaps <-chan agg.StateSnapshot) tea.Cmd {
	return func() tea.Msg {
		snap, ok := <-snaps
		return snapshotMsg{snap: snap, ok: ok}
	}
}

// tickRender produces a tickMsg every renderInterval. Used for header
// elapsed-time updates in the absence of fresh snapshots.
func tickRender() tea.Cmd {
	return tea.Tick(renderInterval, func(time.Time) tea.Msg { return tickMsg{} })
}
