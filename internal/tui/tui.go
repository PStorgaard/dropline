// Package tui implements the bubbletea Model/Update/View for the live
// dashboard. Spec § TUI is the long-term target; this slice ships the
// skeleton (header, forward hop table, stream summary, loss/sec
// sparkline, q/ctrl+c quit). [p]ause / [r]eset / [c]opy / [s]ave
// keybindings, ▲ rolling-baseline indicators, and the column-collapse
// ladder land with later slices — see progress.md.
package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/PStorgaard/dropline/internal/agg"
)

// Options carries everything the TUI needs to render its header and
// elapsed/total counters. The aggregator's channel supplies the live
// data; Options is purely for chrome plus optional callbacks the TUI
// invokes from keybindings.
type Options struct {
	Target    string
	RateBPS   int64
	Duration  time.Duration
	StartedAt time.Time
	// ResetFn, when non-nil, is called on [r] keypresses. The TUI
	// expects it to clear the aggregator's per-second history; the
	// callback is responsible for emitting any follow-up StateSnapshot.
	ResetFn func()
	// SaveFn, when non-nil, is called on [s] keypresses (only on the
	// test-complete view). Returns the path written, or an error. The
	// TUI surfaces either outcome in the footer for ~3s.
	SaveFn func() (string, error)
	// CopyFn, when non-nil, is called on [c] keypresses (only on the
	// test-complete view). Returns the byte count of the JSON copied
	// to the system clipboard, or an error. The escape-write is the
	// callback's job (the TUI package stays free of OS-side IO); the
	// TUI only surfaces the outcome in the footer.
	CopyFn func() (int, error)
	// PauseFn, when non-nil, is called on [p] keypresses while the
	// test is running. It toggles the sender's pause state and the
	// server's stats forwarder via a wire-level Pause message.
	// Returns the new paused state and any error from the wire send.
	// The TUI tracks its own paused-display flag from the returned
	// state.
	PauseFn func() (paused bool, err error)
}

// Run starts the bubbletea program and blocks until the user quits or
// ctx is cancelled. The caller is responsible for closing snaps when
// the test ends — that signals "test complete, awaiting quit" in the
// model. Returns nil on normal exit (tea.Quit), the program's error
// otherwise.
func Run(ctx context.Context, snaps <-chan agg.StateSnapshot, opts Options) error {
	// In bubbletea v2 alt-screen is a per-frame property of the View
	// struct rather than a program option. The model sets it on every
	// View() call (see model.go).
	p := tea.NewProgram(
		newModel(opts, snaps),
		tea.WithContext(ctx),
	)
	_, err := p.Run()
	return err
}
