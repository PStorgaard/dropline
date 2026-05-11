// Package agg is the broker between business goroutines and the TUI.
//
// It maintains per-second history buckets indexed by floor(elapsed_seconds)
// (spec § Aggregator + correlation, lines 177–191) and pushes immutable
// StateSnapshot values onto a single channel that the TUI consumes via a
// re-arming tea.Cmd (spec § Architecture, lines 75–78).
//
// This slice ships stream-side ingestion and StateSnapshot construction.
// Hop ingestion (forward + reverse) and the end-of-test correlator
// (loss-event detection, suspect-hop ranking) are deferred to the slice
// that lands alongside internal/probe — they need real hop history to be
// testable. See progress.md.
package agg
