// Package control implements the TCP control channel and JSON protocol.
//
// Wire format: 4-byte big-endian length prefix followed by a JSON payload.
// Spec § Control channel defines five message types — hello, ready, error,
// stats, final. ReverseHopUpdate is conversation-derived (2026-05-09) and
// rides on the same channel.
//
// The package is a pure transport: it marshals/unmarshals frames and runs
// the listener/dialer. Session ID minting, max-session enforcement, and
// stats production live in higher layers.
package control
