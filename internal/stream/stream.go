// Package stream implements the UDP loss-test sender and receiver.
//
// Wire format (spec § internal/stream): a 32-byte big-endian header —
// magic | flow_id | seq | tx_unix_ns | reserved — followed by a
// packet_size-32-byte body of random bytes generated once at startup
// and reused. Receiver computes loss, jitter, out-of-order, duplicates,
// and a burst histogram; emission of those onto the control channel is
// the caller's job.
//
// Concurrency: Receiver runs a drainer goroutine that does only
// ReadFromUDP into a fixed-size ring channel, and a separate stats
// goroutine that decodes headers and updates the accumulator. Ring
// overflow is a *local* bottleneck and is counted distinctly from
// network loss.
package stream
