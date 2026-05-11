// Package privcheck reports whether the current process can open raw ICMP
// sockets. Callers decide policy: dropline trace fails loud on !OK; dropline
// serve degrades by advertising reverse_trace=unavailable in the control Hello.
package privcheck

// Status is the result of a raw-ICMP capability probe.
//
// Invariants: OK == true implies Reason == ""; OK == false implies Reason
// is non-empty and human-readable.
type Status struct {
	OK     bool
	Reason string
}
