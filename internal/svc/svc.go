// Package svc wraps the Windows-side install/uninstall/run-as-service
// flow for `dropline serve`. The Windows implementation uses
// golang.org/x/sys/windows/svc; non-Windows builds get a stub that
// returns a clear error so callers can compile cross-platform without
// build tags of their own.
//
// On Linux, the deployment story is the systemd unit at
// deploy/dropline.service — there is no service manager equivalent in
// this package.
package svc

// ServiceName is the SCM identifier used by `sc query dropline`,
// `sc start dropline`, etc. Keep stable; renaming would orphan
// existing installations.
const ServiceName = "dropline"

// DisplayName is shown in services.msc and `sc query`.
const DisplayName = "dropline loss-test server"

// Description is the long-form copy stored alongside the service
// registration.
const Description = "Server for the dropline network diagnostic tool. " +
	"Listens on a TCP control channel and a UDP loss-test stream."
