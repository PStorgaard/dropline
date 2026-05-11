package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/PStorgaard/dropline/internal/svc"
)

// runService dispatches the second-level subcommand of `dropline
// service` (install / uninstall / run). On non-Windows builds the
// internal/svc functions return a clear errNotWindows error which
// surfaces verbatim — no flag in this layer prevents that.
func runService(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "service: missing subcommand (expected install / uninstall / run)")
		os.Exit(2)
	}
	switch args[0] {
	case "install":
		runServiceInstall(args[1:])
	case "uninstall":
		runServiceUninstall(args[1:])
	case "run":
		runServiceRun(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "service: unknown subcommand %q (expected install / uninstall / run)\n", args[0])
		os.Exit(2)
	}
}

// runServiceInstall validates the supplied serve flags, builds the
// argv that the SCM will pass to `dropline service run`, and asks
// internal/svc to register it.
func runServiceInstall(args []string) {
	cfg, err := parseServeArgs(args, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "service install: %s\n", err)
		os.Exit(2)
	}
	registered := []string{"service", "run",
		"--listen", cfg.listen,
		"--max-sessions", strconv.Itoa(cfg.maxSessions),
	}
	if err := svc.Install(registered); err != nil {
		fmt.Fprintf(os.Stderr, "service install: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("service installed (%s). Start with `sc start %s`.\n", svc.ServiceName, svc.ServiceName)
}

// runServiceUninstall removes the service. Idempotent in svc.Uninstall.
func runServiceUninstall(args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "service uninstall: unexpected arguments: %v\n", args)
		os.Exit(2)
	}
	if err := svc.Uninstall(); err != nil {
		fmt.Fprintf(os.Stderr, "service uninstall: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("service uninstalled (%s).\n", svc.ServiceName)
}

// runServiceRun is the entry point the SCM invokes when the service
// starts. Re-parses the serve flags the SCM passed (registered at
// install time) and hands a serveRun closure to internal/svc.
//
// On non-Windows this returns the errNotWindows error immediately,
// which is fine — Linux uses the systemd unit instead and shouldn't
// invoke `dropline service run` directly.
func runServiceRun(args []string) {
	cfg, err := parseServeArgs(args, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "service run: %s\n", err)
		os.Exit(2)
	}
	work := func(ctx context.Context) error {
		// Forward platform signals into the same context the SCM's
		// Stop/Shutdown will cancel — useful when an operator runs
		// `dropline service run` from a console for local debugging.
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		return serveRun(ctx, cfg)
	}
	if err := svc.Run(work); err != nil {
		fmt.Fprintf(os.Stderr, "service run: %s\n", err)
		os.Exit(1)
	}
}
