//go:build windows

package svc

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// Run is the entry point invoked by `dropline service run` (which the
// SCM launches when the service starts). It blocks until the SCM sends
// Stop or Shutdown, at which point the supplied work function's
// context is cancelled and Run waits for it to return before reporting
// the service stopped.
//
// work should run the server until ctx cancels and return whatever
// error it observed. Returning a non-nil error surfaces as a
// service-specific exit code (1) in the SCM's status.
func Run(work func(ctx context.Context) error) error {
	if work == nil {
		return errors.New("svc: nil work function")
	}
	return svc.Run(ServiceName, &handler{work: work})
}

// Install registers the running executable with the Service Control
// Manager as auto-start, running under NT AUTHORITY\LocalService. args
// become the registered command-line tokens following the exe path —
// typically `[]string{"service", "run", "--listen", ":5301", "--max-sessions", "4"}`.
//
// Returns ERROR_ACCESS_DENIED if the caller isn't running elevated.
// Returns a clear error if the service is already installed; uninstall
// first to overwrite.
func Install(args []string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("svc: locate exe: %w", err)
	}

	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("svc: connect to SCM (needs Administrator): %w", err)
	}
	defer func() { _ = m.Disconnect() }()

	if existing, err := m.OpenService(ServiceName); err == nil {
		_ = existing.Close()
		return fmt.Errorf("svc: service %q already installed; uninstall first", ServiceName)
	}

	cfg := mgr.Config{
		ServiceType:      windows.SERVICE_WIN32_OWN_PROCESS,
		StartType:        mgr.StartAutomatic,
		ErrorControl:     mgr.ErrorNormal,
		DisplayName:      DisplayName,
		Description:      Description,
		ServiceStartName: `NT AUTHORITY\LocalService`,
	}
	s, err := m.CreateService(ServiceName, exe, cfg, args...)
	if err != nil {
		return fmt.Errorf("svc: create service: %w", err)
	}
	_ = s.Close()
	return nil
}

// Uninstall removes the service registration. Idempotent: returns nil
// if the service is already absent. Best-effort sends a Stop control
// before deletion so a running service unwinds cleanly.
func Uninstall() error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("svc: connect to SCM (needs Administrator): %w", err)
	}
	defer func() { _ = m.Disconnect() }()

	s, err := m.OpenService(ServiceName)
	if err != nil {
		if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
			return nil
		}
		return fmt.Errorf("svc: open service: %w", err)
	}
	defer func() { _ = s.Close() }()

	// Best-effort stop. Errors are common here (service not running,
	// no permission to stop) and should not block deletion.
	_, _ = s.Control(svc.Stop)

	if err := s.Delete(); err != nil {
		return fmt.Errorf("svc: delete service: %w", err)
	}
	return nil
}

// handler implements svc.Handler. The Execute method receives change
// requests from the SCM and translates Stop / Shutdown into context
// cancellation on the supplied work function.
type handler struct {
	work func(ctx context.Context) error
}

func (h *handler) Execute(_ []string, r <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	const accepted = svc.AcceptStop | svc.AcceptShutdown
	status <- svc.Status{State: svc.StartPending}

	ctx, cancel := context.WithCancel(context.Background())
	workDone := make(chan struct{})
	var workErr error
	go func() {
		defer close(workDone)
		workErr = h.work(ctx)
	}()

	status <- svc.Status{State: svc.Running, Accepts: accepted}

loop:
	for {
		select {
		case req, ok := <-r:
			if !ok {
				// SCM closed the request channel unexpectedly — exit
				// rather than spin on zero-value receives.
				cancel()
				break loop
			}
			switch req.Cmd {
			case svc.Interrogate:
				status <- req.CurrentStatus
			case svc.Stop, svc.Shutdown:
				cancel()
				break loop
			default:
				// ignore unhandled control codes
			}
		case <-workDone:
			// work returned on its own (e.g. fatal error before SCM
			// requested stop) — exit and report a service-specific
			// error to the SCM.
			break loop
		}
	}

	status <- svc.Status{State: svc.StopPending}
	cancel()
	<-workDone
	status <- svc.Status{State: svc.Stopped}

	if workErr != nil {
		return true, 1
	}
	return false, 0
}
