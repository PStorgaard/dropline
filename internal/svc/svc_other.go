//go:build !windows

package svc

import (
	"context"
	"errors"
)

// errNotWindows is returned by all three exported functions on
// non-Windows builds. The Linux deployment path is the systemd unit at
// deploy/dropline.service — there's no SCM equivalent in this package.
var errNotWindows = errors.New("svc: Windows-only; use the systemd unit on Linux (deploy/dropline.service)")

// Run is a no-op stub on non-Windows platforms.
func Run(_ func(ctx context.Context) error) error { return errNotWindows }

// Install is a no-op stub on non-Windows platforms.
func Install(_ []string) error { return errNotWindows }

// Uninstall is a no-op stub on non-Windows platforms.
func Uninstall() error { return errNotWindows }
