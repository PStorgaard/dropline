# dropline build
#
# Cross-compiles to linux/amd64, linux/arm64, windows/amd64.
# Pure Go (CGO_ENABLED=0) so binaries are static and free of platform deps.
#
# Detects host shell: on Windows make spawns cmd.exe; elsewhere it's sh.
# That changes how env vars and directory ops are spelled in recipes.

GO         ?= go
PKG        := ./cmd/dropline
DIST       := dist

# VERSION stamps the binary's `dropline version` output. Defaults to
# `git describe --tags --always --dirty` so a tag-built artifact prints
# e.g. v1.0.0 and a dev build prints e.g. aed0b2f-dirty. Falls back to
# "dev" when git isn't available (extracted tarball, no .git, etc.).
# Override on the command line: `make all VERSION=v1.0.0`.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)
BUILDFLAGS := -trimpath -ldflags="$(LDFLAGS)"

# Make's $(SHELL) is sh on both branches in this project (sh.exe via
# Git for Windows on Windows, /bin/sh on Linux), so POSIX commands are
# the portable form across both. sha256sum ships with Git for Windows.
# If you ever swap Make's SHELL to cmd.exe, swap these recipes back to
# cmd-style (`cmd /C "set X=Y&& ..."`, certutil for hashing, etc.).
build_for = CGO_ENABLED=0 GOOS=$(1) GOARCH=$(2) $(GO) build $(BUILDFLAGS) -o $@ $(PKG)
ensure_dist = mkdir -p $(DIST)
clean_dist = rm -rf $(DIST)
hash_cmd = cd $(DIST) && sha256sum dropline-* > SHA256SUMS

ifeq ($(OS),Windows_NT)
  e2e_bin = $(DIST)/dropline-windows-amd64.exe
else
  e2e_bin = $(DIST)/dropline-linux-amd64
endif

# e2e runs through Make's $(SHELL) (sh on Windows-with-ezwinports and on
# Linux), so a POSIX env-prefix is the simplest portable form. If you
# ever swap Make's SHELL to cmd.exe, switch to `cmd /C "set X=Y&& ..."`.
e2e_run = DROPLINE_BIN=$(e2e_bin) $(GO) run ./cmd/e2e

TARGETS := \
	$(DIST)/dropline-linux-amd64 \
	$(DIST)/dropline-linux-arm64 \
	$(DIST)/dropline-windows-amd64.exe

.PHONY: all linux-amd64 linux-arm64 windows-amd64 test vet lint fmt run clean checksums e2e

all: $(TARGETS) checksums

linux-amd64:   $(DIST)/dropline-linux-amd64
linux-arm64:   $(DIST)/dropline-linux-arm64
windows-amd64: $(DIST)/dropline-windows-amd64.exe

$(DIST):
	@$(ensure_dist)

$(DIST)/dropline-linux-amd64: | $(DIST)
	$(call build_for,linux,amd64)

$(DIST)/dropline-linux-arm64: | $(DIST)
	$(call build_for,linux,arm64)

$(DIST)/dropline-windows-amd64.exe: | $(DIST)
	$(call build_for,windows,amd64)

checksums: $(TARGETS)
	$(hash_cmd)

# Black-box smoke test. Requires raw-ICMP privilege for the trace
# subprocess: elevated PowerShell on Windows, or `setcap cap_net_raw+ep`
# on the binary on Linux. See cmd/e2e/main.go.
e2e: $(e2e_bin)
	$(e2e_run)

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

fmt:
	$(GO) fmt ./...

run:
	$(GO) run $(PKG)

clean:
	@$(clean_dist)
