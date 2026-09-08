# Installation and Upgrade Guide

This document contains the detailed install, upgrade, uninstall, and source-build flows for Looper.

## Requirements

For the default supported install path:

- macOS (`darwin-arm64`) or Linux (`linux-amd64`)
- `git`
- `gh` for GitHub projects; Forgejo-only installs do not require `gh`

For source development:

- Go `1.22`
- `git`
- `gh` for GitHub projects; Forgejo-only development does not require `gh`
- `osascript` if macOS notifications stay enabled

`looperd` auto-detects tool paths from `PATH`, but startup validation fails if required tools cannot be resolved. `git` is always required. `gh` is required when any configured project uses the GitHub provider, but a Forgejo-only config starts without `gh` when the Forgejo provider and token environment variable are valid.

Forgejo reviewer/fixer conflict checks require Git 2.38+ (`merge-tree --write-tree`). Repair ancestry checks need enough local history to distinguish a divergent head; an ambiguous shallow clone reports an error until its history is completed. Tea-backed Forgejo authentication is also supported with an explicit existing login.

## Install

Looper uses Go binaries as the default supported implementation.

The quickest first-time setup is:

```bash
curl -fsSL https://raw.githubusercontent.com/nexu-io/looper/main/scripts/install.sh | sh
looper bootstrap --yes --project-path /path/to/repo --agent-vendor opencode
```

`looper bootstrap` creates an initial config, installs or reuses the managed daemon, optionally registers a project, and starts the daemon.

### Install the CLI manually

1. Download the matching `looper` release artifact for your platform from GitHub Releases.
2. Rename it to `looper` if needed.
3. Place it on your `PATH`, for example `/usr/local/bin/looper` or `~/.local/bin/looper`.

GitHub Releases publish standalone Go binaries for both `looper` and `looperd` on `darwin-arm64` and `linux-amd64`.

### Install the daemon manually

If you prefer the manual daemon flow instead of `looper bootstrap`:

```bash
looper daemon install
looper daemon start
looper status
```

This flow:

- detects the current supported platform and architecture
- downloads the matching GitHub Release artifact
- installs it to `~/.looper/bin/looperd`

Current release binaries are unsigned. If macOS Gatekeeper blocks the first launch, you may need to allow the binary manually in System Settings.

Manual fallback:

- download the matching `looperd` release artifact yourself
- place it at `~/.looper/bin/looperd` or somewhere on your `PATH`

Daemon lookup order is fixed to `~/.looper/bin/looperd`, then `$PATH`.

By default, `looper daemon start` launches `looperd` detached and prints `started detached, not supervised`. Detached mode writes `~/.looper/looperd.pid` and `~/.looper/looperd.state.json`, but it does not restart after crashes, logout, or reboot.

### Supervised daemon mode on macOS

For actively supervised `looperd` lifecycle management on macOS, use the user LaunchAgent mode:

```bash
looper daemon start --daemon-mode launchd
looper daemon status
looper daemon status --json
looper daemon logs
```

Launchd mode:

- creates a user LaunchAgent plist at `~/Library/LaunchAgents/io.nexu.looper.looperd.plist` unless `daemon.plistPath` is set (any leftover plist at the legacy `com.nexu-io.looper.looperd.plist` path is unloaded and removed automatically on the next `daemon start --daemon-mode launchd`)
- stores launchd stdout/stderr logs under `~/.looper/logs/launchd/`
- stores startup logs under `~/.looper/logs/startup/`
- stores lifecycle state in `~/.looper/looperd.state.json`
- maps `daemon.restartPolicy` to launchd `KeepAlive` behavior
- uses `daemon.restartThrottleSeconds` as the launchd `ThrottleInterval`
- may recover after login/system restart when launchd loads the user agent

Supported restart policies are `never`, `on-failure`, and `always`; the default is `on-failure` with a 10 second throttle. Linux/systemd supervision is not implemented yet; on unsupported platforms, `--daemon-mode launchd` returns an actionable error instead of silently falling back.

Troubleshooting commands:

```bash
looper daemon status
looper daemon status --json
looper daemon logs --startup
```

Status output distinguishes detached mode from launchd-supervised mode and includes PID, start time, supervisor, restart policy, stale/exited state, last error/reason, and log locations.

## Verify the install

In another shell:

```bash
looper status
looper daemon status
```

## Upgrade

Unified upgrade entrypoint:

```bash
looper upgrade
looper upgrade --check
looper upgrade --cli
looper upgrade --daemon
```

Current behavior:

- `looper upgrade --check` shows current/latest CLI and daemon versions
- `looper upgrade` attempts CLI self-upgrade when safe, then upgrades the managed daemon
- `looper upgrade --cli` upgrades the `looper` binary only when the current install looks like a release-binary install
- `looper upgrade --daemon` installs or upgrades the managed daemon binary
- Homebrew and dev / `go install` installs refuse CLI self-upgrade and print the matching manual command instead
- after a daemon upgrade, restart manually with `looper daemon restart`
- each GitHub release also publishes `manifest.json` to Cloudflare R2 at `https://releases.looper.powerformer.com/`
- `looper upgrade --check` reads latest versions from that CDN first (`/channels/stable.json`), then falls back to GitHub Releases metadata
- manifest-gated rollback and channel switching are not implemented yet

CDN object layout:

- `https://releases.looper.powerformer.com/manifest.json` — latest stable pointer
- `https://releases.looper.powerformer.com/channels/stable.json` — same stable pointer
- `https://releases.looper.powerformer.com/channels/beta.json` — latest beta pointer
- `https://releases.looper.powerformer.com/<tag>/manifest.json` — immutable per-release copy

## Compatibility and version policy

- CLI and daemon are published from the same git tag and should normally share the same version
- short-lived version skew is allowed while the HTTP API remains compatible
- management endpoints stay under `/api/v1/*`
- if the daemon is running, the CLI reads its current version from `/api/v1/status`; otherwise it falls back to `looperd --version`
- release builds are tag-driven (`vX.Y.Z` / `vX.Y.Z-rc.N`); local default builds use `0.0.0-dev`

## Uninstall

```bash
curl -fsSL https://raw.githubusercontent.com/nexu-io/looper/main/scripts/uninstall.sh | sh
```

The uninstall script removes the CLI binary, the managed daemon binary, and updater state. It asks before deleting config, the SQLite DB, backups, logs, and worktrees.

## From source

Clone the repo:

```bash
git clone https://github.com/nexu-io/looper.git
cd looper
```

Then build or run the Go binaries:

```bash
go build ./cmd/looper
go build ./cmd/looperd
go run ./cmd/looperd
```

In another shell, run the CLI from source:

```bash
go run ./cmd/looper status
```
