package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nexu-io/looper/internal/forge"
	"github.com/nexu-io/looper/internal/version"
	"github.com/spf13/cobra"
)

const (
	autoUpgradeStateSchemaVersion = 1
	autoUpgradeCheckInterval      = 24 * time.Hour
	autoUpgradeInFlightTimeout    = 30 * time.Minute
	autoUpgradeBusyRetryDelay     = 5 * time.Minute
)

type autoUpgradeState struct {
	SchemaVersion int                       `json:"schemaVersion"`
	LastCheckedAt *time.Time                `json:"lastCheckedAt,omitempty"`
	RetryAfter    *time.Time                `json:"retryAfter,omitempty"`
	InFlight      *autoUpgradeInFlightState `json:"inFlight,omitempty"`
	Ready         *autoUpgradeReadyState    `json:"ready,omitempty"`
}

type autoUpgradeInFlightState struct {
	PID       int        `json:"pid"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
}

type autoUpgradeReadyState struct {
	CLIChanged            bool      `json:"cliChanged,omitempty"`
	CLIVersion            string    `json:"cliVersion,omitempty"`
	DaemonChanged         bool      `json:"daemonChanged,omitempty"`
	DaemonVersion         string    `json:"daemonVersion,omitempty"`
	DaemonRestartRequired bool      `json:"daemonRestartRequired,omitempty"`
	RunningDaemonVersion  string    `json:"runningDaemonVersion,omitempty"`
	CompletedAt           time.Time `json:"completedAt"`
}

type autoUpgradeLockState struct {
	PID        int    `json:"pid"`
	Executable string `json:"executable,omitempty"`
	Command    string `json:"command,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
}

type autoUpgradeLockProcessState struct {
	Executable     string
	Command        string
	ElapsedSeconds int
}

type upgradeCheckSummary struct {
	CLI    upgradeCLISummary    `json:"cli"`
	Daemon upgradeDaemonSummary `json:"daemon"`
}

type upgradeCLISummary struct {
	CurrentVersion  string `json:"currentVersion"`
	LatestVersion   string `json:"latestVersion"`
	UpdateAvailable bool   `json:"updateAvailable"`
}

type upgradeDaemonSummary struct {
	CurrentVersion  *string `json:"currentVersion"`
	LatestVersion   string  `json:"latestVersion"`
	UpdateAvailable bool    `json:"updateAvailable"`
	Installed       bool    `json:"installed"`
	Source          string  `json:"source"`
	BinaryPath      *string `json:"binaryPath"`
}

type latestDaemonReleaseInfo struct {
	Version string `json:"version"`
	Tag     string `json:"tag"`
}

type upgradeDaemonVersionState struct {
	Version    string
	Source     string
	BinaryPath *string
}

type daemonUpgradeOutput struct {
	Changed         bool    `json:"changed"`
	CurrentVersion  *string `json:"currentVersion,omitempty"`
	PreviousVersion *string `json:"previousVersion,omitempty"`
	LatestVersion   string  `json:"latestVersion"`
	BinaryPath      *string `json:"binaryPath,omitempty"`
	InstallPath     *string `json:"installPath,omitempty"`
	DownloadedFrom  *string `json:"downloadedFrom,omitempty"`
	Skipped         *bool   `json:"skipped,omitempty"`
}

type cliUpgradeOutput struct {
	Changed         bool    `json:"changed"`
	CurrentVersion  string  `json:"currentVersion"`
	LatestVersion   string  `json:"latestVersion"`
	BinaryPath      *string `json:"binaryPath,omitempty"`
	PreviousBinary  *string `json:"previousBinary,omitempty"`
	DownloadedFrom  *string `json:"downloadedFrom,omitempty"`
	InstallSource   string  `json:"installSource"`
	Skipped         *bool   `json:"skipped,omitempty"`
	Refused         *bool   `json:"refused,omitempty"`
	RefusedGuidance *string `json:"refusedGuidance,omitempty"`
}

type unifiedUpgradeOutput struct {
	CLI    cliUpgradeOutput    `json:"cli"`
	Daemon daemonUpgradeOutput `json:"daemon"`
}

type cliInstallSource string

const (
	cliInstallSourceRelease  cliInstallSource = "release-binary"
	cliInstallSourceHomebrew cliInstallSource = "homebrew"
	cliInstallSourceDev      cliInstallSource = "dev"
	cliInstallSourceUnknown  cliInstallSource = "unknown"
)

// cliInstallChannelStable is the value of version.Channel for tagged release
// builds; anything else (notably the "dev" default) must be treated as a
// non-upgradable local build.
const cliInstallChannelStable = "stable"

type cliUpgradeRefusedError struct {
	message string
}

type preparedDaemonUpgrade struct {
	output        daemonUpgradeOutput
	managedDaemon *upgradeDaemonVersionState
	pathDaemon    *upgradeDaemonVersionState
	install       *preparedDaemonInstall
}

func (e *cliUpgradeRefusedError) Error() string {
	return e.message
}

func (r *commandRuntime) maybeRunAutoUpgrade(cmd *cobra.Command, args []string) error {
	_ = args
	// Trusted review-submit children are credential-bearing and receive a
	// one-shot config snapshot FD. Auto-upgrade must not run here: it is
	// unrelated to publication and historically consumed the FD before
	// `review submit` could load the daemon-bound snapshot (EBADF).
	if forge.TrustedReviewProxyChildConfigured() {
		return nil
	}
	if shouldSkipAutoUpgrade(cmd) {
		return nil
	}
	execPath, err := r.executablePath()
	if err != nil {
		return nil
	}
	if r.detectCLIInstallSource(execPath) != cliInstallSourceRelease {
		return nil
	}

	loaded, err := r.loadConfig()
	if err != nil {
		return nil
	}
	if !loaded.Config.Package.AutoUpgradeEnabled {
		return nil
	}

	statePath, err := r.resolveAutoUpgradeStatePath()
	if err != nil {
		return nil
	}
	lockPath, err := r.resolveAutoUpgradeLockPath()
	if err != nil {
		return nil
	}
	unlock, acquired, err := r.tryAcquireAutoUpgradeLock(lockPath)
	if err != nil || !acquired {
		return nil
	}
	defer unlock()
	state, err := r.readAutoUpgradeState(statePath)
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade skipped: read state: %v\n", err)
		return nil
	}
	state, changed, err := r.reconcileAutoUpgradeState(cmd.Context(), state)
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade skipped: reconcile state: %v\n", err)
		return nil
	}
	if changed {
		if err := r.writeAutoUpgradeState(statePath, *state); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade state update failed: %v\n", err)
		}
	}
	if state != nil && state.Ready != nil {
		if err := r.writeAutoUpgradeReadyNotice(cmd.ErrOrStderr(), *state.Ready); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade notice failed: %v\n", err)
		}
		if !state.Ready.DaemonRestartRequired {
			state.Ready = nil
			if err := r.writeAutoUpgradeState(statePath, *state); err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade state update failed: %v\n", err)
			}
		} else {
			return nil
		}
	}
	if state != nil && state.InFlight != nil {
		return nil
	}
	now := time.Now().UTC()
	if state != nil && state.RetryAfter != nil {
		if now.Before(*state.RetryAfter) {
			return nil
		}
		state.RetryAfter = nil
		if err := r.writeAutoUpgradeState(statePath, *state); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade state update failed: %v\n", err)
		}
	}
	if !shouldRunAutoUpgradeCheck(state, now) {
		return nil
	}

	state, started, err := r.startBackgroundAutoUpgrade(cmd, state)
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade skipped: start background download: %v\n", err)
		return nil
	}
	if !started {
		return nil
	}
	state, err = r.persistAutoUpgradeInFlightState(statePath, *state)
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade state update failed: %v\n", err)
		return nil
	}
	if state != nil && state.Ready != nil {
		if err := r.writeAutoUpgradeReadyNotice(cmd.ErrOrStderr(), *state.Ready); err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade notice failed: %v\n", err)
		}
		if !state.Ready.DaemonRestartRequired {
			state.Ready = nil
			if err := r.writeAutoUpgradeState(statePath, *state); err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Auto-upgrade state update failed: %v\n", err)
			}
		}
	}
	return nil
}

func shouldSkipAutoUpgrade(cmd *cobra.Command) bool {
	if cmd == nil {
		return true
	}
	if cmd.Name() == "help" {
		return true
	}
	if cmd.RunE != nil {
		if reflect.ValueOf(cmd.RunE).Pointer() == reflect.ValueOf(helpCommand).Pointer() {
			return true
		}
	}
	path := strings.TrimSpace(cmd.CommandPath())
	return path == "looper" || path == "looper upgrade"
}

func newAutoUpgradeCommand(parent *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{}
	if parent != nil {
		cmd.SetContext(parent.Context())
		cmd.SetOut(parent.ErrOrStderr())
		cmd.SetErr(parent.ErrOrStderr())
	}
	return cmd
}

func (r *commandRuntime) startBackgroundAutoUpgrade(cmd *cobra.Command, state *autoUpgradeState) (*autoUpgradeState, bool, error) {
	execPath, err := r.executablePath()
	if err != nil {
		return state, false, err
	}
	if state == nil {
		state = &autoUpgradeState{}
	}
	workingDir, err := r.getwd()
	if err != nil {
		workingDir = ""
	}
	forwardedArgs := normalizeForwardedConfigPathArgs(ExtractConfigArgs(r.argv), workingDir)
	args := append([]string{"upgrade", "--background-auto"}, forwardedArgs...)
	pid, err := r.spawnDetached(execPath, args, workingDir, os.Environ())
	if err != nil {
		return state, false, err
	}
	now := time.Now().UTC()
	state.InFlight = &autoUpgradeInFlightState{PID: pid, StartedAt: &now}
	state.RetryAfter = nil
	state.Ready = nil
	return state, true, nil
}

func (r *commandRuntime) resolveAutoUpgradeStatePath() (string, error) {
	homeDir, err := r.homeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, ".looper", "auto-upgrade.state.json"), nil
}

func (r *commandRuntime) resolveAutoUpgradeLockPath() (string, error) {
	homeDir, err := r.homeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, ".looper", "auto-upgrade.state.lock"), nil
}

func (r *commandRuntime) resolveAutoUpgradeRunLockPath() (string, error) {
	homeDir, err := r.homeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, ".looper", "auto-upgrade.run.lock"), nil
}

func (r *commandRuntime) readAutoUpgradeState(path string) (*autoUpgradeState, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var state autoUpgradeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode auto-upgrade state: %w", err)
	}
	if state.SchemaVersion != 0 && state.SchemaVersion != autoUpgradeStateSchemaVersion {
		return nil, fmt.Errorf("unsupported auto-upgrade state schemaVersion %d", state.SchemaVersion)
	}
	return &state, nil
}

func (r *commandRuntime) writeAutoUpgradeState(path string, state autoUpgradeState) error {
	state.SchemaVersion = autoUpgradeStateSchemaVersion
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (r *commandRuntime) persistAutoUpgradeInFlightState(path string, pending autoUpgradeState) (*autoUpgradeState, error) {
	current, err := r.readAutoUpgradeState(path)
	if err != nil {
		return nil, err
	}
	if current != nil && current.Ready != nil && current.InFlight == nil {
		return current, nil
	}
	if current != nil && current.LastCheckedAt != nil {
		pending.LastCheckedAt = current.LastCheckedAt
	}
	if current != nil && current.RetryAfter != nil {
		pending.RetryAfter = current.RetryAfter
	}
	if err := r.writeAutoUpgradeState(path, pending); err != nil {
		return nil, err
	}
	return &pending, nil
}

func (r *commandRuntime) tryAcquireAutoUpgradeLock(path string) (func(), bool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, false, err
	}
	for {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if !os.IsExist(err) {
				return nil, false, err
			}
			if !r.shouldBreakAutoUpgradeLock(path) {
				return func() {}, false, nil
			}
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return nil, false, err
			}
			continue
		}
		_ = json.NewEncoder(file).Encode(r.currentAutoUpgradeLockState())
		unlock := func() {
			_ = file.Close()
			_ = os.Remove(path)
		}
		return unlock, true, nil
	}
}

func (r *commandRuntime) shouldBreakAutoUpgradeLock(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return time.Since(info.ModTime()) >= autoUpgradeBusyRetryDelay
	}
	lock, ok := parseAutoUpgradeLockState(raw)
	if !ok || lock.PID <= 0 {
		return time.Since(info.ModTime()) >= autoUpgradeBusyRetryDelay
	}
	if !r.isProcessAlive(lock.PID) {
		return true
	}
	process, err := r.readAutoUpgradeLockProcess(context.Background(), lock.PID)
	if err != nil {
		return time.Since(info.ModTime()) >= autoUpgradeBusyRetryDelay
	}
	if !r.isExpectedAutoUpgradeLockProcess(path, process) {
		return true
	}
	if lock.Executable == "" || lock.Command == "" || lock.ObservedAt == "" {
		return false
	}
	observedAt, err := time.Parse(time.RFC3339Nano, lock.ObservedAt)
	if err != nil {
		return time.Since(info.ModTime()) >= autoUpgradeBusyRetryDelay
	}
	if process.ElapsedSeconds < 0 {
		return time.Since(info.ModTime()) >= autoUpgradeBusyRetryDelay
	}
	if process.Executable != lock.Executable || process.Command != lock.Command {
		return true
	}
	earliestPossibleStart := time.Now().UTC().Add(-time.Duration(process.ElapsedSeconds+1) * time.Second)
	return earliestPossibleStart.After(observedAt)
}

func (r *commandRuntime) isExpectedAutoUpgradeLockProcess(path string, process autoUpgradeLockProcessState) bool {
	if process.Executable == "" || process.Command == "" {
		return false
	}
	if process.Executable != r.autoUpgradeLockExecutable() {
		return false
	}
	switch filepath.Base(path) {
	case "auto-upgrade.run.lock":
		for _, token := range splitProcessCommand(process.Command)[1:] {
			if token == "upgrade" {
				return true
			}
		}
		return false
	case "auto-upgrade.state.lock":
		return true
	default:
		return r.isBackgroundAutoUpgradeProcess(process)
	}
}

func parseAutoUpgradeLockState(raw []byte) (autoUpgradeLockState, bool) {
	var lock autoUpgradeLockState
	if err := json.Unmarshal(raw, &lock); err == nil {
		return lock, true
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return autoUpgradeLockState{}, false
	}
	return autoUpgradeLockState{PID: pid}, true
}

func (r *commandRuntime) autoUpgradeLockExecutable() string {
	if base := filepath.Base(strings.TrimSpace(r.app.deps.ExecutablePath)); base != "" && base != "." {
		return base
	}
	return "looper"
}

func (r *commandRuntime) currentAutoUpgradeLockState() autoUpgradeLockState {
	lock := autoUpgradeLockState{PID: os.Getpid(), Executable: r.autoUpgradeLockExecutable(), ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	process, err := r.readAutoUpgradeLockProcess(context.Background(), lock.PID)
	if err != nil {
		return lock
	}
	if process.Executable != "" {
		lock.Executable = process.Executable
	}
	lock.Command = process.Command
	return lock
}

func (r *commandRuntime) readAutoUpgradeLockProcess(ctx context.Context, pid int) (autoUpgradeLockProcessState, error) {
	command, err := r.readProcessCommand(ctx, pid)
	if err != nil {
		return autoUpgradeLockProcessState{}, err
	}
	elapsedSeconds, err := r.readProcessElapsedSeconds(ctx, pid)
	if err != nil {
		return autoUpgradeLockProcessState{}, err
	}
	if command == "" {
		return autoUpgradeLockProcessState{}, nil
	}
	tokens := splitProcessCommand(command)
	if len(tokens) == 0 {
		return autoUpgradeLockProcessState{}, nil
	}
	return autoUpgradeLockProcessState{Executable: filepath.Base(tokens[0]), Command: command, ElapsedSeconds: elapsedSeconds}, nil
}

func (r *commandRuntime) isBackgroundAutoUpgradeProcess(process autoUpgradeLockProcessState) bool {
	if process.Executable == "" || process.Command == "" {
		return false
	}
	if process.Executable != r.autoUpgradeLockExecutable() {
		return false
	}
	tokens := splitProcessCommand(process.Command)
	if len(tokens) < 3 {
		return false
	}
	hasUpgrade := false
	hasBackgroundAuto := false
	for _, token := range tokens[1:] {
		switch token {
		case "upgrade":
			hasUpgrade = true
		case "--background-auto":
			hasBackgroundAuto = true
		}
	}
	return hasUpgrade && hasBackgroundAuto
}

func (r *commandRuntime) readProcessElapsedSeconds(ctx context.Context, pid int) (int, error) {
	result, err := r.runCommand(ctx, "ps", []string{"-p", fmt.Sprintf("%d", pid), "-o", "etime="}, daemonCommandTimeout)
	if err != nil {
		return -1, fmt.Errorf("inspect process %d elapsed time with ps: %w", pid, err)
	}
	if result.ExitCode != 0 {
		return -1, nil
	}
	elapsed, err := parsePSElapsedSeconds(strings.TrimSpace(result.Stdout))
	if err != nil {
		return -1, fmt.Errorf("parse process %d elapsed time %q: %w", pid, strings.TrimSpace(result.Stdout), err)
	}
	return elapsed, nil
}

func parsePSElapsedSeconds(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty elapsed time")
	}
	dayParts := strings.SplitN(raw, "-", 2)
	days := 0
	timePart := raw
	if len(dayParts) == 2 {
		parsedDays, err := strconv.Atoi(dayParts[0])
		if err != nil {
			return 0, err
		}
		days = parsedDays
		timePart = dayParts[1]
	}
	parts := strings.Split(timePart, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("unexpected elapsed time format")
	}
	values := make([]int, len(parts))
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return 0, err
		}
		values[i] = value
	}
	hours := 0
	minutes := values[0]
	seconds := values[1]
	if len(values) == 3 {
		hours = values[0]
		minutes = values[1]
		seconds = values[2]
	}
	return (((days*24)+hours)*60+minutes)*60 + seconds, nil
}

func (r *commandRuntime) reconcileAutoUpgradeState(ctx context.Context, state *autoUpgradeState) (*autoUpgradeState, bool, error) {
	if state == nil {
		return nil, false, nil
	}
	changed := false
	if state.InFlight != nil && r.shouldClearAutoUpgradeInFlight(ctx, state.InFlight) {
		state.InFlight = nil
		changed = true
	}
	if state.Ready == nil {
		return state, changed, nil
	}
	if !state.Ready.DaemonRestartRequired {
		return state, changed, nil
	}
	managedDaemon, err := r.readManagedUpgradeDaemonVersion(ctx)
	if err != nil {
		return state, changed, err
	}
	statusPayload, err := r.currentDaemonStatusPayload(ctx)
	if err != nil {
		return state, changed, err
	}
	pathDaemon, err := r.readPathUpgradeDaemonVersion(ctx)
	if err != nil {
		return state, changed, err
	}
	runningDaemon := selectUpgradeDaemonVersionState(statusPayload, managedDaemon, pathDaemon)
	if managedDaemon == nil || runningDaemon == nil || runningDaemon.Version == managedDaemon.Version {
		state.Ready = nil
		changed = true
		return state, changed, nil
	}
	state.Ready.DaemonVersion = managedDaemon.Version
	state.Ready.RunningDaemonVersion = runningDaemon.Version
	return state, changed, nil
}

func (r *commandRuntime) shouldClearAutoUpgradeInFlight(ctx context.Context, state *autoUpgradeInFlightState) bool {
	if state == nil {
		return false
	}
	if state.PID <= 0 {
		return true
	}
	if !r.isProcessAlive(state.PID) {
		return true
	}
	result, err := r.runCommand(ctx, "ps", []string{"-p", fmt.Sprintf("%d", state.PID), "-o", "command="}, daemonCommandTimeout)
	if err != nil || result.ExitCode != 0 {
		return false
	}
	return !r.isBackgroundAutoUpgradeProcess(autoUpgradeLockProcessState{Executable: r.autoUpgradeLockExecutable(), Command: result.Stdout})
}

func (r *commandRuntime) clearAutoUpgradeReadyState(ctx context.Context) error {
	statePath, err := r.resolveAutoUpgradeStatePath()
	if err != nil {
		return err
	}
	state, err := r.readAutoUpgradeState(statePath)
	if err != nil || state == nil || state.Ready == nil {
		return err
	}
	state, changed, err := r.reconcileAutoUpgradeState(ctx, state)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	return r.writeAutoUpgradeState(statePath, *state)
}

func (r *commandRuntime) writeAutoUpgradeReadyNotice(w io.Writer, ready autoUpgradeReadyState) error {
	if ready.CLIChanged {
		if _, err := fmt.Fprintf(w, "Auto-upgrade ready: looper %s is installed.\n", ready.CLIVersion); err != nil {
			return err
		}
	}
	if ready.DaemonChanged {
		if ready.DaemonRestartRequired {
			if strings.TrimSpace(ready.RunningDaemonVersion) != "" {
				if _, err := fmt.Fprintf(w, "Auto-upgrade ready: looperd %s is installed, but the running daemon is still %s. Restart to activate it:\n", ready.DaemonVersion, ready.RunningDaemonVersion); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(w, "Auto-upgrade ready: looperd %s is installed. Restart to activate it:\n", ready.DaemonVersion); err != nil {
					return err
				}
			}
			_, err := fmt.Fprintln(w, "  looper daemon restart")
			return err
		}
		if _, err := fmt.Fprintf(w, "Auto-upgrade ready: looperd %s is installed.\n", ready.DaemonVersion); err != nil {
			return err
		}
	}
	return nil
}

func shouldRunAutoUpgradeCheck(state *autoUpgradeState, now time.Time) bool {
	if state == nil || state.LastCheckedAt == nil {
		return true
	}
	return now.Sub(*state.LastCheckedAt) >= autoUpgradeCheckInterval
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func (r *commandRuntime) upgrade(cmd *cobra.Command, args []string) error {
	_ = args
	if getBoolFlag(cmd, "background-auto") {
		return r.runBackgroundAutoUpgrade(cmd)
	}

	check := getBoolFlag(cmd, "check")
	cliOnly := getBoolFlag(cmd, "cli")
	daemonOnly := getBoolFlag(cmd, "daemon")

	selectedPaths := 0
	if check {
		selectedPaths++
	}
	if cliOnly {
		selectedPaths++
	}
	if daemonOnly {
		selectedPaths++
	}
	if selectedPaths > 1 {
		return fmt.Errorf("--check, --cli, and --daemon cannot be combined")
	}

	if check {
		summary, err := r.collectUpgradeCheckSummary(cmd.Context())
		if err != nil {
			return err
		}
		if getBoolFlag(cmd, "json") {
			return writeJSON(cmd.OutOrStdout(), summary)
		}
		return writeHumanUpgradeSummary(cmd.OutOrStdout(), summary)
	}
	runLockPath, err := r.resolveAutoUpgradeRunLockPath()
	if err != nil {
		return err
	}
	unlock, acquired, err := r.tryAcquireAutoUpgradeLock(runLockPath)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("another looper upgrade is already running; try again in a moment")
	}
	defer unlock()

	if daemonOnly {
		return r.upgradeDaemon(cmd)
	}

	if cliOnly {
		_, err := r.upgradeCLI(cmd)
		return err
	}

	return r.upgradeUnified(cmd)
}

func (r *commandRuntime) runBackgroundAutoUpgrade(cmd *cobra.Command) error {
	statePath, err := r.resolveAutoUpgradeStatePath()
	if err != nil {
		return err
	}
	state, err := r.readAutoUpgradeState(statePath)
	if err != nil {
		return err
	}
	runLockPath, err := r.resolveAutoUpgradeRunLockPath()
	if err != nil {
		return err
	}
	unlock, acquired, err := r.tryAcquireAutoUpgradeLock(runLockPath)
	if err != nil {
		return err
	}
	if !acquired {
		if state == nil {
			state = &autoUpgradeState{}
		}
		retryAfter := time.Now().UTC().Add(autoUpgradeBusyRetryDelay)
		state.RetryAfter = &retryAfter
		state.InFlight = nil
		return r.writeAutoUpgradeState(statePath, *state)
	}
	defer unlock()
	workerCtx, cancel := context.WithTimeout(cmd.Context(), autoUpgradeInFlightTimeout)
	defer cancel()
	autoCmd := newAutoUpgradeCommand(cmd)
	autoCmd.SetContext(workerCtx)
	ready := r.collectBackgroundAutoUpgradeResult(autoCmd)
	if state == nil {
		state = &autoUpgradeState{}
	}
	if ready == nil && state.Ready != nil && state.Ready.DaemonRestartRequired {
		ready = state.Ready
	}
	now := time.Now().UTC()
	state.LastCheckedAt = &now
	state.RetryAfter = nil
	state.InFlight = nil
	state.Ready = ready
	return r.writeAutoUpgradeState(statePath, *state)
}

func (r *commandRuntime) collectBackgroundAutoUpgradeResult(cmd *cobra.Command) *autoUpgradeReadyState {
	ready := &autoUpgradeReadyState{CompletedAt: time.Now().UTC()}
	cliOutput, cliErr := r.upgradeCLIWithOutput(cmd, false)
	if cliErr == nil && cliOutput.Changed {
		ready.CLIChanged = true
		ready.CLIVersion = cliOutput.LatestVersion
	}

	managedDaemon, managedErr := r.readManagedUpgradeDaemonVersion(cmd.Context())
	if managedErr != nil {
		return autoUpgradeReadyOrNil(ready)
	}
	if managedDaemon == nil {
		return autoUpgradeReadyOrNil(ready)
	}
	statusPayload, statusErr := r.currentDaemonStatusPayload(cmd.Context())
	if statusErr != nil {
		return autoUpgradeReadyOrNil(ready)
	}
	pathDaemon, pathErr := r.readPathUpgradeDaemonVersion(cmd.Context())
	if pathErr != nil {
		return autoUpgradeReadyOrNil(ready)
	}
	runningDaemon := selectUpgradeDaemonVersionState(statusPayload, managedDaemon, pathDaemon)

	daemonOutput, daemonErr := r.upgradeDaemonWithOutput(cmd, false)
	if daemonErr == nil && daemonOutput.Changed {
		ready.DaemonChanged = true
		ready.DaemonVersion = daemonOutput.LatestVersion
		if len(statusPayload) > 0 && runningDaemon != nil && runningDaemon.Source == "installed-binary" && runningDaemon.Version != daemonOutput.LatestVersion {
			ready.DaemonRestartRequired = true
			ready.RunningDaemonVersion = runningDaemon.Version
		}
	}
	return autoUpgradeReadyOrNil(ready)
}

func autoUpgradeReadyOrNil(ready *autoUpgradeReadyState) *autoUpgradeReadyState {
	if ready == nil {
		return nil
	}
	if !ready.CLIChanged && !ready.DaemonChanged {
		return nil
	}
	return ready
}

func (r *commandRuntime) collectUpgradeCheckSummary(ctx context.Context) (upgradeCheckSummary, error) {
	latestCLIVersion, err := r.fetchLatestCLIVersion(ctx)
	if err != nil {
		return upgradeCheckSummary{}, err
	}

	latestDaemonRelease, err := r.fetchLatestDaemonRelease(ctx)
	if err != nil {
		return upgradeCheckSummary{}, err
	}

	statusPayload, err := r.currentDaemonStatusPayload(ctx)
	if err != nil {
		return upgradeCheckSummary{}, err
	}
	managedDaemon, err := r.readManagedUpgradeDaemonVersion(ctx)
	if err != nil {
		return upgradeCheckSummary{}, err
	}
	pathDaemon, err := r.readPathUpgradeDaemonVersion(ctx)
	if err != nil {
		return upgradeCheckSummary{}, err
	}
	currentDaemon := selectUpgradeDaemonVersionState(statusPayload, managedDaemon, pathDaemon)

	summary := upgradeCheckSummary{
		CLI: upgradeCLISummary{
			CurrentVersion: version.Current().Version,
			LatestVersion:  latestCLIVersion,
		},
		Daemon: upgradeDaemonSummary{LatestVersion: latestDaemonRelease.Version, Source: "not-installed"},
	}
	cliUpgradeAvailable, err := isSemverUpgradeAvailable(version.Current().Version, latestCLIVersion)
	if err != nil {
		return upgradeCheckSummary{}, fmt.Errorf("compare CLI versions: %w", err)
	}
	summary.CLI.UpdateAvailable = cliUpgradeAvailable

	if currentDaemon != nil {
		summary.Daemon.CurrentVersion = stringPtr(currentDaemon.Version)
		daemonUpgradeAvailable, err := isSemverUpgradeAvailable(currentDaemon.Version, latestDaemonRelease.Version)
		if err != nil {
			return upgradeCheckSummary{}, fmt.Errorf("compare daemon versions: %w", err)
		}
		summary.Daemon.UpdateAvailable = daemonUpgradeAvailable
		summary.Daemon.Installed = currentDaemon.Source == "installed-binary"
		summary.Daemon.Source = currentDaemon.Source
		summary.Daemon.BinaryPath = currentDaemon.BinaryPath
	} else {
		summary.Daemon.UpdateAvailable = true
	}

	return summary, nil
}

func (r *commandRuntime) upgradeDaemon(cmd *cobra.Command) error {
	_, err := r.upgradeDaemonWithOutput(cmd, true)
	return err
}

func (r *commandRuntime) upgradeDaemonWithOutput(cmd *cobra.Command, emitOutput bool) (daemonUpgradeOutput, error) {
	prepared, err := r.prepareDaemonUpgrade(cmd)
	if err != nil {
		return daemonUpgradeOutput{}, err
	}
	return r.finishPreparedDaemonUpgrade(cmd, prepared, emitOutput)
}

func (r *commandRuntime) prepareDaemonUpgrade(cmd *cobra.Command) (preparedDaemonUpgrade, error) {
	ctx := cmd.Context()
	statusPayload, err := r.currentDaemonStatusPayload(ctx)
	if err != nil {
		return preparedDaemonUpgrade{}, err
	}
	managedDaemon, err := r.readManagedUpgradeDaemonVersion(ctx)
	if err != nil {
		return preparedDaemonUpgrade{}, err
	}
	pathDaemon, err := r.readPathUpgradeDaemonVersion(ctx)
	if err != nil {
		return preparedDaemonUpgrade{}, err
	}
	current := selectUpgradeDaemonVersionState(statusPayload, managedDaemon, pathDaemon)
	latestRelease, err := r.fetchLatestDaemonRelease(ctx)
	if err != nil {
		return preparedDaemonUpgrade{}, err
	}

	var currentVersion *string
	if managedDaemon != nil {
		currentVersion = stringPtr(managedDaemon.Version)
	} else if current != nil {
		currentVersion = stringPtr(current.Version)
	}

	needsInstall := managedDaemon == nil
	needsUpgrade := needsInstall || currentVersion == nil
	if !needsUpgrade {
		available, err := isSemverUpgradeAvailable(*currentVersion, latestRelease.Version)
		if err != nil {
			return preparedDaemonUpgrade{}, fmt.Errorf("compare daemon versions: %w", err)
		}
		needsUpgrade = available
	}
	if !needsUpgrade {
		output := daemonUpgradeOutput{
			Changed:        false,
			CurrentVersion: currentVersion,
			LatestVersion:  latestRelease.Version,
		}
		if managedDaemon != nil {
			output.BinaryPath = managedDaemon.BinaryPath
		} else if current != nil {
			output.BinaryPath = current.BinaryPath
		}
		return preparedDaemonUpgrade{output: output, managedDaemon: managedDaemon, pathDaemon: pathDaemon}, nil
	}

	result, err := r.prepareManagedDaemonInstall(ctx, true, latestRelease.Tag, cmd.ErrOrStderr())
	if err != nil {
		return preparedDaemonUpgrade{}, fmt.Errorf("Failed to upgrade looperd: %w", err)
	}

	output := daemonUpgradeOutput{
		Changed:         true,
		PreviousVersion: daemonVersionPointer(current),
		LatestVersion:   latestRelease.Version,
		InstallPath:     stringPtr(result.result.InstallPath),
		DownloadedFrom:  result.result.DownloadedFrom,
		Skipped:         boolPtr(result.result.Skipped),
	}
	return preparedDaemonUpgrade{output: output, managedDaemon: managedDaemon, pathDaemon: pathDaemon, install: &result}, nil
}

func (r *commandRuntime) finishPreparedDaemonUpgrade(cmd *cobra.Command, prepared preparedDaemonUpgrade, emitOutput bool) (daemonUpgradeOutput, error) {
	if prepared.install != nil {
		if err := commitPreparedDaemonInstall(*prepared.install); err != nil {
			return daemonUpgradeOutput{}, fmt.Errorf("Failed to upgrade looperd: %w", err)
		}
	}
	output := prepared.output
	if emitOutput && getBoolFlag(cmd, "json") {
		return output, writeJSON(cmd.OutOrStdout(), output)
	}
	if !emitOutput {
		return output, nil
	}
	if !output.Changed {
		if output.CurrentVersion != nil {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "looperd is already up to date (%s)\n", *output.CurrentVersion); err != nil {
				return daemonUpgradeOutput{}, err
			}
		}
		if output.BinaryPath != nil {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Managed binary: %s\n", *output.BinaryPath)
			return output, err
		}
		return output, nil
	}
	if prepared.managedDaemon == nil && prepared.pathDaemon != nil {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Installed managed looperd %s to %s (previously using %s)\n", output.LatestVersion, *output.InstallPath, *prepared.pathDaemon.BinaryPath); err != nil {
			return daemonUpgradeOutput{}, err
		}
	} else if prepared.managedDaemon == nil {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Installed looperd %s to %s\n", output.LatestVersion, *output.InstallPath); err != nil {
			return daemonUpgradeOutput{}, err
		}
	} else {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Upgraded looperd %s → %s at %s\n", prepared.managedDaemon.Version, output.LatestVersion, *output.InstallPath); err != nil {
			return daemonUpgradeOutput{}, err
		}
	}
	if _, err := fmt.Fprintln(cmd.OutOrStdout(), "Restart the daemon to use the new version:"); err != nil {
		return daemonUpgradeOutput{}, err
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), "  looper daemon restart")
	return output, err
}

func (r *commandRuntime) fetchLatestCLIVersion(ctx context.Context) (string, error) {
	release, err := r.fetchReleaseMetadata(ctx, "")
	if err != nil {
		return "", err
	}

	versionText := normalizeVersion(release.TagName)
	if versionText == "" {
		return "", fmt.Errorf("latest looper release metadata is missing tag_name")
	}

	return versionText, nil
}

func (r *commandRuntime) fetchLatestDaemonRelease(ctx context.Context) (latestDaemonReleaseInfo, error) {
	release, err := r.fetchReleaseMetadata(ctx, "")
	if err != nil {
		return latestDaemonReleaseInfo{}, err
	}

	versionText := normalizeVersion(release.TagName)
	if versionText == "" {
		return latestDaemonReleaseInfo{}, fmt.Errorf("latest looperd release metadata is missing tag_name")
	}

	tag := strings.TrimSpace(release.TagName)
	if tag == "" {
		tag = "v" + versionText
	}

	return latestDaemonReleaseInfo{Version: versionText, Tag: tag}, nil
}

func (r *commandRuntime) currentDaemonStatusPayload(ctx context.Context) (json.RawMessage, error) {
	client, err := r.apiClient()
	if err != nil {
		return nil, err
	}
	statusPayload, err := r.getJSONWithClient(ctx, client, "/api/v1/status")
	if err != nil {
		var apiError *DaemonAPIError
		if strings.Contains(err.Error(), "looperd is not reachable") || errors.As(err, &apiError) {
			return nil, nil
		}
		return nil, err
	}
	return statusPayload, nil
}

func (r *commandRuntime) readManagedUpgradeDaemonVersion(ctx context.Context) (*upgradeDaemonVersionState, error) {
	binaryPath, err := r.managedDaemonBinaryPath()
	if err != nil {
		return nil, err
	}
	return r.readUpgradeDaemonVersion(ctx, binaryPath, "installed-binary")
}

func (r *commandRuntime) readPathUpgradeDaemonVersion(ctx context.Context) (*upgradeDaemonVersionState, error) {
	return r.readUpgradeDaemonVersion(ctx, looperdBinaryName, "path-binary")
}

func (r *commandRuntime) readUpgradeDaemonVersion(ctx context.Context, command string, source string) (*upgradeDaemonVersionState, error) {
	versionText, err := r.runVersionCommand(ctx, command)
	if err != nil {
		return nil, err
	}
	if versionText == "" {
		return nil, nil
	}
	return &upgradeDaemonVersionState{Version: versionText, Source: source, BinaryPath: stringPtr(command)}, nil
}

func normalizeVersion(value string) string {
	return strings.TrimPrefix(strings.TrimSpace(value), "v")
}

func (r *commandRuntime) upgradeUnified(cmd *cobra.Command) error {
	jsonOutput := getBoolFlag(cmd, "json")

	// In JSON mode, both upgrade lanes write a single combined JSON document
	// at the end and individual progress is silenced apart from stderr. We
	// can run them concurrently with a shared muxed stderr to overlap the
	// large binary downloads.
	if jsonOutput {
		return r.upgradeUnifiedConcurrent(cmd)
	}

	// In human mode we still parallelize the two downloads, but we buffer
	// per-lane stdout so the two lanes do not interleave their progress and
	// status messages on the user's terminal.
	return r.upgradeUnifiedConcurrent(cmd)
}

// upgradeUnifiedConcurrent runs the CLI and daemon upgrade lanes concurrently.
// Each lane writes its human-readable status output into its own buffer so
// the two lanes do not interleave on stdout. Progress (stderr) goes through a
// shared concurrent multiplexer that rewrites carriage returns into newlines
// so simultaneous "Downloading X" updates stay readable on TTYs and CI logs
// alike.
func (r *commandRuntime) upgradeUnifiedConcurrent(cmd *cobra.Command) error {
	jsonOutput := getBoolFlag(cmd, "json")

	cliBuf := &bytes.Buffer{}
	daemonBuf := &bytes.Buffer{}

	progressMux := newConcurrentProgressMux(cmd.ErrOrStderr())
	progressClosed := false
	defer func() {
		if !progressClosed {
			progressMux.close()
		}
	}()

	cliCmd := mirrorCommandWithIO(cmd, cliBuf, progressMux)
	daemonCmd := mirrorCommandWithIO(cmd, daemonBuf, progressMux)

	type cliResult struct {
		output cliUpgradeOutput
		err    error
	}
	type daemonResult struct {
		prepared preparedDaemonUpgrade
		err      error
	}

	cliCh := make(chan cliResult, 1)
	daemonCh := make(chan daemonResult, 1)

	go func() {
		out, err := r.upgradeCLIWithOutput(cliCmd, !jsonOutput)
		cliCh <- cliResult{output: out, err: err}
	}()
	go func() {
		prepared, err := r.prepareDaemonUpgrade(daemonCmd)
		daemonCh <- daemonResult{prepared: prepared, err: err}
	}()

	cliRes := <-cliCh
	daemonRes := <-daemonCh
	progressMux.close()
	progressClosed = true

	// Surface CLI lane output, accepting refusal as a non-fatal outcome since
	// non-release installs (Homebrew, dev builds, ...) deliberately reject
	// self-upgrade with guidance instead of failing the whole flow.
	if cliRes.err != nil {
		var refused *cliUpgradeRefusedError
		if !errors.As(cliRes.err, &refused) {
			if _, copyErr := cmd.OutOrStdout().Write(cliBuf.Bytes()); copyErr != nil {
				return copyErr
			}
			return cliRes.err
		}
		if !jsonOutput {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "CLI self-upgrade skipped: %s\n", refused.message); err != nil {
				return err
			}
		}
	} else if !jsonOutput {
		if _, err := cmd.OutOrStdout().Write(cliBuf.Bytes()); err != nil {
			return err
		}
	}

	if daemonRes.err != nil {
		if _, copyErr := cmd.OutOrStdout().Write(daemonBuf.Bytes()); copyErr != nil {
			return copyErr
		}
		return daemonRes.err
	}
	daemonOutput, err := r.finishPreparedDaemonUpgrade(daemonCmd, daemonRes.prepared, !jsonOutput)
	if err != nil {
		if _, copyErr := cmd.OutOrStdout().Write(daemonBuf.Bytes()); copyErr != nil {
			return copyErr
		}
		return err
	}
	if !jsonOutput {
		if _, err := cmd.OutOrStdout().Write(daemonBuf.Bytes()); err != nil {
			return err
		}
	}

	if jsonOutput {
		return writeJSON(cmd.OutOrStdout(), unifiedUpgradeOutput{CLI: cliRes.output, Daemon: daemonOutput})
	}
	return nil
}

// mirrorCommandWithIO returns a cobra.Command that delegates context and flag
// lookup to parent but redirects stdout/stderr to dedicated writers. This is
// used by upgradeUnifiedConcurrent so each lane can be ordered after both
// complete without touching the parent command's writers mid-flight.
func mirrorCommandWithIO(parent *cobra.Command, stdout io.Writer, stderr io.Writer) *cobra.Command {
	clone := &cobra.Command{}
	if parent != nil {
		clone.SetContext(parent.Context())
		// Inherit the parent's flag set so getBoolFlag continues to find
		// --json, --check, --cli, --daemon, etc.
		if flags := parent.Flags(); flags != nil {
			clone.Flags().AddFlagSet(flags)
		}
	}
	if stdout != nil {
		clone.SetOut(stdout)
	}
	if stderr != nil {
		clone.SetErr(stderr)
	}
	return clone
}

func (r *commandRuntime) upgradeCLI(cmd *cobra.Command) (cliUpgradeOutput, error) {
	return r.upgradeCLIWithOutput(cmd, true)
}

func (r *commandRuntime) upgradeCLIWithOutput(cmd *cobra.Command, emitOutput bool) (cliUpgradeOutput, error) {
	ctx := cmd.Context()
	execPath, err := r.executablePath()
	if err != nil {
		return cliUpgradeOutput{}, fmt.Errorf("resolve current looper path: %w", err)
	}
	installSource := r.detectCLIInstallSource(execPath)
	guidance := cliRefusalGuidance(installSource, execPath)
	if installSource != cliInstallSourceRelease {
		refused := true
		result := cliUpgradeOutput{Changed: false, CurrentVersion: version.Current().Version, BinaryPath: stringPtr(execPath), InstallSource: string(installSource), Refused: &refused, RefusedGuidance: &guidance}
		if emitOutput && getBoolFlag(cmd, "json") {
			if err := writeJSON(cmd.OutOrStdout(), result); err != nil {
				return cliUpgradeOutput{}, err
			}
		}
		return result, &cliUpgradeRefusedError{message: guidance}
	}
	latestRelease, err := r.fetchReleaseMetadata(ctx, "")
	if err != nil {
		return cliUpgradeOutput{}, err
	}
	latestVersion := normalizeVersion(latestRelease.TagName)
	if latestVersion == "" {
		return cliUpgradeOutput{}, fmt.Errorf("latest looper release metadata is missing tag_name")
	}

	available, err := isSemverUpgradeAvailable(version.Current().Version, latestVersion)
	if err != nil {
		return cliUpgradeOutput{}, fmt.Errorf("compare CLI versions: %w", err)
	}
	if !available {
		skipped := true
		result := cliUpgradeOutput{Changed: false, CurrentVersion: version.Current().Version, LatestVersion: latestVersion, BinaryPath: stringPtr(execPath), InstallSource: string(installSource), Skipped: &skipped}
		if emitOutput && getBoolFlag(cmd, "json") {
			if err := writeJSON(cmd.OutOrStdout(), result); err != nil {
				return cliUpgradeOutput{}, err
			}
		}
		if emitOutput && !getBoolFlag(cmd, "json") {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "looper is already up to date (%s)\n", version.Current().Version); err != nil {
				return cliUpgradeOutput{}, err
			}
		}
		return result, nil
	}
	if err := preflightSelfUpgradeReplace(execPath); err != nil {
		refused := true
		guidance := cliSelfUpgradeWriteGuidance(execPath, err)
		result := cliUpgradeOutput{Changed: false, CurrentVersion: version.Current().Version, LatestVersion: latestVersion, BinaryPath: stringPtr(execPath), InstallSource: string(installSource), Refused: &refused, RefusedGuidance: &guidance}
		if emitOutput && getBoolFlag(cmd, "json") {
			if err := writeJSON(cmd.OutOrStdout(), result); err != nil {
				return cliUpgradeOutput{}, err
			}
		}
		return result, &cliUpgradeRefusedError{message: guidance}
	}

	target, err := resolveLooperTarget(r.platform(), r.arch())
	if err != nil {
		return cliUpgradeOutput{}, err
	}
	binaryName := "looper-" + target
	asset, err := findReleaseAssetSet(latestRelease, binaryName)
	if err != nil {
		latestRelease, err = r.fetchReleaseMetadataMatching(ctx, latestRelease.TagName, func(payload githubReleasePayload) error {
			_, findErr := findReleaseAssetSet(payload, binaryName)
			return findErr
		})
		if err != nil {
			return cliUpgradeOutput{}, fmt.Errorf("looper release: %w", err)
		}
		asset, err = findReleaseAssetSet(latestRelease, binaryName)
	}
	if err != nil {
		return cliUpgradeOutput{}, fmt.Errorf("looper release: %w", err)
	}
	binaryBytes, err := r.fetchAndExtractBinary(ctx, asset, cmd.ErrOrStderr())
	if err != nil {
		return cliUpgradeOutput{}, fmt.Errorf("failed to fetch looper release: %w", err)
	}
	if err := replaceBinaryAtomically(execPath, binaryBytes); err != nil {
		return cliUpgradeOutput{}, err
	}

	prevPath := execPath + ".prev"
	result := cliUpgradeOutput{Changed: true, CurrentVersion: version.Current().Version, LatestVersion: latestVersion, BinaryPath: stringPtr(execPath), PreviousBinary: stringPtr(prevPath), DownloadedFrom: stringPtr(asset.PreferredURL), InstallSource: string(installSource)}
	if emitOutput && getBoolFlag(cmd, "json") {
		if err := writeJSON(cmd.OutOrStdout(), result); err != nil {
			return cliUpgradeOutput{}, err
		}
		return result, nil
	}
	if !emitOutput {
		return result, nil
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Upgraded looper %s → %s at %s\n", version.Current().Version, latestVersion, execPath); err != nil {
		return cliUpgradeOutput{}, err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Previous binary kept at %s\n", prevPath); err != nil {
		return cliUpgradeOutput{}, err
	}
	return result, nil
}

func (r *commandRuntime) cliChannel() string {
	if c := strings.TrimSpace(r.app.deps.CLIChannel); c != "" {
		return c
	}
	return version.Channel
}

func (r *commandRuntime) detectCLIInstallSource(execPath string) cliInstallSource {
	return detectCLIInstallSourceForChannel(execPath, r.cliChannel())
}

// detectCLIInstallSourceForChannel returns the install source for a CLI
// binary, short-circuiting to dev whenever the binary itself reports a
// non-stable channel. Without this guard a locally-built `go build -o
// ~/.local/bin/looper` — the same path the public installer uses — is
// classified by path alone as release-binary and auto-upgrade silently
// replaces it with the latest release, which is a downgrade for any source
// build that is ahead of the most recent tag.
func detectCLIInstallSourceForChannel(execPath, channel string) cliInstallSource {
	if channel != cliInstallChannelStable {
		return cliInstallSourceDev
	}
	path := strings.ToLower(execPath)
	if strings.Contains(path, "/cellar/") || strings.Contains(path, "/homebrew/") {
		return cliInstallSourceHomebrew
	}
	base := strings.ToLower(filepath.Base(path))
	if base != "looper" {
		return cliInstallSourceUnknown
	}
	if strings.HasSuffix(path, "/.local/bin/looper") || strings.HasSuffix(path, "/usr/local/bin/looper") || strings.HasSuffix(path, "/.looper/bin/looper") {
		return cliInstallSourceRelease
	}
	if strings.Contains(path, "/go/bin/") || strings.Contains(path, "/go-build/") {
		return cliInstallSourceDev
	}
	if isInstallerSelectedUserBinPath(execPath) {
		return cliInstallSourceRelease
	}
	if strings.Contains(path, "/tmp/") {
		return cliInstallSourceDev
	}
	return cliInstallSourceUnknown
}

func isInstallerSelectedUserBinPath(execPath string) bool {
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		return false
	}
	execDir := filepath.Clean(filepath.Dir(execPath))
	homeDir = filepath.Clean(homeDir)
	if execDir == homeDir || !strings.HasPrefix(execDir, homeDir+string(os.PathSeparator)) {
		return false
	}

	for _, entry := range filepath.SplitList(os.Getenv("PATH")) {
		if filepath.Clean(entry) == execDir {
			return true
		}
	}
	return false
}

func (r *commandRuntime) executablePath() (string, error) {
	resolve := func(path string) string {
		resolvedPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			return path
		}
		resolvedPath = strings.TrimSpace(resolvedPath)
		if resolvedPath == "" {
			return path
		}
		return resolvedPath
	}

	if value := strings.TrimSpace(r.app.deps.ExecutablePath); value != "" {
		return resolve(value), nil
	}
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return path, nil
	}
	return resolve(path), nil
}

func cliRefusalGuidance(source cliInstallSource, execPath string) string {
	switch source {
	case cliInstallSourceHomebrew:
		return "this looper binary looks Homebrew-managed. Upgrade with `brew upgrade looper` (or your tap formula)."
	case cliInstallSourceDev:
		return "this looper binary looks like a dev/go-install build. Reinstall with `go install ./cmd/looper` (or rebuild locally)."
	default:
		return fmt.Sprintf("cannot safely self-upgrade looper from %q. Reinstall from a release binary or use your package manager.", execPath)
	}
}

func cliSelfUpgradeWriteGuidance(execPath string, err error) string {
	return fmt.Sprintf("cannot self-upgrade looper at %q because the install location is not writable: %v. Reinstall looper into a user-writable directory or use your package manager for this installation.", execPath, err)
}

func preflightSelfUpgradeReplace(installPath string) error {
	installDir := filepath.Dir(installPath)
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return fmt.Errorf("prepare install directory %q: %w", installDir, err)
	}
	tempFile, err := os.CreateTemp(installDir, ".looper-upgrade-check-*")
	if err != nil {
		return fmt.Errorf("create test file in %q: %w", installDir, err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = removeTempInstallFile(tempPath)
		return fmt.Errorf("close test file in %q: %w", installDir, err)
	}
	renamedPath := tempPath + ".rename"
	_ = os.Remove(renamedPath)
	if err := os.Rename(tempPath, renamedPath); err != nil {
		_ = removeTempInstallFile(tempPath)
		return fmt.Errorf("rename test file in %q: %w", installDir, err)
	}
	if err := removeTempInstallFile(renamedPath); err != nil {
		return fmt.Errorf("remove test file in %q: %w", installDir, err)
	}
	return nil
}

func resolveLooperTarget(platform string, arch string) (string, error) {
	if platform == "darwin" && arch == "arm64" {
		return "darwin-arm64", nil
	}
	if platform == "linux" && arch == "amd64" {
		return "linux-amd64", nil
	}
	return "", fmt.Errorf("unsupported platform/arch for looper upgrade: %s-%s. Supported targets: darwin-arm64, linux-amd64", platform, arch)
}

func replaceBinaryAtomically(installPath string, binaryBytes []byte) error {
	return replaceBinaryAtomicallyWithRename(installPath, binaryBytes, os.Rename)
}

func replaceBinaryAtomicallyWithRename(installPath string, binaryBytes []byte, rename func(string, string) error) error {
	installDir := filepath.Dir(installPath)
	tempInstallPath := installPath + ".new"
	prevPath := installPath + ".prev"
	previousPreserved := false
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return fmt.Errorf("create install directory: %w", err)
	}
	if err := os.WriteFile(tempInstallPath, binaryBytes, 0o755); err != nil {
		_ = removeTempInstallFile(tempInstallPath)
		return fmt.Errorf("write staged binary: %w", err)
	}
	if err := os.Chmod(tempInstallPath, 0o755); err != nil {
		_ = removeTempInstallFile(tempInstallPath)
		return fmt.Errorf("chmod staged binary: %w", err)
	}
	if _, err := os.Stat(installPath); err == nil {
		_ = os.Remove(prevPath)
		if err := rename(installPath, prevPath); err != nil {
			_ = removeTempInstallFile(tempInstallPath)
			return fmt.Errorf("preserve previous looper binary: %w", err)
		}
		previousPreserved = true
	}
	if err := rename(tempInstallPath, installPath); err != nil {
		_ = removeTempInstallFile(tempInstallPath)
		if previousPreserved {
			if restoreErr := rename(prevPath, installPath); restoreErr != nil {
				return fmt.Errorf("replace looper binary: %w (failed to restore previous binary from %s: %v)", err, prevPath, restoreErr)
			}
		}
		return fmt.Errorf("replace looper binary: %w", err)
	}
	return nil
}

func writeHumanUpgradeSummary(w io.Writer, summary upgradeCheckSummary) error {
	daemonCurrent := any("not installed")
	if summary.Daemon.CurrentVersion != nil {
		daemonCurrent = *summary.Daemon.CurrentVersion
	}
	daemonBinaryPath := any("-")
	if summary.Daemon.BinaryPath != nil {
		daemonBinaryPath = *summary.Daemon.BinaryPath
	}
	printSection(w, "Upgrade check", [][2]any{{"cliCurrent", summary.CLI.CurrentVersion}, {"cliLatest", summary.CLI.LatestVersion}, {"cliUpdateAvailable", summary.CLI.UpdateAvailable}, {"daemonCurrent", daemonCurrent}, {"daemonLatest", summary.Daemon.LatestVersion}, {"daemonUpdateAvailable", summary.Daemon.UpdateAvailable}, {"daemonSource", summary.Daemon.Source}, {"daemonBinaryPath", daemonBinaryPath}})
	return nil
}

func selectUpgradeDaemonVersionState(statusPayload json.RawMessage, managedDaemon *upgradeDaemonVersionState, pathDaemon *upgradeDaemonVersionState) *upgradeDaemonVersionState {
	serviceBinary := extractDaemonServiceBinary(statusPayload)
	if serviceBinary.Version != "" {
		state := &upgradeDaemonVersionState{Version: serviceBinary.Version, Source: "api"}
		if serviceBinary.Path != "" {
			state.BinaryPath = stringPtr(serviceBinary.Path)
			if managedDaemon != nil && managedDaemon.BinaryPath != nil && serviceBinary.Path == *managedDaemon.BinaryPath {
				state.Source = managedDaemon.Source
			} else if pathDaemon != nil && pathDaemon.BinaryPath != nil && serviceBinary.Path == *pathDaemon.BinaryPath {
				state.Source = pathDaemon.Source
			}
		}
		return state
	}
	if managedDaemon != nil {
		return managedDaemon
	}
	return pathDaemon
}

func daemonVersionPointer(state *upgradeDaemonVersionState) *string {
	if state == nil {
		return nil
	}
	return stringPtr(state.Version)
}

func boolPtr(value bool) *bool {
	return &value
}
