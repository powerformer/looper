package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/release"
)

func TestResolveLooperdTarget(t *testing.T) {
	t.Parallel()

	target, err := resolveLooperdTarget("darwin", "arm64")
	if err != nil {
		t.Fatalf("resolveLooperdTarget(darwin, arm64) error = %v", err)
	}
	if target != "darwin-arm64" {
		t.Fatalf("resolveLooperdTarget(darwin, arm64) = %q, want %q", target, "darwin-arm64")
	}

	target, err = resolveLooperdTarget("linux", "amd64")
	if err != nil {
		t.Fatalf("resolveLooperdTarget(linux, amd64) error = %v", err)
	}
	if target != "linux-amd64" {
		t.Fatalf("resolveLooperdTarget(linux, amd64) = %q, want %q", target, "linux-amd64")
	}

	for _, arch := range []string{"amd64", "x64"} {
		_, err = resolveLooperdTarget("darwin", arch)
		if err == nil {
			t.Fatalf("resolveLooperdTarget(darwin, %s) error = nil, want unsupported", arch)
		}
	}
}

func TestInstallManagedDaemonInstallsBinary(t *testing.T) {
	t.Parallel()

	homeDir := t.TempDir()
	binary := []byte{1, 2, 3, 4}
	checksum := sha256.Sum256(binary)
	checksumText := hex.EncodeToString(checksum[:]) + "  looperd-darwin-arm64\n"

	app := New(Deps{
		HTTPClient: newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case "https://api.github.com/repos/nexu-io/looper/releases/latest":
				return jsonResponse(t, http.StatusOK, `{"assets":[{"name":"looperd-darwin-arm64","browser_download_url":"https://example.invalid/looperd-darwin-arm64"},{"name":"looperd-darwin-arm64.sha256","browser_download_url":"https://example.invalid/looperd-darwin-arm64.sha256"}]}`), nil
			case "https://example.invalid/looperd-darwin-arm64":
				return binaryResponse(t, http.StatusOK, binary), nil
			case "https://example.invalid/looperd-darwin-arm64.sha256":
				return textResponse(t, http.StatusOK, checksumText), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		}),
		HomeDir:  homeDir,
		Platform: "darwin",
		Arch:     "arm64",
	})

	runtime := newCommandRuntime(app, nil)
	result, err := runtime.installManagedDaemon(context.Background(), false, "", nil)
	if err != nil {
		t.Fatalf("installManagedDaemon() error = %v", err)
	}

	installPath := filepath.Join(homeDir, ".looper", "bin", "looperd")
	if result.Target != "darwin-arm64" {
		t.Fatalf("result.Target = %q, want %q", result.Target, "darwin-arm64")
	}
	if result.InstallPath != installPath {
		t.Fatalf("result.InstallPath = %q, want %q", result.InstallPath, installPath)
	}
	if result.DownloadedFrom == nil || *result.DownloadedFrom != "https://example.invalid/looperd-darwin-arm64" {
		t.Fatalf("result.DownloadedFrom = %#v, want asset URL", result.DownloadedFrom)
	}
	if result.Skipped {
		t.Fatalf("result.Skipped = true, want false")
	}

	installedBytes, err := os.ReadFile(installPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", installPath, err)
	}
	if !bytes.Equal(installedBytes, binary) {
		t.Fatalf("installed bytes = %v, want %v", installedBytes, binary)
	}
	if _, err := os.Stat(installPath + ".new"); !os.IsNotExist(err) {
		t.Fatalf("temp install file exists unexpectedly: %v", err)
	}
}

func TestInstallManagedDaemonSkipsExistingBinary(t *testing.T) {
	t.Parallel()

	homeDir := t.TempDir()
	installPath := filepath.Join(homeDir, ".looper", "bin", "looperd")
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(installPath, []byte("existing"), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	app := New(Deps{
		HTTPClient: newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("unexpected network request %q", req.URL.String())
			return nil, nil
		}),
		HomeDir:  homeDir,
		Platform: "darwin",
		Arch:     "arm64",
		RunCommand: func(ctx context.Context, command string, args []string, timeout time.Duration) (commandExecutionResult, error) {
			_ = ctx
			_ = timeout
			if command == installPath && strings.Join(args, " ") == "--version" {
				return commandExecutionResult{ExitCode: 0, Stdout: "1.2.3\n"}, nil
			}
			return commandExecutionResult{ExitCode: 1, Stderr: "not found"}, nil
		},
	})

	runtime := newCommandRuntime(app, nil)
	result, err := runtime.installManagedDaemon(context.Background(), false, "", nil)
	if err != nil {
		t.Fatalf("installManagedDaemon() error = %v", err)
	}
	if !result.Skipped {
		t.Fatalf("result.Skipped = false, want true")
	}
	if result.DownloadedFrom != nil {
		t.Fatalf("result.DownloadedFrom = %#v, want nil", result.DownloadedFrom)
	}
	if result.Target != "darwin-arm64" {
		t.Fatalf("result.Target = %q, want %q", result.Target, "darwin-arm64")
	}
}

func TestInstallManagedDaemonReportsNonExecutableExistingBinary(t *testing.T) {
	t.Parallel()

	homeDir := t.TempDir()
	installPath := filepath.Join(homeDir, ".looper", "bin", "looperd")
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(installPath, []byte("existing"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	app := New(Deps{HomeDir: homeDir, Platform: "darwin", Arch: "arm64"})
	runtime := newCommandRuntime(app, nil)
	_, err := runtime.installManagedDaemon(context.Background(), false, "", nil)
	if err == nil {
		t.Fatalf("installManagedDaemon() error = nil, want permission error")
	}
	for _, want := range []string{"not executable", "chmod +x " + installPath, "looper daemon install --force"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want to contain %q", err.Error(), want)
		}
	}
}

func TestInstallManagedDaemonReportsInvalidExistingBinary(t *testing.T) {
	t.Parallel()

	homeDir := t.TempDir()
	installPath := filepath.Join(homeDir, ".looper", "bin", "looperd")
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(installPath, []byte("corrupt"), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	app := New(Deps{
		HomeDir:  homeDir,
		Platform: "darwin",
		Arch:     "arm64",
		RunCommand: func(ctx context.Context, command string, args []string, timeout time.Duration) (commandExecutionResult, error) {
			_ = ctx
			_ = timeout
			if command == installPath && strings.Join(args, " ") == "--version" {
				return commandExecutionResult{ExitCode: 1, Stderr: "exec format error"}, nil
			}
			return commandExecutionResult{ExitCode: 1, Stderr: "not found"}, nil
		},
	})
	runtime := newCommandRuntime(app, nil)
	_, err := runtime.installManagedDaemon(context.Background(), false, "", nil)
	if err == nil {
		t.Fatalf("installManagedDaemon() error = nil, want invalid binary error")
	}
	for _, want := range []string{"version check failed", "exec format error", "looper daemon install --force"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want to contain %q", err.Error(), want)
		}
	}
}

func TestDaemonInstallCommandPrintsHumanOutput(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	binary := []byte{9, 8, 7}
	checksum := sha256.Sum256(binary)
	checksumText := hex.EncodeToString(checksum[:]) + "  looperd-darwin-arm64\n"

	app := New(Deps{
		Stdout: stdout,
		Stderr: stderr,
		HTTPClient: newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case "https://api.github.com/repos/nexu-io/looper/releases/latest":
				return jsonResponse(t, http.StatusOK, `{"assets":[{"name":"looperd-darwin-arm64","browser_download_url":"https://example.invalid/looperd-darwin-arm64"},{"name":"looperd-darwin-arm64.sha256","browser_download_url":"https://example.invalid/looperd-darwin-arm64.sha256"}]}`), nil
			case "https://example.invalid/looperd-darwin-arm64":
				return binaryResponse(t, http.StatusOK, binary), nil
			case "https://example.invalid/looperd-darwin-arm64.sha256":
				return textResponse(t, http.StatusOK, checksumText), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		}),
		HomeDir:  t.TempDir(),
		Platform: "darwin",
		Arch:     "arm64",
	})

	exitCode := app.Run(context.Background(), []string{"daemon", "install"})
	if exitCode != 0 {
		t.Fatalf("Run([daemon install]) exit code = %d, want 0; stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Downloaded looperd (3 B)") {
		t.Fatalf("Run([daemon install]) stderr = %q, want download progress", stderr.String())
	}
	if !strings.Contains(stdout.String(), "Installed looperd (darwin-arm64) to ") {
		t.Fatalf("Run([daemon install]) stdout = %q, want install confirmation", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Downloaded from https://example.invalid/looperd-darwin-arm64") {
		t.Fatalf("Run([daemon install]) stdout = %q, want download URL", stdout.String())
	}
}

func TestDownloadBinaryProgressFallsBackWhenLengthUnknown(t *testing.T) {
	t.Parallel()

	stderr := &bytes.Buffer{}
	app := New(Deps{
		HTTPClient: newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://example.invalid/looperd-darwin-arm64" {
				t.Fatalf("unexpected request URL %q", req.URL.String())
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        http.StatusText(http.StatusOK),
				Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
				Body:          io.NopCloser(strings.NewReader("abcd")),
				ContentLength: -1,
			}, nil
		}),
	})
	runtime := newCommandRuntime(app, nil)

	data, err := runtime.downloadBinary(context.Background(), "https://example.invalid/looperd-darwin-arm64", "looperd-darwin-arm64", stderr)
	if err != nil {
		t.Fatalf("downloadBinary() error = %v", err)
	}
	if string(data) != "abcd" {
		t.Fatalf("downloadBinary() data = %q, want abcd", string(data))
	}
	if !strings.Contains(stderr.String(), "Downloaded looperd (4 B)") {
		t.Fatalf("progress = %q, want unknown-size fallback", stderr.String())
	}
}

func TestDownloadBinaryProgressDoesNotReportSuccessOnReadError(t *testing.T) {
	t.Parallel()

	stderr := &bytes.Buffer{}
	app := New(Deps{
		HTTPClient: newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://example.invalid/looperd-darwin-arm64" {
				t.Fatalf("unexpected request URL %q", req.URL.String())
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        http.StatusText(http.StatusOK),
				Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
				Body:          errReadCloser{Reader: strings.NewReader("abcd"), err: io.ErrUnexpectedEOF},
				ContentLength: -1,
			}, nil
		}),
	})
	runtime := newCommandRuntime(app, nil)

	_, err := runtime.downloadBinary(context.Background(), "https://example.invalid/looperd-darwin-arm64", "looperd-darwin-arm64", stderr)
	if err == nil {
		t.Fatal("downloadBinary() error = nil, want read error")
	}
	if !strings.Contains(err.Error(), io.ErrUnexpectedEOF.Error()) {
		t.Fatalf("downloadBinary() error = %q, want %q", err.Error(), io.ErrUnexpectedEOF)
	}
	if strings.Contains(stderr.String(), "Downloaded looperd") {
		t.Fatalf("progress = %q, did not expect success line after read failure", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Downloading looperd…") {
		t.Fatalf("progress = %q, want initial download line", stderr.String())
	}
}

func TestMPBDownloadProgressAbortsKnownSizeReadError(t *testing.T) {
	t.Parallel()

	progress := newMPBDownloadProgressFactory(io.Discard)
	tracker, ok := progress.newTracker("looperd-darwin-arm64", 4).(*mpbDownloadProgressTracker)
	if !ok {
		t.Fatal("newTracker() did not return *mpbDownloadProgressTracker")
	}
	reader := tracker.wrap(errReadCloser{Reader: strings.NewReader("abcd"), err: io.ErrUnexpectedEOF})
	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("ReadAll() error = nil, want read error")
	}
	if tracker.bar.Completed() {
		t.Fatal("tracker.bar.Completed() = true, did not expect completed after read error")
	}
	tracker.finish(false)
	progress.close()
	if tracker.bar.Completed() {
		t.Fatal("tracker.bar.Completed() = true, did not expect completed after read error")
	}
}

type errReadCloser struct {
	io.Reader
	err error
}

func (r errReadCloser) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF && r.err != nil {
		return n, r.err
	}
	return n, err
}

func (errReadCloser) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func newTestHTTPClient(fn roundTripFunc) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if isCDNReleaseMetadataURL(req.URL.String()) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Body:       io.NopCloser(strings.NewReader(`{"message":"test CDN skipped"}`)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Request:    req,
			}, nil
		}
		return fn(req)
	})}
}

func jsonResponse(t *testing.T, status int, body string) *http.Response {
	t.Helper()
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func textResponse(t *testing.T, status int, body string) *http.Response {
	t.Helper()
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func binaryResponse(t *testing.T, status int, body []byte) *http.Response {
	t.Helper()
	return &http.Response{
		StatusCode:    status,
		Status:        http.StatusText(status),
		Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func TestBuildReleaseManifestURL(t *testing.T) {
	t.Parallel()

	if got, want := buildReleaseManifestURL(defaultReleaseManifestBaseURL, ""), defaultReleaseManifestBaseURL+"/channels/stable.json"; got != want {
		t.Fatalf("latest URL = %q, want %q", got, want)
	}
	if got, want := buildReleaseManifestURL(defaultReleaseManifestBaseURL+"/", "v0.13.0"), defaultReleaseManifestBaseURL+"/v0.13.0/manifest.json"; got != want {
		t.Fatalf("tag URL = %q, want %q", got, want)
	}
}

func TestDecodeReleaseMetadataAcceptsManifestAndGitHubPayloads(t *testing.T) {
	t.Parallel()

	githubPayload, err := decodeReleaseMetadata([]byte(`{"tag_name":"v1.2.3","assets":[{"name":"looperd-darwin-arm64","browser_download_url":"https://evil.example/looperd-darwin-arm64"}]}`))
	if err != nil {
		t.Fatalf("decode GitHub payload error = %v", err)
	}
	if githubPayload.TagName != "v1.2.3" || len(githubPayload.Assets) != 1 {
		t.Fatalf("github payload = %#v", githubPayload)
	}
	if githubPayload.Assets[0].BrowserDownloadURL != "https://evil.example/looperd-darwin-arm64" {
		t.Fatalf("github asset URL = %q", githubPayload.Assets[0].BrowserDownloadURL)
	}

	manifestPayload, err := decodeReleaseMetadata([]byte(`{
		"manifestVersion": 1,
		"version": "1.2.3",
		"tag": "v1.2.3",
		"artifacts": {
			"looperd-darwin-arm64.tar.gz": {
				"url": "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz",
				"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"size": 12
			}
		}
	}`))
	if err != nil {
		t.Fatalf("decode manifest payload error = %v", err)
	}
	if manifestPayload.TagName != "v1.2.3" {
		t.Fatalf("manifest tag = %q, want v1.2.3", manifestPayload.TagName)
	}
	names := map[string]string{}
	for _, asset := range manifestPayload.Assets {
		names[asset.Name] = asset.BrowserDownloadURL
	}
	if names["looperd-darwin-arm64.tar.gz"] != "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz" {
		t.Fatalf("archive URL = %q", names["looperd-darwin-arm64.tar.gz"])
	}
	if names["looperd-darwin-arm64.tar.gz.sha256"] != "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz.sha256" {
		t.Fatalf("checksum URL = %q", names["looperd-darwin-arm64.tar.gz.sha256"])
	}
	if release.ManifestVersion != 1 {
		t.Fatalf("manifest version constant drifted: %d", release.ManifestVersion)
	}
}

func TestGithubReleaseFromManifestIgnoresArtifactURL(t *testing.T) {
	t.Parallel()

	payload := githubReleaseFromManifest(release.Manifest{
		Tag: "v1.2.3",
		Artifacts: map[string]release.Artifact{
			"looperd-darwin-arm64.tar.gz": {URL: "https://evil.example/looperd"},
			"../escape.tar.gz":            {URL: "https://evil.example/escape"},
			"nested/path.tar.gz":          {URL: "https://evil.example/nested"},
		},
	})
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}

	names := map[string]string{}
	for _, asset := range payload.Assets {
		names[asset.Name] = asset.BrowserDownloadURL
	}

	wantArchive := "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz"
	if names["looperd-darwin-arm64.tar.gz"] != wantArchive {
		t.Fatalf("archive URL = %q, want %q", names["looperd-darwin-arm64.tar.gz"], wantArchive)
	}
	if names["looperd-darwin-arm64.tar.gz.sha256"] != wantArchive+".sha256" {
		t.Fatalf("checksum URL = %q, want %q", names["looperd-darwin-arm64.tar.gz.sha256"], wantArchive+".sha256")
	}
	if _, ok := names["../escape.tar.gz"]; ok {
		t.Fatalf("path-escape asset was accepted: %#v", names)
	}
	if _, ok := names["nested/path.tar.gz"]; ok {
		t.Fatalf("nested path asset was accepted: %#v", names)
	}
}

func TestGithubReleaseFromManifestCanonicalizesTag(t *testing.T) {
	t.Parallel()

	payload := githubReleaseFromManifest(release.Manifest{
		Tag: "1.2.3",
		Artifacts: map[string]release.Artifact{
			"looperd-darwin-arm64.tar.gz": {URL: "https://evil.example/looperd"},
		},
	})
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	want := "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz"
	found := ""
	for _, asset := range payload.Assets {
		if asset.Name == "looperd-darwin-arm64.tar.gz" {
			found = asset.BrowserDownloadURL
		}
	}
	if found != want {
		t.Fatalf("archive URL = %q, want %q", found, want)
	}
}

func TestDecodeReleaseMetadataRejectsUnsupportedManifestVersion(t *testing.T) {
	t.Parallel()

	_, err := decodeReleaseMetadata([]byte(`{
		"manifestVersion": 2,
		"version": "1.2.3",
		"tag": "v1.2.3",
		"artifacts": {
			"looperd-darwin-arm64.tar.gz": {
				"url": "https://example.invalid/looperd-darwin-arm64.tar.gz",
				"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"size": 12
			}
		}
	}`))
	if err == nil {
		t.Fatal("decodeReleaseMetadata() error = nil, want unsupported version")
	}
	if !strings.Contains(err.Error(), "unsupported release manifest version 2") {
		t.Fatalf("error = %q", err)
	}
}

func TestFetchReleaseMetadataPrefersCDNManifest(t *testing.T) {
	t.Parallel()

	var seen []string
	app := New(Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = append(seen, req.URL.String())
			if req.URL.String() != defaultReleaseManifestBaseURL+"/channels/stable.json" {
				t.Fatalf("unexpected request URL %q", req.URL.String())
			}
			return jsonResponse(t, http.StatusOK, `{
				"manifestVersion": 1,
				"version": "1.2.3",
				"tag": "v1.2.3",
				"artifacts": {
					"looperd-darwin-arm64.tar.gz": {
						"url": "https://example.invalid/looperd-darwin-arm64.tar.gz",
						"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
						"size": 12
					}
				}
			}`), nil
		})},
	})
	runtime := newCommandRuntime(app, nil)
	payload, err := runtime.fetchReleaseMetadata(context.Background(), "")
	if err != nil {
		t.Fatalf("fetchReleaseMetadata() error = %v", err)
	}
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	urls := map[string]string{}
	for _, asset := range payload.Assets {
		urls[asset.Name] = asset.BrowserDownloadURL
	}
	wantArchive := "https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz"
	if urls["looperd-darwin-arm64.tar.gz"] != wantArchive {
		t.Fatalf("cdn artifact URL = %q, want %q", urls["looperd-darwin-arm64.tar.gz"], wantArchive)
	}
	if urls["looperd-darwin-arm64.tar.gz.sha256"] != wantArchive+".sha256" {
		t.Fatalf("cdn checksum URL = %q, want %q", urls["looperd-darwin-arm64.tar.gz.sha256"], wantArchive+".sha256")
	}
	if len(seen) != 1 || seen[0] != defaultReleaseManifestBaseURL+"/channels/stable.json" {
		t.Fatalf("requests = %#v", seen)
	}
}

func TestFetchReleaseMetadataFallsBackToGitHubAPI(t *testing.T) {
	t.Parallel()

	var seen []string
	app := New(Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = append(seen, req.URL.String())
			switch req.URL.String() {
			case defaultReleaseManifestBaseURL + "/channels/stable.json":
				return jsonResponse(t, http.StatusForbidden, `{"message":"rate limited"}`), nil
			case "https://api.github.com/repos/nexu-io/looper/releases/latest":
				return jsonResponse(t, http.StatusOK, `{"tag_name":"v1.2.3","assets":[]}`), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		})},
	})
	runtime := newCommandRuntime(app, nil)
	payload, err := runtime.fetchReleaseMetadata(context.Background(), "")
	if err != nil {
		t.Fatalf("fetchReleaseMetadata() error = %v", err)
	}
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	if len(seen) != 2 {
		t.Fatalf("requests = %#v", seen)
	}
}

func TestFetchReleaseMetadataRejectsMismatchedVersionedTag(t *testing.T) {
	t.Parallel()

	var seen []string
	app := New(Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = append(seen, req.URL.String())
			switch req.URL.String() {
			case defaultReleaseManifestBaseURL + "/v1.2.3/manifest.json":
				return jsonResponse(t, http.StatusOK, `{
					"manifestVersion": 1,
					"version": "9.9.9",
					"tag": "v9.9.9",
					"artifacts": {
						"looperd-darwin-arm64.tar.gz": {
							"url": "https://example.invalid/wrong",
							"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							"size": 12
						}
					}
				}`), nil
			case "https://api.github.com/repos/nexu-io/looper/releases/tags/v1.2.3":
				return jsonResponse(t, http.StatusOK, `{"tag_name":"v1.2.3","assets":[{"name":"looperd-darwin-arm64.tar.gz","browser_download_url":"https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz"}]}`), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		})},
	})
	runtime := newCommandRuntime(app, nil)
	payload, err := runtime.fetchReleaseMetadata(context.Background(), "v1.2.3")
	if err != nil {
		t.Fatalf("fetchReleaseMetadata() error = %v", err)
	}
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	if len(seen) != 2 {
		t.Fatalf("requests = %#v", seen)
	}
}

func TestFetchReleaseMetadataFallsBackWhenCDNAssetsIncomplete(t *testing.T) {
	t.Parallel()

	var seen []string
	app := New(Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = append(seen, req.URL.String())
			switch req.URL.String() {
			case defaultReleaseManifestBaseURL + "/channels/stable.json":
				return jsonResponse(t, http.StatusOK, `{
					"manifestVersion": 1,
					"version": "1.2.3",
					"tag": "v1.2.3",
					"artifacts": {
						"looperd-linux-amd64.tar.gz": {
							"url": "https://example.invalid/looperd-linux-amd64.tar.gz",
							"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							"size": 12
						}
					}
				}`), nil
			case "https://api.github.com/repos/nexu-io/looper/releases/latest":
				return jsonResponse(t, http.StatusOK, `{"tag_name":"v1.2.3","assets":[{"name":"looperd-darwin-arm64.tar.gz","browser_download_url":"https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz"},{"name":"looperd-darwin-arm64.tar.gz.sha256","browser_download_url":"https://github.com/nexu-io/looper/releases/download/v1.2.3/looperd-darwin-arm64.tar.gz.sha256"}]}`), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		})},
	})
	runtime := newCommandRuntime(app, nil)
	payload, err := runtime.fetchReleaseMetadataMatching(context.Background(), "", func(payload githubReleasePayload) error {
		_, err := findReleaseAssetSet(payload, "looperd-darwin-arm64")
		return err
	})
	if err != nil {
		t.Fatalf("fetchReleaseMetadataMatching() error = %v", err)
	}
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	if len(seen) != 2 {
		t.Fatalf("requests = %#v", seen)
	}
}

func TestFetchCDNGitHubShapedPayloadFallsBackToGitHubAPI(t *testing.T) {
	t.Parallel()

	var seen []string
	app := New(Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = append(seen, req.URL.String())
			switch req.URL.String() {
			case defaultReleaseManifestBaseURL + "/channels/stable.json":
				return jsonResponse(t, http.StatusOK, `{"tag_name":"v1.2.3","assets":[{"name":"looperd-darwin-arm64.tar.gz","browser_download_url":"https://evil.example/looperd"}]}`), nil
			case "https://api.github.com/repos/nexu-io/looper/releases/latest":
				return jsonResponse(t, http.StatusOK, `{"tag_name":"v1.2.3","assets":[{"name":"looperd-darwin-arm64.tar.gz","browser_download_url":"https://example.invalid/looperd-darwin-arm64.tar.gz"}]}`), nil
			default:
				t.Fatalf("unexpected request URL %q", req.URL.String())
				return nil, nil
			}
		})},
	})
	runtime := newCommandRuntime(app, nil)
	payload, err := runtime.fetchReleaseMetadata(context.Background(), "")
	if err != nil {
		t.Fatalf("fetchReleaseMetadata() error = %v", err)
	}
	if payload.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", payload.TagName)
	}
	if len(payload.Assets) != 1 || payload.Assets[0].BrowserDownloadURL != "https://example.invalid/looperd-darwin-arm64.tar.gz" {
		t.Fatalf("assets = %#v", payload.Assets)
	}
	if len(seen) != 2 {
		t.Fatalf("requests = %#v", seen)
	}
}
