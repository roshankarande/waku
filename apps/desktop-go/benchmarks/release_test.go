package benchmarks

import (
	"context"
	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestReleaseRunnerReportsConcretePrerequisiteBlockers(t *testing.T) {
	t.Setenv("DISPLAY", "")
	t.Setenv("WAYLAND_DISPLAY", "")
	report, err := RunReleaseBenchmarks(context.Background(), ReleaseRunConfig{GUIBlocker: "DISPLAY and WAYLAND_DISPLAY are unset"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != validation.StatusBlocked || len(report.Comparisons) != 0 || len(report.Checks) != 1 || report.Checks[0].Blocker == "" {
		t.Fatalf("report=%+v", report)
	}
	report, err = RunReleaseBenchmarks(context.Background(), ReleaseRunConfig{BaselineBlocker: "Rust desktop toolchain is unavailable"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != validation.StatusBlocked || report.Checks[0].Name != "Rust baseline executable" {
		t.Fatalf("report=%+v", report)
	}
}

func TestTypedExitThreeIsBlockedOnlyWhenNonceBound(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	nonce := "private-nonce"
	command := exec.Command("sh", "-c", `printf '%s\n' '{"schemaVersion":1,"protocol":"waku-benchmark-driver-v1","status":"blocked","runNonce":"private-nonce","blocker":"no compositor"}'; exit 3`)
	output, err := command.CombinedOutput()
	status, ok := decodeDriverBlocked(output, err, nonce)
	if !ok || status.Blocker != "no compositor" {
		t.Fatalf("typed blocker=%+v ok=%v err=%v output=%s", status, ok, err, output)
	}
	if _, ok := decodeDriverBlocked(output, err, "other"); ok {
		t.Fatal("wrong-nonce blocked output passed")
	}
}

func TestExecutableSnapshotAndOutputsRejectSymlinksAndStaleFiles(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	if _, _, err := snapshotExecutable(link, filepath.Join(root, "private"), "app"); err == nil {
		t.Fatal("symlink executable passed immutable snapshot")
	}
	now := time.Now()
	if err := validateFreshOutputs([]string{link}, now.Add(-time.Second), now.Add(time.Second)); err == nil {
		t.Fatal("symlink output passed")
	}
	stale := filepath.Join(root, "stale")
	if err := os.WriteFile(stale, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-time.Hour)
	_ = os.Chtimes(stale, old, old)
	if err := validateFreshOutputs([]string{stale}, now, now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("stale output passed: %v", err)
	}
}

func TestRunIDsRequireExactCryptographicNonce(t *testing.T) {
	if err := validateEvidenceRunIDs([]validation.Event{{RunID: "prefix-secret-suffix"}}, []validation.Sample{{RunID: "secret"}}, "secret"); err == nil {
		t.Fatal("substring event run ID passed")
	}
	if err := validateEvidenceRunIDs([]validation.Event{{RunID: "secret"}}, []validation.Sample{{RunID: "secret"}}, "secret"); err != nil {
		t.Fatal(err)
	}
	a, err := cryptographicNonce()
	if err != nil {
		t.Fatal(err)
	}
	b, err := cryptographicNonce()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 48 || a == b {
		t.Fatalf("nonces a=%q b=%q", a, b)
	}
}

func TestReleaseRunnerRequiresObservedGUISession(t *testing.T) {
	t.Setenv("DISPLAY", "")
	t.Setenv("WAYLAND_DISPLAY", "")
	report, err := RunReleaseBenchmarks(context.Background(), ReleaseRunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != validation.StatusBlocked || len(report.Checks) != 1 || report.Checks[0].Name != "release GUI benchmark capability" {
		t.Fatalf("missing observed GUI session bypassed capability gate: %+v", report)
	}
}

func TestGUIObserverMustBindItsProcessIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	observer := filepath.Join(t.TempDir(), "observer")
	program := `#!/bin/sh
printf '{"schemaVersion":1,"protocol":"waku-gui-observer-v1","capturedAt":"%s","platform":"%s","display":"test","renderer":"test","observerPid":1}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "` + runtime.GOOS + `"
`
	if err := os.WriteFile(observer, []byte(program), 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := observeNativeGUI(context.Background(), observer); err == nil {
		t.Fatal("GUI observer with fabricated process identity passed")
	}
}
