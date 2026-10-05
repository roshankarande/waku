package validation

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestUnsignedStagingRejectsMissingReviewedInputs(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "file")
	if err := os.WriteFile(regular, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	base := ReleaseStageSpec{Platform: "linux", DesktopExecutable: regular, DaemonExecutable: regular, WakuLicense: regular, MyGoLicense: regular, GoLicense: regular, Metadata: []StageFile{{Kind: "desktop-entry", SourcePath: regular, Path: "share/applications/waku.desktop"}, {Kind: "icon", SourcePath: regular, Path: "share/icons/waku.png"}}}
	_, err := StageUnsignedGoRelease(filepath.Join(root, "stage"), filepath.Join(root, "release.tar.gz"), base)
	if err == nil || !strings.Contains(err.Error(), RetainedGhosttyNative) {
		t.Fatalf("missing retained assets passed: %v", err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(regular, link); err != nil {
		t.Skip(err)
	}
	base.DesktopExecutable = link
	_, err = StageUnsignedGoRelease(filepath.Join(root, "stage2"), filepath.Join(root, "release2.tar.gz"), base)
	if err == nil || !strings.Contains(err.Error(), "non-symlink") {
		t.Fatalf("symlink input passed: %v", err)
	}
}

func TestUnsignedStagingRejectsMetadataAndAssetKindSpoofing(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	_ = os.WriteFile(file, []byte("x"), 0600)
	spec := ReleaseStageSpec{Platform: "linux", DesktopExecutable: file, DaemonExecutable: file, WakuLicense: file, MyGoLicense: file, GoLicense: file, Metadata: []StageFile{{Kind: "desktop-entry", SourcePath: file, Path: "share/applications/not-an-entry.txt"}, {Kind: "icon", SourcePath: file, Path: "share/icons/waku.png"}}}
	_, err := StageUnsignedGoRelease(filepath.Join(root, "stage"), filepath.Join(root, "release.tar.gz"), spec)
	if err == nil || !strings.Contains(err.Error(), ".desktop") {
		t.Fatalf("metadata kind spoof passed: %v", err)
	}
	spec.Metadata[0].Path = "share/applications/waku.desktop"
	spec.RetainedAssets = []StageRetainedAsset{{Kind: RetainedGhosttyNative, SourcePath: file, Path: "share/waku/ghostty.js", Platforms: []string{"linux"}, LicensePath: "share/licenses/waku/GHOSTTY", LicenseSourcePath: file}}
	_, err = StageUnsignedGoRelease(filepath.Join(root, "stage2"), filepath.Join(root, "release2.tar.gz"), spec)
	if err == nil || !strings.Contains(err.Error(), "shared-library") {
		t.Fatalf("native kind path spoof passed: %v", err)
	}
}

func TestDeterministicArchiveBytesAndNoOverwrite(t *testing.T) {
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "bin"), 0755)
	_ = os.WriteFile(filepath.Join(root, "bin", "waku"), []byte("payload"), 0755)
	for _, extension := range []string{".tar.gz", ".zip"} {
		a := filepath.Join(t.TempDir(), "a"+extension)
		b := filepath.Join(t.TempDir(), "b"+extension)
		if err := createDeterministicArchive(root, a, extension); err != nil {
			t.Fatal(err)
		}
		if err := createDeterministicArchive(root, b, extension); err != nil {
			t.Fatal(err)
		}
		ah, _ := fileSHA256(a)
		bh, _ := fileSHA256(b)
		if ah != bh {
			t.Fatalf("%s archive is not reproducible: %s != %s", extension, ah, bh)
		}
		if err := createDeterministicArchive(root, a, extension); err == nil {
			t.Fatal("archive overwrite passed")
		}
	}
}

// This exercises staging mechanics with unit fixtures. Its blocked result is
// not signing, installer, clean-machine, or release evidence.
func TestUnitOnlyUnsignedLinuxStageIsInspectedThenBlocked(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("unit fixture uses a runner-provided ELF shared library")
	}
	shared := "/usr/lib/chromium/libEGL.so"
	if info, err := os.Lstat(shared); err != nil || !info.Mode().IsRegular() {
		t.Skip("regular shared-library fixture unavailable")
	}
	root := t.TempDir()
	desktop := filepath.Join(root, "desktop")
	daemon := filepath.Join(root, "daemon")
	moduleRoot, _ := filepath.Abs(filepath.Join("..", ".."))
	for _, item := range []struct{ out, role string }{{desktop, "desktop"}, {daemon, "daemon"}} {
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-ldflags", "-X main.releaseRole="+item.role, "-o", item.out, "./internal/validation/stagefixturecmd")
		cmd.Dir = moduleRoot
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build fixture: %v: %s", err, output)
		}
	}
	deps := executableDependencyUnion(desktop, daemon)
	licenseByModule := map[string]string{"github.com/roshankarande/mygo": "/workspace/mygo/LICENSE", "github.com/ebitengine/purego": "/home/agent/go/pkg/mod/github.com/ebitengine/purego@v0.11.1/LICENSE", "github.com/go-text/typesetting": "/home/agent/go/pkg/mod/github.com/go-text/typesetting@v0.3.5/LICENSE", "golang.org/x/image": "/home/agent/go/pkg/mod/golang.org/x/image@v0.46.0/LICENSE"}
	dependencyLicenses := []StageDependencyLicense{}
	for i, dep := range deps {
		source := licenseByModule[dep.Path]
		if source == "" {
			t.Fatalf("unmapped fixture dependency %s", dep.Path)
		}
		path := fmt.Sprintf("share/licenses/waku/dependency-%d", i)
		if strings.HasSuffix(dep.Path, "/mygo") {
			path = "share/licenses/waku/MYGO-LICENSE"
		}
		dependencyLicenses = append(dependencyLicenses, StageDependencyLicense{Path: dep.Path, Version: dep.Version, SourcePath: source, LicensePath: path})
	}
	hex := strings.Repeat("a", 64)
	cuaLicense := "/workspace/waku/resources/computer-use/CUA-LICENSE"
	ghosttyLicense := "/workspace/waku/node_modules/.cache/@wterm/ghostty@0.3.3@@@1/LICENSE"
	spec := ReleaseStageSpec{Platform: "linux", DesktopExecutable: desktop, DaemonExecutable: daemon, WakuLicense: "/workspace/waku/LICENSE", MyGoLicense: "/workspace/mygo/LICENSE", GoLicense: "/workspace/.tools/go/LICENSE", Metadata: []StageFile{{Kind: "desktop-entry", SourcePath: "/workspace/waku/resources/linux/sh.waku.desktop", Path: "share/applications/sh.waku.desktop"}, {Kind: "icon", SourcePath: "/workspace/waku/website/public/app-icon.png", Path: "share/icons/hicolor/256x256/apps/sh.waku.png"}}, DependencyLicenses: dependencyLicenses, RetainedAssets: []StageRetainedAsset{
		{Kind: RetainedGhosttyNative, SourcePath: shared, Path: "share/waku/libghostty-vt.so", Purpose: "unit fixture", Source: "https://example.invalid/ghostty.tar.gz", Platforms: []string{"linux"}, LicensePath: "share/licenses/waku/GHOSTTY", LicenseSourcePath: ghosttyLicense, Identity: "ghostty", Version: "fixture", SourceSHA256: hex},
		{Kind: RetainedCuaNative, SourcePath: shared, Path: "share/waku/libcua_driver_sdk.so", Purpose: "unit fixture", Source: "https://example.invalid/cua.tar.gz", Platforms: []string{"linux"}, LicensePath: "share/licenses/waku/CUA", LicenseSourcePath: cuaLicense, Identity: "cua", Version: "0.28.0-fixture", SourceSHA256: hex},
		{Kind: RetainedJavaScript, SourcePath: "/workspace/waku/resources/computer-use/pi-extension.ts", Path: "share/waku/computer-use/pi-extension.ts", Purpose: "Pi integration", Source: "https://github.com/roshankarande/waku", Platforms: []string{"linux"}, LicensePath: "share/licenses/waku/CUA", LicenseSourcePath: cuaLicense, Identity: "waku-pi-extension", Version: "fixture", SourceSHA256: hex}}}
	stage := filepath.Join(root, "stage")
	archive := filepath.Join(root, "waku.tar.gz")
	report, err := StageUnsignedGoRelease(stage, archive, spec)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != StatusBlocked || report.Manifest == nil || report.ArchiveSHA256 == "" {
		t.Fatalf("report=%+v", report)
	}
	if _, err := os.Stat(filepath.Join(stage, "share/notices/waku/SHA256SUMS")); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(stage, "share/applications/sh.waku.desktop")
	original, _ := os.ReadFile(metadataPath)
	if err := os.WriteFile(metadataPath, []byte("tampered"), 0644); err != nil {
		t.Fatal(err)
	}
	if inspection := InspectDistribution(stage, *report.Manifest); inspection.Status != StatusFail {
		t.Fatal("tampered required platform metadata passed")
	}
	if err := os.WriteFile(metadataPath, original, 0644); err != nil {
		t.Fatal(err)
	}
	for _, check := range report.Checks {
		if check.Name != "unsigned deterministic stage/archive" && check.Status != StatusBlocked {
			t.Fatalf("release prerequisite incorrectly passed: %+v", check)
		}
	}
}
