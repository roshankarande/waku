package validation

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSummarizeNearestRank(t *testing.T) {
	got := Summarize([]float64{10, 1, 3, 2, 100})
	if got.Count != 5 || got.P50 != 3 || got.P95 != 100 || got.P99 != 100 || got.Max != 100 {
		t.Fatalf("unexpected summary: %+v", got)
	}
}

func TestMetricsFromEvents(t *testing.T) {
	input := `{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"correlationId":"startup","name":"process_start","atNs":0}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"correlationId":"startup","name":"interactive","atNs":12000000}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"correlationId":"key-1","name":"input","atNs":20000000}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"correlationId":"key-1","name":"paint","atNs":23000000}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"correlationId":"key-1","name":"present","atNs":25000000}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"name":"frame_work","atNs":26000000,"value":2000000}
{"version":1,"runId":"a","build":"go","workloadId":"long-transcript","fixtureSha256":"4f76c2ce237cbaa7bfeba8d944db8cc58cc1658d7c4b6483aa125a21255a2180","iteration":0,"name":"pane_render","atNs":27000000,"fields":{"pane":"transcript"}}
`
	events, err := DecodeEvents(bytes.NewBufferString(input))
	if err != nil {
		t.Fatal(err)
	}
	metrics, iterations, err := MetricsFromEvents(events, "go", "long-transcript", KnownWorkloadDigests["long-transcript"])
	if err != nil {
		t.Fatal(err)
	}
	if metrics.StartupMS.P50 != 12 || metrics.InputToPaintMS.P50 != 3 || metrics.InputToPresentMS.P50 != 5 || metrics.FrameWorkMS.P50 != 2 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if metrics.PaneRenders["transcript"] != 1 {
		t.Fatalf("pane count: %+v", metrics.PaneRenders)
	}
	if len(iterations) != 1 {
		t.Fatalf("iterations: %+v", iterations)
	}
}

func TestEventsCorrelateOverlappingOperationsAndRejectUnmatched(t *testing.T) {
	digest := KnownWorkloadDigests["large-diff"]
	good := `{"version":1,"runId":"r","build":"go","workloadId":"large-diff","fixtureSha256":"392bda7fe4f761d2f0848924eb15dcd0f589fd4862a6dc5b7d6ddc71b439bc85","iteration":0,"correlationId":"a","name":"input","atNs":1}
{"version":1,"runId":"r","build":"go","workloadId":"large-diff","fixtureSha256":"392bda7fe4f761d2f0848924eb15dcd0f589fd4862a6dc5b7d6ddc71b439bc85","iteration":0,"correlationId":"b","name":"input","atNs":2}
{"version":1,"runId":"r","build":"go","workloadId":"large-diff","fixtureSha256":"392bda7fe4f761d2f0848924eb15dcd0f589fd4862a6dc5b7d6ddc71b439bc85","iteration":0,"correlationId":"b","name":"paint","atNs":3}
{"version":1,"runId":"r","build":"go","workloadId":"large-diff","fixtureSha256":"392bda7fe4f761d2f0848924eb15dcd0f589fd4862a6dc5b7d6ddc71b439bc85","iteration":0,"correlationId":"a","name":"paint","atNs":4}`
	events, err := DecodeEvents(bytes.NewBufferString(good))
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := MetricsFromEvents(events, "go", "large-diff", digest)
	if err != nil || m.InputToPaintMS.Count != 2 {
		t.Fatalf("metrics=%+v err=%v", m, err)
	}
	_, _, err = MetricsFromEvents(events[:3], "go", "large-diff", digest)
	if err == nil {
		t.Fatal("expected unmatched operation rejection")
	}
}

func TestEventsRejectCorrelationReuseAndPresentWithoutInput(t *testing.T) {
	base := func(name, corr string, at int64) Event {
		return Event{Version: 1, RunID: "r", Build: "go", WorkloadID: "large-diff", FixtureSHA256: KnownWorkloadDigests["large-diff"], CorrelationID: corr, Name: name, AtNS: at}
	}
	_, _, err := MetricsFromEvents([]Event{base(EventInput, "x", 1), base(EventHydrateStart, "x", 2), base(EventPaint, "x", 3), base(EventHydrateReady, "x", 4)}, "go", "large-diff", KnownWorkloadDigests["large-diff"])
	if err == nil {
		t.Fatal("expected correlation reuse rejection")
	}
	_, _, err = MetricsFromEvents([]Event{base(EventPresent, "x", 1)}, "go", "large-diff", KnownWorkloadDigests["large-diff"])
	if err == nil {
		t.Fatal("expected present without input rejection")
	}
	warmInput := base(EventInput, "warm", 2)
	warmInput.Warmup = true
	warmPaint := base(EventPaint, "warm", 1)
	warmPaint.Warmup = true
	if _, _, err := MetricsFromEvents([]Event{warmInput, warmPaint}, "go", "large-diff", KnownWorkloadDigests["large-diff"]); err == nil {
		t.Fatal("expected warmup temporal ordering rejection")
	}
}

func TestSampleDeltasAndAttribution(t *testing.T) {
	digest := KnownWorkloadDigests["fast-reasoning"]
	s := []Sample{{Version: 1, RunID: "r", Build: "go", WorkloadID: "fast-reasoning", FixtureSHA256: digest, Iteration: 0, WindowStartNS: 0, WindowEndNS: 1e9, CPUTimeNS: 1e8, IdleWakeups: 10}, {Version: 1, RunID: "r", Build: "go", WorkloadID: "fast-reasoning", FixtureSHA256: digest, Iteration: 0, WindowStartNS: 1e9, WindowEndNS: 2e9, CPUTimeNS: 3e8, IdleWakeups: 30, RSSBytes: 100, AllocationBytes: 50, Allocations: 5}}
	var m Metrics
	iterations, err := AddSamples(&m, s, "go", "fast-reasoning", digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(iterations) != 1 || m.CPUPercent.P50 != 20 || m.IdleWakeupsPerSec.P50 != 20 || m.AllocationBytes != 50 {
		t.Fatalf("metrics=%+v iterations=%+v", m, iterations)
	}
	s[1].Build = "rust"
	if _, err := AddSamples(&m, s, "go", "fast-reasoning", digest); err == nil {
		t.Fatal("expected attribution rejection")
	}
	s[1].Build = "go"
	s[1].WindowStartNS = s[0].WindowStartNS
	s[1].WindowEndNS = s[0].WindowEndNS
	if _, err := AddSamples(&m, s, "go", "fast-reasoning", digest); err == nil {
		t.Fatal("expected duplicate window rejection")
	}
}

func TestDecodeSamplesRequiresEveryMetricField(t *testing.T) {
	line := `{"version":1,"runId":"r","build":"go","workloadId":"fast-reasoning","fixtureSha256":"146d22951537176d3c7df7a1431a7904137a324c62c8b1a71c0800c401a64f78","iteration":0,"windowStartNs":0,"windowEndNs":1,"cpuTimeNs":0,"idleWakeups":0,"rssBytes":0,"heapBytes":0,"allocationBytes":0}`
	if _, err := DecodeSamples(strings.NewReader(line)); err == nil || !strings.Contains(err.Error(), "allocations") {
		t.Fatalf("expected missing metric field rejection, got %v", err)
	}
}

func TestFixtureManifestsValidate(t *testing.T) {
	data, err := os.ReadFile("testdata/feature_matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	var matrix Matrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatal(err)
	}
	if err := matrix.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile("testdata/upgrade_rollback_plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan UpgradeRollbackPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveEntriesRejectTraversal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.tar.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	archive := tar.NewWriter(gz)
	if err := archive.WriteHeader(&tar.Header{Name: "../escape", Mode: 0600}); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ArchiveEntries(path); err == nil {
		t.Fatal("expected traversal rejection")
	}
}

func TestArchiveEntriesRejectWindowsTraversal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(file)
	if _, err := archive.Create(`..\escape`); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ArchiveEntries(path); err == nil {
		t.Fatal("expected Windows traversal rejection")
	}
}

func TestArchiveEntriesRejectDriveCollisionAndEscapingLinks(t *testing.T) {
	for name, entries := range map[string][]string{"drive.zip": {"C:/waku.exe"}, "collision.zip": {"a/../b", "b"}} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			f, _ := os.Create(path)
			z := zip.NewWriter(f)
			for _, entry := range entries {
				_, _ = z.Create(entry)
			}
			_ = z.Close()
			_ = f.Close()
			if _, err := ArchiveEntries(path); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "link.tar.gz")
	f, _ := os.Create(path)
	gz := gzip.NewWriter(f)
	tr := tar.NewWriter(gz)
	_ = tr.WriteHeader(&tar.Header{Name: "root/link", Typeflag: tar.TypeSymlink, Linkname: "../../escape"})
	_ = tr.Close()
	_ = gz.Close()
	_ = f.Close()
	if _, err := ArchiveEntries(path); err == nil {
		t.Fatal("expected escaping link rejection")
	}
	path = filepath.Join(t.TempDir(), "hardlink.tar.gz")
	f, _ = os.Create(path)
	gz = gzip.NewWriter(f)
	tr = tar.NewWriter(gz)
	_ = tr.WriteHeader(&tar.Header{Name: "root/hard", Typeflag: tar.TypeLink, Linkname: "root/missing"})
	_ = tr.Close()
	_ = gz.Close()
	_ = f.Close()
	if _, err := ArchiveEntries(path); err == nil {
		t.Fatal("expected missing hardlink target rejection")
	}
}

func TestArchiveEntriesRejectLinkCyclesAndExpandedSize(t *testing.T) {
	cycle := filepath.Join(t.TempDir(), "cycle.tar.gz")
	f, _ := os.Create(cycle)
	gz := gzip.NewWriter(f)
	tr := tar.NewWriter(gz)
	_ = tr.WriteHeader(&tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: "b"})
	_ = tr.WriteHeader(&tar.Header{Name: "b", Typeflag: tar.TypeSymlink, Linkname: "a"})
	_ = tr.Close()
	_ = gz.Close()
	_ = f.Close()
	if _, err := ArchiveEntries(cycle); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle rejection, got %v", err)
	}

	oversized := filepath.Join(t.TempDir(), "oversized.tar.gz")
	f, _ = os.Create(oversized)
	gz = gzip.NewWriter(f)
	tr = tar.NewWriter(gz)
	_ = tr.WriteHeader(&tar.Header{Name: "payload", Typeflag: tar.TypeReg, Size: MaxArchiveEntryBytes + 1})
	_ = tr.Close() // Deliberately incomplete body; preflight must reject from the header.
	_ = gz.Close()
	_ = f.Close()
	if _, err := ArchiveEntries(oversized); err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("expected size rejection, got %v", err)
	}
}

func TestArchiveEntriesBoundsEntryCountAndTotalExpandedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.zip")
	f, _ := os.Create(path)
	z := zip.NewWriter(f)
	for _, name := range []string{"one", "two", "three"} {
		entry, _ := z.Create(name)
		_, _ = entry.Write([]byte("1234"))
	}
	_ = z.Close()
	_ = f.Close()
	if _, err := archiveEntries(path, 2, 100, 100); err == nil || !strings.Contains(err.Error(), "more than 2 entries") {
		t.Fatalf("expected entry count rejection, got %v", err)
	}
	if _, err := archiveEntries(path, 10, 100, 10); err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("expected total expanded size rejection, got %v", err)
	}
}

func TestDistributionReportsMissingEvidence(t *testing.T) {
	root := t.TempDir()
	report := InspectDistribution(root, DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}, RetainedAssets: []RetainedAsset{{Path: "share/sdk.so", Purpose: "computer use", LicensePath: "share/LICENSE"}}})
	if report.Status != StatusFail || len(report.Issues) < 7 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestDistributionAcceptsDeclaredPureGoExecutableAndAssets(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"bin", "share/sdk"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
	}
	buildPureGoFixture(t, filepath.Join(root, "bin/waku"))
	if err := os.WriteFile(filepath.Join(root, "share/sdk/asset.bin"), []byte("native fixture"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "share/sdk/LICENSE"), []byte("fixture license"), 0644); err != nil {
		t.Fatal(err)
	}
	assetDigest, err := fileSHA256(filepath.Join(root, "share/sdk/asset.bin"))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := applicationMetadata("bin/waku", filepath.Join(root, "bin/waku"))
	if err != nil {
		t.Fatal(err)
	}
	report := InspectDistribution(root, DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}, ApplicationMetadata: []ApplicationMetadata{metadata}, RetainedAssets: []RetainedAsset{{Path: "share/sdk/asset.bin", Purpose: "fixture", Source: "test fixture", Platforms: []string{"linux"}, LicensePath: "share/sdk/LICENSE", SHA256: assetDigest}}, LicensePaths: []string{"share/sdk/LICENSE"}, NoticePaths: []string{"share/sdk/LICENSE"}, NoApplicationRustRuntime: true, NoApplicationJSRuntime: true})
	if report.Status != StatusPass {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func buildPureGoFixture(t *testing.T, output string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", output, source)
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build pure-Go fixture: %v: %s", err, data)
	}
}

func TestDistributionScansDeclaredExecutableMarkersAndRequiresExactAllowlist(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin/waku"), binary, 0755); err != nil {
		t.Fatal(err)
	}
	report := InspectDistribution(root, DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}, EmbeddedMarkerAllowances: []EmbeddedMarkerAllowance{{Path: "bin/waku", Marker: "waku_core", Purpose: "only one exact marker"}}})
	var markerIssues int
	for _, issue := range report.Issues {
		if strings.Contains(issue.Message, "Rust runtime marker") {
			markerIssues++
		}
	}
	if report.Status != StatusFail || markerIssues < 1 {
		t.Fatalf("expected unallowlisted embedded markers, report=%+v", report)
	}
}

func TestDistributionRejectsUndeclaredExecutable(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	testBinary, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin/waku"), testBinary, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin/legacy-daemon"), []byte("legacy"), 0755); err != nil {
		t.Fatal(err)
	}
	report := InspectDistribution(root, DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}})
	if report.Status != StatusFail {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestDistributionRejectsExecutableOutsideBinRetainedRustAndEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "share"), 0755)
	binary, _ := os.ReadFile(os.Args[0])
	_ = os.WriteFile(filepath.Join(root, "waku"), binary, 0755)
	_ = os.WriteFile(filepath.Join(root, "share/retained.exe"), append([]byte("MZxxxx waku_core::runtime"), make([]byte, 32)...), 0644)
	_ = os.WriteFile(filepath.Join(root, "LICENSE"), []byte("license"), 0644)
	_ = os.Symlink("/tmp", filepath.Join(root, "escape"))
	report := InspectDistribution(root, DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"missing"}, RetainedAssets: []RetainedAsset{{Path: "share/retained.exe", Purpose: "third party", LicensePath: "LICENSE"}}})
	if report.Status != StatusFail || len(report.Issues) < 4 {
		t.Fatalf("report=%+v", report)
	}
}

func TestExecutableFormatsDetectedWithoutUnixMode(t *testing.T) {
	for name, magic := range map[string][]byte{"pe": {'M', 'Z', 0, 0}, "macho": {0xfe, 0xed, 0xfa, 0xcf}, "fat-cigam": {0xbe, 0xba, 0xfe, 0xca}, "fat-cigam64": {0xbf, 0xba, 0xfe, 0xca}, "elf": {0x7f, 'E', 'L', 'F'}, "script": {'#', '!', '/', 'b'}} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(path, magic, 0600); err != nil {
				t.Fatal(err)
			}
			ok, _ := executableFormat(path)
			if !ok {
				t.Fatal("not detected")
			}
		})
	}
}

func TestDistributionAllowsOnlyDeclaredRealSharedLibrary(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ELF shared-object fixture is Linux-specific")
	}
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler unavailable for real shared-object fixture")
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "share"), 0755); err != nil {
		t.Fatal(err)
	}
	buildPureGoFixture(t, filepath.Join(root, "bin/waku"))
	source := filepath.Join(t.TempDir(), "fixture.c")
	if err := os.WriteFile(source, []byte("int validation_fixture(void) { return 7; }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	library := filepath.Join(root, "share/libfixture.so")
	command := exec.Command(compiler, "-shared", "-fPIC", "-o", library, source)
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build shared object: %v: %s", err, data)
	}
	if err := os.WriteFile(filepath.Join(root, "share/LICENSE"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	libraryDigest, err := fileSHA256(library)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := applicationMetadata("bin/waku", filepath.Join(root, "bin/waku"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}, ApplicationMetadata: []ApplicationMetadata{metadata}, RetainedAssets: []RetainedAsset{{Path: "share/libfixture.so", Purpose: "test shared library", Source: "compiled test fixture", Platforms: []string{"linux"}, LicensePath: "share/LICENSE", SHA256: libraryDigest}}, LicensePaths: []string{"share/LICENSE"}, NoticePaths: []string{"share/LICENSE"}, NoApplicationRustRuntime: true, NoApplicationJSRuntime: true}
	if report := InspectDistribution(root, manifest); report.Status != StatusPass {
		t.Fatalf("declared library rejected: %+v", report)
	}
	file, err := os.OpenFile(library, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("waku_core::runtime"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	manifest.RetainedAssets[0].SHA256, _ = fileSHA256(library)
	if report := InspectDistribution(root, manifest); report.Status != StatusFail {
		t.Fatalf("declared library bypassed Rust marker scan: %+v", report)
	}
	manifest.RetainedAssets = nil
	if report := InspectDistribution(root, manifest); report.Status != StatusFail {
		t.Fatalf("undeclared library passed: %+v", report)
	}
}

func TestDistributionRejectsUnhashedRuntimeAssetsAndFakeLibraries(t *testing.T) {
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "bin"), 0755)
	_ = os.MkdirAll(filepath.Join(root, "share"), 0755)
	buildPureGoFixture(t, filepath.Join(root, "bin/waku"))
	_ = os.WriteFile(filepath.Join(root, "share/fake.so"), []byte("not an ELF shared object"), 0600)
	_ = os.WriteFile(filepath.Join(root, "share/runtime.js"), []byte("export {}"), 0600)
	_ = os.WriteFile(filepath.Join(root, "share/LICENSE"), []byte("fixture"), 0600)
	manifest := DistributionManifest{SchemaVersion: 1, ApplicationExecutables: []string{"bin/waku"}, RetainedAssets: []RetainedAsset{{Path: "share/fake.so", Purpose: "fake", LicensePath: "share/LICENSE", SHA256: strings.Repeat("0", 64)}, {Path: "share/runtime.js", Purpose: "runtime", LicensePath: "share/LICENSE"}}}
	report := InspectDistribution(root, manifest)
	if report.Status != StatusFail || len(report.Issues) < 3 {
		t.Fatalf("invalid retained runtime assets passed: %+v", report)
	}
}

func TestUpgradeExecutionBlocksMissingAndSyntheticBinaries(t *testing.T) {
	report, err := ExecuteUpgradePlan(context.Background(), t.TempDir(), UpgradeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != StatusBlocked {
		t.Fatalf("report=%+v", report)
	}
	baseline, candidate := writeUnitOnlySyntheticUpgradeExecutables(t, false, false)
	report, err = ExecuteUpgradePlan(context.Background(), t.TempDir(), UpgradeConfig{BaselineExecutable: baseline, CandidateExecutable: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != StatusBlocked {
		t.Fatalf("report=%+v", report)
	}
}

func TestUpgradeHarnessDoesNotTrustSyntheticProbeClaims(t *testing.T) {
	baseline, candidate := writeUnitOnlySyntheticUpgradeExecutables(t, true, false)
	report, err := ExecuteUpgradePlan(context.Background(), t.TempDir(), UpgradeConfig{BaselineExecutable: baseline, CandidateExecutable: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != StatusBlocked {
		t.Fatalf("synthetic mutation probe was trusted: %+v", report)
	}
	baseline, candidate = writeUnitOnlySyntheticUpgradeExecutables(t, false, true)
	report, err = ExecuteUpgradePlan(context.Background(), t.TempDir(), UpgradeConfig{BaselineExecutable: baseline, CandidateExecutable: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != StatusBlocked {
		t.Fatalf("request-ignoring synthetic probe was trusted: %+v", report)
	}
	truePath, trueErr := exec.LookPath("true")
	falsePath, falseErr := exec.LookPath("false")
	if trueErr == nil && falseErr == nil {
		report, err = ExecuteUpgradePlan(context.Background(), t.TempDir(), UpgradeConfig{BaselineExecutable: truePath, CandidateExecutable: falsePath})
		if err != nil {
			t.Fatal(err)
		}
		if report.Status != StatusBlocked {
			t.Fatalf("binaries without validation-probe support passed: %+v", report)
		}
	}
}

// writeUnitOnlySyntheticUpgradeExecutables exercises rejection boundaries. Its
// output must never be accepted as product upgrade/interoperability evidence.
func writeUnitOnlySyntheticUpgradeExecutables(t *testing.T, mutateData, ignoreRequest bool) (string, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("real syscall file-lock probe fixture is unavailable on Windows")
	}
	root := t.TempDir()
	baseline := filepath.Join(root, "baseline")
	candidate := filepath.Join(root, "candidate")
	for _, build := range []struct {
		role, output   string
		mutate, ignore bool
	}{{"baseline", baseline, false, false}, {"candidate", candidate, mutateData, ignoreRequest}} {
		source := filepath.Join(root, build.role+".go")
		program := fmt.Sprintf(`package main
import("crypto/sha256";"encoding/json";"fmt";"os";"strings";"syscall")
const role=%q
const mutate=%t
const ignore=%t
func value(name string)string{for i:=2;i+1<len(os.Args);i++{if os.Args[i]==name{return os.Args[i+1]}};return ""}
func main(){if len(os.Args)<3||os.Args[1]!="--waku-validation-probe"{os.Exit(2)};phase:=os.Args[2];run,sha:=value("--run-id"),value("--build-sha");work,_:=os.Getwd();switch phase{
case "identity": json.NewEncoder(os.Stdout).Encode(map[string]any{"runId":run,"buildSha256":sha,"workingDirectory":work,"pid":os.Getpid()})
case "lock": f,e:=os.OpenFile(os.Args[3],os.O_RDWR,0600);if e!=nil{os.Exit(2)};defer f.Close();if syscall.Flock(int(f.Fd()),syscall.LOCK_EX|syscall.LOCK_NB)!=nil{fmt.Fprint(os.Stderr,"DATA_DIRECTORY_LOCKED");os.Exit(73)}
case "migrate": data,e:=os.ReadFile(os.Args[3]);if e!=nil{os.Exit(2)};data=[]byte(strings.Replace(string(data),"\"schemaVersion\":1","\"schemaVersion\":2",1));if mutate{data=[]byte(strings.Replace(string(data),"never-live","mutated-data",1))};if os.WriteFile(os.Args[3],data,0600)!=nil{os.Exit(2)}
case "migrate-failure": fmt.Fprint(os.Stderr,"INJECTED_MIGRATION_FAILURE");os.Exit(74)
case "protocol-client": json.NewEncoder(os.Stdout).Encode(map[string]any{"runId":run,"buildSha256":sha,"workingDirectory":work,"wireVersion":7,"request":map[string]any{"type":"ping","client":role,"nonce":value("--nonce")}})
case "protocol-server": if ignore{nonce,requestSHA:=value("--nonce"),value("--request-sha");json.NewEncoder(os.Stdout).Encode(map[string]any{"runId":run,"buildSha256":sha,"workingDirectory":work,"wireVersion":7,"requestSha256":requestSHA,"requestNonce":nonce,"response":map[string]any{"type":"pong","server":role,"correlationId":nonce}});break};request,e:=os.ReadFile(os.Args[3]);if e!=nil{os.Exit(2)};sum:=sha256.Sum256(request);var envelope map[string]any;if json.Unmarshal(request,&envelope)!=nil{os.Exit(2)};nonce,_:=envelope["nonce"].(string);requestSHA:=fmt.Sprintf("%%x",sum);json.NewEncoder(os.Stdout).Encode(map[string]any{"runId":run,"buildSha256":sha,"workingDirectory":work,"wireVersion":7,"requestSha256":requestSHA,"requestNonce":nonce,"response":map[string]any{"type":"pong","server":role,"correlationId":nonce}})
case "rollback": data,e:=os.ReadFile(os.Args[4]);if e!=nil{os.Exit(2)};if os.WriteFile(os.Args[3],data,0700)!=nil{os.Exit(2)}
case "reopen": if _,e:=os.ReadFile(os.Args[3]);e!=nil{os.Exit(2)}
default: os.Exit(2)}}`, build.role, build.mutate, build.ignore)
		if err := os.WriteFile(source, []byte(program), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", build.output, source)
		command.Env = append(os.Environ(), "CGO_ENABLED=0")
		if data, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build upgrade probe: %v: %s", err, data)
		}
	}
	return baseline, candidate
}

func TestCompareResultsRequiresPairingAndBudgets(t *testing.T) {
	budgets := Budgets{ExpectedBaselineBuild: "rust", ExpectedCandidateBuild: "go", MaxStartupRegressionPercent: 10, MaxHydrationRegressionPercent: 10, MaxInputRegressionPercent: 10, MaxFrameP95RegressionPercent: 10, MaxCPURegressionPercent: 10, MaxRSSRegressionPercent: 10, MaxFrameP95MS: 2}
	env := Environment{CapturedAt: time.Now(), Commit: "a", Build: "rust", GOOS: "linux", GOARCH: "amd64", Machine: "m", CPU: "c", LogicalCPUs: 8, MemoryBytes: 16 << 30, Display: "d", RefreshHz: 60, Scale: 1, Renderer: "gpu", Instrumentation: "v1"}
	work := Workload{ID: "long-transcript", FixtureSHA256: KnownWorkloadDigests["long-transcript"], Warmups: 1, Iterations: 5}
	metric := Distribution{Count: 5, P50: 1, P95: 1, P99: 1, Max: 1}
	base := NewResult(env, work)
	base.Metrics = Metrics{StartupMS: metric, HydrationMS: metric, InputToPresentMS: metric, FrameWorkMS: metric, CPUPercent: metric, RSSBytes: metric}
	base.Checks = completeBaselineChecks()
	artifacts := writeBaselineArtifacts(t, &base)
	candidate := base
	candidate.Environment.Build = "go"
	comparison, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts)
	if err != nil || comparison.Status != StatusPass || len(comparison.Checks) == 0 || comparison.Checks[0].Name != "baseline evidence gate" || comparison.Checks[0].Status != StatusPass {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
	spliced := base
	spliced.Metrics.StartupMS.P50 = 0
	if _, err := CompareResultsWithArtifacts(spliced, candidate, budgets, artifacts); err == nil {
		t.Fatal("edited low percentile passed raw replay")
	}
	fabricated := base
	fabricated.Checks = append([]Check(nil), base.Checks...)
	fabricated.Checks[0].Evidence = "fabricated pass"
	if _, err := CompareResultsWithArtifacts(fabricated, candidate, budgets, artifacts); err == nil {
		t.Fatal("fabricated checks passed raw replay")
	}
	candidate.Environment.Machine = "other"
	if _, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts); err == nil {
		t.Fatal("expected machine pairing rejection")
	}
	candidate = base
	candidate.Environment.Build = "go"
	candidate.Environment.Instrumentation = "v2"
	if _, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts); err == nil {
		t.Fatal("expected instrumentation pairing rejection")
	}
	candidate = base
	candidate.Environment.Build = "go"
	candidate.Environment.MemoryBytes++
	if _, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts); err == nil {
		t.Fatal("expected memory pairing rejection")
	}
	if _, err := CompareResultsWithArtifacts(base, base, budgets, artifacts); err == nil {
		t.Fatal("expected identical build identity rejection")
	}
	unexpected := candidate
	unexpected.Environment.Build = "other"
	if _, err := CompareResultsWithArtifacts(base, unexpected, budgets, artifacts); err == nil {
		t.Fatal("expected unanticipated build identity rejection")
	}
	candidate = base
	candidate.Environment.Build = "go"
	candidate.Metrics.FrameWorkMS.P95 = 3
	candidate.Metrics.FrameWorkMS.P99 = 3
	candidate.Metrics.FrameWorkMS.Max = 3
	comparison, err = CompareResultsWithArtifacts(base, candidate, budgets, artifacts)
	if err != nil || comparison.Status != StatusFail {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
	if _, err := CompareResults(base, candidate, budgets); err == nil {
		t.Fatal("result JSON digests passed without raw artifacts")
	}
	eventBytes, _ := os.ReadFile(artifacts.EventsPath)
	sampleBytes, _ := os.ReadFile(artifacts.SamplesPath)
	eventBytes = bytes.ReplaceAll(eventBytes, []byte(`"iteration":4`), []byte(`"iteration":7`))
	sampleBytes = bytes.ReplaceAll(sampleBytes, []byte(`"iteration":4`), []byte(`"iteration":7`))
	_ = os.WriteFile(artifacts.EventsPath, eventBytes, 0600)
	_ = os.WriteFile(artifacts.SamplesPath, sampleBytes, 0600)
	noncontiguous := base
	noncontiguous.Evidence = RawEvidence{EventsSHA256: digestDataBytes(eventBytes), SamplesSHA256: digestDataBytes(sampleBytes)}
	if _, err := CompareResultsWithArtifacts(noncontiguous, candidate, budgets, artifacts); err == nil {
		t.Fatal("noncontiguous iteration indices passed raw replay")
	}
	if err := os.WriteFile(artifacts.EventsPath, []byte("tampered\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts); err == nil {
		t.Fatal("tampered raw baseline artifact passed")
	}
}

func TestCompareResultsCarriesAndEnforcesBaselineGate(t *testing.T) {
	budgets := Budgets{ExpectedBaselineBuild: "rust", ExpectedCandidateBuild: "go", MaxFrameP95MS: 16}
	env := Environment{CapturedAt: time.Now(), Commit: "a", Build: "rust", GOOS: "linux", GOARCH: "amd64", Machine: "m", CPU: "c", LogicalCPUs: 8, MemoryBytes: 1, Display: "d", RefreshHz: 60, Scale: 1, Renderer: "r", Instrumentation: "v1"}
	work := Workload{ID: "long-transcript", FixtureSHA256: KnownWorkloadDigests["long-transcript"], Warmups: 1, Iterations: 5}
	base := NewResult(env, work)
	base.Checks = completeBaselineChecks()
	artifacts := writeBaselineArtifacts(t, &base)
	base.Checks[0] = Check{Name: requiredBaselineChecks[0], Status: StatusBlocked, Blocker: "runner unavailable"}
	candidate := base
	candidate.Environment.Build = "go"
	comparison, err := CompareResultsWithArtifacts(base, candidate, budgets, artifacts)
	if err != nil || comparison.Status != StatusBlocked || len(comparison.Checks) != 1 || comparison.Checks[0].Name != "baseline evidence gate" {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
	base.Checks = []Check{{Name: "arbitrary", Status: StatusPass, Evidence: "not sufficient"}}
	comparison, err = CompareResultsWithArtifacts(base, candidate, budgets, artifacts)
	if err != nil || comparison.Status != StatusBlocked {
		t.Fatalf("incomplete baseline gate passed: %+v err=%v", comparison, err)
	}
	base.Checks = completeBaselineChecks()
	base.Checks[0] = Check{Name: requiredBaselineChecks[0], Status: StatusFail, Evidence: "corrupt"}
	comparison, err = CompareResultsWithArtifacts(base, candidate, budgets, artifacts)
	if err != nil || comparison.Status != StatusFail || comparison.Checks[0].Status != StatusFail {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
}

func completeBaselineChecks() []Check {
	checks := make([]Check, 0, len(requiredBaselineChecks))
	for _, name := range requiredBaselineChecks {
		checks = append(checks, Check{Name: name, Status: StatusPass, Evidence: "captured"})
	}
	return checks
}

func writeBaselineArtifacts(t *testing.T, result *Result) BaselineArtifacts {
	t.Helper()
	var eventData, sampleData bytes.Buffer
	eventEncoder, sampleEncoder := json.NewEncoder(&eventData), json.NewEncoder(&sampleData)
	for sequence := 0; sequence < result.Workload.Warmups+result.Workload.Iterations; sequence++ {
		warmup := sequence < result.Workload.Warmups
		iteration := sequence
		if warmup {
			iteration = 0
		} else {
			iteration -= result.Workload.Warmups
		}
		runID := fmt.Sprintf("raw-%t-%d", warmup, iteration)
		base := int64(sequence+1) * 1_000_000_000
		events := []Event{
			{Name: EventProcessStart, CorrelationID: "startup", AtNS: base}, {Name: EventInteractive, CorrelationID: "startup", AtNS: base + 1},
			{Name: EventHydrateStart, CorrelationID: "hydrate", AtNS: base + 2}, {Name: EventHydrateReady, CorrelationID: "hydrate", AtNS: base + 3},
			{Name: EventInput, CorrelationID: "input", AtNS: base + 4}, {Name: EventPaint, CorrelationID: "input", AtNS: base + 5}, {Name: EventPresent, CorrelationID: "input", AtNS: base + 6},
			{Name: EventFrameWork, AtNS: base + 7, Value: 1}, {Name: EventFramePresented, AtNS: base + 8, Value: 1},
			{Name: EventReconnectStart, CorrelationID: "reconnect", AtNS: base + 9}, {Name: EventReconnectReady, CorrelationID: "reconnect", AtNS: base + 10},
			{Name: EventShutdownStart, CorrelationID: "shutdown", AtNS: base + 11}, {Name: EventProcessExit, CorrelationID: "shutdown", AtNS: base + 12},
		}
		for _, event := range events {
			event.Version, event.RunID, event.Build, event.WorkloadID, event.FixtureSHA256, event.Iteration, event.Warmup = SchemaVersion, runID, result.Environment.Build, result.Workload.ID, result.Workload.FixtureSHA256, iteration, warmup
			if err := eventEncoder.Encode(event); err != nil {
				t.Fatal(err)
			}
		}
		for window := 0; window < 2; window++ {
			sample := Sample{Version: SchemaVersion, RunID: runID, Build: result.Environment.Build, WorkloadID: result.Workload.ID, FixtureSHA256: result.Workload.FixtureSHA256, Iteration: iteration, Warmup: warmup, WindowStartNS: int64(window), WindowEndNS: int64(window + 1), CPUTimeNS: uint64(window), IdleWakeups: uint64(window), RSSBytes: 1, HeapBytes: 1, AllocationBytes: uint64(window), Allocations: uint64(window)}
			if err := sampleEncoder.Encode(sample); err != nil {
				t.Fatal(err)
			}
		}
	}
	root := t.TempDir()
	eventsPath, samplesPath := filepath.Join(root, "events.ndjson"), filepath.Join(root, "samples.ndjson")
	if err := os.WriteFile(eventsPath, eventData.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(samplesPath, sampleData.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	result.Evidence = RawEvidence{EventsSHA256: digestDataBytes(eventData.Bytes()), SamplesSHA256: digestDataBytes(sampleData.Bytes())}
	events, err := DecodeEvents(bytes.NewReader(eventData.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	metrics, _, err := MetricsFromEvents(events, result.Environment.Build, result.Workload.ID, result.Workload.FixtureSHA256)
	if err != nil {
		t.Fatal(err)
	}
	samples, err := DecodeSamples(bytes.NewReader(sampleData.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AddSamples(&metrics, samples, result.Environment.Build, result.Workload.ID, result.Workload.FixtureSHA256); err != nil {
		t.Fatal(err)
	}
	result.Metrics = metrics
	result.Checks = InstrumentationChecks(metrics, result.Workload.Iterations)
	return BaselineArtifacts{EventsPath: eventsPath, SamplesPath: samplesPath}
}

func TestResultRejectsInvalidDistributionShapes(t *testing.T) {
	base := Result{SchemaVersion: 1, Environment: Environment{CapturedAt: time.Now(), Commit: "c", Build: "b", GOOS: "linux", GOARCH: "amd64", Machine: "m", CPU: "c", LogicalCPUs: 1, MemoryBytes: 1, Display: "d", RefreshHz: 60, Scale: 1, Renderer: "r", Instrumentation: "v1"}, Workload: Workload{ID: "long-transcript", FixtureSHA256: KnownWorkloadDigests["long-transcript"], Warmups: 1, Iterations: 5}}
	for name, distribution := range map[string]Distribution{
		"negative count": {Count: -1},
		"negative":       {Count: 1, P50: -1},
		"not finite":     {Count: 1, P50: 1, P95: 1, P99: 1, Max: math.Inf(1)},
		"nan":            {Count: 1, P50: math.NaN(), P95: 1, P99: 1, Max: 1},
		"out of order":   {Count: 1, P50: 2, P95: 1, P99: 2, Max: 2},
		"zero mismatch":  {Count: 0, Max: 1},
	} {
		t.Run(name, func(t *testing.T) {
			result := base
			result.Metrics.StartupMS = distribution
			if err := result.Validate(); err == nil {
				t.Fatal("expected invalid distribution rejection")
			}
		})
	}
}

func TestResultRejectsSingleSampleWorkload(t *testing.T) {
	r := Result{SchemaVersion: 1, Environment: Environment{CapturedAt: time.Now(), Commit: "c", Build: "b", GOOS: "linux", GOARCH: "amd64", Machine: "m", CPU: "c", LogicalCPUs: 8, MemoryBytes: 16 << 30, Display: "d", RefreshHz: 60, Scale: 1, Renderer: "r", Instrumentation: "v1"}, Workload: Workload{ID: "long-transcript", FixtureSHA256: KnownWorkloadDigests["long-transcript"], Warmups: 1, Iterations: 1}}
	if err := r.Validate(); err == nil {
		t.Fatal("expected iteration floor rejection")
	}
}

// This unit fixture exercises policy mechanics only. It is not upgrade,
// interoperability, or real-command release evidence.
func TestUnitOnlyGenerateDistributionManifestIsReproducibleAndInspectable(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"bin", "share/licenses/waku", "share/notices/waku"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
	}
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"waku-go", "waku-go-daemon"} {
		role := "desktop"
		if name == "waku-go-daemon" {
			role = "daemon"
		}
		command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-ldflags", "-X main.releaseRole="+role, "-o", filepath.Join(root, "bin", name), "./benchmarks/fixturecmd")
		command.Dir = moduleRoot
		command.Env = append(os.Environ(), "CGO_ENABLED=0")
		if data, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build manifest fixture: %v: %s", err, data)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "share/licenses/waku/LICENSE"), []byte("license"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "share/notices/waku/NOTICE"), []byte("notice"), 0600); err != nil {
		t.Fatal(err)
	}
	sbomPath := filepath.Join(root, "share/sbom.json")
	if err := os.WriteFile(sbomPath, []byte(`{"schemaVersion":1,"assets":[],"dependencies":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	sbomDigest, _ := fileSHA256(sbomPath)
	spec := ManifestSpec{ApplicationExecutables: []string{"bin/waku-go-daemon", "bin/waku-go"}, TargetPlatform: "linux", ApplicationRoles: map[string]string{"bin/waku-go": "desktop", "bin/waku-go-daemon": "daemon"}, LicensePaths: []string{"share/licenses/waku/LICENSE"}, NoticePaths: []string{"share/notices/waku/NOTICE"}, ReviewedSBOMPath: "share/sbom.json", ReviewedSBOMSHA256: sbomDigest}
	first, err := GenerateDistributionManifest(root, spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateDistributionManifest(root, spec)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatal("manifest generation is not reproducible")
	}
	if report := InspectDistribution(root, first); report.Status != StatusPass {
		t.Fatalf("generated manifest failed inspection: %+v", report)
	}
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifestPath, a, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDistributionManifest(manifestPath); err != nil {
		t.Fatal(err)
	}
	withoutSBOM := spec
	withoutSBOM.ReviewedSBOMPath, withoutSBOM.ReviewedSBOMSHA256 = "", ""
	if _, err := GenerateDistributionManifest(root, withoutSBOM); err == nil {
		t.Fatal("distribution without full dependency SBOM passed")
	}
	licensePath := filepath.Join(root, "share/licenses/waku/LICENSE")
	if err := os.WriteFile(licensePath, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if report := InspectDistribution(root, first); report.Status != StatusFail {
		t.Fatal("tampered license content passed")
	}
	if err := os.WriteFile(licensePath, []byte("license"), 0600); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(t.TempDir(), "waku-linux.tar.gz")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(archiveFile)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, relative := range []string{"bin/waku-go", "bin/waku-go-daemon", "share/licenses/waku/LICENSE", "share/notices/waku/NOTICE", "share/sbom.json"} {
		path := filepath.Join(root, filepath.FromSlash(relative))
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: relative, Mode: int64(info.Mode().Perm()), Size: info.Size(), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		input, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(tarWriter, input)
		closeErr := input.Close()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatal(err)
	}
	archiveDigest, err := fileSHA256(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archiveReport, err := InspectDistributionArchive(archivePath, archiveDigest, first)
	if err != nil || archiveReport.Status != StatusPass {
		t.Fatalf("archive inspection = %+v, %v", archiveReport, err)
	}
}

func TestArchiveInspectionRejectsSymlinkSource(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.tar.gz")
	_ = os.WriteFile(target, []byte("not relevant"), 0600)
	link := filepath.Join(root, "link.tar.gz")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	if _, err := InspectDistributionArchive(link, strings.Repeat("0", 64), DistributionManifest{TargetPlatform: "linux"}); err == nil || !strings.Contains(err.Error(), "non-symlink") {
		t.Fatalf("symlink archive source passed: %v", err)
	}
}

func TestDistributionRejectsEveryUnclassifiedPayloadKind(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"unknown", "app.asar", "module.wasm", "node"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	report := InspectDistribution(root, DistributionManifest{})
	classified := 0
	for _, issue := range report.Issues {
		if strings.Contains(issue.Message, "not exhaustively classified") {
			classified++
		}
	}
	if classified != 4 {
		t.Fatalf("unclassified payloads escaped: %+v", report.Issues)
	}
}

func TestRequiredDataCannotDisguiseRuntimePayloadOrSymlink(t *testing.T) {
	root := t.TempDir()
	runtimePath := filepath.Join(root, "app.asar")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(root, "linked-data")
	if err := os.Symlink(runtimePath, linkPath); err != nil {
		t.Skip(err)
	}
	report := InspectDistribution(root, DistributionManifest{RequiredPaths: []string{"app.asar", "linked-data"}})
	wantRuntime, wantSymlink := false, false
	for _, issue := range report.Issues {
		wantRuntime = wantRuntime || issue.Path == "app.asar" && strings.Contains(issue.Message, "runtime asset")
		wantSymlink = wantSymlink || issue.Path == "linked-data" && strings.Contains(issue.Message, "symlinks are not permitted")
	}
	if !wantRuntime || !wantSymlink {
		t.Fatalf("required-path bypass passed: %+v", report.Issues)
	}
}

func TestReviewedSBOMBindsDependencySum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sbom.json")
	if err := os.WriteFile(path, []byte(`{"schemaVersion":1,"assets":[],"dependencies":[{"path":"example.org/dependency","version":"v1.0.0","sum":"h1:attacker"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	err := verifyReviewedSBOM(path, nil, "linux", []sbomDependency{{Path: "example.org/dependency", Version: "v1.0.0", Sum: "h1:trusted"}})
	if err == nil {
		t.Fatal("SBOM with altered dependency sum passed")
	}
}

func TestMacPackagingRequiresInfoPlist(t *testing.T) {
	report := InspectionReport{Status: StatusPass}
	inspectPackagingPolicy(t.TempDir(), map[string]string{}, DistributionManifest{TargetPlatform: "darwin"}, &report)
	found := false
	for _, issue := range report.Issues {
		if issue.Path == "Waku.app/Contents/Info.plist" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing macOS Info.plist was not reported: %+v", report)
	}
}

func TestReleasePackagingLayouts(t *testing.T) {
	cases := map[string][]string{"linux": {"bin/waku-go", "bin/waku-go-daemon", ".tar.gz"}, "darwin": {"Waku.app/Contents/MacOS/waku-go", "Waku.app/Contents/MacOS/waku-go-daemon", ".zip"}, "windows": {"Waku/waku-go.exe", "Waku/waku-go-daemon.exe", ".zip"}}
	for platform, want := range cases {
		layout, err := ReleasePackagingLayout(platform)
		if err != nil {
			t.Fatal(err)
		}
		if layout.DesktopExecutable != want[0] || layout.DaemonExecutable != want[1] || layout.ArchiveExtension != want[2] || layout.LicenseDirectory == "" || layout.NoticeDirectory == "" {
			t.Fatalf("%s layout=%+v", platform, layout)
		}
	}
	if _, err := ReleasePackagingLayout("plan9"); err == nil {
		t.Fatal("unsupported packaging platform passed")
	}
}

func TestGenerateManifestRejectsSwappedCommandRoles(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"bin", "share/licenses/waku", "share/notices/waku"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
	}
	moduleRoot, _ := filepath.Abs(filepath.Join("..", ".."))
	for _, name := range []string{"waku-go", "waku-go-daemon"} {
		role := "desktop"
		if name == "waku-go-daemon" {
			role = "daemon"
		}
		command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-ldflags", "-X main.releaseRole="+role, "-o", filepath.Join(root, "bin", name), "./benchmarks/fixturecmd")
		command.Dir, command.Env = moduleRoot, append(os.Environ(), "CGO_ENABLED=0")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build: %v: %s", err, output)
		}
	}
	_ = os.WriteFile(filepath.Join(root, "share/licenses/waku/LICENSE"), []byte("license"), 0600)
	_ = os.WriteFile(filepath.Join(root, "share/notices/waku/NOTICE"), []byte("notice"), 0600)
	spec := ManifestSpec{TargetPlatform: "linux", ApplicationExecutables: []string{"bin/waku-go", "bin/waku-go-daemon"}, ApplicationRoles: map[string]string{"bin/waku-go": "daemon", "bin/waku-go-daemon": "desktop"}, LicensePaths: []string{"share/licenses/waku/LICENSE"}, NoticePaths: []string{"share/notices/waku/NOTICE"}}
	if _, err := GenerateDistributionManifest(root, spec); err == nil || !strings.Contains(err.Error(), "role") {
		t.Fatalf("swapped roles passed: %v", err)
	}
}

func TestInspectDistributionArchiveBindsDigestExtensionAndRejectsLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "waku-linux.tar.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tr := tar.NewWriter(gz)
	_ = tr.WriteHeader(&tar.Header{Name: "bin/waku-go", Typeflag: tar.TypeSymlink, Linkname: "waku-go-daemon"})
	_ = tr.WriteHeader(&tar.Header{Name: "bin/waku-go-daemon", Typeflag: tar.TypeReg, Mode: 0755, Size: 1})
	_, _ = tr.Write([]byte("x"))
	_ = tr.Close()
	_ = gz.Close()
	_ = f.Close()
	digest, _ := fileSHA256(path)
	manifest := DistributionManifest{TargetPlatform: "linux"}
	if _, err := InspectDistributionArchive(path, strings.Repeat("0", 64), manifest); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("digest bypass passed: %v", err)
	}
	if _, err := InspectDistributionArchive(path, digest, manifest); err == nil || !strings.Contains(err.Error(), "link") {
		t.Fatalf("link extraction passed: %v", err)
	}
	if _, err := InspectDistributionArchive(strings.TrimSuffix(path, ".tar.gz")+".zip", digest, manifest); err == nil {
		t.Fatal("missing/wrong archive passed")
	}
}

func TestManifestRejectsFreeFormRetainedProvenanceAndBrokenLicenses(t *testing.T) {
	hex := strings.Repeat("a", 64)
	base := DistributionManifest{SchemaVersion: 1, TargetPlatform: "linux", ApplicationExecutables: []string{"bin/waku-go", "bin/waku-go-daemon"}, ApplicationMetadata: []ApplicationMetadata{{Path: "bin/waku-go", SHA256: hex, GoVersion: "go1", ModulePath: "m", CGOEnabled: "0", Role: "desktop", RoleProbeSHA256: hex}, {Path: "bin/waku-go-daemon", SHA256: hex, GoVersion: "go1", ModulePath: "m", CGOEnabled: "0", Role: "daemon", RoleProbeSHA256: hex}}, RetainedAssets: []RetainedAsset{{Path: "share/sdk.so", Purpose: "sdk", Source: "free form", Platforms: []string{"linux"}, LicensePath: "share/licenses/waku/SDK", SHA256: hex, Identity: "sdk", Version: "1.0", SourceSHA256: hex}}, LicensePaths: []string{"share/licenses/waku/SDK"}, NoticePaths: []string{"share/notices/waku/NOTICE"}, NoApplicationRustRuntime: true, NoApplicationJSRuntime: true, ReviewedSBOMPath: "share/sbom.json", ReviewedSBOMSHA256: hex}
	load := func(manifest DistributionManifest) error {
		path := filepath.Join(t.TempDir(), "manifest.json")
		data, _ := json.Marshal(manifest)
		_ = os.WriteFile(path, data, 0600)
		_, err := LoadDistributionManifest(path)
		return err
	}
	if err := load(base); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("free-form source passed: %v", err)
	}
	base.RetainedAssets[0].Source = "https://example.invalid/sdk-1.0.tar.gz"
	base.RetainedAssets[0].Platforms = []string{"solaris"}
	if err := load(base); err == nil || !strings.Contains(err.Error(), "platform") {
		t.Fatalf("invalid platform passed: %v", err)
	}
	base.RetainedAssets[0].Platforms = []string{"linux"}
	base.RetainedAssets[0].LicensePath = "undeclared"
	if err := load(base); err == nil || !strings.Contains(err.Error(), "licensePath") {
		t.Fatalf("undeclared license passed: %v", err)
	}
	base.RetainedAssets[0].LicensePath = base.LicensePaths[0]
	base.LicensePaths = append(base.LicensePaths, base.LicensePaths[0])
	if err := load(base); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate license passed: %v", err)
	}
}
