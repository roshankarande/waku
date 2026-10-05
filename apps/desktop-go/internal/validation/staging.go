package validation

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ReleaseStageSpec is an intentionally explicit input to the unsigned release
// staging step. Nothing is discovered from package caches: every non-Waku
// payload and every license must have a reviewed identity and source digest.
type ReleaseStageSpec struct {
	Platform           string                   `json:"platform"`
	DesktopExecutable  string                   `json:"desktopExecutable"`
	DaemonExecutable   string                   `json:"daemonExecutable"`
	WakuLicense        string                   `json:"wakuLicense"`
	MyGoLicense        string                   `json:"mygoLicense"`
	GoLicense          string                   `json:"goLicense"`
	Metadata           []StageFile              `json:"metadata"`
	RetainedAssets     []StageRetainedAsset     `json:"retainedAssets"`
	DependencyLicenses []StageDependencyLicense `json:"dependencyLicenses"`
}

type StageFile struct {
	Kind       string `json:"kind"`
	SourcePath string `json:"sourcePath"`
	Path       string `json:"path"`
}

type StageRetainedAsset struct {
	Kind              string   `json:"kind"`
	SourcePath        string   `json:"sourcePath"`
	Path              string   `json:"path"`
	Purpose           string   `json:"purpose"`
	Source            string   `json:"source"`
	Platforms         []string `json:"platforms"`
	LicensePath       string   `json:"licensePath"`
	LicenseSourcePath string   `json:"licenseSourcePath"`
	Identity          string   `json:"identity"`
	Version           string   `json:"version"`
	SourceSHA256      string   `json:"sourceSha256"`
}

type StageDependencyLicense struct {
	Path        string `json:"path"`
	Version     string `json:"version"`
	SourcePath  string `json:"sourcePath"`
	LicensePath string `json:"licensePath"`
}

type ReleaseStageReport struct {
	SchemaVersion int                   `json:"schemaVersion"`
	Status        Status                `json:"status"`
	StageRoot     string                `json:"stageRoot,omitempty"`
	ArchivePath   string                `json:"archivePath,omitempty"`
	ArchiveSHA256 string                `json:"archiveSha256,omitempty"`
	Manifest      *DistributionManifest `json:"manifest,omitempty"`
	Checks        []Check               `json:"checks"`
}

const (
	RetainedGhosttyNative = "ghostty-native"
	RetainedCuaNative     = "cua-native"
	RetainedCuaSupport    = "cua-support"
	RetainedJavaScript    = "javascript-asset"
)

// StageUnsignedGoRelease creates an inspection-ready archive, never a release
// approval. Signing/notarization/installer verification and a clean-machine
// launch are deliberately outside this unsigned host-local operation.
func StageUnsignedGoRelease(destination, archivePath string, spec ReleaseStageSpec) (ReleaseStageReport, error) {
	report := ReleaseStageReport{SchemaVersion: SchemaVersion, Status: StatusFail}
	layout, err := ReleasePackagingLayout(spec.Platform)
	if err != nil {
		return report, err
	}
	if destination == "" || archivePath == "" {
		return report, errors.New("destination and archive path required")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return report, errors.New("stage destination must not exist")
	}
	if _, err := os.Lstat(archivePath); !errors.Is(err, os.ErrNotExist) {
		return report, errors.New("archive destination must not exist")
	}
	if !strings.HasSuffix(strings.ToLower(archivePath), layout.ArchiveExtension) {
		return report, errors.New("archive extension does not match platform")
	}

	parent, err := filepath.Abs(filepath.Dir(destination))
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(parent, 0755); err != nil {
		return report, err
	}
	tmp, err := os.MkdirTemp(parent, ".waku-release-stage-")
	if err != nil {
		return report, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(tmp)
		}
	}()

	copyInput := func(source, relative string, mode os.FileMode) error {
		if _, err := cleanStageRelative(relative); err != nil {
			return err
		}
		in, err := os.Open(source)
		if err != nil {
			return err
		}
		defer in.Close()
		info, err := in.Stat()
		if err != nil {
			return err
		}
		linfo, err := os.Lstat(source)
		if err != nil || !info.Mode().IsRegular() || linfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("stage input must be a regular non-symlink file")
		}
		outPath := filepath.Join(tmp, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return err
		}
		out, err := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}
	if err := copyInput(spec.DesktopExecutable, layout.DesktopExecutable, 0755); err != nil {
		return report, fmt.Errorf("desktop: %w", err)
	}
	if err := copyInput(spec.DaemonExecutable, layout.DaemonExecutable, 0755); err != nil {
		return report, fmt.Errorf("daemon: %w", err)
	}

	required := []string{}
	metadataKinds := map[string]bool{}
	for _, file := range spec.Metadata {
		if file.Kind == "" || metadataKinds[file.Kind] {
			return report, errors.New("platform metadata kinds must be non-empty and unique")
		}
		metadataKinds[file.Kind] = true
		if err := copyInput(file.SourcePath, file.Path, 0644); err != nil {
			return report, fmt.Errorf("metadata %s: %w", file.Path, err)
		}
		required = append(required, file.Path)
	}
	if err := validateStageMetadata(spec.Platform, spec.Metadata, layout); err != nil {
		return report, err
	}
	for _, kind := range requiredMetadataKinds(spec.Platform) {
		if !metadataKinds[kind] {
			return report, fmt.Errorf("platform metadata kind %s is required", kind)
		}
	}
	if layout.InfoPlist != "" && !containsString(required, layout.InfoPlist) {
		return report, errors.New("macOS staging requires the target Info.plist metadata path")
	}

	licenses := []string{}
	copyLicense := func(source, relative string) error {
		if err := copyInput(source, relative, 0644); err != nil {
			return err
		}
		licenses = append(licenses, relative)
		return nil
	}
	wakuLicense := layout.LicenseDirectory + "/WAKU-LICENSE"
	mygoLicense := layout.LicenseDirectory + "/MYGO-LICENSE"
	goLicense := layout.LicenseDirectory + "/GO-LICENSE"
	if err := copyLicense(spec.WakuLicense, wakuLicense); err != nil {
		return report, fmt.Errorf("Waku license: %w", err)
	}
	if err := copyLicense(spec.MyGoLicense, mygoLicense); err != nil {
		return report, fmt.Errorf("MyGo license: %w", err)
	}
	if err := copyLicense(spec.GoLicense, goLicense); err != nil {
		return report, fmt.Errorf("Go toolchain license: %w", err)
	}

	assetKinds := map[string]int{}
	retained := make([]RetainedAsset, 0, len(spec.RetainedAssets))
	for _, asset := range spec.RetainedAssets {
		switch asset.Kind {
		case RetainedGhosttyNative, RetainedCuaNative, RetainedCuaSupport, RetainedJavaScript:
		default:
			return report, fmt.Errorf("%s: unsupported retained asset kind", asset.Path)
		}
		if !containsString(asset.Platforms, spec.Platform) {
			return report, fmt.Errorf("%s: retained asset does not declare target platform", asset.Path)
		}
		lowerPath := strings.ToLower(asset.Path)
		if (asset.Kind == RetainedGhosttyNative || asset.Kind == RetainedCuaNative) && !isNativeLibrary(lowerPath) {
			return report, fmt.Errorf("%s: native retained asset kind requires a shared-library path", asset.Path)
		}
		if asset.Kind == RetainedJavaScript && !(strings.HasSuffix(lowerPath, ".js") || strings.HasSuffix(lowerPath, ".mjs") || strings.HasSuffix(lowerPath, ".cjs") || strings.HasSuffix(lowerPath, ".ts")) {
			return report, fmt.Errorf("%s: JavaScript retained asset kind requires a JS/TS path", asset.Path)
		}
		if asset.LicenseSourcePath == "" || asset.LicensePath == "" {
			return report, fmt.Errorf("%s: retained asset license required", asset.Path)
		}
		if err := copyInput(asset.SourcePath, asset.Path, 0644); err != nil {
			return report, fmt.Errorf("retained asset %s: %w", asset.Path, err)
		}
		if !containsString(licenses, asset.LicensePath) {
			if err := copyLicense(asset.LicenseSourcePath, asset.LicensePath); err != nil {
				return report, fmt.Errorf("retained license %s: %w", asset.LicensePath, err)
			}
		} else {
			want, e1 := fileSHA256(asset.LicenseSourcePath)
			got, e2 := fileSHA256(filepath.Join(tmp, filepath.FromSlash(asset.LicensePath)))
			if e1 != nil || e2 != nil || want != got {
				return report, fmt.Errorf("%s: shared license path has different content", asset.Path)
			}
		}
		retained = append(retained, RetainedAsset{Path: asset.Path, Purpose: asset.Kind + ": " + asset.Purpose, Source: asset.Source, Platforms: append([]string(nil), asset.Platforms...), LicensePath: asset.LicensePath, Identity: asset.Identity, Version: asset.Version, SourceSHA256: asset.SourceSHA256})
		assetKinds[asset.Kind]++
	}
	for _, kind := range []string{RetainedGhosttyNative, RetainedCuaNative, RetainedJavaScript} {
		if assetKinds[kind] == 0 {
			return report, fmt.Errorf("required retained asset kind %s is absent", kind)
		}
	}

	deps := executableDependencyUnion(filepath.Join(tmp, filepath.FromSlash(layout.DesktopExecutable)), filepath.Join(tmp, filepath.FromSlash(layout.DaemonExecutable)))
	licenseInputs := map[string]StageDependencyLicense{}
	for _, item := range spec.DependencyLicenses {
		key := item.Path + "@" + item.Version
		if _, exists := licenseInputs[key]; exists {
			return report, errors.New("duplicate dependency license " + key)
		}
		licenseInputs[key] = item
	}
	for _, dep := range deps {
		key := dep.Path + "@" + dep.Version
		item, ok := licenseInputs[key]
		if !ok {
			return report, fmt.Errorf("dependency license is missing for %s", key)
		}
		if item.LicensePath == "" || !strings.HasPrefix(item.LicensePath, layout.LicenseDirectory+"/") {
			return report, fmt.Errorf("dependency license path is outside platform license directory for %s", key)
		}
		if containsString(licenses, item.LicensePath) {
			want, e1 := fileSHA256(item.SourcePath)
			got, e2 := fileSHA256(filepath.Join(tmp, filepath.FromSlash(item.LicensePath)))
			if e1 != nil || e2 != nil || want != got {
				return report, fmt.Errorf("dependency license %s conflicts with an existing license", key)
			}
		} else if err := copyLicense(item.SourcePath, item.LicensePath); err != nil {
			return report, fmt.Errorf("dependency license %s: %w", key, err)
		}
		delete(licenseInputs, key)
	}
	if len(licenseInputs) != 0 {
		return report, errors.New("dependency license input contains entries absent from executable build metadata")
	}

	mygo, ok := findMyGoDependency(deps)
	if !ok {
		return report, errors.New("Go build metadata does not contain the pinned MyGo dependency")
	}
	noticePath := layout.NoticeDirectory + "/MYGO-NOTICE"
	notice := fmt.Sprintf("MyGo native UI dependency\nmodule: %s\nversion: %s\nsum: %s\nsource: https://github.com/egoist/mygo\n", mygo.Path, mygo.Version, mygo.Sum)
	if err := writeStageFile(tmp, noticePath, []byte(notice), 0644); err != nil {
		return report, err
	}

	sbomPath := layout.NoticeDirectory + "/go-sbom.json"
	sort.Slice(retained, func(i, j int) bool { return retained[i].Path < retained[j].Path })
	for i := range retained {
		retained[i].SHA256, err = fileSHA256(filepath.Join(tmp, filepath.FromSlash(retained[i].Path)))
		if err != nil {
			return report, err
		}
		sort.Strings(retained[i].Platforms)
	}
	applications := make([]sbomApplication, 0, 2)
	toolchainVersions := map[string]bool{}
	for _, pair := range []struct{ relative, path string }{
		{layout.DesktopExecutable, filepath.Join(tmp, filepath.FromSlash(layout.DesktopExecutable))},
		{layout.DaemonExecutable, filepath.Join(tmp, filepath.FromSlash(layout.DaemonExecutable))},
	} {
		metadata, metadataErr := applicationMetadata(pair.relative, pair.path)
		if metadataErr != nil {
			return report, metadataErr
		}
		applications = append(applications, sbomApplication{Path: metadata.Path, ModulePath: metadata.ModulePath, ModuleVersion: metadata.ModuleVersion, GoVersion: metadata.GoVersion, SHA256: metadata.SHA256})
		toolchainVersions[metadata.GoVersion] = true
	}
	toolchains := make([]sbomToolchain, 0, len(toolchainVersions))
	for version := range toolchainVersions {
		toolchains = append(toolchains, sbomToolchain{Name: "Go", Version: version, LicensePath: goLicense})
	}
	sort.Slice(toolchains, func(i, j int) bool { return toolchains[i].Version < toolchains[j].Version })
	sbom := reviewedSBOM{SchemaVersion: SchemaVersion, Assets: retained, Dependencies: deps, Applications: applications, Toolchains: toolchains}
	sbomData, _ := json.MarshalIndent(sbom, "", "  ")
	sbomData = append(sbomData, '\n')
	if err := writeStageFile(tmp, sbomPath, sbomData, 0644); err != nil {
		return report, err
	}
	sbomSHA, _ := fileSHA256(filepath.Join(tmp, filepath.FromSlash(sbomPath)))

	checksumPath := layout.NoticeDirectory + "/SHA256SUMS"
	if err := writeChecksums(tmp, checksumPath); err != nil {
		return report, err
	}
	required = append(required, checksumPath)
	sort.Strings(required)
	sort.Strings(licenses)
	specification := ManifestSpec{ApplicationExecutables: []string{layout.DesktopExecutable, layout.DaemonExecutable}, RetainedAssets: retained, RequiredPaths: required, LicensePaths: licenses, NoticePaths: []string{noticePath}, TargetPlatform: spec.Platform, ApplicationRoles: map[string]string{layout.DesktopExecutable: "desktop", layout.DaemonExecutable: "daemon"}, ReviewedSBOMPath: sbomPath, ReviewedSBOMSHA256: sbomSHA}
	manifest, err := GenerateDistributionManifest(tmp, specification)
	if err != nil {
		report.Status = StatusBlocked
		report.Checks = append(report.Checks, Check{Name: "unsigned package inspection", Status: StatusBlocked, Blocker: "native role probe/platform inspection unavailable or failed: " + err.Error()})
		return report, nil
	}
	if err := os.Rename(tmp, destination); err != nil {
		return report, err
	}
	keep = true
	manifestCopy := manifest
	report.Manifest = &manifestCopy
	report.StageRoot = destination
	if err := createDeterministicArchive(destination, archivePath, layout.ArchiveExtension); err != nil {
		return report, err
	}
	report.ArchivePath = archivePath
	report.ArchiveSHA256, _ = fileSHA256(archivePath)
	inspection, err := InspectDistributionArchive(archivePath, report.ArchiveSHA256, manifest)
	if err != nil || inspection.Status != StatusPass {
		if err == nil {
			err = errors.New("archive inspection failed")
		}
		return report, err
	}
	report.Status = StatusBlocked
	report.Checks = []Check{{Name: "unsigned deterministic stage/archive", Status: StatusPass, Evidence: "role-probed CGO-disabled Go desktop and daemon, exhaustive payload manifest, dependency SBOM/licenses/notices/checksums, archive re-extraction"}}
	for _, blocker := range releasePrerequisites(spec.Platform) {
		report.Checks = append(report.Checks, Check{Name: blocker, Status: StatusBlocked, Blocker: "not performed by unsigned staging flow"})
	}
	return report, nil
}

func executableDependencyUnion(paths ...string) []sbomDependency {
	set := map[string]sbomDependency{}
	for _, path := range paths {
		for _, dep := range applicationDependencies(path) {
			set[dep.Path+"@"+dep.Version] = dep
		}
	}
	deps := make([]sbomDependency, 0, len(set))
	for _, dep := range set {
		deps = append(deps, dep)
	}
	sort.Slice(deps, func(i, j int) bool {
		if deps[i].Path == deps[j].Path {
			return deps[i].Version < deps[j].Version
		}
		return deps[i].Path < deps[j].Path
	})
	return deps
}

func findMyGoDependency(deps []sbomDependency) (sbomDependency, bool) {
	for _, d := range deps {
		if strings.HasSuffix(d.Path, "/mygo") {
			return d, true
		}
	}
	return sbomDependency{}, false
}
func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func cleanStageRelative(path string) (string, error) {
	if path == "" || filepath.IsAbs(path) || strings.Contains(path, "\\") {
		return "", errors.New("stage path must be normalized relative slash path")
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	if clean != path || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("stage path is not normalized")
	}
	return clean, nil
}
func writeStageFile(root, relative string, data []byte, mode os.FileMode) error {
	if _, err := cleanStageRelative(relative); err != nil {
		return err
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, mode)
}

func writeChecksums(root, checksumPath string) error {
	var lines []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("stage contains symlink")
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if rel == checksumPath {
			return nil
		}
		sum, e := fileSHA256(path)
		if e != nil {
			return e
		}
		lines = append(lines, sum+"  "+rel)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(lines)
	return writeStageFile(root, checksumPath, []byte(strings.Join(lines, "\n")+"\n"), 0644)
}

func releasePrerequisites(platform string) []string {
	base := []string{"clean-machine runtime and GUI observation"}
	switch platform {
	case "darwin":
		return append([]string{"Cua signed helper and permission attribution verification", "Sparkle updater or replacement parity verification", "Developer ID signing verification", "Apple notarization and stapling", "signed installer/DMG verification"}, base...)
	case "windows":
		return append([]string{"Cua UI Automation helper or replacement verification", "Authenticode verification", "Inno Setup installer verification"}, base...)
	default:
		return append([]string{"Cua Wayland helper or replacement verification", "native package/install/uninstall verification"}, base...)
	}
}

func requiredMetadataKinds(platform string) []string {
	switch platform {
	case "linux":
		return []string{"desktop-entry", "icon"}
	case "darwin":
		return []string{"info-plist", "icon"}
	case "windows":
		return []string{"icon", "installer-definition"}
	default:
		return nil
	}
}

func validateStageMetadata(platform string, files []StageFile, layout PackagingLayout) error {
	byKind := map[string]string{}
	for _, file := range files {
		byKind[file.Kind] = strings.ToLower(file.Path)
	}
	switch platform {
	case "linux":
		if !strings.HasSuffix(byKind["desktop-entry"], ".desktop") || !strings.HasSuffix(byKind["icon"], ".png") {
			return errors.New("Linux metadata requires .desktop and .png target paths")
		}
	case "darwin":
		if byKind["info-plist"] != strings.ToLower(layout.InfoPlist) || !strings.HasSuffix(byKind["icon"], ".icns") {
			return errors.New("macOS metadata requires canonical Info.plist and .icns target paths")
		}
	case "windows":
		if !strings.HasSuffix(byKind["icon"], ".ico") || !strings.HasSuffix(byKind["installer-definition"], ".iss") {
			return errors.New("Windows metadata requires .ico and .iss target paths")
		}
	}
	return nil
}

var deterministicTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func createDeterministicArchive(root, output, extension string) error {
	entries := []string{}
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if e.Type()&os.ModeSymlink != 0 {
			return errors.New("archive source contains symlink")
		}
		rel, _ := filepath.Rel(root, path)
		entries = append(entries, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(entries)
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = out.Close()
		if !ok {
			_ = os.Remove(output)
		}
	}()
	if extension == ".zip" {
		z := zip.NewWriter(out)
		for _, rel := range entries {
			info, e := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
			if e != nil {
				return e
			}
			h, _ := zip.FileInfoHeader(info)
			h.Name = rel
			if info.IsDir() {
				h.Name += "/"
			}
			h.Modified = deterministicTime
			h.SetMode(info.Mode())
			w, e := z.CreateHeader(h)
			if e != nil {
				return e
			}
			if !info.IsDir() {
				in, e := os.Open(filepath.Join(root, filepath.FromSlash(rel)))
				if e != nil {
					return e
				}
				_, e = io.Copy(w, in)
				_ = in.Close()
				if e != nil {
					return e
				}
			}
		}
		if err := z.Close(); err != nil {
			return err
		}
	} else {
		gz := gzip.NewWriter(out)
		gz.Header.ModTime = deterministicTime
		gz.Header.OS = 255
		tr := tar.NewWriter(gz)
		for _, rel := range entries {
			info, e := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
			if e != nil {
				return e
			}
			h, e := tar.FileInfoHeader(info, "")
			if e != nil {
				return e
			}
			h.Name = rel
			h.ModTime = deterministicTime
			h.AccessTime = time.Time{}
			h.ChangeTime = time.Time{}
			h.Uid = 0
			h.Gid = 0
			h.Uname = ""
			h.Gname = ""
			if e = tr.WriteHeader(h); e != nil {
				return e
			}
			if !info.IsDir() {
				in, e := os.Open(filepath.Join(root, filepath.FromSlash(rel)))
				if e != nil {
					return e
				}
				_, e = io.Copy(tr, in)
				_ = in.Close()
				if e != nil {
					return e
				}
			}
		}
		if err := tr.Close(); err != nil {
			return err
		}
		if err := gz.Close(); err != nil {
			return err
		}
	}
	if err := out.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}
