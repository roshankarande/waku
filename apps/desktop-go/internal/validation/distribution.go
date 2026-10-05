package validation

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
)

type RetainedAsset struct {
	Path         string   `json:"path"`
	Purpose      string   `json:"purpose"`
	Source       string   `json:"source"`
	Platforms    []string `json:"platforms"`
	LicensePath  string   `json:"licensePath"`
	SHA256       string   `json:"sha256,omitempty"`
	Identity     string   `json:"identity,omitempty"`
	Version      string   `json:"version,omitempty"`
	SourceSHA256 string   `json:"sourceSha256,omitempty"`
}
type ApplicationMetadata struct {
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	GoVersion       string `json:"goVersion"`
	ModulePath      string `json:"modulePath"`
	ModuleVersion   string `json:"moduleVersion,omitempty"`
	VCSRevision     string `json:"vcsRevision,omitempty"`
	CGOEnabled      string `json:"cgoEnabled"`
	Role            string `json:"role,omitempty"`
	RoleProbeSHA256 string `json:"roleProbeSha256,omitempty"`
}
type EmbeddedMarkerAllowance struct {
	Path    string `json:"path"`
	Marker  string `json:"marker"`
	Purpose string `json:"purpose"`
}

// DistributionManifest is checked into or generated beside a staged release.
// ApplicationExecutables are the Go-owned desktop, daemon, updater, and helper
// programs. RetainedAssets makes third-party native/JS payloads auditable.
type DistributionManifest struct {
	SchemaVersion            int                       `json:"schemaVersion"`
	ApplicationExecutables   []string                  `json:"applicationExecutables"`
	ApplicationMetadata      []ApplicationMetadata     `json:"applicationMetadata"`
	RetainedAssets           []RetainedAsset           `json:"retainedAssets"`
	RequiredPaths            []string                  `json:"requiredPaths,omitempty"`
	LicensePaths             []string                  `json:"licensePaths"`
	NoticePaths              []string                  `json:"noticePaths"`
	NoApplicationRustRuntime bool                      `json:"noApplicationOwnedRustRuntime"`
	NoApplicationJSRuntime   bool                      `json:"noApplicationOwnedJsRuntime"`
	EmbeddedMarkerAllowances []EmbeddedMarkerAllowance `json:"embeddedMarkerAllowances,omitempty"`
	TargetPlatform           string                    `json:"targetPlatform"`
	ReviewedSBOMPath         string                    `json:"reviewedSbomPath,omitempty"`
	ReviewedSBOMSHA256       string                    `json:"reviewedSbomSha256,omitempty"`
	LicenseSHA256            map[string]string         `json:"licenseSha256,omitempty"`
	NoticeSHA256             map[string]string         `json:"noticeSha256,omitempty"`
	RequiredSHA256           map[string]string         `json:"requiredSha256,omitempty"`
}

const releaseRoleProbeProtocol = "waku-release-role-v1"

type releaseRoleProbe struct {
	SchemaVersion int    `json:"schemaVersion"`
	Protocol      string `json:"protocol"`
	Role          string `json:"role"`
}

type InspectionIssue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

type InspectionReport struct {
	Status Status            `json:"status"`
	Files  int               `json:"files"`
	Issues []InspectionIssue `json:"issues,omitempty"`
	Checks []Check           `json:"checks"`
}

func LoadDistributionManifest(path string) (DistributionManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DistributionManifest{}, err
	}
	var manifest DistributionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, err
	}
	if manifest.SchemaVersion != SchemaVersion {
		return manifest, fmt.Errorf("manifest schemaVersion: got %d", manifest.SchemaVersion)
	}
	if len(manifest.ApplicationExecutables) == 0 {
		return manifest, errors.New("manifest requires applicationExecutables")
	}
	if len(manifest.LicensePaths) == 0 || len(manifest.NoticePaths) == 0 {
		return manifest, errors.New("manifest requires licensePaths and noticePaths")
	}
	if !manifest.NoApplicationRustRuntime || !manifest.NoApplicationJSRuntime {
		return manifest, errors.New("manifest must explicitly prohibit application-owned Rust and JavaScript runtimes")
	}
	if len(manifest.ApplicationMetadata) != len(manifest.ApplicationExecutables) {
		return manifest, errors.New("applicationMetadata must cover every application executable")
	}
	executables := map[string]bool{}
	for _, path := range manifest.ApplicationExecutables {
		if executables[path] {
			return manifest, errors.New("applicationExecutables contains a duplicate path")
		}
		executables[path] = true
	}
	licenses := map[string]bool{}
	for _, path := range manifest.LicensePaths {
		if path == "" || licenses[path] {
			return manifest, errors.New("licensePaths contains an empty or duplicate path")
		}
		licenses[path] = true
	}
	notices := map[string]bool{}
	for _, path := range manifest.NoticePaths {
		if path == "" || notices[path] {
			return manifest, errors.New("noticePaths contains an empty or duplicate path")
		}
		notices[path] = true
	}
	metadataPaths := map[string]bool{}
	for index, metadata := range manifest.ApplicationMetadata {
		if !executables[metadata.Path] || metadataPaths[metadata.Path] || !validSHA256(metadata.SHA256) || metadata.GoVersion == "" || metadata.ModulePath == "" || metadata.CGOEnabled != "0" || !validReleaseRole(metadata.Role) || !validSHA256(metadata.RoleProbeSHA256) {
			return manifest, fmt.Errorf("applicationMetadata[%d] is invalid or duplicated", index)
		}
		metadataPaths[metadata.Path] = true
	}
	assetPaths := map[string]bool{}
	for i, asset := range manifest.RetainedAssets {
		if assetPaths[asset.Path] {
			return manifest, fmt.Errorf("retainedAssets[%d]: duplicate path", i)
		}
		assetPaths[asset.Path] = true
		if asset.Path == "" || asset.Purpose == "" || asset.Source == "" || len(asset.Platforms) == 0 || asset.LicensePath == "" {
			return manifest, fmt.Errorf("retainedAssets[%d]: path, purpose, source, platforms and licensePath required", i)
		}
		if !validSHA256(asset.SHA256) {
			return manifest, fmt.Errorf("retainedAssets[%d]: every retained asset requires a valid SHA-256", i)
		}
		if asset.Identity == "" || asset.Version == "" || !validSHA256(asset.SourceSHA256) || !validRetainedSource(asset.Source) {
			return manifest, fmt.Errorf("retainedAssets[%d]: reviewed identity, version, source URL and source SHA-256 required", i)
		}
		if !licenses[asset.LicensePath] {
			return manifest, fmt.Errorf("retainedAssets[%d]: licensePath is not declared in licensePaths", i)
		}
		for _, platform := range asset.Platforms {
			if _, err := ReleasePackagingLayout(platform); err != nil {
				return manifest, fmt.Errorf("retainedAssets[%d]: invalid platform %q", i, platform)
			}
		}
	}
	if _, err := ReleasePackagingLayout(manifest.TargetPlatform); err != nil {
		return manifest, err
	}
	if manifest.ReviewedSBOMPath == "" || !validSHA256(manifest.ReviewedSBOMSHA256) {
		return manifest, errors.New("distribution requires a digest-pinned full dependency SBOM")
	}
	if len(manifest.LicenseSHA256) != len(manifest.LicensePaths) || len(manifest.NoticeSHA256) != len(manifest.NoticePaths) {
		return manifest, errors.New("license and notice digests must exactly cover their declared paths")
	}
	for _, path := range manifest.LicensePaths {
		if !validSHA256(manifest.LicenseSHA256[path]) {
			return manifest, errors.New("license digest is missing or invalid")
		}
	}
	for _, path := range manifest.NoticePaths {
		if !validSHA256(manifest.NoticeSHA256[path]) {
			return manifest, errors.New("notice digest is missing or invalid")
		}
	}
	if len(manifest.RequiredSHA256) != len(manifest.RequiredPaths) {
		return manifest, errors.New("required-data digests must exactly cover their declared paths")
	}
	for _, path := range manifest.RequiredPaths {
		if !validSHA256(manifest.RequiredSHA256[path]) {
			return manifest, errors.New("required-data digest is missing or invalid")
		}
	}
	if len(manifest.LicenseSHA256) != len(manifest.LicensePaths) || len(manifest.NoticeSHA256) != len(manifest.NoticePaths) {
		return manifest, errors.New("license/notice digest map has undeclared entries")
	}
	for i, allowance := range manifest.EmbeddedMarkerAllowances {
		if allowance.Path == "" || allowance.Marker == "" || allowance.Purpose == "" {
			return manifest, fmt.Errorf("embeddedMarkerAllowances[%d]: path, marker and purpose required", i)
		}
	}
	return manifest, nil
}

// InspectDistribution checks an unpacked release prefix. Archive staging is a
// separate, read-only step so symlinks and permissions can be inspected by the
// platform-specific release job without extracting untrusted paths here.
func InspectDistribution(root string, manifest DistributionManifest) InspectionReport {
	report := InspectionReport{Status: StatusPass}
	root, err := filepath.Abs(root)
	if err != nil {
		return failedReport("root", err.Error())
	}
	files := map[string]string{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.Type()&os.ModeSymlink != 0 {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "release payload symlinks are not permitted"})
			path = ""
		}
		files[rel] = path
		report.Files++
		return nil
	})
	if err != nil {
		return failedReport("walk", err.Error())
	}
	if manifest.TargetPlatform != "" {
		inspectPackagingPolicy(root, files, manifest, &report)
	}

	require := func(path, reason string) {
		if _, ok := files[path]; !ok {
			report.Issues = append(report.Issues, InspectionIssue{Path: path, Message: reason})
		}
	}
	for _, path := range manifest.RequiredPaths {
		require(path, "required release path is missing")
	}
	for _, path := range manifest.LicensePaths {
		require(path, "declared license is missing")
	}
	for _, path := range manifest.NoticePaths {
		require(path, "declared notice is missing")
	}
	for path, want := range manifest.LicenseSHA256 {
		if actual, err := fileSHA256(files[path]); err != nil || !strings.EqualFold(actual, want) {
			report.Issues = append(report.Issues, InspectionIssue{Path: path, Message: "license SHA-256 mismatch"})
		}
	}
	for path, want := range manifest.NoticeSHA256 {
		if actual, err := fileSHA256(files[path]); err != nil || !strings.EqualFold(actual, want) {
			report.Issues = append(report.Issues, InspectionIssue{Path: path, Message: "notice SHA-256 mismatch"})
		}
	}
	for path, want := range manifest.RequiredSHA256 {
		if actual, err := fileSHA256(files[path]); err != nil || !strings.EqualFold(actual, want) {
			report.Issues = append(report.Issues, InspectionIssue{Path: path, Message: "required data SHA-256 mismatch"})
		}
	}
	if !manifest.NoApplicationRustRuntime {
		report.Issues = append(report.Issues, InspectionIssue{Path: "manifest", Message: "application-owned Rust runtime prohibition is not asserted"})
	}
	if !manifest.NoApplicationJSRuntime {
		report.Issues = append(report.Issues, InspectionIssue{Path: "manifest", Message: "application-owned JavaScript runtime prohibition is not asserted"})
	}
	for _, asset := range manifest.RetainedAssets {
		if asset.Purpose == "" || asset.Source == "" || len(asset.Platforms) == 0 || asset.LicensePath == "" {
			report.Issues = append(report.Issues, InspectionIssue{Path: asset.Path, Message: "retained asset requires purpose, source, platforms and license"})
		}
		require(asset.Path, "declared retained asset is missing")
		require(asset.LicensePath, "retained asset license is missing")
		if !slices.Contains(manifest.LicensePaths, asset.LicensePath) {
			report.Issues = append(report.Issues, InspectionIssue{Path: asset.Path, Message: "retained asset licensePath is not declared in licensePaths"})
		}
		for _, platform := range asset.Platforms {
			if _, err := ReleasePackagingLayout(platform); err != nil {
				report.Issues = append(report.Issues, InspectionIssue{Path: asset.Path, Message: "retained asset has invalid platform " + platform})
			}
		}
		if !validSHA256(asset.SHA256) {
			report.Issues = append(report.Issues, InspectionIssue{Path: asset.Path, Message: "retained asset requires a valid SHA-256"})
		} else if asset.SHA256 != "" {
			path, ok := files[asset.Path]
			if ok {
				digest, err := fileSHA256(path)
				if err != nil || !strings.EqualFold(digest, asset.SHA256) {
					report.Issues = append(report.Issues, InspectionIssue{Path: asset.Path, Message: "retained asset SHA-256 mismatch"})
				}
			}
		}
	}

	declaredExecutables := map[string]bool{}
	metadataByPath := map[string]ApplicationMetadata{}
	for _, metadata := range manifest.ApplicationMetadata {
		metadataByPath[metadata.Path] = metadata
	}
	for _, rel := range manifest.ApplicationExecutables {
		declaredExecutables[rel] = true
		path, ok := files[rel]
		if !ok {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "application executable is missing"})
			continue
		}
		info, err := buildinfo.ReadFile(path)
		if err != nil {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "not a readable Go executable: " + err.Error()})
			continue
		}
		cgo := ""
		for _, setting := range info.Settings {
			if setting.Key == "CGO_ENABLED" {
				cgo = setting.Value
				break
			}
		}
		if cgo != "0" {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "Go build info does not prove CGO_ENABLED=0"})
		}
		actual, metaErr := applicationMetadata(rel, path)
		expected, hasMetadata := metadataByPath[rel]
		if metaErr == nil && hasMetadata && manifest.TargetPlatform != "" {
			actual.Role = expected.Role
			probe, probeErr := probeApplicationRole(path)
			if probeErr != nil || probe.Role != expected.Role {
				report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "command-specific release role probe failed or returned the wrong role"})
			} else {
				actual.RoleProbeSHA256 = roleProbeDigest(probe)
			}
		}
		if metaErr != nil || !hasMetadata || actual != expected {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "application build metadata or SHA-256 does not match manifest"})
		}
	}

	retainedPayload := map[string]bool{}
	for _, asset := range manifest.RetainedAssets {
		retainedPayload[asset.Path] = true
	}
	declaredPayload := map[string]bool{}
	for _, paths := range [][]string{manifest.ApplicationExecutables, manifest.RequiredPaths, manifest.LicensePaths, manifest.NoticePaths} {
		for _, path := range paths {
			declaredPayload[path] = true
		}
	}
	for path := range retainedPayload {
		declaredPayload[path] = true
	}
	if manifest.ReviewedSBOMPath != "" {
		declaredPayload[manifest.ReviewedSBOMPath] = true
	}
	allowedMarkers := map[string]map[string]bool{}
	for _, allowance := range manifest.EmbeddedMarkerAllowances {
		if allowedMarkers[allowance.Path] == nil {
			allowedMarkers[allowance.Path] = map[string]bool{}
		}
		allowedMarkers[allowance.Path][strings.ToLower(allowance.Marker)] = true
	}
	for rel, path := range files {
		lower := strings.ToLower(rel)
		if !declaredPayload[rel] {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "release payload is not exhaustively classified in the manifest"})
		}
		if !retainedPayload[rel] && !declaredExecutables[rel] && isRuntimeAsset(lower) {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "native library or JavaScript runtime asset is not declared with its purpose and license"})
		}
		executable, format := executableFormat(path)
		retainedNativeLibrary := retainedPayload[rel] && isNativeLibrary(lower) && isSharedLibrary(path, format)
		if retainedPayload[rel] && isNativeLibrary(lower) && !isCorrespondingNativeLibrary(path, lower, format) {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "retained native library does not parse as the binary format required by its extension"})
		}
		if executable && !declaredExecutables[rel] && !retainedNativeLibrary {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "executable (" + format + ") is not declared as an application executable; retained asset declarations do not authorize executables"})
		}
		// Retained assets are still scanned: declaration is not an exemption from
		// application-owned Rust runtime detection.
		if executable || isNativeLibrary(lower) {
			for _, lib := range importedLibraries(path, format) {
				if isWakuRustMarker(lib) {
					report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "imports application-owned Rust runtime " + lib})
				}
			}
			for _, marker := range wakuRustMarkers(path) {
				if !allowedMarkers[rel][marker] {
					report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "contains application-owned Rust runtime marker " + marker})
				}
			}
		}
		if !retainedPayload[rel] && (strings.HasSuffix(lower, "cargo.lock") || strings.HasSuffix(lower, "cargo.toml") || strings.Contains(lower, "/rustlib/")) {
			report.Issues = append(report.Issues, InspectionIssue{Path: rel, Message: "Rust build/runtime material is not declared as a retained third-party asset"})
		}
	}

	sort.Slice(report.Issues, func(i, j int) bool {
		if report.Issues[i].Path == report.Issues[j].Path {
			return report.Issues[i].Message < report.Issues[j].Message
		}
		return report.Issues[i].Path < report.Issues[j].Path
	})
	if len(report.Issues) > 0 {
		report.Status = StatusFail
	}
	report.Checks = []Check{{Name: "staged distribution policy inspection", Status: report.Status, Evidence: fmt.Sprintf("inspected %d paths; Go build info, executable magic/permissions, ELF/PE/Mach-O imports, static Waku Rust markers, declarations, licenses, hashes, and symlink containment", report.Files)}}
	return report
}

func validReleaseRole(role string) bool { return role == "desktop" || role == "daemon" }

func inspectPackagingPolicy(root string, files map[string]string, manifest DistributionManifest, report *InspectionReport) {
	layout, err := ReleasePackagingLayout(manifest.TargetPlatform)
	if err != nil {
		report.Issues = append(report.Issues, InspectionIssue{Path: "manifest", Message: err.Error()})
		return
	}
	want := map[string]string{layout.DesktopExecutable: "desktop", layout.DaemonExecutable: "daemon"}
	if layout.InfoPlist != "" {
		if !slices.Contains(manifest.RequiredPaths, layout.InfoPlist) {
			report.Issues = append(report.Issues, InspectionIssue{Path: layout.InfoPlist, Message: "macOS package must declare Info.plist as required data"})
		} else if info, err := os.Stat(files[layout.InfoPlist]); err != nil || info.Size() == 0 {
			report.Issues = append(report.Issues, InspectionIssue{Path: layout.InfoPlist, Message: "macOS Info.plist must be a non-empty regular file"})
		}
	}
	if len(manifest.ApplicationExecutables) != len(want) {
		report.Issues = append(report.Issues, InspectionIssue{Path: "manifest", Message: "target layout requires exactly desktop and daemon executables"})
	}
	seen := map[string]bool{}
	for _, executable := range manifest.ApplicationExecutables {
		if _, ok := want[executable]; !ok || seen[executable] {
			report.Issues = append(report.Issues, InspectionIssue{Path: executable, Message: "executable path does not match target platform layout or is duplicated"})
		}
		seen[executable] = true
		path := files[executable]
		_, format := executableFormat(path)
		expected := map[string]string{"linux": "ELF", "darwin": "Mach-O", "windows": "PE"}[manifest.TargetPlatform]
		if format != expected {
			report.Issues = append(report.Issues, InspectionIssue{Path: executable, Message: "executable format " + format + " does not match target " + expected})
		}
	}
	metadataRoles := map[string]string{}
	for _, metadata := range manifest.ApplicationMetadata {
		metadataRoles[metadata.Path] = metadata.Role
	}
	for path, role := range want {
		if metadataRoles[path] != role {
			report.Issues = append(report.Issues, InspectionIssue{Path: path, Message: "command-specific role identity does not match packaged role " + role})
		}
	}
	seenDocumentDigest := map[string]string{}
	validateDocs := func(paths []string, directory, kind string) {
		for _, relative := range paths {
			prefix := strings.TrimSuffix(directory, "/") + "/"
			if relative != directory && !strings.HasPrefix(relative, prefix) {
				report.Issues = append(report.Issues, InspectionIssue{Path: relative, Message: kind + " is outside target layout directory"})
			}
			path := files[relative]
			info, err := os.Stat(path)
			if err != nil || info.Size() == 0 {
				report.Issues = append(report.Issues, InspectionIssue{Path: relative, Message: kind + " must be a non-empty regular file"})
				continue
			}
			digest, err := fileSHA256(path)
			if err == nil {
				if prior, duplicate := seenDocumentDigest[digest]; duplicate {
					report.Issues = append(report.Issues, InspectionIssue{Path: relative, Message: kind + " duplicates " + prior})
				}
				seenDocumentDigest[digest] = relative
			}
		}
	}
	validateDocs(manifest.LicensePaths, layout.LicenseDirectory, "license")
	validateDocs(manifest.NoticePaths, layout.NoticeDirectory, "notice")
	path := files[manifest.ReviewedSBOMPath]
	digest, err := fileSHA256(path)
	var dependencies []sbomDependency
	for _, executable := range manifest.ApplicationExecutables {
		dependencies = append(dependencies, applicationDependencies(files[executable])...)
	}
	if err != nil || !strings.EqualFold(digest, manifest.ReviewedSBOMSHA256) || verifyReviewedSBOM(path, manifest.RetainedAssets, manifest.TargetPlatform, dependencies) != nil {
		report.Issues = append(report.Issues, InspectionIssue{Path: manifest.ReviewedSBOMPath, Message: "distribution does not match the digest-pinned full dependency SBOM"})
	}
	_ = root
}

func validRetainedSource(source string) bool {
	u, err := url.Parse(source)
	return err == nil && (u.Scheme == "https" || u.Scheme == "git+https") && u.Host != ""
}

func roleProbeDigest(probe releaseRoleProbe) string {
	b, _ := json.Marshal(probe)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func probeApplicationRole(path string) (releaseRoleProbe, error) {
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(path), ".exe") {
		return releaseRoleProbe{}, errors.New("role probe is not a Windows executable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "--waku-release-role-probe")
	output, err := command.Output()
	if err != nil {
		return releaseRoleProbe{}, err
	}
	var probe releaseRoleProbe
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&probe); err != nil {
		return releaseRoleProbe{}, err
	}
	if probe.SchemaVersion != SchemaVersion || probe.Protocol != releaseRoleProbeProtocol || !validReleaseRole(probe.Role) {
		return releaseRoleProbe{}, errors.New("invalid command-specific release role probe")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return releaseRoleProbe{}, errors.New("release role probe emitted trailing data")
	}
	return probe, nil
}

func failedReport(path, message string) InspectionReport {
	return InspectionReport{Status: StatusFail, Issues: []InspectionIssue{{Path: path, Message: message}}}
}
func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}
func applicationMetadata(relative, path string) (ApplicationMetadata, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return ApplicationMetadata{}, err
	}
	digest, err := fileSHA256(path)
	if err != nil {
		return ApplicationMetadata{}, err
	}
	metadata := ApplicationMetadata{Path: relative, SHA256: digest, GoVersion: info.GoVersion, ModulePath: info.Main.Path, ModuleVersion: info.Main.Version}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "CGO_ENABLED":
			metadata.CGOEnabled = setting.Value
		case "vcs.revision":
			metadata.VCSRevision = setting.Value
		}
	}
	return metadata, nil
}
func executableFormat(path string) (bool, string) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false, ""
	}
	f, err := os.Open(path)
	if err != nil {
		return info.Mode()&0111 != 0, "mode"
	}
	defer f.Close()
	var magic [4]byte
	n, _ := io.ReadFull(f, magic[:])
	b := magic[:n]
	lower := strings.ToLower(path)
	switch {
	case n == 4 && string(b) == "\x7fELF":
		return true, "ELF"
	case n >= 2 && string(b[:2]) == "MZ":
		return true, "PE"
	case n == 4 && (string(b) == "\xfe\xed\xfa\xce" || string(b) == "\xce\xfa\xed\xfe" || string(b) == "\xfe\xed\xfa\xcf" || string(b) == "\xcf\xfa\xed\xfe" || string(b) == "\xca\xfe\xba\xbe" || string(b) == "\xca\xfe\xba\xbf" || string(b) == "\xbe\xba\xfe\xca" || string(b) == "\xbf\xba\xfe\xca"):
		return true, "Mach-O"
	case n >= 2 && string(b[:2]) == "#!":
		return true, "script"
	case info.Mode()&0111 != 0:
		return true, "mode"
	case strings.HasSuffix(lower, ".exe") || strings.HasSuffix(lower, ".com") || strings.HasSuffix(lower, ".bat") || strings.HasSuffix(lower, ".cmd") || strings.HasSuffix(lower, ".ps1"):
		return true, "executable extension"
	}
	return false, ""
}

func isSharedLibrary(path, format string) bool {
	switch format {
	case "ELF":
		file, err := elf.Open(path)
		if err != nil || file.Type != elf.ET_DYN {
			return false
		}
		defer file.Close()
		for _, program := range file.Progs {
			if program.Type == elf.PT_INTERP {
				return false
			}
		}
		return true
	case "PE":
		file, err := pe.Open(path)
		if err != nil {
			return false
		}
		defer file.Close()
		return file.FileHeader.Characteristics&pe.IMAGE_FILE_DLL != 0
	case "Mach-O":
		file, err := macho.Open(path)
		if err == nil {
			defer file.Close()
			return file.Type == macho.TypeDylib || file.Type == macho.TypeBundle
		}
		fat, err := macho.OpenFat(path)
		if err != nil {
			return false
		}
		defer fat.Close()
		if len(fat.Arches) == 0 {
			return false
		}
		for _, arch := range fat.Arches {
			if arch.File.Type != macho.TypeDylib && arch.File.Type != macho.TypeBundle {
				return false
			}
		}
		return true
	}
	return false
}
func importedLibraries(path, format string) []string {
	switch format {
	case "ELF":
		f, e := elf.Open(path)
		if e == nil {
			defer f.Close()
			v, _ := f.ImportedLibraries()
			return v
		}
	case "PE":
		f, e := pe.Open(path)
		if e == nil {
			defer f.Close()
			v, _ := f.ImportedLibraries()
			return v
		}
	case "Mach-O":
		f, e := macho.Open(path)
		if e == nil {
			defer f.Close()
			v, _ := f.ImportedLibraries()
			return v
		}
		fat, e := macho.OpenFat(path)
		if e == nil {
			defer fat.Close()
			var all []string
			for _, arch := range fat.Arches {
				v, _ := arch.File.ImportedLibraries()
				all = append(all, v...)
			}
			return all
		}
	}
	return nil
}
func isNativeLibrary(lower string) bool {
	return strings.HasSuffix(lower, ".so") || strings.Contains(lower, ".so.") || strings.HasSuffix(lower, ".dylib") || strings.HasSuffix(lower, ".dll")
}
func isRuntimeAsset(lower string) bool {
	base := filepath.Base(lower)
	return isNativeLibrary(lower) || strings.HasSuffix(lower, ".js") || strings.HasSuffix(lower, ".mjs") || strings.HasSuffix(lower, ".cjs") || strings.HasSuffix(lower, ".asar") || strings.HasSuffix(lower, ".wasm") || strings.HasSuffix(lower, ".node") || base == "node" || base == "node.exe"
}
func isCorrespondingNativeLibrary(path, lower, format string) bool {
	want := ""
	switch {
	case strings.HasSuffix(lower, ".so") || strings.Contains(lower, ".so."):
		want = "ELF"
	case strings.HasSuffix(lower, ".dylib"):
		want = "Mach-O"
	case strings.HasSuffix(lower, ".dll"):
		want = "PE"
	}
	return format == want && isSharedLibrary(path, format)
}
func isWakuRustMarker(value string) bool {
	v := strings.ToLower(value)
	return strings.Contains(v, "waku_core") || strings.Contains(v, "waku-client") || strings.Contains(v, "waku_client") || strings.Contains(v, "waku-daemon") || strings.Contains(v, "waku_daemon")
}
func wakuRustMarkers(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	markers := []string{"waku_core", "waku-client", "waku_client", "waku-daemon", "waku_daemon"}
	found := make(map[string]bool)
	buf := make([]byte, 64*1024)
	tail := []byte{}
	for {
		n, e := f.Read(buf)
		if n > 0 {
			chunk := append(append([]byte{}, tail...), buf[:n]...)
			lower := strings.ToLower(string(chunk))
			for _, m := range markers {
				if strings.Contains(lower, m) {
					found[m] = true
				}
			}
			if len(chunk) > 32 {
				tail = append([]byte{}, chunk[len(chunk)-32:]...)
			}
		}
		if e != nil {
			break
		}
	}
	var result []string
	for marker := range found {
		result = append(result, marker)
	}
	sort.Strings(result)
	return result
}

const MaxArchiveEntries = 100_000
const MaxArchiveEntryBytes int64 = 1 << 30
const MaxArchiveExpandedBytes int64 = 4 << 30

// ArchiveEntries performs shape-only preflight before extraction. A staged
// tree must still pass InspectDistribution with its manifest afterward.
func ArchiveEntries(path string) ([]string, error) {
	return archiveEntries(path, MaxArchiveEntries, MaxArchiveEntryBytes, MaxArchiveExpandedBytes)
}

type ArchiveInspection struct {
	Status        Status           `json:"status"`
	ArchiveSHA256 string           `json:"archiveSha256"`
	Entries       int              `json:"entries"`
	Distribution  InspectionReport `json:"distribution"`
}

// InspectDistributionArchive binds inspection to the exact archive bytes,
// extracts only preflighted regular files/directories into a fresh directory,
// and applies the same staged distribution policy to those extracted bytes.
// Links are rejected at this release boundary even when shape preflight can
// prove containment: release packages do not need link semantics.
func InspectDistributionArchive(path, expectedSHA256 string, manifest DistributionManifest) (ArchiveInspection, error) {
	if !validSHA256(expectedSHA256) {
		return ArchiveInspection{}, errors.New("expected archive SHA-256 is required")
	}
	layout, err := ReleasePackagingLayout(manifest.TargetPlatform)
	if err != nil {
		return ArchiveInspection{}, err
	}
	if !strings.HasSuffix(strings.ToLower(path), layout.ArchiveExtension) {
		return ArchiveInspection{}, errors.New("archive extension does not match target platform")
	}
	privateArchive, digest, cleanup, err := snapshotRegularFile(path, layout.ArchiveExtension, "waku-release-archive-")
	if err != nil {
		return ArchiveInspection{}, err
	}
	defer cleanup()
	if !strings.EqualFold(digest, expectedSHA256) {
		return ArchiveInspection{}, errors.New("archive SHA-256 mismatch")
	}
	entries, err := ArchiveEntries(privateArchive)
	if err != nil {
		return ArchiveInspection{}, err
	}
	root, err := os.MkdirTemp("", "waku-release-extract-")
	if err != nil {
		return ArchiveInspection{}, err
	}
	defer os.RemoveAll(root)
	extracted, err := extractReleaseArchive(privateArchive, root)
	if err != nil {
		return ArchiveInspection{}, err
	}
	sort.Strings(extracted)
	if !slices.Equal(entries, extracted) {
		return ArchiveInspection{}, errors.New("archive extraction did not exactly match preflighted entries")
	}
	report := InspectDistribution(root, manifest)
	return ArchiveInspection{Status: report.Status, ArchiveSHA256: digest, Entries: len(entries), Distribution: report}, nil
}

func snapshotRegularFile(source, suffix, prefix string) (string, string, func(), error) {
	before, err := os.Lstat(source)
	if err != nil || !before.Mode().IsRegular() {
		return "", "", func() {}, errors.New("source must be a regular non-symlink file")
	}
	input, err := os.Open(source)
	if err != nil {
		return "", "", func() {}, err
	}
	opened, err := input.Stat()
	if err != nil || !os.SameFile(before, opened) {
		input.Close()
		return "", "", func() {}, errors.New("source changed before immutable snapshot")
	}
	directory, err := os.MkdirTemp("", prefix)
	if err != nil {
		input.Close()
		return "", "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	destination := filepath.Join(directory, "payload"+suffix)
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, before.Mode().Perm())
	if err != nil {
		input.Close()
		cleanup()
		return "", "", func() {}, err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	closeOut, closeIn := output.Close(), input.Close()
	if copyErr != nil || closeOut != nil || closeIn != nil {
		cleanup()
		return "", "", func() {}, errors.Join(copyErr, closeOut, closeIn)
	}
	if err := os.Chmod(destination, before.Mode().Perm()&0555); err != nil {
		cleanup()
		return "", "", func() {}, err
	}
	after, err := os.Lstat(source)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		cleanup()
		return "", "", func() {}, errors.New("source changed while immutable snapshot was captured")
	}
	return destination, hex.EncodeToString(hash.Sum(nil)), cleanup, nil
}

func releaseExtractPath(root, name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	clean := filepath.ToSlash(filepath.Clean(name))
	if clean == "." || strings.HasPrefix(clean, "/") || clean == ".." || strings.HasPrefix(clean, "../") || len(clean) >= 2 && clean[1] == ':' {
		return "", errors.New("unsafe archive extraction path")
	}
	path := filepath.Join(root, filepath.FromSlash(clean))
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("archive extraction path escapes destination")
	}
	return path, nil
}

func extractReleaseArchive(path, root string) ([]string, error) {
	var extracted []string
	add := func(name string) (string, error) {
		destination, err := releaseExtractPath(root, name)
		if err != nil {
			return "", err
		}
		extracted = append(extracted, filepath.ToSlash(filepath.Clean(strings.ReplaceAll(name, "\\", "/"))))
		return destination, nil
	}
	if strings.HasSuffix(strings.ToLower(path), ".zip") {
		reader, err := zip.OpenReader(path)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		for _, entry := range reader.File {
			destination, err := add(entry.Name)
			if err != nil {
				return nil, err
			}
			if entry.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("release archive link %q is unsupported", entry.Name)
			}
			if entry.FileInfo().IsDir() {
				if err := os.MkdirAll(destination, 0755); err != nil {
					return nil, err
				}
				continue
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
				return nil, err
			}
			input, err := entry.Open()
			if err != nil {
				return nil, err
			}
			output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, entry.Mode().Perm())
			if err == nil {
				_, err = io.CopyN(output, input, int64(entry.UncompressedSize64))
			}
			closeOut := error(nil)
			if output != nil {
				closeOut = output.Close()
			}
			closeIn := input.Close()
			if err != nil {
				return nil, err
			}
			if closeOut != nil {
				return nil, closeOut
			}
			if closeIn != nil {
				return nil, closeIn
			}
		}
		return extracted, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		destination, err := add(header.Name)
		if err != nil {
			return nil, err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destination, os.FileMode(header.Mode).Perm()); err != nil {
				return nil, err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
				return nil, err
			}
			output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode).Perm())
			if err == nil {
				_, err = io.CopyN(output, reader, header.Size)
			}
			closeErr := error(nil)
			if output != nil {
				closeErr = output.Close()
			}
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
		case tar.TypeSymlink, tar.TypeLink:
			return nil, fmt.Errorf("release archive link %q is unsupported", header.Name)
		default:
			return nil, fmt.Errorf("unsupported release archive entry %q", header.Name)
		}
	}
	return extracted, nil
}

func archiveEntries(path string, maxEntries int, maxEntryBytes, maxExpandedBytes int64) ([]string, error) {
	type link struct {
		name, target string
		relative     bool
	}
	var names []string
	seen := map[string]string{}
	var links []link
	var expanded int64
	normalize := func(name string) (string, error) {
		name = strings.ReplaceAll(name, "\\", "/")
		if len(name) >= 2 && ((name[0] >= 'a' && name[0] <= 'z') || (name[0] >= 'A' && name[0] <= 'Z')) && name[1] == ':' {
			return "", fmt.Errorf("Windows drive path %q", name)
		}
		clean := filepath.ToSlash(filepath.Clean(name))
		if clean == "." || strings.HasPrefix(clean, "/") || clean == ".." || strings.HasPrefix(clean, "../") {
			return "", fmt.Errorf("unsafe archive path %q", name)
		}
		return clean, nil
	}
	add := func(name string) (string, error) {
		if len(names) >= maxEntries {
			return "", fmt.Errorf("archive has more than %d entries", maxEntries)
		}
		clean, err := normalize(name)
		if err != nil {
			return "", err
		}
		key := strings.ToLower(clean)
		if previous, ok := seen[key]; ok {
			return "", fmt.Errorf("normalized archive collision %q and %q", previous, name)
		}
		seen[key] = name
		names = append(names, clean)
		return clean, nil
	}
	if strings.HasSuffix(strings.ToLower(path), ".zip") {
		archive, err := zip.OpenReader(path)
		if err != nil {
			return nil, err
		}
		defer archive.Close()
		for _, file := range archive.File {
			if file.UncompressedSize64 > uint64(maxEntryBytes) || file.UncompressedSize64 > uint64(maxExpandedBytes-expanded) {
				return nil, fmt.Errorf("archive expanded size limit exceeded at %q", file.Name)
			}
			expanded += int64(file.UncompressedSize64)
			name, err := add(file.Name)
			if err != nil {
				return nil, err
			}
			modeType := file.Mode().Type()
			if modeType != 0 && modeType != os.ModeDir && modeType != os.ModeSymlink {
				return nil, fmt.Errorf("unsupported special ZIP entry %q", name)
			}
			if file.Mode()&os.ModeSymlink != 0 {
				reader, err := file.Open()
				if err != nil {
					return nil, err
				}
				data, readErr := io.ReadAll(io.LimitReader(reader, 4097))
				closeErr := reader.Close()
				if readErr != nil {
					return nil, readErr
				}
				if closeErr != nil {
					return nil, closeErr
				}
				if len(data) > 4096 {
					return nil, fmt.Errorf("symlink target too long for %q", name)
				}
				links = append(links, link{name, string(data), true})
			}
		}
	} else if strings.HasSuffix(strings.ToLower(path), ".tar.gz") || strings.HasSuffix(strings.ToLower(path), ".tgz") {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		gz, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		archive := tar.NewReader(gz)
		for {
			header, err := archive.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			name, err := add(header.Name)
			if err != nil {
				return nil, err
			}
			if header.Size < 0 || header.Size > maxEntryBytes || header.Size > maxExpandedBytes-expanded {
				return nil, fmt.Errorf("archive expanded size limit exceeded at %q", name)
			}
			expanded += header.Size
			switch header.Typeflag {
			case tar.TypeSymlink:
				links = append(links, link{name, header.Linkname, true})
			case tar.TypeLink:
				target, err := normalize(header.Linkname)
				if err != nil {
					return nil, fmt.Errorf("hardlink %q: %w", name, err)
				}
				links = append(links, link{name, target, false})
			case tar.TypeReg, tar.TypeRegA, tar.TypeDir:
			default:
				return nil, fmt.Errorf("unsupported special tar entry %q", name)
			}
		}
	} else {
		return nil, fmt.Errorf("unsupported archive %q", path)
	}
	linkGraph := map[string]string{}
	for _, item := range links {
		target := item.target
		if item.relative {
			raw := strings.ReplaceAll(target, "\\", "/")
			if strings.HasPrefix(raw, "/") || (len(raw) >= 2 && ((raw[0] >= 'a' && raw[0] <= 'z') || (raw[0] >= 'A' && raw[0] <= 'Z')) && raw[1] == ':') {
				return nil, fmt.Errorf("link %q has absolute target %q", item.name, target)
			}
			target = filepath.ToSlash(filepath.Join(filepath.Dir(item.name), strings.ReplaceAll(target, "\\", "/")))
		}
		clean, err := normalize(target)
		if err != nil {
			return nil, fmt.Errorf("link %q: %w", item.name, err)
		}
		if _, ok := seen[strings.ToLower(clean)]; !ok {
			return nil, fmt.Errorf("link %q targets missing archive entry %q", item.name, clean)
		}
		linkGraph[strings.ToLower(item.name)] = strings.ToLower(clean)
	}
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(node string) error {
		if state[node] == 1 {
			return fmt.Errorf("archive link cycle at %q", node)
		}
		if state[node] == 2 {
			return nil
		}
		state[node] = 1
		if next, ok := linkGraph[node]; ok {
			if _, isLink := linkGraph[next]; isLink {
				if err := visit(next); err != nil {
					return err
				}
			}
		}
		state[node] = 2
		return nil
	}
	for node := range linkGraph {
		if err := visit(node); err != nil {
			return nil, err
		}
	}
	sort.Strings(names)
	return names, nil
}
