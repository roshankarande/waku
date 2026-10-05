package validation

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

type ManifestSpec struct {
	ApplicationExecutables []string          `json:"applicationExecutables"`
	RetainedAssets         []RetainedAsset   `json:"retainedAssets"`
	RequiredPaths          []string          `json:"requiredPaths"`
	LicensePaths           []string          `json:"licensePaths"`
	NoticePaths            []string          `json:"noticePaths"`
	TargetPlatform         string            `json:"targetPlatform"`
	ApplicationRoles       map[string]string `json:"applicationRoles"`
	ReviewedSBOMPath       string            `json:"reviewedSbomPath,omitempty"`
	ReviewedSBOMSHA256     string            `json:"reviewedSbomSha256,omitempty"`
}

func GenerateDistributionManifest(root string, spec ManifestSpec) (DistributionManifest, error) {
	if len(spec.ApplicationExecutables) == 0 {
		return DistributionManifest{}, errors.New("applicationExecutables required")
	}
	layout, err := ReleasePackagingLayout(spec.TargetPlatform)
	if err != nil {
		return DistributionManifest{}, err
	}
	wantRoles := map[string]string{layout.DesktopExecutable: "desktop", layout.DaemonExecutable: "daemon"}
	if len(spec.ApplicationExecutables) != len(wantRoles) {
		return DistributionManifest{}, errors.New("release must declare exactly the desktop and daemon executables")
	}
	manifest := DistributionManifest{SchemaVersion: SchemaVersion, ApplicationExecutables: append([]string(nil), spec.ApplicationExecutables...), RetainedAssets: append([]RetainedAsset(nil), spec.RetainedAssets...), RequiredPaths: append([]string(nil), spec.RequiredPaths...), LicensePaths: append([]string(nil), spec.LicensePaths...), NoticePaths: append([]string(nil), spec.NoticePaths...), NoApplicationRustRuntime: true, NoApplicationJSRuntime: true, TargetPlatform: spec.TargetPlatform, ReviewedSBOMPath: spec.ReviewedSBOMPath, ReviewedSBOMSHA256: spec.ReviewedSBOMSHA256}
	manifest.LicenseSHA256, manifest.NoticeSHA256, manifest.RequiredSHA256 = map[string]string{}, map[string]string{}, map[string]string{}
	sort.Strings(manifest.ApplicationExecutables)
	sort.Strings(manifest.RequiredPaths)
	sort.Strings(manifest.LicensePaths)
	sort.Strings(manifest.NoticePaths)
	dependencySet := map[string]sbomDependency{}
	for _, relative := range manifest.ApplicationExecutables {
		role, ok := wantRoles[relative]
		if !ok || spec.ApplicationRoles[relative] != role {
			return DistributionManifest{}, errors.New(relative + ": command role does not match target packaging layout")
		}
		path, err := releasePath(root, relative)
		if err != nil {
			return DistributionManifest{}, err
		}
		metadata, err := applicationMetadata(relative, path)
		if err != nil {
			return DistributionManifest{}, err
		}
		if metadata.CGOEnabled != "0" {
			return DistributionManifest{}, errors.New(relative + ": CGO_ENABLED=0 not proven")
		}
		probe, err := probeApplicationRole(path)
		if err != nil || probe.Role != role {
			return DistributionManifest{}, errors.New(relative + ": command-specific role probe did not prove role " + role)
		}
		metadata.Role = role
		metadata.RoleProbeSHA256 = roleProbeDigest(probe)
		manifest.ApplicationMetadata = append(manifest.ApplicationMetadata, metadata)
		for _, dependency := range applicationDependencies(path) {
			dependencySet[dependency.Path+"@"+dependency.Version] = dependency
		}
	}
	for index := range manifest.RetainedAssets {
		asset := &manifest.RetainedAssets[index]
		path, err := releasePath(root, asset.Path)
		if err != nil {
			return DistributionManifest{}, err
		}
		asset.SHA256, err = fileSHA256(path)
		if err != nil {
			return DistributionManifest{}, err
		}
		sort.Strings(asset.Platforms)
	}
	sort.Slice(manifest.RetainedAssets, func(i, j int) bool { return manifest.RetainedAssets[i].Path < manifest.RetainedAssets[j].Path })
	if spec.ReviewedSBOMPath == "" || !validSHA256(spec.ReviewedSBOMSHA256) {
		return DistributionManifest{}, errors.New("distribution requires reviewedSbomPath and reviewedSbomSha256")
	}
	{
		sbomPath, err := releasePath(root, spec.ReviewedSBOMPath)
		if err != nil {
			return DistributionManifest{}, err
		}
		digest, err := fileSHA256(sbomPath)
		if err != nil || !strings.EqualFold(digest, spec.ReviewedSBOMSHA256) {
			return DistributionManifest{}, errors.New("reviewed SBOM digest mismatch")
		}
		dependencies := make([]sbomDependency, 0, len(dependencySet))
		for _, dependency := range dependencySet {
			dependencies = append(dependencies, dependency)
		}
		if err := verifyReviewedSBOM(sbomPath, manifest.RetainedAssets, spec.TargetPlatform, dependencies); err != nil {
			return DistributionManifest{}, err
		}
	}
	for _, path := range append(append([]string{}, manifest.LicensePaths...), manifest.NoticePaths...) {
		if _, err := releasePath(root, path); err != nil {
			return DistributionManifest{}, err
		}
	}
	for _, path := range manifest.LicensePaths {
		full, _ := releasePath(root, path)
		manifest.LicenseSHA256[path], _ = fileSHA256(full)
	}
	for _, path := range manifest.NoticePaths {
		full, _ := releasePath(root, path)
		manifest.NoticeSHA256[path], _ = fileSHA256(full)
	}
	for _, path := range manifest.RequiredPaths {
		full, _ := releasePath(root, path)
		manifest.RequiredSHA256[path], _ = fileSHA256(full)
	}
	report := InspectDistribution(root, manifest)
	if report.Status != StatusPass {
		return DistributionManifest{}, errors.New("generated distribution does not pass inspection")
	}
	return manifest, nil
}

type reviewedSBOM struct {
	SchemaVersion int               `json:"schemaVersion"`
	Assets        []RetainedAsset   `json:"assets"`
	Dependencies  []sbomDependency  `json:"dependencies"`
	Applications  []sbomApplication `json:"applications,omitempty"`
	Toolchains    []sbomToolchain   `json:"toolchains,omitempty"`
}

type sbomApplication struct {
	Path          string `json:"path"`
	ModulePath    string `json:"modulePath"`
	ModuleVersion string `json:"moduleVersion,omitempty"`
	GoVersion     string `json:"goVersion"`
	SHA256        string `json:"sha256"`
}
type sbomToolchain struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	LicensePath string `json:"licensePath"`
}

type sbomDependency struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Sum     string `json:"sum,omitempty"`
}

func verifyReviewedSBOM(path string, assets []RetainedAsset, platform string, expectedDependencies []sbomDependency) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var sbom reviewedSBOM
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&sbom); err != nil || sbom.SchemaVersion != SchemaVersion {
		return errors.New("reviewed SBOM is invalid")
	}
	if sbom.Dependencies == nil || sbom.Assets == nil {
		return errors.New("reviewed SBOM must explicitly enumerate dependencies and retained assets")
	}
	seenDependency := map[string]string{}
	for _, dependency := range sbom.Dependencies {
		key := dependency.Path + "@" + dependency.Version
		if dependency.Path == "" || dependency.Version == "" {
			return errors.New("reviewed SBOM contains invalid or duplicate dependency")
		}
		if _, duplicate := seenDependency[key]; duplicate {
			return errors.New("reviewed SBOM contains invalid or duplicate dependency")
		}
		seenDependency[key] = dependency.Sum
	}
	expectedSet := map[string]string{}
	for _, dependency := range expectedDependencies {
		expectedSet[dependency.Path+"@"+dependency.Version] = dependency.Sum
	}
	if len(seenDependency) != len(expectedSet) {
		return errors.New("reviewed SBOM does not exactly cover executable dependencies")
	}
	for dependency, sum := range expectedSet {
		if reviewedSum, ok := seenDependency[dependency]; !ok || reviewedSum != sum {
			return errors.New("reviewed SBOM is missing an executable dependency")
		}
	}
	byPath := make(map[string]RetainedAsset, len(sbom.Assets))
	for _, asset := range sbom.Assets {
		if _, duplicate := byPath[asset.Path]; duplicate {
			return errors.New("reviewed SBOM contains duplicate asset path")
		}
		byPath[asset.Path] = asset
	}
	for _, asset := range assets {
		reviewed, ok := byPath[asset.Path]
		if !ok || reviewed.Identity != asset.Identity || reviewed.Version != asset.Version || reviewed.Source != asset.Source || reviewed.SourceSHA256 != asset.SourceSHA256 || reviewed.SHA256 != asset.SHA256 || reviewed.LicensePath != asset.LicensePath || !slices.Equal(reviewed.Platforms, asset.Platforms) {
			return errors.New(asset.Path + ": retained asset does not exactly match reviewed SBOM")
		}
		if !slices.Contains(asset.Platforms, platform) || !validRetainedSource(asset.Source) || !validSHA256(asset.SourceSHA256) {
			return errors.New(asset.Path + ": reviewed retained asset platform/source identity is invalid")
		}
		for _, target := range asset.Platforms {
			if _, err := ReleasePackagingLayout(target); err != nil {
				return errors.New(asset.Path + ": reviewed retained asset contains unsupported platform")
			}
		}
	}
	return nil
}

func applicationDependencies(path string) []sbomDependency {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return nil
	}
	dependencies := make([]sbomDependency, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		if dependency.Replace != nil {
			dependency = dependency.Replace
		}
		dependencies = append(dependencies, sbomDependency{Path: dependency.Path, Version: dependency.Version, Sum: dependency.Sum})
	}
	return dependencies
}

func releasePath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", errors.New("release path must be relative")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || len(rel) > 3 && rel[:3] == ".."+string(filepath.Separator) {
		return "", errors.New("release path escapes root")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("release path is not a regular file")
	}
	return path, nil
}

type PackagingLayout struct {
	Platform          string
	DesktopExecutable string
	DaemonExecutable  string
	LicenseDirectory  string
	NoticeDirectory   string
	ArchiveExtension  string
	InfoPlist         string
}

func ReleasePackagingLayout(platform string) (PackagingLayout, error) {
	switch platform {
	case "linux":
		return PackagingLayout{platform, "bin/waku-go", "bin/waku-go-daemon", "share/licenses/waku", "share/notices/waku", ".tar.gz", ""}, nil
	case "darwin":
		return PackagingLayout{platform, "Waku.app/Contents/MacOS/waku-go", "Waku.app/Contents/MacOS/waku-go-daemon", "Waku.app/Contents/Resources/licenses", "Waku.app/Contents/Resources/notices", ".zip", "Waku.app/Contents/Info.plist"}, nil
	case "windows":
		return PackagingLayout{platform, "Waku/waku-go.exe", "Waku/waku-go-daemon.exe", "Waku/licenses", "Waku/notices", ".zip", ""}, nil
	default:
		return PackagingLayout{}, errors.New("unsupported release platform")
	}
}
