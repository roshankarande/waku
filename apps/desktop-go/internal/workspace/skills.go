package workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	SkillFile         = "SKILL.md"
	DisabledSkillFile = "SKILL.md.disabled"
)

var (
	ErrSkillLimitExceeded  = errors.New("skills operation exceeded its configured limit")
	ErrSkillOutsideCatalog = errors.New("skill is not a direct child of a catalog root")
)

// SkillSource has the same externally-tagged JSON representation as the Rust
// protocol: "shared" or {"provider":"codex"}.
type SkillSource struct {
	Provider string
}

func SharedSkillSource() SkillSource                  { return SkillSource{} }
func ProviderSkillSource(provider string) SkillSource { return SkillSource{Provider: provider} }

func (s SkillSource) MarshalJSON() ([]byte, error) {
	if s.Provider == "" {
		return []byte(`"shared"`), nil
	}
	return json.Marshal(struct {
		Provider string `json:"provider"`
	}{Provider: s.Provider})
}

func (s *SkillSource) UnmarshalJSON(data []byte) error {
	var shared string
	if err := json.Unmarshal(data, &shared); err == nil {
		if shared != "shared" {
			return fmt.Errorf("unknown skill source %q", shared)
		}
		s.Provider = ""
		return nil
	}
	var provider struct {
		Provider string `json:"provider"`
	}
	if err := json.Unmarshal(data, &provider); err != nil || provider.Provider == "" {
		return errors.New("skill source must be shared or a provider")
	}
	s.Provider = provider.Provider
	return nil
}

type SkillScope string

const (
	SkillScopeUser    SkillScope = "user"
	SkillScopeProject SkillScope = "project"
)

// Paths in these wire-shaped values are absolute paths on the daemon host.
type SkillLocation struct {
	Source  SkillSource `json:"source"`
	Scope   SkillScope  `json:"scope"`
	Root    string      `json:"root"`
	Project *string     `json:"project"`
}

type SkillInstall struct {
	Source    SkillSource `json:"source"`
	Dir       string      `json:"dir"`
	SkillFile string      `json:"skillFile"`
	Enabled   bool        `json:"enabled"`
}

type SkillEntry struct {
	Name            string         `json:"name"`
	Description     string         `json:"description"`
	Scope           SkillScope     `json:"scope"`
	Project         *string        `json:"project"`
	Installs        []SkillInstall `json:"installs"`
	Enabled         bool           `json:"enabled"`
	AllowedTools    *string        `json:"allowedTools"`
	Body            string         `json:"body"`
	SupportingFiles int            `json:"supportingFiles"`
	TotalBytes      uint64         `json:"totalBytes"`
	ModifiedAt      *uint64        `json:"modifiedAt"`
	Duplicates      int            `json:"duplicates"`
	RowKey          uint64         `json:"rowKey"`
}

type SkillsCatalog struct {
	Skills []SkillEntry `json:"skills"`
}

type SkillProject struct {
	Name string
	Root HostPath
}

type SkillsLimits struct {
	MaxSkills        int
	MaxScanEntries   int
	MaxSkillBytes    int64
	MaxTreeDepth     int
	MaxTreeEntries   int
	MaxBodyBytes     int
	MaxRecoveries    int
	MaxRecoveryBytes int64
}

func DefaultSkillsLimits() SkillsLimits {
	return SkillsLimits{
		MaxSkills: 500, MaxScanEntries: 10_000, MaxSkillBytes: 256 << 10,
		MaxTreeDepth: 6, MaxTreeEntries: 500, MaxBodyBytes: 32 << 10,
		MaxRecoveries: 1024, MaxRecoveryBytes: 1 << 20,
	}
}

// SkillsService owns skill discovery and reversible filesystem mutations.
// Home, optional ClaudeConfigRoot, project roots, and TrashRoot are paths on
// the daemon host; the service never consults the client's environment.
type SkillsService struct {
	home                HostPath
	claudeConfigRoot    *HostPath
	trashRoot           HostPath
	limits              SkillsLimits
	mu                  sync.Mutex
	beforeRename        func(from, to string) error // test seam, called under mu
	afterSkillRename    func(from, to string) error // injected post-commit durability failure
	afterManifestRename func(string) error          // injected post-replace durability failure
}

func NewSkillsService(home HostPath, claudeConfigRoot *HostPath, trashRoot HostPath, limits SkillsLimits) (*SkillsService, error) {
	if home.String() == "" || trashRoot.String() == "" {
		return nil, errors.New("skills home and trash root must be absolute daemon-host paths")
	}
	if claudeConfigRoot != nil && claudeConfigRoot.String() == "" {
		return nil, errors.New("Claude config root must be an absolute daemon-host path")
	}
	defaults := DefaultSkillsLimits()
	if limits.MaxSkills == 0 {
		limits.MaxSkills = defaults.MaxSkills
	}
	if limits.MaxScanEntries == 0 {
		limits.MaxScanEntries = defaults.MaxScanEntries
	}
	if limits.MaxSkillBytes == 0 {
		limits.MaxSkillBytes = defaults.MaxSkillBytes
	}
	if limits.MaxTreeDepth == 0 {
		limits.MaxTreeDepth = defaults.MaxTreeDepth
	}
	if limits.MaxTreeEntries == 0 {
		limits.MaxTreeEntries = defaults.MaxTreeEntries
	}
	if limits.MaxBodyBytes == 0 {
		limits.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if limits.MaxRecoveries == 0 {
		limits.MaxRecoveries = defaults.MaxRecoveries
	}
	if limits.MaxRecoveryBytes == 0 {
		limits.MaxRecoveryBytes = defaults.MaxRecoveryBytes
	}
	if limits.MaxSkills < 1 || limits.MaxScanEntries < 1 || limits.MaxSkillBytes < 1 || limits.MaxTreeDepth < 1 || limits.MaxTreeEntries < 1 || limits.MaxBodyBytes < 1 || limits.MaxRecoveries < 1 || limits.MaxRecoveryBytes < 1 {
		return nil, errors.New("skills limits must be positive")
	}
	return &SkillsService{home: home, claudeConfigRoot: claudeConfigRoot, trashRoot: trashRoot, limits: limits}, nil
}

// Locations returns all supported roots in preferred scan order without
// touching the filesystem.
func (s *SkillsService) Locations(projects []SkillProject) []SkillLocation {
	home := s.home.String()
	claude := filepath.Join(home, ".claude")
	if s.claudeConfigRoot != nil {
		claude = s.claudeConfigRoot.String()
	}
	locations := []SkillLocation{
		location(SharedSkillSource(), SkillScopeUser, filepath.Join(home, ".agents", "skills"), nil),
		location(ProviderSkillSource("claude"), SkillScopeUser, filepath.Join(claude, "skills"), nil),
		location(ProviderSkillSource("codex"), SkillScopeUser, filepath.Join(home, ".codex", "skills"), nil),
		location(ProviderSkillSource("openCode"), SkillScopeUser, filepath.Join(home, ".config", "opencode", "skills"), nil),
		location(ProviderSkillSource("cursor"), SkillScopeUser, filepath.Join(home, ".cursor", "skills"), nil),
		location(ProviderSkillSource("fx"), SkillScopeUser, filepath.Join(home, ".fx", "skills"), nil),
		location(ProviderSkillSource("pi"), SkillScopeUser, filepath.Join(home, ".pi", "agent", "skills"), nil),
		location(ProviderSkillSource("ohMyPi"), SkillScopeUser, filepath.Join(home, ".omp", "agent", "skills"), nil),
		location(ProviderSkillSource("amp"), SkillScopeUser, filepath.Join(home, ".config", "agents", "skills"), nil),
	}
	projectRoots := []struct {
		source SkillSource
		suffix string
	}{
		{SharedSkillSource(), filepath.Join(".agents", "skills")},
		{ProviderSkillSource("claude"), filepath.Join(".claude", "skills")},
		{ProviderSkillSource("codex"), filepath.Join(".codex", "skills")},
		{ProviderSkillSource("openCode"), filepath.Join(".opencode", "skills")},
		{ProviderSkillSource("cursor"), filepath.Join(".cursor", "skills")},
		{ProviderSkillSource("fx"), "skills"},
		{ProviderSkillSource("pi"), filepath.Join(".pi", "skills")},
		{ProviderSkillSource("ohMyPi"), filepath.Join(".omp", "skills")},
	}
	for _, project := range projects {
		name := project.Name
		for _, root := range projectRoots {
			locations = append(locations, location(root.source, SkillScopeProject, filepath.Join(project.Root.String(), root.suffix), &name))
		}
	}
	return locations
}

func location(source SkillSource, scope SkillScope, root string, project *string) SkillLocation {
	return SkillLocation{Source: source, Scope: scope, Root: filepath.Clean(root), Project: project}
}

type rawSkill struct {
	name, description string
	scope             SkillScope
	project           *string
	install           SkillInstall
	allowedTools      *string
	body              string
	supportingFiles   int
	totalBytes        uint64
	modifiedAt        *uint64
}

type skillRename struct{ from, to string }

func (s *SkillsService) Scan(ctx context.Context, locations []SkillLocation) (SkillsCatalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SkillsCatalog{}, err
	}
	raw := make([]rawSkill, 0)
	scanned := 0
	for _, loc := range locations {
		if !filepath.IsAbs(loc.Root) {
			return SkillsCatalog{}, errors.New("skill root is not an absolute daemon-host path")
		}
		if len(raw) >= s.limits.MaxSkills || scanned >= s.limits.MaxScanEntries {
			break
		}
		// Roots count toward the scan budget too, so a caller cannot create an
		// unbounded walk by supplying only missing or empty locations.
		scanned++
		if err := s.scanLocation(ctx, loc, &raw, &scanned); err != nil {
			return SkillsCatalog{}, err
		}
	}
	return buildSkillsCatalog(raw), nil
}

func (s *SkillsService) scanLocation(ctx context.Context, loc SkillLocation, out *[]rawSkill, scanned *int) error {
	dir, err := os.Open(loc.Root)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return nil
	}
	if err != nil {
		return nil
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(64)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if *scanned >= s.limits.MaxScanEntries || len(*out) >= s.limits.MaxSkills {
				return nil
			}
			*scanned++
			if strings.HasPrefix(entry.Name(), ".") {
				continue
			}
			skillDir := filepath.Join(loc.Root, entry.Name())
			info, err := os.Stat(skillDir) // deliberately follows linked skill directories
			if err != nil || !info.IsDir() {
				continue
			}
			file := filepath.Join(skillDir, SkillFile)
			enabled := true
			if !regularFileFollowingLinks(file) {
				file, enabled = filepath.Join(skillDir, DisabledSkillFile), false
				if !regularFileFollowingLinks(file) {
					continue
				}
			}
			contents, modified, err := readBoundedSkill(file, s.limits.MaxSkillBytes)
			if errors.Is(err, ErrSkillLimitExceeded) {
				continue
			}
			if err != nil {
				continue
			}
			name, description, tools, body := parseSkillFrontmatter(contents)
			if name == "" {
				name = entry.Name()
			}
			body = truncateUTF8(strings.TrimSpace(body), s.limits.MaxBodyBytes)
			files, bytes := s.measureSkillDir(ctx, skillDir)
			project := cloneString(loc.Project)
			*out = append(*out, rawSkill{name: name, description: description, scope: loc.Scope, project: project,
				install:      SkillInstall{Source: loc.Source, Dir: skillDir, SkillFile: file, Enabled: enabled},
				allowedTools: tools, body: body, supportingFiles: max(0, files-1), totalBytes: bytes, modifiedAt: modified})
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return nil
		}
	}
}

func regularFileFollowingLinks(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func readBoundedSkill(path string, limit int64) (string, *uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return "", nil, err
	}
	if int64(len(data)) > limit {
		return "", nil, ErrSkillLimitExceeded
	}
	if !utf8.Valid(data) {
		return "", nil, errors.New("skill document is not UTF-8")
	}
	var modified *uint64
	if info, err := file.Stat(); err == nil {
		seconds := uint64(max(int64(0), info.ModTime().Unix()))
		modified = &seconds
	}
	return string(data), modified, nil
}

func (s *SkillsService) measureSkillDir(ctx context.Context, root string) (int, uint64) {
	type item struct {
		path  string
		depth int
	}
	stack := []item{{root, 0}}
	entries, files := 0, 0
	var bytes uint64
	for len(stack) > 0 && entries < s.limits.MaxTreeEntries {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.depth > s.limits.MaxTreeDepth || ctx.Err() != nil {
			continue
		}
		dir, err := os.Open(current.path)
		if err != nil {
			continue
		}
		for entries < s.limits.MaxTreeEntries {
			batch, readErr := dir.ReadDir(64)
			for _, child := range batch {
				if entries >= s.limits.MaxTreeEntries {
					break
				}
				entries++
				if strings.HasPrefix(child.Name(), ".") {
					continue
				}
				path := filepath.Join(current.path, child.Name())
				info, err := os.Stat(path) // follows links, bounded by depth and entries
				if err != nil {
					continue
				}
				if info.IsDir() {
					stack = append(stack, item{path, current.depth + 1})
				} else if info.Mode().IsRegular() {
					files++
					bytes += uint64(info.Size())
				}
			}
			if readErr != nil {
				break
			}
		}
		dir.Close()
	}
	return files, bytes
}

func buildSkillsCatalog(raw []rawSkill) SkillsCatalog {
	skills := make([]SkillEntry, 0, len(raw))
	byIdentity := make(map[string]int)
	for _, found := range raw {
		project := "\x00"
		if found.project != nil {
			project = *found.project
		}
		key := project + "\x00" + found.name
		if index, ok := byIdentity[key]; ok {
			entry := &skills[index]
			entry.Enabled = entry.Enabled || found.install.Enabled
			if entry.Description == "" {
				entry.Description = found.description
			}
			if entry.AllowedTools == nil {
				entry.AllowedTools = found.allowedTools
			}
			if entry.Body == "" {
				entry.Body = found.body
			}
			if newer(found.modifiedAt, entry.ModifiedAt) {
				entry.ModifiedAt = found.modifiedAt
			}
			entry.Installs = append(entry.Installs, found.install)
			continue
		}
		byIdentity[key] = len(skills)
		skills = append(skills, SkillEntry{Name: found.name, Description: found.description, Scope: found.scope,
			Project: found.project, Installs: []SkillInstall{found.install}, Enabled: found.install.Enabled,
			AllowedTools: found.allowedTools, Body: found.body, SupportingFiles: found.supportingFiles,
			TotalBytes: found.totalBytes, ModifiedAt: found.modifiedAt})
	}
	sort.SliceStable(skills, func(i, j int) bool {
		pi, pj := "", ""
		if skills[i].Project != nil {
			pi = "\x01" + *skills[i].Project
		}
		if skills[j].Project != nil {
			pj = "\x01" + *skills[j].Project
		}
		if pi != pj {
			return pi < pj
		}
		return strings.ToLower(skills[i].Name) < strings.ToLower(skills[j].Name)
	})
	counts := make(map[string]int)
	for _, skill := range skills {
		counts[skill.Name]++
	}
	for i := range skills {
		skills[i].Duplicates = counts[skills[i].Name] - 1
		h := fnv.New64a()
		for _, install := range skills[i].Installs {
			_, _ = h.Write([]byte(install.Dir))
			if install.Enabled {
				_, _ = h.Write([]byte{1})
			} else {
				_, _ = h.Write([]byte{0})
			}
		}
		skills[i].RowKey = h.Sum64()
	}
	if skills == nil {
		skills = []SkillEntry{}
	}
	return SkillsCatalog{Skills: skills}
}

func newer(a, b *uint64) bool { return a != nil && (b == nil || *a > *b) }
func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// SetEnabled atomically renames each skill marker. The batch is compensated
// in reverse order if a later rename fails.
func (s *SkillsService) SetEnabled(ctx context.Context, locations []SkillLocation, dirs []string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	validated, err := validateSkillDirs(locations, dirs)
	if err != nil {
		return err
	}
	moved := make([]skillRename, 0, len(validated))
	for _, dir := range validated {
		from, to := filepath.Join(dir, SkillFile), filepath.Join(dir, DisabledSkillFile)
		if enabled {
			from, to = to, from
			// A live file always wins over a stale disabled copy.
			if regularFileFollowingLinks(to) {
				continue
			}
		}
		if !regularFileFollowingLinks(from) {
			continue
		}
		result, err := s.rename(from, to)
		if result.Committed {
			moved = append(moved, skillRename{from: to, to: from})
		}
		if err != nil {
			return fmt.Errorf("set skill enabled: %w", withSkillRollback(err, moved))
		}
	}
	return nil
}

type SkillRecovery struct {
	Items []SkillRecoveryItem `json:"items"`
}

type SkillRecoveryItem struct {
	Identity    string `json:"identity"`
	OriginalDir string `json:"originalDir"`
}

const skillRecoveryManifest = "recovery-v1.json"

type skillRecoveryState struct {
	SchemaVersion int               `json:"schemaVersion"`
	ByOriginal    map[string]string `json:"byOriginal"`
}

// Trash moves installs into a configured recoverable daemon-host directory.
func (s *SkillsService) Trash(ctx context.Context, locations []SkillLocation, dirs []string) (SkillRecovery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SkillRecovery{}, err
	}
	validated, err := validateSkillDirs(locations, dirs)
	if err != nil {
		return SkillRecovery{}, err
	}
	if err := os.MkdirAll(s.trashRoot.String(), 0o700); err != nil {
		return SkillRecovery{}, fmt.Errorf("create skills trash: %w", err)
	}
	state, err := s.loadRecoveryState()
	if err != nil {
		return SkillRecovery{}, err
	}
	for _, dir := range validated {
		if state.ByOriginal[dir] != "" {
			return SkillRecovery{}, fmt.Errorf("skill already has a recoverable trashed copy: %s", dir)
		}
	}
	if len(state.ByOriginal)+len(validated) > s.limits.MaxRecoveries {
		return SkillRecovery{}, ErrSkillLimitExceeded
	}
	recovery := SkillRecovery{Items: make([]SkillRecoveryItem, 0, len(validated))}
	moved := make([]skillRename, 0, len(validated))
	for _, dir := range validated {
		if err := ctx.Err(); err != nil {
			return SkillRecovery{}, withSkillRollback(err, moved)
		}
		if !regularFileFollowingLinks(filepath.Join(dir, SkillFile)) && !regularFileFollowingLinks(filepath.Join(dir, DisabledSkillFile)) {
			return SkillRecovery{}, withSkillRollback(errors.New("trash skill: directory has no skill marker"), moved)
		}
		identity, target, err := s.uniqueTrashTarget()
		if err != nil {
			return SkillRecovery{}, withSkillRollback(err, moved)
		}
		result, err := s.rename(dir, target)
		if result.Committed {
			moved = append(moved, skillRename{from: target, to: dir})
		}
		if err != nil {
			return SkillRecovery{}, fmt.Errorf("trash skill: %w", withSkillRollback(err, moved))
		}
		recovery.Items = append(recovery.Items, SkillRecoveryItem{Identity: identity, OriginalDir: dir})
		state.ByOriginal[dir] = identity
	}
	if err := s.saveRecoveryState(state); err != nil {
		return SkillRecovery{}, fmt.Errorf("persist skills recovery metadata: %w", withSkillRollback(err, moved))
	}
	return recovery, nil
}

// Recover restores a prior Trash result. Existing destinations are never
// replaced; a partial restore is moved back to trash if a later item fails.
func (s *SkillsService) Recover(ctx context.Context, locations []SkillLocation, recovery SkillRecovery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverLocked(ctx, locations, recovery)
}

// RecoverPaths resolves durable recovery identities by original daemon-host
// path, so a restarted daemon can restore a prior acknowledged trash action.
func (s *SkillsService) RecoverPaths(ctx context.Context, locations []SkillLocation, dirs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadRecoveryState()
	if err != nil {
		return err
	}
	recovery := SkillRecovery{Items: make([]SkillRecoveryItem, 0, len(dirs))}
	for _, dir := range dirs {
		identity := state.ByOriginal[dir]
		if identity == "" {
			return fmt.Errorf("no recoverable trashed skill for %s", dir)
		}
		recovery.Items = append(recovery.Items, SkillRecoveryItem{Identity: identity, OriginalDir: dir})
	}
	return s.recoverLocked(ctx, locations, recovery)
}

func (s *SkillsService) recoverLocked(ctx context.Context, locations []SkillLocation, recovery SkillRecovery) error {
	dirs := make([]string, len(recovery.Items))
	for i, item := range recovery.Items {
		dirs[i] = item.OriginalDir
	}
	if _, err := validateSkillDirs(locations, dirs); err != nil {
		return err
	}
	state, err := s.loadRecoveryState()
	if err != nil {
		return err
	}
	moved := make([]skillRename, 0, len(recovery.Items))
	for _, item := range recovery.Items {
		if err := ctx.Err(); err != nil {
			return withSkillRollback(err, moved)
		}
		if len(item.Identity) != 32 {
			return withSkillRollback(errors.New("invalid skills recovery identity"), moved)
		}
		if _, err := hex.DecodeString(item.Identity); err != nil {
			return withSkillRollback(errors.New("invalid skills recovery identity"), moved)
		}
		if state.ByOriginal[item.OriginalDir] != item.Identity {
			return withSkillRollback(errors.New("skills recovery metadata does not match the requested item"), moved)
		}
		source := filepath.Join(s.trashRoot.String(), item.Identity)
		if filepath.Dir(source) != filepath.Clean(s.trashRoot.String()) {
			return withSkillRollback(errors.New("invalid skills recovery identity"), moved)
		}
		if _, err := os.Lstat(item.OriginalDir); err == nil {
			return withSkillRollback(fmt.Errorf("recover skill: destination already exists: %s", item.OriginalDir), moved)
		} else if !errors.Is(err, os.ErrNotExist) {
			return withSkillRollback(err, moved)
		}
		result, err := s.rename(source, item.OriginalDir)
		if result.Committed {
			moved = append(moved, skillRename{from: item.OriginalDir, to: source})
		}
		if err != nil {
			return fmt.Errorf("recover skill: %w", withSkillRollback(err, moved))
		}
		delete(state.ByOriginal, item.OriginalDir)
	}
	if err := s.saveRecoveryState(state); err != nil {
		return fmt.Errorf("persist restored skills recovery metadata: %w", withSkillRollback(err, moved))
	}
	return nil
}

func (s *SkillsService) loadRecoveryState() (skillRecoveryState, error) {
	state := skillRecoveryState{SchemaVersion: 1, ByOriginal: map[string]string{}}
	path := filepath.Join(s.trashRoot.String(), skillRecoveryManifest)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, s.limits.MaxRecoveryBytes+1))
	if err != nil {
		return state, err
	}
	if int64(len(data)) > s.limits.MaxRecoveryBytes {
		return state, ErrSkillLimitExceeded
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode skills recovery metadata: %w", err)
	}
	if state.SchemaVersion != 1 || state.ByOriginal == nil || len(state.ByOriginal) > s.limits.MaxRecoveries {
		return state, errors.New("invalid skills recovery metadata")
	}
	pruned := false
	for original, identity := range state.ByOriginal {
		if !filepath.IsAbs(original) || filepath.Clean(original) != original || len(identity) != 32 {
			return state, errors.New("invalid skills recovery metadata entry")
		}
		if _, err := hex.DecodeString(identity); err != nil {
			return state, errors.New("invalid skills recovery identity")
		}
		target := filepath.Join(s.trashRoot.String(), identity)
		_, targetErr := os.Lstat(target)
		originalRestored := regularFileFollowingLinks(filepath.Join(original, SkillFile)) || regularFileFollowingLinks(filepath.Join(original, DisabledSkillFile))
		if errors.Is(targetErr, os.ErrNotExist) && originalRestored {
			delete(state.ByOriginal, original)
			pruned = true
		}
	}
	if pruned {
		if err := s.saveRecoveryState(state); err != nil {
			return state, fmt.Errorf("prune stale skills recovery metadata: %w", err)
		}
	}
	return state, nil
}

func (s *SkillsService) saveRecoveryState(state skillRecoveryState) (err error) {
	if len(state.ByOriginal) > s.limits.MaxRecoveries {
		return ErrSkillLimitExceeded
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if int64(len(data)) > s.limits.MaxRecoveryBytes {
		return ErrSkillLimitExceeded
	}
	if err := os.MkdirAll(s.trashRoot.String(), 0o700); err != nil {
		return err
	}
	manifestPath := filepath.Join(s.trashRoot.String(), skillRecoveryManifest)
	prior, existed, err := s.readRecoveryManifestBytes(manifestPath)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.trashRoot.String(), ".recovery-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(data)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	committed := false
	if err == nil {
		err = os.Rename(temporaryPath, manifestPath)
		committed = err == nil
	}
	if err == nil && s.afterManifestRename != nil {
		err = s.afterManifestRename(manifestPath)
	}
	if err == nil {
		err = syncSkillDirectory(s.trashRoot.String())
	}
	if err != nil && committed {
		err = errors.Join(err, s.restoreRecoveryManifest(manifestPath, prior, existed))
	}
	return err
}

func (s *SkillsService) readRecoveryManifestBytes(path string) ([]byte, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, s.limits.MaxRecoveryBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > s.limits.MaxRecoveryBytes {
		return nil, false, ErrSkillLimitExceeded
	}
	return data, true, nil
}

func (s *SkillsService) restoreRecoveryManifest(path string, prior []byte, existed bool) error {
	if !existed {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return syncSkillDirectory(s.trashRoot.String())
	}
	temporary, err := os.CreateTemp(s.trashRoot.String(), ".recovery-restore-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(prior)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temporaryPath, path)
	}
	if err == nil {
		err = syncSkillDirectory(s.trashRoot.String())
	}
	return err
}

func (s *SkillsService) uniqueTrashTarget() (string, string, error) {
	for attempts := 0; attempts < 8; attempts++ {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return "", "", err
		}
		identity := hex.EncodeToString(bytes)
		target := filepath.Join(s.trashRoot.String(), identity)
		if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
			return identity, target, nil
		}
	}
	return "", "", errors.New("could not allocate collision-free skills trash identity")
}

type skillRenameResult struct{ Committed, Durable bool }

func (s *SkillsService) rename(from, to string) (skillRenameResult, error) {
	if s.beforeRename != nil {
		if err := s.beforeRename(from, to); err != nil {
			return skillRenameResult{}, err
		}
	}
	if err := os.Rename(from, to); err != nil {
		return skillRenameResult{}, err
	}
	result := skillRenameResult{Committed: true}
	durabilityErr := error(nil)
	if s.afterSkillRename != nil {
		durabilityErr = s.afterSkillRename(from, to)
	}
	if durabilityErr == nil {
		durabilityErr = syncSkillDirectory(filepath.Dir(from))
	}
	if durabilityErr == nil && filepath.Dir(to) != filepath.Dir(from) {
		durabilityErr = syncSkillDirectory(filepath.Dir(to))
	}
	if durabilityErr == nil {
		result.Durable = true
		return result, nil
	}
	// A rename that committed but was not made durable is not reported as a
	// clean failure until its visible namespace change has been compensated.
	if rollbackErr := os.Rename(to, from); rollbackErr != nil {
		return result, errors.Join(durabilityErr, fmt.Errorf("compensate committed skill rename: %w", rollbackErr))
	}
	result.Committed = false
	rollbackDurability := errors.Join(syncSkillDirectory(filepath.Dir(from)))
	if filepath.Dir(to) != filepath.Dir(from) {
		rollbackDurability = errors.Join(rollbackDurability, syncSkillDirectory(filepath.Dir(to)))
	}
	return result, errors.Join(durabilityErr, rollbackDurability)
}

func rollbackRenames(moves []skillRename) error {
	var joined error
	for i := len(moves) - 1; i >= 0; i-- {
		move := moves[i]
		if err := os.Rename(move.from, move.to); err != nil {
			joined = errors.Join(joined, fmt.Errorf("rollback rename %s: %w", move.from, err))
			continue
		}
		joined = errors.Join(joined, syncSkillDirectory(filepath.Dir(move.from)))
		if filepath.Dir(move.from) != filepath.Dir(move.to) {
			joined = errors.Join(joined, syncSkillDirectory(filepath.Dir(move.to)))
		}
	}
	return joined
}

func withSkillRollback(cause error, moves []skillRename) error {
	return errors.Join(cause, rollbackRenames(moves))
}

func validateSkillDirs(locations []SkillLocation, dirs []string) ([]string, error) {
	roots := make(map[string]struct{}, len(locations))
	for _, loc := range locations {
		if !filepath.IsAbs(loc.Root) {
			return nil, errors.New("skill root is not an absolute daemon-host path")
		}
		roots[filepath.Clean(loc.Root)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(dirs))
	validated := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
			return nil, ErrSkillOutsideCatalog
		}
		if _, ok := roots[filepath.Dir(dir)]; !ok || filepath.Base(dir) == "." || strings.HasPrefix(filepath.Base(dir), ".") {
			return nil, ErrSkillOutsideCatalog
		}
		if _, duplicate := seen[dir]; duplicate {
			return nil, errors.New("duplicate skill directory")
		}
		seen[dir] = struct{}{}
		validated = append(validated, dir)
	}
	return validated, nil
}

func truncateUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	end := maximum
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return strings.TrimSpace(value[:end])
}

func parseSkillFrontmatter(contents string) (name, description string, tools *string, body string) {
	body = contents
	if !strings.HasPrefix(contents, "---\n") && !strings.HasPrefix(contents, "---\r\n") {
		return
	}
	normalized := strings.ReplaceAll(contents, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	end := -1
	for i := 1; i < len(lines); i++ {
		if lines[i] == "---" {
			end = i
			break
		}
	}
	if end < 0 {
		return
	}
	body = strings.Join(lines[end+1:], "\n")
	for i := 1; i < end; i++ {
		key, value, ok := strings.Cut(lines[i], ":")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if value == ">" || value == ">-" || value == "|" || value == "|-" {
			parts := []string{}
			for i+1 < end && (strings.HasPrefix(lines[i+1], " ") || lines[i+1] == "") {
				i++
				parts = append(parts, strings.TrimSpace(lines[i]))
			}
			if strings.HasPrefix(value, ">") {
				value = strings.Join(parts, " ")
			} else {
				value = strings.Join(parts, "\n")
			}
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		switch key {
		case "name":
			name = value
		case "description":
			description = value
		case "allowed-tools":
			copied := value
			tools = &copied
		}
	}
	return
}
