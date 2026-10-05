package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// DaemonSettingsValue is the protocol-v7 settings document. Paths and binary
// overrides are daemon-host values; clients must never reinterpret them as
// local paths. Extra preserves settings introduced by a newer daemon.
type DaemonSettingsValue struct {
	ComputerUseEnabled      bool
	ComputerUseAllowedApps  []ComputerAppGrant
	DisabledProviders       []string
	ProviderBinaryOverrides map[string]string
	Extra                   map[string]json.RawMessage
}

type ComputerAppGrant struct {
	BundleID string `json:"bundleId"`
	AppName  string `json:"appName"`
}

func DecodeDaemonSettings(raw json.RawMessage) (DaemonSettingsValue, error) {
	if err := validateDaemonSettings(raw); err != nil {
		return DaemonSettingsValue{}, err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return DaemonSettingsValue{}, err
	}
	value := DaemonSettingsValue{Extra: make(map[string]json.RawMessage)}
	for key, field := range object {
		switch key {
		case "computer_use_enabled":
			if err := Decode(field, &value.ComputerUseEnabled); err != nil {
				return DaemonSettingsValue{}, err
			}
		case "computer_use_allowed_apps":
			if err := Decode(field, &value.ComputerUseAllowedApps); err != nil {
				return DaemonSettingsValue{}, err
			}
		case "disabled_providers":
			if err := Decode(field, &value.DisabledProviders); err != nil {
				return DaemonSettingsValue{}, err
			}
		case "provider_binary_overrides":
			if err := Decode(field, &value.ProviderBinaryOverrides); err != nil {
				return DaemonSettingsValue{}, err
			}
		default:
			value.Extra[key] = append(json.RawMessage(nil), field...)
		}
	}
	if value.ComputerUseAllowedApps == nil {
		value.ComputerUseAllowedApps = []ComputerAppGrant{}
	}
	if value.DisabledProviders == nil {
		value.DisabledProviders = []string{}
	}
	if value.ProviderBinaryOverrides == nil {
		value.ProviderBinaryOverrides = map[string]string{}
	}
	return value, nil
}

func (s DaemonSettingsValue) MarshalJSON() ([]byte, error) {
	object := make(map[string]json.RawMessage, len(s.Extra)+4)
	for key, value := range s.Extra {
		object[key] = append(json.RawMessage(nil), value...)
	}
	put := func(key string, value any) error {
		raw, err := json.Marshal(value)
		if err == nil {
			object[key] = raw
		}
		return err
	}
	if err := put("computer_use_enabled", s.ComputerUseEnabled); err != nil {
		return nil, err
	}
	if err := put("computer_use_allowed_apps", nonNil(s.ComputerUseAllowedApps)); err != nil {
		return nil, err
	}
	if err := put("disabled_providers", nonNil(s.DisabledProviders)); err != nil {
		return nil, err
	}
	if len(s.ProviderBinaryOverrides) != 0 {
		if err := put("provider_binary_overrides", s.ProviderBinaryOverrides); err != nil {
			return nil, err
		}
	}
	return json.Marshal(object)
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

type UsageWindowValue struct {
	Kind  string
	Count uint32
}

func DecodeUsageWindow(raw json.RawMessage) (UsageWindowValue, error) {
	if err := validateUsageWindow(raw); err != nil {
		return UsageWindowValue{}, err
	}
	var unit string
	if Decode(raw, &unit) == nil {
		return UsageWindowValue{Kind: unit}, nil
	}
	var object map[string]uint32
	if err := Decode(raw, &object); err != nil {
		return UsageWindowValue{}, err
	}
	for kind, count := range object {
		return UsageWindowValue{Kind: kind, Count: count}, nil
	}
	return UsageWindowValue{}, fmt.Errorf("usage window has no variant")
}

func (w UsageWindowValue) MarshalJSON() ([]byte, error) {
	switch w.Kind {
	case "thisMonth", "lastMonth":
		return json.Marshal(w.Kind)
	case "trailingDays", "months":
		return json.Marshal(map[string]uint32{w.Kind: w.Count})
	default:
		return nil, fmt.Errorf("unknown usage window %q", w.Kind)
	}
}

type TokenTotalsValue struct {
	UncachedInput uint64 `json:"uncachedInput"`
	CachedInput   uint64 `json:"cachedInput"`
	CacheCreation uint64 `json:"cacheCreation"`
	Output        uint64 `json:"output"`
	Reasoning     uint64 `json:"reasoning"`
}
type ProviderDayValue struct {
	CostUSD     float64 `json:"costUsd"`
	TotalTokens uint64  `json:"totalTokens"`
}
type CostQualityValue struct {
	ProviderReportedShare float64 `json:"providerReportedShare"`
	ModelPricedShare      float64 `json:"modelPricedShare"`
	UnpricedShare         float64 `json:"unpricedShare"`
	CacheSavingsUSD       float64 `json:"cacheSavingsUsd"`
}
type UsageProviderSlice struct {
	Provider    string  `json:"provider"`
	CostUSD     float64 `json:"costUsd"`
	TotalTokens uint64  `json:"totalTokens"`
	CostShare   float64 `json:"costShare"`
	TokenShare  float64 `json:"tokenShare"`
}
type UsageModelSlice struct {
	Provider    string  `json:"provider"`
	Model       string  `json:"model"`
	CostUSD     float64 `json:"costUsd"`
	TotalTokens uint64  `json:"totalTokens"`
	CostShare   float64 `json:"costShare"`
}
type UsageDaySlice struct {
	Day         string              `json:"day"`
	CostUSD     float64             `json:"costUsd"`
	TotalTokens uint64              `json:"totalTokens"`
	ByProvider  [2]ProviderDayValue `json:"byProvider"`
}
type UsageMonthSlice struct {
	FirstDay    string              `json:"firstDay"`
	CostUSD     float64             `json:"costUsd"`
	TotalTokens uint64              `json:"totalTokens"`
	ByProvider  [2]ProviderDayValue `json:"byProvider"`
	Sessions    uint64              `json:"sessions"`
	ActiveDays  uint32              `json:"activeDays"`
	TopModels   [][2]any            `json:"topModels"`
}
type UsageProjectSlice struct {
	Path        string              `json:"path"`
	CostUSD     float64             `json:"costUsd"`
	TotalTokens uint64              `json:"totalTokens"`
	ByProvider  [2]ProviderDayValue `json:"byProvider"`
	Sessions    uint64              `json:"sessions"`
	CostShare   float64             `json:"costShare"`
	LastDay     *string             `json:"lastDay"`
	TopModels   [][2]any            `json:"topModels"`
}
type RustDurationValue struct {
	Secs  uint64 `json:"secs"`
	Nanos uint32 `json:"nanos"`
}

// UsageHistoryValue uses explicit uint64 counters so values above JavaScript's
// safe integer range remain exact in Go.
type UsageHistoryValue struct {
	Window                     UsageWindowValue
	SinceDay, UntilDay         string
	Totals                     TokenTotalsValue
	TotalTokens                uint64
	CostUSD                    float64
	Records, Sessions          uint64
	Providers                  []UsageProviderSlice
	Models                     []UsageModelSlice
	Daily                      []UsageDaySlice
	Months                     []UsageMonthSlice
	Projects                   []UsageProjectSlice
	Quality                    CostQualityValue
	Pricing                    string
	ScannedFiles, SkippedFiles uint64
	Errors                     []string
	ScanDuration               RustDurationValue
}

func DecodeUsageHistoryValue(raw json.RawMessage) (UsageHistoryValue, error) {
	if err := validateUsageHistory(raw); err != nil {
		return UsageHistoryValue{}, err
	}
	var wire struct {
		Window   json.RawMessage `json:"window"`
		SinceDay string          `json:"sinceDay"`
		UntilDay string          `json:"untilDay"`
		Totals   struct {
			UncachedInput uint64 `json:"uncachedInput"`
			CachedInput   uint64 `json:"cachedInput"`
			CacheCreation uint64 `json:"cacheCreation"`
			Output        uint64 `json:"output"`
			Reasoning     uint64 `json:"reasoning"`
		} `json:"totals"`
		TotalTokens uint64  `json:"totalTokens"`
		CostUSD     float64 `json:"costUsd"`
		Records     uint64  `json:"records"`
		Sessions    uint64  `json:"sessions"`
		Providers   []struct {
			Provider    string  `json:"provider"`
			CostUSD     float64 `json:"costUsd"`
			TotalTokens uint64  `json:"totalTokens"`
			CostShare   float64 `json:"costShare"`
			TokenShare  float64 `json:"tokenShare"`
		} `json:"providers"`
		Models []struct {
			Provider    string  `json:"provider"`
			Model       string  `json:"model"`
			CostUSD     float64 `json:"costUsd"`
			TotalTokens uint64  `json:"totalTokens"`
			CostShare   float64 `json:"costShare"`
		} `json:"models"`
		Daily []struct {
			Day         string  `json:"day"`
			CostUSD     float64 `json:"costUsd"`
			TotalTokens uint64  `json:"totalTokens"`
			ByProvider  [2]struct {
				CostUSD     float64 `json:"costUsd"`
				TotalTokens uint64  `json:"totalTokens"`
			} `json:"byProvider"`
		} `json:"daily"`
		Months   []json.RawMessage `json:"months"`
		Projects []json.RawMessage `json:"projects"`
		Quality  struct {
			ProviderReportedShare float64 `json:"providerReportedShare"`
			ModelPricedShare      float64 `json:"modelPricedShare"`
			UnpricedShare         float64 `json:"unpricedShare"`
			CacheSavingsUSD       float64 `json:"cacheSavingsUsd"`
		} `json:"quality"`
		Pricing      string            `json:"pricing"`
		ScannedFiles uint64            `json:"scannedFiles"`
		SkippedFiles uint64            `json:"skippedFiles"`
		Errors       []string          `json:"errors"`
		ScanDuration RustDurationValue `json:"scanDuration"`
	}
	if err := Decode(raw, &wire); err != nil {
		return UsageHistoryValue{}, err
	}
	window, _ := DecodeUsageWindow(wire.Window)
	value := UsageHistoryValue{Window: window, SinceDay: wire.SinceDay, UntilDay: wire.UntilDay, Totals: TokenTotalsValue{wire.Totals.UncachedInput, wire.Totals.CachedInput, wire.Totals.CacheCreation, wire.Totals.Output, wire.Totals.Reasoning}, TotalTokens: wire.TotalTokens, CostUSD: wire.CostUSD, Records: wire.Records, Sessions: wire.Sessions, Pricing: wire.Pricing, ScannedFiles: wire.ScannedFiles, SkippedFiles: wire.SkippedFiles, Errors: nonNil(wire.Errors), ScanDuration: wire.ScanDuration}
	value.Quality = CostQualityValue{wire.Quality.ProviderReportedShare, wire.Quality.ModelPricedShare, wire.Quality.UnpricedShare, wire.Quality.CacheSavingsUSD}
	for _, p := range wire.Providers {
		value.Providers = append(value.Providers, UsageProviderSlice{p.Provider, p.CostUSD, p.TotalTokens, p.CostShare, p.TokenShare})
	}
	for _, m := range wire.Models {
		value.Models = append(value.Models, UsageModelSlice{m.Provider, m.Model, m.CostUSD, m.TotalTokens, m.CostShare})
	}
	for _, d := range wire.Daily {
		value.Daily = append(value.Daily, UsageDaySlice{d.Day, d.CostUSD, d.TotalTokens, [2]ProviderDayValue{{d.ByProvider[0].CostUSD, d.ByProvider[0].TotalTokens}, {d.ByProvider[1].CostUSD, d.ByProvider[1].TotalTokens}}})
	}
	// Month/project shapes are exposed as typed decoders below; retaining the
	// raw loop here avoids lossy float/string tuples in the aggregate path.
	for _, m := range wire.Months {
		typed, err := decodeUsageMonth(m)
		if err != nil {
			return UsageHistoryValue{}, err
		}
		value.Months = append(value.Months, typed)
	}
	for _, p := range wire.Projects {
		typed, err := decodeUsageProject(p)
		if err != nil {
			return UsageHistoryValue{}, err
		}
		value.Projects = append(value.Projects, typed)
	}
	value.Providers = nonNil(value.Providers)
	value.Models = nonNil(value.Models)
	value.Daily = nonNil(value.Daily)
	value.Months = nonNil(value.Months)
	value.Projects = nonNil(value.Projects)
	return value, nil
}

func decodeUsageMonth(raw json.RawMessage) (UsageMonthSlice, error) {
	var v struct {
		FirstDay    string              `json:"firstDay"`
		CostUSD     float64             `json:"costUsd"`
		TotalTokens uint64              `json:"totalTokens"`
		ByProvider  [2]ProviderDayValue `json:"byProvider"`
		Sessions    uint64              `json:"sessions"`
		ActiveDays  uint32              `json:"activeDays"`
		TopModels   []json.RawMessage   `json:"topModels"`
	}
	if err := Decode(raw, &v); err != nil {
		return UsageMonthSlice{}, err
	}
	pairs, err := decodeModelPairs(v.TopModels)
	return UsageMonthSlice{v.FirstDay, v.CostUSD, v.TotalTokens, v.ByProvider, v.Sessions, v.ActiveDays, pairs}, err
}
func decodeUsageProject(raw json.RawMessage) (UsageProjectSlice, error) {
	var v struct {
		Path        string              `json:"path"`
		CostUSD     float64             `json:"costUsd"`
		TotalTokens uint64              `json:"totalTokens"`
		ByProvider  [2]ProviderDayValue `json:"byProvider"`
		Sessions    uint64              `json:"sessions"`
		CostShare   float64             `json:"costShare"`
		LastDay     *string             `json:"lastDay"`
		TopModels   []json.RawMessage   `json:"topModels"`
	}
	if err := Decode(raw, &v); err != nil {
		return UsageProjectSlice{}, err
	}
	pairs, err := decodeModelPairs(v.TopModels)
	return UsageProjectSlice{v.Path, v.CostUSD, v.TotalTokens, v.ByProvider, v.Sessions, v.CostShare, v.LastDay, pairs}, err
}
func decodeModelPairs(raw []json.RawMessage) ([][2]any, error) {
	out := make([][2]any, 0, len(raw))
	for _, pair := range raw {
		var p []json.RawMessage
		if err := Decode(pair, &p); err != nil || len(p) != 2 {
			return nil, fmt.Errorf("invalid model pair")
		}
		var name string
		var cost float64
		if err := Decode(p[0], &name); err != nil {
			return nil, err
		}
		if err := Decode(p[1], &cost); err != nil {
			return nil, err
		}
		out = append(out, [2]any{name, cost})
	}
	return out, nil
}

type SkillSourceValue struct {
	Shared   bool
	Provider *string
}
type SkillInstallValue struct {
	Source         SkillSourceValue
	Dir, SkillFile string
	Enabled        bool
}
type SkillEntryValue struct {
	Name, Description, Scope string
	Project                  *string
	Installs                 []SkillInstallValue
	Enabled                  bool
	AllowedTools             *string
	Body                     string
	SupportingFiles          uint64
	TotalBytes               uint64
	ModifiedAt               *uint64
	Duplicates, RowKey       uint64
}
type SkillsCatalogValue struct{ Skills []SkillEntryValue }

func DecodeSkillsCatalogValue(raw json.RawMessage) (SkillsCatalogValue, error) {
	if err := validateSkillsCatalog(raw); err != nil {
		return SkillsCatalogValue{}, err
	}
	var root struct {
		Skills []json.RawMessage `json:"skills"`
	}
	_ = Decode(raw, &root)
	out := SkillsCatalogValue{Skills: make([]SkillEntryValue, 0, len(root.Skills))}
	for _, item := range root.Skills {
		var v struct {
			Name            string            `json:"name"`
			Description     string            `json:"description"`
			Scope           string            `json:"scope"`
			Project         *string           `json:"project"`
			Installs        []json.RawMessage `json:"installs"`
			Enabled         bool              `json:"enabled"`
			AllowedTools    *string           `json:"allowedTools"`
			Body            string            `json:"body"`
			SupportingFiles uint64            `json:"supportingFiles"`
			TotalBytes      uint64            `json:"totalBytes"`
			ModifiedAt      *uint64           `json:"modifiedAt"`
			Duplicates      uint64            `json:"duplicates"`
			RowKey          uint64            `json:"rowKey"`
		}
		if err := Decode(item, &v); err != nil {
			return SkillsCatalogValue{}, err
		}
		e := SkillEntryValue{Name: v.Name, Description: v.Description, Scope: v.Scope, Project: v.Project, Enabled: v.Enabled, AllowedTools: v.AllowedTools, Body: v.Body, SupportingFiles: v.SupportingFiles, TotalBytes: v.TotalBytes, ModifiedAt: v.ModifiedAt, Duplicates: v.Duplicates, RowKey: v.RowKey}
		for _, rawInstall := range v.Installs {
			var i struct {
				Source    json.RawMessage `json:"source"`
				Dir       string          `json:"dir"`
				SkillFile string          `json:"skillFile"`
				Enabled   bool            `json:"enabled"`
			}
			_ = Decode(rawInstall, &i)
			source, err := decodeSkillSource(i.Source)
			if err != nil {
				return SkillsCatalogValue{}, err
			}
			e.Installs = append(e.Installs, SkillInstallValue{source, i.Dir, i.SkillFile, i.Enabled})
		}
		out.Skills = append(out.Skills, e)
	}
	return out, nil
}
func decodeSkillSource(raw json.RawMessage) (SkillSourceValue, error) {
	var unit string
	if Decode(raw, &unit) == nil {
		return SkillSourceValue{Shared: true}, nil
	}
	var v struct {
		Provider string `json:"provider"`
	}
	if err := Decode(raw, &v); err != nil {
		return SkillSourceValue{}, err
	}
	return SkillSourceValue{Provider: &v.Provider}, nil
}

// Workspace wire paths always belong to the daemon host.
type WorkspaceOperationValue struct {
	Type   string
	Fields map[string]json.RawMessage
}
type WorkspaceResultValue struct {
	Type   string
	Fields map[string]json.RawMessage
}

type WorkspaceListTree struct {
	Type          string   `json:"type"`
	Root          string   `json:"root"`
	ExpandedPaths []string `json:"expanded_paths"`
}
type WorkspaceBrowseDirectory struct {
	Type string  `json:"type"`
	Path *string `json:"path"`
}
type WorkspaceReadTextFile struct {
	Type         string `json:"type"`
	Root         string `json:"root"`
	RelativePath string `json:"relative_path"`
}
type WorkspaceWriteTextFile struct {
	Type         string `json:"type"`
	Root         string `json:"root"`
	RelativePath string `json:"relative_path"`
	Content      string `json:"content"`
}
type WorkspaceListProjectFiles struct {
	Type string `json:"type"`
	Root string `json:"root"`
	Cap  uint64 `json:"cap"`
}
type WorkspaceDiscoverSlashCommands struct {
	Type           string  `json:"type"`
	Provider       string  `json:"provider"`
	ProjectRoot    string  `json:"project_root"`
	BinaryOverride *string `json:"binary_override,omitempty"`
}
type WorkspaceCreateProjectless struct {
	Type   string  `json:"type"`
	Prompt *string `json:"prompt"`
}
type WorkspacePathOperation struct {
	Type string `json:"type"`
	Path string `json:"path"`
}
type WorkspaceCWDOperation struct {
	Type string `json:"type"`
	CWD  string `json:"cwd"`
}
type WorkspaceCheckoutBranch struct {
	Type   string `json:"type"`
	CWD    string `json:"cwd"`
	Branch string `json:"branch"`
	Create bool   `json:"create"`
}
type WorkspaceCreateWorktree struct {
	Type        string  `json:"type"`
	ProjectPath string  `json:"project_path"`
	ProjectID   UUID    `json:"project_id"`
	SessionID   UUID    `json:"session_id"`
	Prompt      string  `json:"prompt"`
	BaseBranch  *string `json:"base_branch"`
}
type AgentInvocationValue struct {
	Provider        string  `json:"provider"`
	Binary          string  `json:"binary"`
	Model           *string `json:"model"`
	ReasoningEffort *string `json:"reasoning_effort"`
}
type WorkspaceGenerateCommitMessage struct {
	Type            string               `json:"type"`
	CWD             string               `json:"cwd"`
	IncludeUnstaged bool                 `json:"include_unstaged"`
	Invocation      AgentInvocationValue `json:"invocation"`
}
type WorkspaceCommit struct {
	Type            string `json:"type"`
	CWD             string `json:"cwd"`
	Message         string `json:"message"`
	IncludeUnstaged bool   `json:"include_unstaged"`
	Push            bool   `json:"push"`
}
type WorkspaceCaptureTurn struct {
	Type      string `json:"type"`
	CWD       string `json:"cwd"`
	SessionID UUID   `json:"session_id"`
	TurnCount uint64 `json:"turn_count"`
}
type WorkspaceRefOperation struct {
	Type   string `json:"type"`
	CWD    string `json:"cwd"`
	GitRef string `json:"git_ref"`
}
type WorkspaceSessionRefs struct {
	Type      string `json:"type"`
	CWD       string `json:"cwd"`
	SessionID UUID   `json:"session_id"`
}
type WorkspaceDeleteTurnRefsAfter struct {
	Type              string `json:"type"`
	CWD               string `json:"cwd"`
	SessionID         UUID   `json:"session_id"`
	RetainedTurnCount uint64 `json:"retained_turn_count"`
	PreviousTurnCount uint64 `json:"previous_turn_count"`
}
type WorkspaceCopySessionRefs struct {
	Type             string `json:"type"`
	CWD              string `json:"cwd"`
	SourceSessionID  UUID   `json:"source_session_id"`
	TargetSessionID  UUID   `json:"target_session_id"`
	ThroughTurnCount uint64 `json:"through_turn_count"`
}
type WorkspaceCollectReviewDiff struct {
	Type   string          `json:"type"`
	CWD    string          `json:"cwd"`
	Source json.RawMessage `json:"source"`
}

// DecodeWorkspaceOperationAny returns a concrete variant after applying the
// same required/null/type checks as Rust serde. All path strings remain opaque
// daemon-host paths.
func DecodeWorkspaceOperationAny(raw json.RawMessage) (any, error) {
	value, err := DecodeWorkspaceOperationValue(raw)
	if err != nil {
		return nil, err
	}
	var dst any
	switch value.Type {
	case "listTree":
		dst = &WorkspaceListTree{}
	case "browseDirectory":
		dst = &WorkspaceBrowseDirectory{}
	case "readTextFile":
		dst = &WorkspaceReadTextFile{}
	case "writeTextFile":
		dst = &WorkspaceWriteTextFile{}
	case "listProjectFiles":
		dst = &WorkspaceListProjectFiles{}
	case "discoverSlashCommands":
		dst = &WorkspaceDiscoverSlashCommands{}
	case "createProjectlessWorkspace":
		dst = &WorkspaceCreateProjectless{}
	case "migrateProjectlessWorkspace":
		dst = &WorkspacePathOperation{}
	case "inspectBranches", "inspectCommit", "push":
		dst = &WorkspaceCWDOperation{}
	case "checkoutBranch":
		dst = &WorkspaceCheckoutBranch{}
	case "createWorktree":
		dst = &WorkspaceCreateWorktree{}
	case "generateCommitMessage":
		dst = &WorkspaceGenerateCommitMessage{}
	case "commit":
		dst = &WorkspaceCommit{}
	case "captureTurnStart", "captureTurn":
		dst = &WorkspaceCaptureTurn{}
	case "captureRef", "restoreRef", "hasRef", "deleteRef":
		dst = &WorkspaceRefOperation{}
	case "sessionTurnRefs", "deleteSessionRefs":
		dst = &WorkspaceSessionRefs{}
	case "deleteTurnRefsAfter":
		dst = &WorkspaceDeleteTurnRefsAfter{}
	case "copySessionRefs":
		dst = &WorkspaceCopySessionRefs{}
	case "collectReviewDiff":
		dst = &WorkspaceCollectReviewDiff{}
	default:
		return nil, fmt.Errorf("unknown workspace operation %q", value.Type)
	}
	if err := Decode(raw, dst); err != nil {
		return nil, err
	}
	return dst, nil
}

type WorkspaceAckResult struct {
	Type string `json:"type"`
}
type WorkspaceWorkingTreeResult struct {
	Type    string                  `json:"type"`
	Entries []WorkingTreeEntryValue `json:"entries"`
}
type WorkspaceDirectoryResult struct {
	Type           string                  `json:"type"`
	Path           string                  `json:"path"`
	Parent         *string                 `json:"parent"`
	Home           string                  `json:"home"`
	FilesystemRoot string                  `json:"filesystem_root"`
	Entries        []WorkingTreeEntryValue `json:"entries"`
}
type WorkspaceTextFileResult struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}
type FileEntryValue struct {
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
}
type WorkspaceProjectFilesResult struct {
	Type    string           `json:"type"`
	Entries []FileEntryValue `json:"entries"`
}
type SlashCommandValue struct {
	Name         string  `json:"name"`
	Description  string  `json:"description"`
	Scope        string  `json:"scope"`
	ArgumentHint *string `json:"argument_hint"`
	Template     *string `json:"template"`
}
type WorkspaceSlashCommandsResult struct {
	Type     string              `json:"type"`
	Commands []SlashCommandValue `json:"commands"`
}
type WorkspaceProjectlessResult struct {
	Type string `json:"type"`
	CWD  string `json:"cwd"`
}
type WorkspaceBranchesResult struct {
	Type     string               `json:"type"`
	Snapshot *BranchSnapshotValue `json:"snapshot"`
}
type WorkspaceBranchChangedResult struct {
	Type     string              `json:"type"`
	Snapshot BranchSnapshotValue `json:"snapshot"`
}
type WorkspaceWorktreeCreatedResult struct {
	Type     string               `json:"type"`
	Worktree CreatedWorktreeValue `json:"worktree"`
}
type WorkspaceCommitSnapshotResult struct {
	Type     string              `json:"type"`
	Snapshot CommitSnapshotValue `json:"snapshot"`
}
type WorkspaceCommitMessageResult struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
type WorkspaceCheckpointResult struct {
	Type       string          `json:"type"`
	Checkpoint CheckpointValue `json:"checkpoint"`
}
type WorkspaceBoolResult struct {
	Type  string `json:"type"`
	Value bool   `json:"value"`
}
type WorkspaceTurnRefsResult struct {
	Type       string   `json:"type"`
	TurnCounts []uint64 `json:"turn_counts"`
}
type WorkspaceReviewDiffResult struct {
	Type string              `json:"type"`
	Data ReviewDiffDataValue `json:"data"`
}

func DecodeWorkspaceResultAny(raw json.RawMessage) (any, error) {
	value, err := DecodeWorkspaceResultValue(raw)
	if err != nil {
		return nil, err
	}
	var dst any
	switch value.Type {
	case "ack":
		dst = &WorkspaceAckResult{}
	case "workingTree":
		dst = &WorkspaceWorkingTreeResult{}
	case "directory":
		dst = &WorkspaceDirectoryResult{}
	case "textFile":
		dst = &WorkspaceTextFileResult{}
	case "projectFiles":
		dst = &WorkspaceProjectFilesResult{}
	case "slashCommands":
		dst = &WorkspaceSlashCommandsResult{}
	case "projectlessWorkspace":
		dst = &WorkspaceProjectlessResult{}
	case "branches":
		dst = &WorkspaceBranchesResult{}
	case "branchChanged":
		dst = &WorkspaceBranchChangedResult{}
	case "worktreeCreated":
		dst = &WorkspaceWorktreeCreatedResult{}
	case "commitSnapshot":
		dst = &WorkspaceCommitSnapshotResult{}
	case "commitMessage":
		dst = &WorkspaceCommitMessageResult{}
	case "checkpoint":
		dst = &WorkspaceCheckpointResult{}
	case "bool":
		dst = &WorkspaceBoolResult{}
	case "turnRefs":
		dst = &WorkspaceTurnRefsResult{}
	case "reviewDiff":
		dst = &WorkspaceReviewDiffResult{}
	default:
		return nil, fmt.Errorf("unknown workspace result %q", value.Type)
	}
	if err := Decode(raw, dst); err != nil {
		return nil, err
	}
	return dst, nil
}

func DecodeWorkspaceOperationValue(raw json.RawMessage) (WorkspaceOperationValue, error) {
	if err := validateWorkspaceOperation(raw); err != nil {
		return WorkspaceOperationValue{}, err
	}
	return decodeTaggedFields(raw)
}
func DecodeWorkspaceResultValue(raw json.RawMessage) (WorkspaceResultValue, error) {
	if err := validateWorkspaceResult(raw); err != nil {
		return WorkspaceResultValue{}, err
	}
	return decodeTaggedResult(raw)
}
func decodeTaggedFields(raw json.RawMessage) (WorkspaceOperationValue, error) {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return WorkspaceOperationValue{}, err
	}
	var kind string
	if err := Decode(object["type"], &kind); err != nil {
		return WorkspaceOperationValue{}, err
	}
	delete(object, "type")
	return WorkspaceOperationValue{kind, object}, nil
}
func decodeTaggedResult(raw json.RawMessage) (WorkspaceResultValue, error) {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return WorkspaceResultValue{}, err
	}
	var kind string
	if err := Decode(object["type"], &kind); err != nil {
		return WorkspaceResultValue{}, err
	}
	delete(object, "type")
	return WorkspaceResultValue{kind, object}, nil
}

func (v WorkspaceOperationValue) MarshalJSON() ([]byte, error) {
	return marshalTagged(v.Type, v.Fields)
}
func (v WorkspaceResultValue) MarshalJSON() ([]byte, error) { return marshalTagged(v.Type, v.Fields) }
func marshalTagged(kind string, fields map[string]json.RawMessage) ([]byte, error) {
	if kind == "" {
		return nil, fmt.Errorf("tag is empty")
	}
	object := map[string]json.RawMessage{"type": json.RawMessage(strconvQuote(kind))}
	for key, value := range fields {
		if key == "type" {
			return nil, fmt.Errorf("type field is reserved")
		}
		object[key] = value
	}
	return json.Marshal(object)
}

type WorkingTreeEntryValue struct {
	RelativePath string `json:"relativePath"`
	AbsolutePath string `json:"absolutePath"`
	Name         string `json:"name"`
	IsDir        bool   `json:"isDir"`
	Expanded     bool   `json:"expanded"`
	Depth        uint64 `json:"depth"`
}
type BranchEntryValue struct {
	Name                string `json:"name"`
	CheckedOutElsewhere bool   `json:"checked_out_elsewhere"`
}
type BranchSnapshotValue struct {
	Repository    string             `json:"repository"`
	Current       *string            `json:"current"`
	DetachedHead  *string            `json:"detached_head"`
	DefaultBranch *string            `json:"default_branch"`
	Branches      []BranchEntryValue `json:"branches"`
	Additions     uint64             `json:"additions"`
	Deletions     uint64             `json:"deletions"`
}
type CommitSnapshotValue struct {
	Branch          string `json:"branch"`
	Additions       uint64 `json:"additions"`
	Deletions       uint64 `json:"deletions"`
	StagedAdditions uint64 `json:"staged_additions"`
	StagedDeletions uint64 `json:"staged_deletions"`
	HasStaged       bool   `json:"has_staged"`
	HasUnstaged     bool   `json:"has_unstaged"`
	CanPush         bool   `json:"can_push"`
}
type CreatedWorktreeValue struct {
	Path   string `json:"path"`
	Branch string `json:"branch"`
}
type CheckpointFileValue struct {
	Path      string `json:"path"`
	Additions uint64 `json:"additions"`
	Deletions uint64 `json:"deletions"`
}
type CheckpointValue struct {
	TurnCount uint64                `json:"turn_count"`
	GitRef    string                `json:"git_ref"`
	Status    string                `json:"status"`
	Files     []CheckpointFileValue `json:"files"`
	Additions uint64                `json:"additions"`
	Deletions uint64                `json:"deletions"`
	CreatedAt uint64                `json:"created_at"`
}
type ReviewDiffDataValue struct {
	Source          json.RawMessage `json:"source"`
	Numstat         string          `json:"numstat"`
	Patch           string          `json:"patch"`
	CompleteContext bool            `json:"completeContext"`
}

func DecodeCheckpointValue(raw json.RawMessage) (CheckpointValue, error) {
	if err := validateCheckpoint(raw); err != nil {
		return CheckpointValue{}, err
	}
	var v CheckpointValue
	if err := Decode(raw, &v); err != nil {
		return CheckpointValue{}, err
	}
	v.Files = nonNil(v.Files)
	return v, nil
}

func rawIsNull(raw json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }
