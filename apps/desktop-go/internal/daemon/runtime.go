package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
	"github.com/roshankarande/waku/apps/desktop-go/internal/providers"
	"github.com/roshankarande/waku/apps/desktop-go/internal/storage"
	terminaltransport "github.com/roshankarande/waku/apps/desktop-go/internal/terminal"
	workspacesvc "github.com/roshankarande/waku/apps/desktop-go/internal/workspace"
)

type providerRuntime struct {
	id            protocol.UUID
	session       providers.Session
	adapter       providers.Adapter
	mu            sync.Mutex
	pending       map[pendingKey]*pendingProviderRequest
	pendingBytes  int
	pendingCursor json.RawMessage
	failureErr    error
	exited        bool
}

type pendingKey struct{ kind, wireID string }
type pendingProviderRequest struct {
	raw       json.RawMessage
	resolving bool
	bytes     int
}

const (
	maxPendingProviderRequests = 128
	maxPendingProviderBytes    = 1 << 20
)

var ErrProviderPendingOverload = errors.New("provider pending request admission limit exceeded")

type runtimeStore interface {
	Session(context.Context, string) (json.RawMessage, bool, error)
	SaveWithProviderCursors(context.Context, []json.RawMessage, []json.RawMessage, map[string]json.RawMessage) error
	Load(context.Context) ([]json.RawMessage, []json.RawMessage, error)
	RemoveSession(context.Context, string) error
	SetProviderCursor(context.Context, string, json.RawMessage) error
}

type runtimeBackend struct {
	store          runtimeStore
	adapters       map[providers.Provider]providers.Adapter
	hub            *Hub
	settings       *storage.SettingsStore
	terminals      *terminaltransport.Manager
	skills         *workspacesvc.SkillsService
	usage          *workspacesvc.UsageHistoryService
	planUsage      planUsageFetcher
	mu             sync.Mutex
	runtimes       map[protocol.UUID]*providerRuntime
	skillLocations []workspacesvc.SkillLocation
}

type planUsageFetcher interface {
	Fetch(context.Context, string, *string, *string) (json.RawMessage, error)
}

type gatedPlanUsageFetcher struct{}

func (gatedPlanUsageFetcher) Fetch(_ context.Context, provider string, binaryOverride, cliVersion *string) (json.RawMessage, error) {
	return nil, fmt.Errorf("live plan usage for provider %q is unavailable without a configured authenticated fetcher (binary override %v, CLI version %v)", provider, binaryOverride != nil, cliVersion != nil)
}

func newRuntimeBackend(store runtimeStore, adapters map[providers.Provider]providers.Adapter, hub *Hub) *runtimeBackend {
	b := &runtimeBackend{store: store, adapters: adapters, hub: hub, runtimes: make(map[protocol.UUID]*providerRuntime)}
	b.planUsage = gatedPlanUsageFetcher{}
	b.terminals, _ = terminaltransport.NewManager(terminaltransport.UnsupportedBackend{}, b.emitTerminal, terminaltransport.DefaultManagerLimits())
	return b
}

func responsePayload(kind string, fields map[string]any) ([]byte, error) {
	object := map[string]any{"type": kind}
	for key, value := range fields {
		object[key] = value
	}
	return json.Marshal(object)
}

func validatedResponsePayload(kind string, fields map[string]any) ([]byte, error) {
	raw, err := responsePayload(kind, fields)
	if err != nil {
		return nil, err
	}
	if _, err := protocol.DecodeResponsePayloadAny(raw); err != nil {
		return nil, fmt.Errorf("encode %s response: %w", kind, err)
	}
	return raw, nil
}

func (b *runtimeBackend) handle(ctx context.Context, request Request) ([]byte, error) {
	var wire protocol.Request
	if err := protocol.Decode(request.Payload, &wire); err != nil {
		return nil, err
	}
	decoded, err := protocol.DecodeCommand(wire.Command)
	if err != nil {
		return nil, err
	}
	switch command := decoded.(type) {
	case *protocol.StartCommand:
		return b.start(ctx, wire, *command)
	case *protocol.UpdateSettingsCommand:
		if b.settings == nil {
			return nil, errors.New("daemon settings persistence is unavailable")
		}
		settings, err := protocol.DecodeDaemonSettings(command.Settings)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(settings)
		if err != nil {
			return nil, err
		}
		if err := b.settings.Save(ctx, encoded); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.WorkspaceCommand:
		return b.handleWorkspace(ctx, command.Operation)
	case *protocol.OpenTerminalCommand:
		if wire.SessionID == protocol.NilUUID || wire.RuntimeID == protocol.NilUUID {
			return nil, errors.New("terminal session and runtime IDs are required")
		}
		b.mu.Lock()
		providerRuntimeActive := b.runtimes[wire.SessionID] != nil
		b.mu.Unlock()
		if providerRuntimeActive {
			return nil, errors.New("terminal session ID collides with an active provider session")
		}
		owner := terminaltransport.Owner{SessionID: string(wire.SessionID), RuntimeID: string(wire.RuntimeID)}
		if b.terminals.HasSession(owner.SessionID) {
			return nil, errors.New("terminal is already open for this session")
		}
		b.hub.BeginRuntime(owner.SessionID, owner.RuntimeID)
		if err := b.terminals.Open(ctx, owner, command.CWD, command.Cols, command.Rows); err != nil {
			b.hub.EndRuntime(owner.SessionID, owner.RuntimeID)
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.WriteTerminalCommand:
		owner := terminaltransport.Owner{SessionID: string(wire.SessionID), RuntimeID: string(wire.RuntimeID)}
		if err := b.terminals.Write(ctx, owner, command.Data); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.ResizeTerminalCommand:
		owner := terminaltransport.Owner{SessionID: string(wire.SessionID), RuntimeID: string(wire.RuntimeID)}
		if err := b.terminals.Resize(ctx, owner, command.Cols, command.Rows); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.ProbeProviderCommand:
		adapter, err := b.adapter(providers.Provider(command.Provider))
		if err != nil {
			return nil, err
		}
		options := providers.StartOptions{DiscoverModels: command.DiscoverModels, ProbeVersion: command.ProbeVersion}
		if command.BinaryOverride != nil {
			options.Binary = *command.BinaryOverride
		}
		discovery, err := adapter.Discover(ctx, options)
		if err != nil {
			return nil, err
		}
		models := make([]json.RawMessage, 0, len(discovery.Models))
		if command.DiscoverModels {
			for _, model := range discovery.Models {
				encoded, err := providerModelPayload(model)
				if err != nil {
					return nil, fmt.Errorf("provider %q model %q: %w", command.Provider, model.ID, err)
				}
				models = append(models, encoded)
			}
		}
		probe := map[string]any{"provider": command.Provider, "installed": discovery.Available, "path": nullable(discovery.Executable), "models": models, "agent_presets": []any{}}
		var version any
		if command.ProbeVersion && discovery.Version != "" {
			version = discovery.Version
		}
		return validatedResponsePayload("providerProbe", map[string]any{"probe": probe, "version": version})
	case *protocol.FetchPlanUsageCommand:
		switch providers.Provider(command.Provider) {
		case providers.ProviderClaude, providers.ProviderCodex, providers.ProviderOpenCode, providers.ProviderGrok:
		default:
			return nil, fmt.Errorf("provider %q has no plan usage fetcher", command.Provider)
		}
		usage, err := b.planUsage.Fetch(ctx, command.Provider, command.BinaryOverride, command.CLIVersion)
		if err != nil {
			return nil, err
		}
		if len(usage) == 0 {
			usage = json.RawMessage("null")
		}
		return validatedResponsePayload("planUsage", map[string]any{"usage": usage})
	case *protocol.LoadUsageHistoryCommand:
		if b.usage == nil {
			return nil, errors.New("daemon usage history service is unavailable")
		}
		window, err := protocol.DecodeUsageWindow(command.Window)
		if err != nil {
			return nil, err
		}
		history, err := b.usage.Load(ctx, workspacesvc.UsageWindow{Kind: window.Kind, Count: window.Count}, command.ProjectRoots)
		if err != nil {
			return nil, err
		}
		return validatedResponsePayload("usageHistory", map[string]any{"history": history})
	case *protocol.LoadSkillsCommand:
		if b.skills == nil {
			return nil, errors.New("daemon skills service is unavailable")
		}
		projects := make([]workspacesvc.SkillProject, 0, len(command.Projects))
		for _, project := range command.Projects {
			root, err := workspacesvc.NewHostPath(project[1])
			if err != nil {
				return nil, fmt.Errorf("skill project %q: %w", project[0], err)
			}
			projects = append(projects, workspacesvc.SkillProject{Name: project[0], Root: root})
		}
		locations := b.skills.Locations(projects)
		catalog, err := b.skills.Scan(ctx, locations)
		if err != nil {
			return nil, err
		}
		b.mu.Lock()
		b.skillLocations = append([]workspacesvc.SkillLocation(nil), locations...)
		b.mu.Unlock()
		return validatedResponsePayload("skillsCatalog", map[string]any{"catalog": catalog})
	case *protocol.SetSkillsEnabledCommand:
		if b.skills == nil {
			return nil, errors.New("daemon skills service is unavailable")
		}
		locations := b.currentSkillLocations()
		if err := b.skills.SetEnabled(ctx, locations, command.Dirs, command.Enabled); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.TrashSkillsCommand:
		if b.skills == nil {
			return nil, errors.New("daemon skills service is unavailable")
		}
		locations := b.currentSkillLocations()
		if _, err := b.skills.Trash(ctx, locations, command.Dirs); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.ListProviderSessionsCommand:
		_, catalog, err := b.sessionCatalog(providers.Provider(command.Provider), false)
		if err != nil {
			return nil, err
		}
		limit := command.Limit
		if limit > 500 {
			limit = 500
		}
		if limit == 0 {
			return validatedResponsePayload("providerSessions", map[string]any{"sessions": []json.RawMessage{}})
		}
		sessions, err := catalog.ListSessions(ctx, limit)
		if err != nil {
			return nil, err
		}
		if uint64(len(sessions)) > limit {
			sessions = sessions[:limit]
		}
		return validatedResponsePayload("providerSessions", map[string]any{"sessions": sessions})
	case *protocol.LoadProviderSessionCommand:
		provider, err := providerFromRaw(command.Cursor)
		if err != nil {
			return nil, err
		}
		_, catalog, err := b.sessionCatalog(provider, false)
		if err != nil {
			return nil, err
		}
		history, err := catalog.LoadSession(ctx, command.Cursor, command.CWD)
		if err != nil {
			return nil, err
		}
		return validatedResponsePayload("providerSessionHistory", map[string]any{"history": json.RawMessage(history)})
	case *protocol.ForkProviderSessionCommand:
		provider, err := providerFromRaw(command.Request)
		if err != nil {
			return nil, err
		}
		_, catalog, err := b.sessionCatalog(provider, true)
		if err != nil {
			return nil, err
		}
		result, err := catalog.ForkSession(ctx, command.Request)
		if err != nil {
			return nil, err
		}
		return validatedResponsePayload("providerSessionForked", map[string]any{"result": json.RawMessage(result)})
	case *protocol.PromptCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		if err := runtime.session.Prompt(ctx, command.Prompt); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.SteerCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		if err := runtime.session.Steer(ctx, command.Prompt); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.RespondCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		key := pendingKey{kind: "permission", wireID: command.RequestID}
		nativeID, ok := runtime.beginPending(key)
		if !ok {
			return nil, errors.New("permission request is no longer pending")
		}
		if err := runtime.session.RespondPermission(ctx, nativeID, command.OptionID); err != nil {
			runtime.finishPending(key, false)
			return nil, err
		}
		runtime.finishPending(key, true)
		return responsePayload("ack", nil)
	case *protocol.RespondUserInputCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		key := pendingKey{kind: "question", wireID: command.RequestID}
		nativeID, ok := runtime.beginPending(key)
		if !ok {
			return nil, errors.New("user-input request is no longer pending")
		}
		answers := make([]providers.UserInputAnswer, len(command.Answers))
		for i, answer := range command.Answers {
			answers[i] = providers.UserInputAnswer{QuestionID: answer.QuestionID, Answers: answer.Answers}
		}
		if err := runtime.session.RespondQuestions(ctx, nativeID, answers); err != nil {
			runtime.finishPending(key, false)
			return nil, err
		}
		runtime.finishPending(key, true)
		return responsePayload("ack", nil)
	case *protocol.ApplyOptionsCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		options := providers.SessionOptions{Access: providers.AccessMode(command.Options.Mode)}
		if command.Options.Model != nil {
			options.Model = *command.Options.Model
		}
		if command.Options.ReasoningEffort != nil {
			options.ReasoningEffort = *command.Options.ReasoningEffort
		}
		if command.Options.ServiceTier != nil {
			options.ServiceTier = *command.Options.ServiceTier
		}
		applied, err := runtime.session.ApplyOptions(ctx, options)
		if err != nil {
			return nil, err
		}
		return responsePayload("optionsApplied", map[string]any{"applied": applied})
	case *protocol.GoalCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		var operation providers.GoalOperation
		if err := json.Unmarshal(command.Operation, &operation); err != nil {
			return nil, err
		}
		if err := runtime.session.Goal(ctx, operation); err != nil {
			return nil, err
		}
		return responsePayload("ack", nil)
	case *protocol.TurnsCommand:
		if command.Type != protocol.CommandRollback {
			break
		}
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		if !runtime.adapter.Descriptor().Capabilities.Rewind {
			return nil, fmt.Errorf("provider %q does not support rewind", runtime.adapter.Descriptor().Provider)
		}
		cursor, err := runtime.session.Rollback(ctx, command.Turns)
		if err != nil {
			return nil, err
		}
		if len(cursor) == 0 || string(cursor) == "null" {
			return nil, errors.New("provider rollback returned no resumable cursor")
		}
		if err := b.persistAdoptedCursor(ctx, wire.SessionID, wire.RuntimeID, runtime, cursor); err != nil {
			return nil, err
		}
		return responsePayload("cursor", map[string]any{"cursor": json.RawMessage(cursor)})
	case *protocol.ForkCommand:
		runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
		if err != nil {
			return nil, err
		}
		if !runtime.adapter.Descriptor().Capabilities.Fork {
			return nil, fmt.Errorf("provider %q does not support fork", runtime.adapter.Descriptor().Provider)
		}
		cursor, err := runtime.session.Fork(ctx, command.TurnsToRemove)
		if err != nil {
			return nil, err
		}
		return responsePayload("cursor", map[string]any{"cursor": json.RawMessage(cursor)})
	case *protocol.HydrateSessionCommand:
		raw, found, err := b.store.Session(ctx, string(command.SessionID))
		if err != nil {
			return nil, err
		}
		if !found {
			return responsePayload("session", map[string]any{"session": nil})
		}
		return responsePayload("session", map[string]any{"session": raw})
	case *protocol.SaveTaskStateCommand:
		if err := b.saveTaskState(ctx, command.Projects, command.Sessions); err != nil {
			return nil, err
		}
		return responsePayload("taskStateSaved", map[string]any{"sessions": command.Sessions})
	case *protocol.Command:
		switch command.Type {
		case string(protocol.CommandGetSettings):
			if b.settings == nil {
				return nil, errors.New("daemon settings persistence is unavailable")
			}
			raw, err := b.settings.Load(ctx)
			if err != nil {
				return nil, err
			}
			settings, err := protocol.DecodeDaemonSettings(raw)
			if err != nil {
				return nil, fmt.Errorf("load daemon settings: %w", err)
			}
			encoded, err := json.Marshal(settings)
			if err != nil {
				return nil, err
			}
			return validatedResponsePayload("settings", map[string]any{"settings": json.RawMessage(encoded)})
		case string(protocol.CommandAttachSession):
			b.mu.Lock()
			runtime := b.runtimes[wire.SessionID]
			b.mu.Unlock()
			if runtime == nil {
				return responsePayload("sessionRuntime", map[string]any{"runtimeId": nil, "supportsSteer": false})
			}
			return responsePayload("sessionRuntime", map[string]any{"runtimeId": runtime.id, "supportsSteer": runtime.adapter.Descriptor().Capabilities.Steering})
		case string(protocol.CommandLoadTaskState):
			projects, sessions, err := b.store.Load(ctx)
			if err != nil {
				return nil, err
			}
			return responsePayload("taskState", map[string]any{"projects": projects, "sessions": sessions, "defaultCwd": ".", "projectlessRoot": nil})
		case string(protocol.CommandCancel):
			runtime, err := b.runtime(wire.SessionID, wire.RuntimeID)
			if err != nil {
				return nil, err
			}
			if err := runtime.session.Cancel(ctx); err != nil {
				return nil, err
			}
			return responsePayload("ack", nil)
		case string(protocol.CommandCloseSession):
			if err := b.closeRuntime(wire.SessionID, wire.RuntimeID); err != nil {
				return nil, err
			}
			return responsePayload("ack", nil)
		case string(protocol.CommandCloseTerminal):
			owner := terminaltransport.Owner{SessionID: string(wire.SessionID), RuntimeID: string(wire.RuntimeID)}
			if err := b.terminals.CloseTerminal(ctx, owner); err != nil && !errors.Is(err, terminaltransport.ErrTerminalNotFound) {
				return nil, err
			}
			b.hub.EndRuntime(owner.SessionID, owner.RuntimeID)
			return responsePayload("ack", nil)
		case string(protocol.CommandRemoveSession):
			_ = b.closeRuntime(wire.SessionID, wire.RuntimeID)
			if err := b.store.RemoveSession(ctx, string(wire.SessionID)); err != nil {
				return nil, err
			}
			return responsePayload("ack", nil)
		}
	}
	return nil, fmt.Errorf("command %q is not implemented by the Go daemon", commandType(wire.Command))
}

// providerModelPayload translates the adapter's identity fields while retaining
// its native metadata object byte-for-byte at the JSON-value level. The final
// providerProbe validation below remains authoritative for protocol-shaped
// fields such as reasoning_efforts, context_windows, and defaults. Keeping raw
// values as RawMessage also prevents large native integers from passing through
// float64 and losing precision.
func providerModelPayload(model providers.Model) (json.RawMessage, error) {
	object := make(map[string]json.RawMessage)
	if len(model.Raw) != 0 {
		if err := json.Unmarshal(model.Raw, &object); err != nil || object == nil {
			if err == nil {
				err = errors.New("metadata must be a JSON object")
			}
			return nil, fmt.Errorf("invalid metadata: %w", err)
		}
	}
	if model.ID == "" || model.Label == "" {
		return nil, errors.New("id and name are required")
	}
	id, _ := json.Marshal(model.ID)
	name, _ := json.Marshal(model.Label)
	object["id"] = id
	object["name"] = name
	return json.Marshal(object)
}

func openWorkspaceService(path string) (*workspacesvc.Service, error) {
	host, err := workspacesvc.NewHostPath(path)
	if err != nil {
		return nil, err
	}
	root, err := workspacesvc.OpenRoot(host)
	if err != nil {
		return nil, err
	}
	return workspacesvc.NewService(root, workspacesvc.Limits{})
}

func (b *runtimeBackend) handleWorkspace(ctx context.Context, raw json.RawMessage) ([]byte, error) {
	operation, err := protocol.DecodeWorkspaceOperationAny(raw)
	if err != nil {
		return nil, err
	}
	respond := func(result any) ([]byte, error) {
		return validatedResponsePayload("workspace", map[string]any{"result": result})
	}
	withRoot := func(path string) (*workspacesvc.Service, error) { return openWorkspaceService(path) }
	switch value := operation.(type) {
	case *protocol.WorkspaceListTree:
		service, err := withRoot(value.Root)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		expanded := make(map[string]bool, len(value.ExpandedPaths))
		for _, path := range value.ExpandedPaths {
			expanded[filepath.Clean(path)] = true
		}
		entries, err := workspaceTree(ctx, service, value.Root, "", 0, expanded, 10_000)
		if err != nil {
			return nil, err
		}
		return respond(map[string]any{"type": "workingTree", "entries": entries})
	case *protocol.WorkspaceBrowseDirectory:
		path := ""
		if value.Path != nil {
			path = *value.Path
		} else {
			path, err = os.UserHomeDir()
			if err != nil {
				return nil, err
			}
		}
		service, err := withRoot(path)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		children, err := service.Browse(ctx, "")
		if err != nil {
			return nil, err
		}
		entries := workspaceEntries(children, false, 0)
		home, _ := os.UserHomeDir()
		clean := service.HostRoot().String()
		var parent any
		if next := filepath.Dir(clean); next != clean {
			parent = next
		}
		volume := filepath.VolumeName(clean) + string(filepath.Separator)
		return respond(map[string]any{"type": "directory", "path": clean, "parent": parent, "home": home, "filesystem_root": volume, "entries": entries})
	case *protocol.WorkspaceReadTextFile:
		service, err := withRoot(value.Root)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		content, _, err := service.ReadText(ctx, value.RelativePath)
		if err != nil {
			return nil, err
		}
		return respond(map[string]any{"type": "textFile", "content": content})
	case *protocol.WorkspaceWriteTextFile:
		service, err := withRoot(value.Root)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		if !service.RaceSafeMutations() {
			return nil, workspacesvc.ErrRaceSafeMutationUnsupported
		}
		_, expected, readErr := service.ReadText(ctx, value.RelativePath)
		if errors.Is(readErr, os.ErrNotExist) {
			empty := sha256.Sum256(nil)
			expected = hex.EncodeToString(empty[:])
		} else if readErr != nil {
			return nil, readErr
		}
		if _, err := service.WriteTextCAS(ctx, value.RelativePath, expected, value.Content); err != nil {
			return nil, err
		}
		return respond(map[string]any{"type": "ack"})
	case *protocol.WorkspaceCWDOperation:
		if value.Type != "inspectCommit" {
			return nil, fmt.Errorf("workspace operation %q is not implemented by the Go daemon", value.Type)
		}
		service, err := withRoot(value.CWD)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		status, err := service.GitStatus(ctx)
		if err != nil {
			return nil, err
		}
		snapshot := map[string]any{"branch": status.Branch, "additions": status.Additions, "deletions": status.Deletions, "staged_additions": status.StagedAdditions, "staged_deletions": status.StagedDeletions, "has_staged": status.HasStaged, "has_unstaged": status.HasUnstaged, "can_push": false}
		return respond(map[string]any{"type": "commitSnapshot", "snapshot": snapshot})
	case *protocol.WorkspaceCommit:
		if value.Push {
			return nil, errors.New("Git push is not implemented by the Go daemon")
		}
		service, err := withRoot(value.CWD)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		if _, err := service.Commit(ctx, value.Message, value.IncludeUnstaged); err != nil {
			return nil, err
		}
		return respond(map[string]any{"type": "ack"})
	case *protocol.WorkspaceCaptureTurn:
		return nil, fmt.Errorf("workspace operation %q is gated until Rust-compatible turn-start, diff-base, and per-file checkpoint metadata are implemented", value.Type)
	case *protocol.WorkspaceRefOperation:
		service, err := withRoot(value.CWD)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		switch value.Type {
		case "restoreRef":
			return nil, errors.New("restoreRef is gated until checkpoint restore has a platform-validated transactional recovery guarantee")
		case "hasRef":
			return respond(map[string]any{"type": "bool", "value": service.HasCheckpoint(ctx, value.GitRef)})
		default:
			return nil, fmt.Errorf("workspace operation %q is not implemented by the Go daemon", value.Type)
		}
	case *protocol.WorkspaceCollectReviewDiff:
		var source string
		if err := json.Unmarshal(value.Source, &source); err != nil || (source != "staged" && source != "unstaged") {
			return nil, errors.New("only staged and unstaged review diffs are implemented by the Go daemon")
		}
		service, err := withRoot(value.CWD)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		diff, err := service.GitDiff(ctx, source == "staged", "")
		if err != nil {
			return nil, err
		}
		data := map[string]any{"source": json.RawMessage(value.Source), "numstat": diff.Numstat, "patch": diff.Patch, "completeContext": !diff.Truncated}
		return respond(map[string]any{"type": "reviewDiff", "data": data})
	default:
		return nil, fmt.Errorf("workspace operation %T is not implemented by the Go daemon", operation)
	}
}

func workspaceEntries(entries []workspacesvc.Entry, expanded bool, depth uint64) []protocol.WorkingTreeEntryValue {
	result := make([]protocol.WorkingTreeEntryValue, 0, len(entries))
	for _, entry := range entries {
		result = append(result, protocol.WorkingTreeEntryValue{RelativePath: filepath.ToSlash(entry.Relative), AbsolutePath: entry.HostPath.String(), Name: entry.Name, IsDir: entry.IsDir, Expanded: expanded && entry.IsDir, Depth: depth})
	}
	return result
}

func workspaceTree(ctx context.Context, service *workspacesvc.Service, root, relative string, depth uint64, expanded map[string]bool, remaining int) ([]protocol.WorkingTreeEntryValue, error) {
	children, err := service.Browse(ctx, relative)
	if err != nil {
		return nil, err
	}
	if len(children) > remaining {
		return nil, workspacesvc.ErrLimitExceeded
	}
	result := make([]protocol.WorkingTreeEntryValue, 0, len(children))
	for _, child := range children {
		isExpanded := child.IsDir && (expanded[filepath.Clean(child.HostPath.String())] || expanded[filepath.Clean(filepath.Join(root, child.Relative))])
		result = append(result, protocol.WorkingTreeEntryValue{RelativePath: filepath.ToSlash(child.Relative), AbsolutePath: child.HostPath.String(), Name: child.Name, IsDir: child.IsDir, Expanded: isExpanded, Depth: depth})
		if isExpanded {
			nested, err := workspaceTree(ctx, service, root, child.Relative, depth+1, expanded, remaining-len(result))
			if err != nil {
				return nil, err
			}
			result = append(result, nested...)
		}
	}
	return result, nil
}

func (b *runtimeBackend) adapter(provider providers.Provider) (providers.Adapter, error) {
	adapter := b.adapters[provider]
	if adapter == nil || !adapter.Descriptor().Implemented {
		return nil, fmt.Errorf("provider %q is not implemented by this Go daemon", provider)
	}
	return adapter, nil
}

func (b *runtimeBackend) sessionCatalog(provider providers.Provider, fork bool) (providers.Adapter, providers.SessionCatalogAdapter, error) {
	adapter, err := b.adapter(provider)
	if err != nil {
		return nil, nil, err
	}
	capabilities := adapter.Descriptor().Capabilities
	catalog, ok := adapter.(providers.SessionCatalogAdapter)
	if fork {
		// Descriptor.Fork describes a live Session.Fork operation only. Offline
		// ForkProviderSession additionally requires a concrete native catalog
		// implementation and native-history reachability; Pi's live fork must
		// never imply a cold/native-session fork capability.
		if !ok || !capabilities.ImportedHistory {
			return nil, nil, fmt.Errorf("provider %q does not support offline native session fork", provider)
		}
		if !capabilities.Fork {
			return nil, nil, fmt.Errorf("provider %q does not support native session fork", provider)
		}
	} else if !capabilities.Resume || !capabilities.ImportedHistory {
		return nil, nil, fmt.Errorf("provider %q does not support native session history", provider)
	}
	if !ok {
		return nil, nil, fmt.Errorf("provider %q native session operation is unavailable in this Go daemon", provider)
	}
	return adapter, catalog, nil
}

func providerFromRaw(raw json.RawMessage) (providers.Provider, error) {
	var envelope struct {
		Provider providers.Provider `json:"provider"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Provider == "" {
		if err != nil {
			return "", err
		}
		return "", errors.New("provider payload is missing provider")
	}
	return envelope.Provider, nil
}

func (b *runtimeBackend) persistAdoptedCursor(ctx context.Context, sessionID, runtimeID protocol.UUID, runtime *providerRuntime, cursor json.RawMessage) error {
	b.mu.Lock()
	if b.runtimes[sessionID] != runtime || runtime.id != runtimeID {
		b.mu.Unlock()
		return errors.New("runtime detached before cursor adoption")
	}
	if err := b.store.SetProviderCursor(ctx, string(sessionID), cursor); err != nil {
		b.mu.Unlock()
		cause := fmt.Errorf("persist adopted provider cursor: %w", err)
		b.failRuntime(sessionID, runtimeID, runtime, cause)
		return cause
	}
	runtime.mu.Lock()
	runtime.pendingCursor = append(json.RawMessage(nil), cursor...)
	runtime.mu.Unlock()
	b.mu.Unlock()
	return nil
}

func commandType(raw json.RawMessage) string {
	kind, _ := protocol.CommandTypeOf(raw)
	return string(kind)
}

func (b *runtimeBackend) currentSkillLocations() []workspacesvc.SkillLocation {
	b.mu.Lock()
	locations := append([]workspacesvc.SkillLocation(nil), b.skillLocations...)
	b.mu.Unlock()
	if len(locations) == 0 {
		locations = b.skills.Locations(nil)
	}
	return locations
}

func (b *runtimeBackend) start(ctx context.Context, request protocol.Request, command protocol.StartCommand) ([]byte, error) {
	if b.terminals.HasSession(string(request.SessionID)) {
		return nil, errors.New("provider session ID collides with an active terminal session")
	}
	adapter := b.adapters[providers.Provider(command.Options.Provider)]
	if adapter == nil || !adapter.Descriptor().Implemented {
		return nil, fmt.Errorf("provider %q is not implemented by this Go daemon", command.Options.Provider)
	}
	if request.RuntimeID == protocol.NilUUID {
		return nil, errors.New("start requires a non-nil runtime ID")
	}
	b.mu.Lock()
	if existing := b.runtimes[request.SessionID]; existing != nil {
		b.mu.Unlock()
		if existing.id == request.RuntimeID {
			return responsePayload("started", map[string]any{"supportsSteer": existing.adapter.Descriptor().Capabilities.Steering})
		}
		return nil, errors.New("session already has a different runtime")
	}
	b.mu.Unlock()
	options := providers.StartOptions{Binary: command.Options.Binary, WorkingDirectory: command.Options.CWD, Access: providers.AccessMode(command.Options.Mode), ComputerUse: command.Options.ComputerUseEnabled}
	if command.Options.Model != nil {
		options.Model = *command.Options.Model
	}
	if command.Options.ReasoningEffort != nil {
		options.ReasoningEffort = *command.Options.ReasoningEffort
	}
	if command.Options.ServiceTier != nil {
		options.ServiceTier = *command.Options.ServiceTier
	}
	if len(command.Options.ProviderCursor) != 0 && string(command.Options.ProviderCursor) != "null" {
		options.ResumeCursor = append(json.RawMessage(nil), command.Options.ProviderCursor...)
	} else if raw, found, err := b.store.Session(ctx, string(request.SessionID)); err != nil {
		return nil, fmt.Errorf("load provider resume cursor: %w", err)
	} else if found {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, fmt.Errorf("load provider resume cursor: %w", err)
		}
		if string(object["provider_cursor"]) != "null" {
			options.ResumeCursor = object["provider_cursor"]
		}
	}
	b.hub.BeginRuntime(string(request.SessionID), string(request.RuntimeID))
	runtime := &providerRuntime{id: request.RuntimeID, adapter: adapter, pending: make(map[pendingKey]*pendingProviderRequest)}
	// Publish the provisional runtime before Adapter.Start: a task-state save on
	// the global actor can race the provider's synchronous Connected callback,
	// and must see its queued cursor. Session-scoped commands remain ordered
	// behind Start by ActorGroup and cannot observe the nil session.
	b.mu.Lock()
	if b.runtimes[request.SessionID] != nil {
		b.mu.Unlock()
		b.hub.EndRuntime(string(request.SessionID), string(request.RuntimeID))
		return nil, errors.New("runtime raced with another start")
	}
	b.runtimes[request.SessionID] = runtime
	b.mu.Unlock()
	sink := providers.SinkFunc(func(event providers.Event) { b.emit(request.SessionID, request.RuntimeID, runtime, event) })
	session, err := adapter.Start(ctx, options, sink)
	if err != nil {
		b.removeRuntime(request.SessionID, runtime)
		b.hub.EndRuntime(string(request.SessionID), string(request.RuntimeID))
		return nil, err
	}
	runtime.mu.Lock()
	runtime.session = session
	failureErr := runtime.failureErr
	exited := runtime.exited
	runtime.mu.Unlock()
	if failureErr != nil || exited {
		_ = session.Close()
		b.hub.EndRuntime(string(request.SessionID), string(request.RuntimeID))
		if failureErr != nil {
			return nil, failureErr
		}
		return nil, errors.New("provider process exited during startup")
	}
	b.mu.Lock()
	attached := b.runtimes[request.SessionID] == runtime
	b.mu.Unlock()
	if !attached {
		_ = session.Close()
		b.hub.EndRuntime(string(request.SessionID), string(request.RuntimeID))
		return nil, errors.New("runtime detached during startup")
	}
	return responsePayload("started", map[string]any{"supportsSteer": adapter.Descriptor().Capabilities.Steering})
}

func (b *runtimeBackend) removeRuntime(sessionID protocol.UUID, runtime *providerRuntime) {
	b.mu.Lock()
	if b.runtimes[sessionID] == runtime {
		delete(b.runtimes, sessionID)
	}
	b.mu.Unlock()
}

func (r *providerRuntime) beginPending(key pendingKey) (json.RawMessage, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pending := r.pending[key]
	if pending == nil || pending.resolving {
		return nil, false
	}
	pending.resolving = true
	return append(json.RawMessage(nil), pending.raw...), true
}
func (r *providerRuntime) finishPending(key pendingKey, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if success {
		if pending := r.pending[key]; pending != nil {
			r.pendingBytes -= pending.bytes
		}
		delete(r.pending, key)
	} else if pending := r.pending[key]; pending != nil {
		pending.resolving = false
	}
}

func (r *providerRuntime) addPending(key pendingKey, raw json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.pending[key]; exists {
		return errors.New("provider reused a pending request ID")
	}
	size := len(key.kind) + len(key.wireID) + len(raw)
	if len(r.pending) >= maxPendingProviderRequests || size > maxPendingProviderBytes || r.pendingBytes+size > maxPendingProviderBytes {
		return ErrProviderPendingOverload
	}
	r.pending[key] = &pendingProviderRequest{raw: append(json.RawMessage(nil), raw...), bytes: size}
	r.pendingBytes += size
	return nil
}

func (b *runtimeBackend) runtime(sessionID, runtimeID protocol.UUID) (*providerRuntime, error) {
	b.mu.Lock()
	runtime := b.runtimes[sessionID]
	b.mu.Unlock()
	if runtime == nil || runtime.id != runtimeID {
		return nil, errors.New("runtime is not active for this session")
	}
	return runtime, nil
}

func (b *runtimeBackend) closeRuntime(sessionID, runtimeID protocol.UUID) error {
	b.mu.Lock()
	runtime := b.runtimes[sessionID]
	if runtime == nil || (runtimeID != protocol.NilUUID && runtime.id != runtimeID) {
		b.mu.Unlock()
		return nil
	}
	delete(b.runtimes, sessionID)
	b.mu.Unlock()
	b.hub.EndRuntime(string(sessionID), string(runtime.id))
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = b.terminals.CloseTerminal(closeCtx, terminaltransport.Owner{SessionID: string(sessionID), RuntimeID: string(runtime.id)})
	cancel()
	runtime.clearPending()
	return runtime.session.Close()
}

func (b *runtimeBackend) close() {
	b.mu.Lock()
	runtimes := b.runtimes
	b.runtimes = make(map[protocol.UUID]*providerRuntime)
	b.mu.Unlock()
	for sessionID, runtime := range runtimes {
		b.hub.EndRuntime(string(sessionID), string(runtime.id))
		runtime.clearPending()
		_ = runtime.session.Close()
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = b.terminals.Shutdown(closeCtx)
	cancel()
}

func (b *runtimeBackend) emitTerminal(owner terminaltransport.Owner, event terminaltransport.BackendEvent) {
	var wire protocol.WireDriverEvent
	switch {
	case event.Err != nil:
		wire = protocol.WireDriverEvent{Kind: string(protocol.EventTerminalError), Payload: mustJSON(event.Err.Error())}
	case event.Exit:
		wire = protocol.WireDriverEvent{Kind: string(protocol.EventTerminalExited), Payload: mustJSON(nil)}
	case len(event.Data) != 0:
		wire = protocol.WireDriverEvent{Kind: string(protocol.EventTerminalOutput), Payload: mustJSON(protocol.TerminalOutputPayload{Data: event.Data})}
	default:
		return
	}
	encoded, err := protocol.Marshal(wire)
	if err != nil {
		return
	}
	// PTY bytes are ephemeral. Reconnect never replays terminal transcripts
	// into the durable provider event journal.
	b.hub.Emit(owner.SessionID, owner.RuntimeID, encoded, false)
	if event.Err != nil || event.Exit {
		b.hub.EndRuntime(owner.SessionID, owner.RuntimeID)
	}
}

func (r *providerRuntime) clearPending() {
	r.mu.Lock()
	r.pending = make(map[pendingKey]*pendingProviderRequest)
	r.pendingBytes = 0
	r.mu.Unlock()
}

func requestKey(raw json.RawMessage) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}
func wireRequestID(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return requestKey(raw)
}

func (b *runtimeBackend) emit(sessionID, runtimeID protocol.UUID, runtime *providerRuntime, event providers.Event) {
	// Provider sinks outlive process callbacks. Reject stale callbacks before
	// they can persist a cursor, consume pending-request budget, or emit into a
	// replacement runtime that reused the session ID.
	b.mu.Lock()
	if b.runtimes[sessionID] != runtime || runtime.id != runtimeID {
		b.mu.Unlock()
		return
	}
	if event.Kind == providers.EventConnected && len(event.Cursor) != 0 {
		runtime.mu.Lock()
		runtime.pendingCursor = append(json.RawMessage(nil), event.Cursor...)
		runtime.mu.Unlock()
		err := b.store.SetProviderCursor(context.Background(), string(sessionID), event.Cursor)
		if err != nil && !errors.Is(err, storage.ErrTaskSessionNotFound) {
			b.mu.Unlock()
			b.failRuntime(sessionID, runtimeID, runtime, fmt.Errorf("persist provider cursor: %w", err))
			return
		}
	}
	if (event.Kind == providers.EventPermission || event.Kind == providers.EventUserInputRequested) && len(event.RequestID) != 0 {
		kind := "permission"
		if event.Kind == providers.EventUserInputRequested {
			kind = "question"
		}
		wireID := kind + ":" + requestKey(event.RequestID)
		if err := runtime.addPending(pendingKey{kind: kind, wireID: wireID}, event.RequestID); err != nil {
			b.mu.Unlock()
			b.failRuntime(sessionID, runtimeID, runtime, err)
			return
		}
		event.RequestID = mustJSON(wireID)
	}
	b.mu.Unlock()
	wire, err := providerEventWire(event)
	if err != nil {
		wire = protocol.WireDriverEvent{Kind: string(protocol.EventError), Payload: mustJSON("provider event mapping: " + err.Error())}
	}
	encoded, err := protocol.Marshal(wire)
	if err != nil {
		return
	}
	b.hub.Emit(string(sessionID), string(runtimeID), encoded, true)
	if event.Kind == providers.EventProcessExited {
		b.detachRuntime(sessionID, runtimeID, runtime)
	}
}

func (b *runtimeBackend) currentCursors(sessions []json.RawMessage) map[string]json.RawMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentCursorsLocked(sessions)
}

func (b *runtimeBackend) currentCursorsLocked(sessions []json.RawMessage) map[string]json.RawMessage {
	relevant := make(map[string]bool, len(sessions))
	for _, raw := range sessions {
		var value struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &value) == nil && value.ID != "" {
			relevant[value.ID] = true
		}
	}
	cursors := make(map[string]json.RawMessage, len(b.runtimes))
	for id, runtime := range b.runtimes {
		if !relevant[string(id)] {
			continue
		}
		runtime.mu.Lock()
		cursor := append(json.RawMessage(nil), runtime.pendingCursor...)
		runtime.mu.Unlock()
		if len(cursor) != 0 {
			cursors[string(id)] = cursor
		}
	}
	return cursors
}

// saveTaskState holds the same ownership lock used by provider sinks from
// cursor snapshot through transaction commit. Therefore Connected is ordered
// wholly before the snapshot (cursor merged in the transaction) or wholly
// after commit (the single-field cursor UPDATE sees the new row).
func (b *runtimeBackend) saveTaskState(ctx context.Context, projects, sessions []json.RawMessage) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.store.SaveWithProviderCursors(ctx, projects, sessions, b.currentCursorsLocked(sessions))
}

func (b *runtimeBackend) detachRuntime(sessionID, runtimeID protocol.UUID, runtime *providerRuntime) {
	b.mu.Lock()
	if b.runtimes[sessionID] == runtime {
		delete(b.runtimes, sessionID)
	}
	b.mu.Unlock()
	runtime.mu.Lock()
	runtime.exited = true
	session := runtime.session
	runtime.pending = make(map[pendingKey]*pendingProviderRequest)
	runtime.pendingBytes = 0
	runtime.mu.Unlock()
	b.hub.EndRuntime(string(sessionID), string(runtimeID))
	go b.closeTerminalBounded(sessionID, runtimeID)
	if session != nil {
		go session.Close()
	}
}

func (b *runtimeBackend) failRuntime(sessionID, runtimeID protocol.UUID, runtime *providerRuntime, cause error) {
	b.mu.Lock()
	active := b.runtimes[sessionID] == runtime
	if active {
		delete(b.runtimes, sessionID)
	}
	b.mu.Unlock()
	runtime.mu.Lock()
	if runtime.failureErr != nil || runtime.exited {
		runtime.mu.Unlock()
		return
	}
	runtime.failureErr = cause
	session := runtime.session
	runtime.pending = make(map[pendingKey]*pendingProviderRequest)
	runtime.pendingBytes = 0
	runtime.mu.Unlock()
	if active {
		wire := protocol.WireDriverEvent{Kind: string(protocol.EventError), Payload: mustJSON(cause.Error())}
		if encoded, err := protocol.Marshal(wire); err == nil {
			b.hub.Emit(string(sessionID), string(runtimeID), encoded, true)
		}
		b.hub.EndRuntime(string(sessionID), string(runtimeID))
		go b.closeTerminalBounded(sessionID, runtimeID)
	}
	if session != nil {
		go session.Close()
	}
}

func (b *runtimeBackend) closeTerminalBounded(sessionID, runtimeID protocol.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.terminals.CloseTerminal(ctx, terminaltransport.Owner{SessionID: string(sessionID), RuntimeID: string(runtimeID)})
}

func mustJSON(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }

func providerEventWire(event providers.Event) (protocol.WireDriverEvent, error) {
	kind := string(event.Kind)
	var payload any
	switch event.Kind {
	case providers.EventConnected:
		payload = json.RawMessage(event.Cursor)
	case providers.EventAvailableCommands:
		commands, err := availableCommandsPayload(event.Raw)
		if err != nil {
			return protocol.WireDriverEvent{}, err
		}
		payload = commands
	case providers.EventTurnStarted, providers.EventProcessExited:
		payload = nil
	case providers.EventTextDelta, providers.EventReasoningDelta, providers.EventError, providers.EventAutoTitleUpdated:
		payload = event.Text
	case providers.EventSteerAccepted:
		payload = map[string]any{"message": event.Text}
	case providers.EventSteerRejected:
		payload = map[string]any{"message": event.Text, "reason": event.Summary}
	case providers.EventActivity:
		if event.Activity == nil {
			return protocol.WireDriverEvent{}, errors.New("activity payload missing")
		}
		payload = map[string]any{"id": nullable(event.Activity.ID), "kind": event.Activity.Kind, "title": event.Activity.Title, "detail": nullable(event.Activity.Detail), "complete": event.Activity.Complete}
	case providers.EventPermission:
		if event.Permission == nil {
			return protocol.WireDriverEvent{}, errors.New("permission payload missing")
		}
		options := make([]map[string]any, len(event.Permission.Options))
		for i, option := range event.Permission.Options {
			options[i] = map[string]any{"id": option.ID, "label": option.Label, "allow": option.Allow}
		}
		payload = map[string]any{"requestId": wireRequestID(event.RequestID), "title": event.Permission.Title, "detail": event.Permission.Detail, "options": options}
	case providers.EventUserInputRequested:
		questions := make([]map[string]any, len(event.Questions))
		for i, question := range event.Questions {
			options := make([]map[string]any, len(question.Options))
			for j, option := range question.Options {
				options[j] = map[string]any{"label": option.Label, "description": nullable(option.Description)}
			}
			questions[i] = map[string]any{"id": question.ID, "header": question.Header, "question": question.Question, "options": options, "multiSelect": question.MultiSelect}
		}
		payload = map[string]any{"requestId": wireRequestID(event.RequestID), "questions": questions}
	case providers.EventUsageUpdated:
		payload = map[string]any{"contextTokens": event.ContextTokens, "contextWindow": event.ContextWindow}
	case providers.EventGoalUpdated:
		if event.GoalPresent {
			payload = json.RawMessage(event.Goal)
		} else {
			payload = nil
		}
	case providers.EventTurnFinished:
		payload = map[string]any{"success": event.Success, "summary": nullable(event.Summary)}
	case providers.EventBackgroundWork:
		if event.Background == nil {
			return protocol.WireDriverEvent{}, errors.New("background payload missing")
		}
		key := map[string]any{"kind": event.Background.Kind, "providerId": event.Background.ID}
		if event.Background.OutputDelta != "" {
			payload = map[string]any{"type": "outputDelta", "key": key, "delta": event.Background.OutputDelta}
		} else {
			status := event.Background.Status
			if status == "" {
				status = "running"
			}
			payload = map[string]any{"type": "upsert", "key": key, "title": event.Background.Title, "outputTruncated": false, "startedAtMs": uint64(0), "updatedAtMs": uint64(0), "background": true, "canStop": event.Background.ControlID != "", "status": status}
		}
	default:
		return protocol.WireDriverEvent{}, fmt.Errorf("event %q is not implemented", event.Kind)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return protocol.WireDriverEvent{}, err
	}
	wire := protocol.WireDriverEvent{Kind: kind, Payload: raw}
	if _, err := protocol.DecodeEventPayloadAny(wire); err != nil {
		return protocol.WireDriverEvent{}, err
	}
	return wire, nil
}

// availableCommandsPayload accepts the narrow provider payload (an array) and
// the ACP JSON-RPC envelope retained by older adapters. In both cases the
// protocol decoder below validates the exact v7 command shape before emit.
func availableCommandsPayload(raw json.RawMessage) (json.RawMessage, error) {
	var commands []protocol.ReportedCommandPayload
	if protocol.Decode(raw, &commands) == nil {
		return append(json.RawMessage(nil), raw...), nil
	}
	var envelope struct {
		AvailableCommands json.RawMessage `json:"availableCommands"`
		Update            json.RawMessage `json:"update"`
		Params            json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, errors.New("availableCommands payload is invalid")
	}
	for _, candidate := range []json.RawMessage{envelope.AvailableCommands, envelope.Update, envelope.Params} {
		if len(candidate) == 0 {
			continue
		}
		if protocol.Decode(candidate, &commands) == nil {
			return append(json.RawMessage(nil), candidate...), nil
		}
		var nested struct {
			AvailableCommands json.RawMessage `json:"availableCommands"`
			Update            json.RawMessage `json:"update"`
		}
		if json.Unmarshal(candidate, &nested) != nil {
			continue
		}
		for _, value := range []json.RawMessage{nested.AvailableCommands, nested.Update} {
			if len(value) == 0 {
				continue
			}
			if protocol.Decode(value, &commands) == nil {
				return append(json.RawMessage(nil), value...), nil
			}
			var update struct {
				AvailableCommands json.RawMessage `json:"availableCommands"`
			}
			if json.Unmarshal(value, &update) == nil && protocol.Decode(update.AvailableCommands, &commands) == nil {
				return append(json.RawMessage(nil), update.AvailableCommands...), nil
			}
		}
	}
	return nil, errors.New("availableCommands event is missing parsed commands")
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
