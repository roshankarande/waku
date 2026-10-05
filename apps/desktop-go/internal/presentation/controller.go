package presentation

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	codecomponent "github.com/roshankarande/waku/apps/desktop-go/internal/components/code"
	"github.com/roshankarande/waku/apps/desktop-go/internal/components/terminalui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

// Transport is the narrow live-client surface used by the presentation
// controller. *client.Client satisfies it; tests use a deterministic fake.
type Transport interface {
	Request(context.Context, protocol.UUID, protocol.UUID, any) (json.RawMessage, error)
	Notify(context.Context, protocol.UUID, protocol.UUID, any) error
	Subscribe(protocol.UUID, protocol.UUID) (<-chan protocol.SequencedEvent, func())
	SubscribeTaskState() (<-chan uint64, func())
	Done() <-chan struct{}
	Err() error
}

type UISurfaceTransport interface {
	GetSettings(context.Context) (protocol.DaemonSettingsValue, error)
	UpdateSettings(context.Context, protocol.DaemonSettingsValue) error
	Workspace(context.Context, any) (any, error)
}
type PreparedTerminalProvider interface {
	// PrepareTerminal must return a pane backed by a remote daemon connection.
	// Implementations must never fall back to starting a local shell.
	PrepareTerminal(context.Context, string) (*terminalui.Pane, error)
}

type Controller struct {
	Model *Model
	post  func(func())

	mu               sync.Mutex
	transport        Transport
	generation       uint64
	queue            chan Command
	closed           chan struct{}
	closeOnce        sync.Once
	isClosed         atomic.Bool
	taskCancel       func()
	subCancels       map[string]func()
	runtimes         map[string]protocol.UUID
	startOptions     map[string]protocol.WireDriverStartOptions
	runtimeFlights   map[string]*runtimeFlight
	pendingBySession map[string]PendingInteraction
	Terminal         PreparedTerminalProvider
	terminalPanes    map[*terminalui.Pane]struct{}
}

type runtimeFlight struct {
	done    chan struct{}
	runtime protocol.UUID
	err     error
}

const maxTerminalPresentationBytes = 1 << 20

func NewController(model *Model, post func(func())) *Controller {
	if post == nil {
		panic("presentation.NewController requires a UI post dispatcher")
	}
	c := &Controller{Model: model, post: post, queue: make(chan Command, 128), closed: make(chan struct{}), subCancels: make(map[string]func()), runtimes: make(map[string]protocol.UUID), startOptions: make(map[string]protocol.WireDriverStartOptions), runtimeFlights: make(map[string]*runtimeFlight), pendingBySession: make(map[string]PendingInteraction), terminalPanes: make(map[*terminalui.Pane]struct{})}
	go c.runCommands()
	return c
}

// SetTransport advances the connection generation. Results and event pumps
// from older transports are rejected when their main-thread commits arrive.
func (c *Controller) SetTransport(transport Transport) {
	if c.isClosed.Load() {
		return
	}
	c.mu.Lock()
	if c.isClosed.Load() {
		c.mu.Unlock()
		return
	}
	c.generation++
	gen := c.generation
	if c.taskCancel != nil {
		c.taskCancel()
		c.taskCancel = nil
	}
	for id, stop := range c.subCancels {
		stop()
		delete(c.subCancels, id)
	}
	c.runtimes = make(map[string]protocol.UUID)
	c.startOptions = make(map[string]protocol.WireDriverStartOptions)
	c.runtimeFlights = make(map[string]*runtimeFlight)
	c.pendingBySession = make(map[string]PendingInteraction)
	c.transport = transport
	c.mu.Unlock()
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		for i := range c.Model.Sessions {
			c.Model.Sessions[i].Activity = "Reconnecting…"
			c.Model.Sessions[i].Pending = PendingInteraction{}
			c.Model.Sessions[i].commandTokens = nil
			c.Model.Sessions[i].commandStateToken = 0
			c.Model.Sessions[i].CommandBusy = false
			if c.Model.Sessions[i].Status == SessionWaitingApproval || c.Model.Sessions[i].Status == SessionResolvingApproval {
				c.Model.Sessions[i].Status = SessionPaused
			}
		}
	})
	if transport != nil {
		go c.loadSummaries(gen, transport)
	}
}

func (c *Controller) Dispatch(command Command) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed.Load() {
		return false
	}
	select {
	case <-c.closed:
		return false
	case c.queue <- command:
		return true
	default:
		return false
	}
}

// SessionSelected is called by the shell after navigation. Hydration and
// subscriptions happen on workers and are scoped to the current generation.
func (c *Controller) SessionSelected(id string) {
	transport, gen := c.current()
	if transport == nil || id == "" {
		return
	}
	if session := c.Model.SessionByID(id); session != nil {
		if session.Hydrated {
			go c.attachForReplay(gen, transport, id)
			return
		}
		if session.Hydrating {
			return
		}
		session.Hydrating = true
		session.Activity = "Loading conversation…"
	}
	go c.hydrate(gen, transport, id)
}

// RequestPane keeps capability gating out of the view. Until the corresponding
// protocol query exists, panes show an explicit unavailable result rather than
// fabricating local filesystem/browser state.
func (c *Controller) RequestPane(_ string, kind PaneKind) {
	work := c.Model.BeginPaneWork(kind)
	transport, gen := c.current()
	root := c.workspaceRoot(work.SessionID)
	go func() {
		if kind == PaneTerminal && c.Terminal != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pane, err := c.Terminal.PrepareTerminal(ctx, work.SessionID)
			if err == nil && pane == nil {
				err = errors.New("terminal provider returned no remote pane")
			}
			if pane != nil {
				c.mu.Lock()
				closed := c.isClosed.Load()
				if !closed {
					c.terminalPanes[pane] = struct{}{}
				}
				c.mu.Unlock()
				if closed {
					_ = pane.Close()
					return
				}
			}
			c.post(func() {
				if !c.currentGeneration(gen, transport) || c.isClosed.Load() || !c.Model.CommitPaneWork(work, "", err) {
					if pane != nil {
						go pane.Close()
					}
					return
				}
				if err != nil {
					if pane != nil {
						go pane.Close()
					}
					return
				}
				if s := c.Model.SessionByID(work.SessionID); s != nil {
					s.Pane.Terminal = pane
					s.Pane.CapabilityNote = "Remote daemon terminal. Search reports match counts; match highlighting and next/previous navigation are not available in this MyGo terminal revision."
				}
			})
			return
		}
		if kind == PaneFiles || kind == PaneReview {
			surface, ok := transport.(UISurfaceTransport)
			if !ok || root == "" {
				message := "A typed daemon workspace connection and project root are required. Paths remain opaque to this desktop."
				c.post(func() {
					if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
						c.Model.CommitPaneWork(work, message, nil)
					}
				})
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if kind == PaneFiles {
				value, err := surface.Workspace(ctx, protocol.WorkspaceListTree{Type: "listTree", Root: root, ExpandedPaths: []string{}})
				result, ok := value.(*protocol.WorkspaceWorkingTreeResult)
				if err == nil && !ok {
					err = fmt.Errorf("list workspace returned %T", value)
				}
				c.post(func() {
					if c.currentGeneration(gen, transport) && !c.isClosed.Load() && c.Model.CommitPaneWork(work, "", err) {
						if err != nil {
							return
						}
						if s := c.Model.SessionByID(work.SessionID); s != nil {
							s.Pane.WorkspaceRoot = root
							s.Pane.WorkspaceEntries = workspaceEntries(result)
							s.Pane.CapabilityNote = "Remote files are listed and read through the daemon. Editing is local to this view; save is unavailable because the workspace wire does not accept a caller revision for compare-and-swap."
						}
					}
				})
				return
			}
			statusValue, err := surface.Workspace(ctx, protocol.WorkspaceCWDOperation{Type: "inspectCommit", CWD: root})
			status, ok := statusValue.(*protocol.WorkspaceCommitSnapshotResult)
			if err == nil && !ok {
				err = fmt.Errorf("inspect commit returned %T", statusValue)
			}
			if err == nil {
				preparedStatus := gitState(status.Snapshot)
				c.post(func() {
					if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
						c.Model.CommitPaneSummary(work, fmt.Sprintf("Branch %s · preparing unstaged diff", preparedStatus.Branch), preparedStatus)
					}
				})
			}
			var diff *protocol.WorkspaceReviewDiffResult
			if err == nil {
				diffValue, diffErr := surface.Workspace(ctx, protocol.WorkspaceCollectReviewDiff{Type: "collectReviewDiff", CWD: root, Source: json.RawMessage(`"unstaged"`)})
				err = diffErr
				diff, ok = diffValue.(*protocol.WorkspaceReviewDiffResult)
				if err == nil && !ok {
					err = fmt.Errorf("collect review diff returned %T", diffValue)
				}
			}
			c.post(func() {
				if c.currentGeneration(gen, transport) && !c.isClosed.Load() && c.Model.CommitPaneWork(work, "", err) {
					if err != nil {
						return
					}
					if s := c.Model.SessionByID(work.SessionID); s != nil {
						s.Pane.WorkspaceRoot = root
						s.Pane.ReviewSource = "unstaged"
						s.Pane.ReviewSnapshot = codecomponent.PrepareDiff(diff.Data.Patch, work.Generation)
						s.Pane.Review = codecomponent.PrepareReview(s.Pane.ReviewSnapshot)
						s.Pane.ReviewView = &codecomponent.ViewState{}
						s.Pane.Git = gitState(status.Snapshot)
						s.Pane.CapabilityNote = "Status and staged/unstaged diffs are daemon-backed. Commit is unavailable because the wire cannot bind it to the reviewed index/tree revision. Stage/unstage operations are not in the workspace wire; checkpoint actions are gated because the UI lacks an authoritative provider turn count."
					}
				}
			})
			return
		}
		message := kind.Label() + " is not exposed by this daemon capability yet."
		c.post(func() {
			if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
				c.Model.CommitPaneWork(work, message, nil)
			}
		})
	}()
}

func (c *Controller) workspaceRoot(sessionID string) string {
	session := c.Model.SessionByID(sessionID)
	if session == nil {
		return ""
	}
	for i := range c.Model.Projects {
		if c.Model.Projects[i].ID == session.ProjectID {
			return c.Model.Projects[i].PathHint
		}
	}
	return ""
}

func workspaceEntries(result *protocol.WorkspaceWorkingTreeResult) []WorkspaceEntry {
	if result == nil {
		return nil
	}
	rows := make([]WorkspaceEntry, 0, len(result.Entries))
	for _, entry := range result.Entries {
		rows = append(rows, WorkspaceEntry{Path: entry.RelativePath, AbsolutePath: entry.AbsolutePath, Name: entry.Name, IsDir: entry.IsDir, Depth: entry.Depth})
	}
	return rows
}

func gitState(value protocol.CommitSnapshotValue) GitState {
	return GitState{Branch: value.Branch, Additions: value.Additions, Deletions: value.Deletions, StagedAdditions: value.StagedAdditions, StagedDeletions: value.StagedDeletions, HasStaged: value.HasStaged, HasUnstaged: value.HasUnstaged, CanPush: value.CanPush}
}

func (c *Controller) RequestWorkspaceEntry(sessionID, path, absolutePath string, directory bool) {
	work, ok := c.Model.BeginPaneRefresh(sessionID, PaneFiles)
	if !ok {
		return
	}
	session := c.Model.SessionByID(sessionID)
	root := session.Pane.WorkspaceRoot
	transport, gen := c.current()
	surface, ok := transport.(UISurfaceTransport)
	if !ok {
		c.post(func() {
			if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
				c.Model.CommitPaneWork(work, "", errors.New("typed workspace capability is unavailable"))
			}
		})
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if directory {
			value, err := surface.Workspace(ctx, protocol.WorkspaceBrowseDirectory{Type: "browseDirectory", Path: &absolutePath})
			result, valid := value.(*protocol.WorkspaceDirectoryResult)
			if err == nil && !valid {
				err = fmt.Errorf("browse workspace returned %T", value)
			}
			c.post(func() {
				if !c.currentGeneration(gen, transport) || !c.Model.CommitPaneWork(work, "", err) || err != nil {
					return
				}
				if s := c.Model.SessionByID(sessionID); s != nil {
					if s.Pane.Editor != nil {
						go s.Pane.Editor.Close()
					}
					s.Pane.WorkspaceRoot, s.Pane.WorkspaceParent = result.Path, result.Parent
					s.Pane.WorkspaceEntries = directoryEntries(result)
					s.Pane.Editor, s.Pane.SelectedPath = nil, ""
				}
			})
			return
		}
		value, err := surface.Workspace(ctx, protocol.WorkspaceReadTextFile{Type: "readTextFile", Root: root, RelativePath: path})
		result, valid := value.(*protocol.WorkspaceTextFileResult)
		if err == nil && !valid {
			err = fmt.Errorf("read workspace file returned %T", value)
		}
		var editor *codecomponent.EditorSession
		if err == nil {
			editor = &codecomponent.EditorSession{Model: codecomponent.EditorModel{Generation: 1, Path: path, Text: result.Content}}
		}
		c.post(func() {
			if !c.currentGeneration(gen, transport) || !c.Model.CommitPaneWork(work, "", err) || err != nil {
				if editor != nil {
					go editor.Close()
				}
				return
			}
			if s := c.Model.SessionByID(sessionID); s != nil {
				if s.Pane.Editor != nil {
					go s.Pane.Editor.Close()
				}
				s.Pane.Editor, s.Pane.SelectedPath = editor, path
			}
		})
	}()
}

func directoryEntries(result *protocol.WorkspaceDirectoryResult) []WorkspaceEntry {
	if result == nil {
		return nil
	}
	rows := make([]WorkspaceEntry, 0, len(result.Entries))
	for _, entry := range result.Entries {
		rows = append(rows, WorkspaceEntry{Path: entry.RelativePath, AbsolutePath: entry.AbsolutePath, Name: entry.Name, IsDir: entry.IsDir, Depth: entry.Depth})
	}
	return rows
}

func (c *Controller) RequestReviewSource(sessionID, source string) {
	if source != "staged" && source != "unstaged" {
		return
	}
	work, ok := c.Model.BeginPaneRefresh(sessionID, PaneReview)
	if !ok {
		return
	}
	root := c.Model.SessionByID(sessionID).Pane.WorkspaceRoot
	transport, gen := c.current()
	surface, ok := transport.(UISurfaceTransport)
	if !ok {
		c.post(func() {
			if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
				c.Model.CommitPaneWork(work, "", errors.New("typed workspace capability is unavailable"))
			}
		})
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		value, err := surface.Workspace(ctx, protocol.WorkspaceCollectReviewDiff{Type: "collectReviewDiff", CWD: root, Source: json.RawMessage(strconv.Quote(source))})
		result, valid := value.(*protocol.WorkspaceReviewDiffResult)
		if err == nil && !valid {
			err = fmt.Errorf("collect review diff returned %T", value)
		}
		var snapshot *codecomponent.Snapshot
		if err == nil {
			snapshot = codecomponent.PrepareDiff(result.Data.Patch, work.Generation)
		}
		c.post(func() {
			if !c.currentGeneration(gen, transport) || !c.Model.CommitPaneWork(work, "", err) || err != nil {
				return
			}
			if s := c.Model.SessionByID(sessionID); s != nil {
				s.Pane.ReviewSource = source
				s.Pane.ReviewSnapshot, s.Pane.Review, s.Pane.ReviewView = snapshot, codecomponent.PrepareReview(snapshot), &codecomponent.ViewState{}
			}
		})
	}()
}

func (c *Controller) RequestHub(kind HubKind) {
	generation := c.Model.BeginHubWork(kind)
	transport, gen := c.current()
	projects := append([]Project(nil), c.Model.Projects...)
	provider := ""
	if session := c.Model.CurrentSession(); session != nil {
		provider = session.StartOptions.Provider
	}
	if transport == nil {
		c.post(func() {
			c.Model.CommitHubWork(kind, generation, nil, "Connect to a daemon to load "+strings.ToLower(kind.Label())+".", nil)
		})
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if kind == HubSettings {
			surface, ok := transport.(UISurfaceTransport)
			if !ok {
				c.commitHubUnavailable(gen, transport, kind, generation, "Connected client does not expose typed settings operations.")
				return
			}
			settings, err := surface.GetSettings(ctx)
			if err != nil {
				c.commitHubUnavailable(gen, transport, kind, generation, "Daemon capability unavailable: "+err.Error())
				return
			}
			rows := settingsValueRows(settings)
			c.post(func() {
				if c.currentGeneration(gen, transport) && !c.isClosed.Load() && c.Model.CommitHubWork(kind, generation, rows, "", nil) {
					copy := cloneSettings(settings)
					c.Model.Hub.Settings = &copy
				}
			})
			return
		}
		if kind == HubProviders {
			c.loadProviderHub(ctx, gen, transport, generation, provider)
			return
		}
		if kind == HubUsage {
			c.loadUsageHub(ctx, gen, transport, generation, provider, projects)
			return
		}
		var command any
		switch kind {
		case HubSkills:
			items := make([]protocol.SkillProject, 0, len(projects))
			for _, project := range projects {
				items = append(items, protocol.SkillProject{project.ID, project.PathHint})
			}
			command = protocol.LoadSkillsCommand{Type: protocol.CommandLoadSkills, Projects: items}
		default:
			c.commitHubUnavailable(gen, transport, kind, generation, "Unknown panel.")
			return
		}
		raw, err := transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, command)
		if err != nil {
			c.commitHubUnavailable(gen, transport, kind, generation, "Daemon capability unavailable: "+err.Error())
			return
		}
		decoded, err := protocol.DecodeResponsePayloadAny(raw)
		if err != nil {
			c.commitHubError(gen, transport, kind, generation, err)
			return
		}
		rows, err := hubRows(decoded)
		if err != nil {
			c.commitHubError(gen, transport, kind, generation, err)
			return
		}
		c.post(func() {
			if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
				c.Model.CommitHubWork(kind, generation, rows, "", nil)
			}
		})
	}()
}

func cloneSettings(settings protocol.DaemonSettingsValue) protocol.DaemonSettingsValue {
	settings.ComputerUseAllowedApps = append([]protocol.ComputerAppGrant(nil), settings.ComputerUseAllowedApps...)
	settings.DisabledProviders = append([]string(nil), settings.DisabledProviders...)
	overrides := make(map[string]string, len(settings.ProviderBinaryOverrides))
	for key, value := range settings.ProviderBinaryOverrides {
		overrides[key] = value
	}
	settings.ProviderBinaryOverrides = overrides
	extra := make(map[string]json.RawMessage, len(settings.Extra))
	for key, value := range settings.Extra {
		extra[key] = bytes.Clone(value)
	}
	settings.Extra = extra
	return settings
}

func settingsValueRows(settings protocol.DaemonSettingsValue) []HubRow {
	enabled := "Disabled"
	if settings.ComputerUseEnabled {
		enabled = "Enabled"
	}
	return []HubRow{
		{Key: "settings-write", Title: "Settings editing unavailable", Detail: "The daemon replaces the whole document and exposes no revision or field-patch precondition; editing here could overwrite concurrent or newer settings.", Status: "Read-only"},
		{Key: "computer-use", Title: "Computer use", Detail: enabled},
		{Key: "allowed-apps", Title: "Allowed computer-use apps", Detail: fmt.Sprintf("%d configured", len(settings.ComputerUseAllowedApps))},
		{Key: "disabled-providers", Title: "Disabled providers", Detail: fmt.Sprintf("%d disabled", len(settings.DisabledProviders))},
		{Key: "provider-overrides", Title: "Provider binary overrides", Detail: fmt.Sprintf("%d configured (paths hidden)", len(settings.ProviderBinaryOverrides))},
		{Key: "other-settings", Title: "Other settings", Detail: fmt.Sprintf("%d preserved setting fields hidden from this view", len(settings.Extra))},
	}
}

func (c *Controller) loadUsageHub(ctx context.Context, gen uint64, transport Transport, work uint64, provider string, projects []Project) {
	roots := make([]string, 0, len(projects))
	for _, project := range projects {
		if project.PathHint != "" {
			roots = append(roots, project.PathHint)
		}
	}
	rows := []HubRow{}
	raw, err := transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.LoadUsageHistoryCommand{Type: protocol.CommandLoadUsageHistory, Window: json.RawMessage(`"thisMonth"`), ProjectRoots: roots})
	if err != nil {
		rows = append(rows, HubRow{Key: "history-unavailable", Title: "Usage history unavailable", Detail: err.Error(), Status: "Blocked"})
	} else if decoded, decodeErr := protocol.DecodeResponsePayloadAny(raw); decodeErr != nil {
		rows = append(rows, HubRow{Key: "history-invalid", Title: "Usage history invalid", Detail: decodeErr.Error(), Status: "Error"})
	} else if prepared, prepareErr := hubRows(decoded); prepareErr != nil {
		rows = append(rows, HubRow{Key: "history-invalid", Title: "Usage history invalid", Detail: prepareErr.Error(), Status: "Error"})
	} else {
		rows = append(rows, prepared...)
	}
	if provider == "" {
		rows = append(rows, HubRow{Key: "plan-no-provider", Title: "Plan usage unavailable", Detail: "Select a session with a configured provider.", Status: "Blocked"})
	} else {
		raw, err = transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.FetchPlanUsageCommand{Type: protocol.CommandFetchPlanUsage, Provider: provider})
		if err != nil {
			rows = append(rows, HubRow{Key: "plan-unavailable", Title: "Plan usage unavailable", Detail: err.Error(), Status: "Blocked"})
		} else if decoded, decodeErr := protocol.DecodeResponsePayloadAny(raw); decodeErr != nil {
			rows = append(rows, HubRow{Key: "plan-invalid", Title: "Plan usage invalid", Detail: decodeErr.Error(), Status: "Error"})
		} else if prepared, prepareErr := hubRows(decoded); prepareErr != nil {
			rows = append(rows, HubRow{Key: "plan-unavailable", Title: "Plan usage unavailable", Detail: prepareErr.Error(), Status: "Blocked"})
		} else {
			for i := range prepared {
				prepared[i].Key = "plan:" + prepared[i].Key
			}
			rows = append(rows, prepared...)
		}
	}
	c.post(func() {
		if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
			c.Model.CommitHubWork(HubUsage, work, rows, "", nil)
		}
	})
}

func (c *Controller) loadProviderHub(ctx context.Context, gen uint64, transport Transport, work uint64, selected string) {
	providers := []string{"codex", "claude", "amp", "cursor", "deepSeek", "fx", "grok", "kimi", "openCode", "ohMyPi", "pi"}
	if selected != "" {
		for i, provider := range providers {
			if provider == selected {
				providers[0], providers[i] = providers[i], providers[0]
				break
			}
		}
	}
	rows := make([]HubRow, 0, len(providers)*2)
	successes := 0
	for _, provider := range providers {
		raw, err := transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.ProbeProviderCommand{Type: protocol.CommandProbeProvider, Provider: provider, DiscoverModels: true, ProbeVersion: true})
		if err != nil {
			rows = append(rows, HubRow{Key: provider, Title: provider, Detail: err.Error(), Status: "Unavailable"})
			continue
		}
		decoded, err := protocol.DecodeResponsePayloadAny(raw)
		if err != nil {
			rows = append(rows, HubRow{Key: provider, Title: provider, Detail: err.Error(), Status: "Invalid response"})
			continue
		}
		payload, ok := decoded.(*protocol.ProviderProbePayload)
		if !ok {
			rows = append(rows, HubRow{Key: provider, Title: provider, Detail: fmt.Sprintf("unexpected response %T", decoded), Status: "Unavailable"})
			continue
		}
		prepared, err := providerRows(payload)
		if err != nil {
			rows = append(rows, HubRow{Key: provider, Title: provider, Detail: err.Error(), Status: "Invalid response"})
			continue
		}
		successes++
		rows = append(rows, prepared...)
	}
	unavailable := ""
	if successes == 0 {
		rows = append([]HubRow{{Key: "provider-probe-blocked", Title: "Provider probing unavailable", Detail: "This daemon did not accept provider readiness/model probes. See each provider reason below.", Status: "Blocked"}}, rows...)
	}
	c.post(func() {
		if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
			c.Model.CommitHubWork(HubProviders, work, rows, unavailable, nil)
		}
	})
}

func (c *Controller) commitHubUnavailable(gen uint64, transport Transport, kind HubKind, work uint64, reason string) {
	c.post(func() {
		if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
			c.Model.CommitHubWork(kind, work, nil, reason, nil)
		}
	})
}
func (c *Controller) commitHubError(gen uint64, transport Transport, kind HubKind, work uint64, err error) {
	c.post(func() {
		if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
			c.Model.CommitHubWork(kind, work, nil, "", err)
		}
	})
}

func hubRows(value any) ([]HubRow, error) {
	switch payload := value.(type) {
	case *protocol.SettingsPayload:
		rows, err := settingsRows(payload.Settings)
		return append([]HubRow{{Key: "settings-write", Title: "Settings editing", Detail: "This panel is read-only; changes require the daemon updateSettings capability and full unknown-field round-tripping.", Status: "Read-only"}}, rows...), err
	case *protocol.SkillsCatalogPayload:
		rows, err := skillRows(payload.Catalog)
		return append([]HubRow{{Key: "skills-write", Title: "Skill management", Detail: "Enable, disable, and trash actions are not exposed by this read-only panel.", Status: "Read-only"}}, rows...), err
	case *protocol.UsageHistoryPayload:
		return usageRows(payload.History)
	case *protocol.PlanUsagePayload:
		if len(payload.Usage) == 0 || string(bytes.TrimSpace(payload.Usage)) == "null" {
			return nil, errors.New("plan usage is unavailable")
		}
		var usage protocol.PlanUsageValue
		if err := json.Unmarshal(payload.Usage, &usage); err != nil {
			return nil, err
		}
		rows := []HubRow{}
		if usage.PlanLabel != nil {
			rows = append(rows, HubRow{Key: "plan-label", Title: "Plan", Detail: *usage.PlanLabel})
		}
		for i, window := range usage.Windows {
			detail := fmt.Sprintf("%.1f%% used", window.Percent)
			if window.ResetsAt != nil {
				detail += fmt.Sprintf(" · resets %d", *window.ResetsAt)
			}
			rows = append(rows, HubRow{Key: fmt.Sprintf("window:%d", i), Title: window.Label, Detail: detail, Status: "Plan usage"})
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unexpected hub response %T", value)
	}
}

func providerRows(payload *protocol.ProviderProbePayload) ([]HubRow, error) {
	var probe struct {
		Provider  string `json:"provider"`
		Installed bool   `json:"installed"`
		Models    []struct {
			ID, Name  string
			IsDefault bool `json:"is_default"`
		} `json:"models"`
	}
	if err := json.Unmarshal(payload.Probe, &probe); err != nil {
		return nil, err
	}
	status := "Not installed"
	if probe.Installed {
		status = "Ready"
	}
	detail := "No CLI version reported"
	if payload.Version != nil {
		detail = "CLI " + *payload.Version
	}
	rows := []HubRow{{Key: probe.Provider, Title: probe.Provider, Detail: detail, Status: status}}
	for _, model := range probe.Models {
		label := model.Name
		if label == "" {
			label = model.ID
		}
		state := "Model"
		if model.IsDefault {
			state = "Default model"
		}
		rows = append(rows, HubRow{Key: probe.Provider + ":model:" + model.ID, Title: label, Detail: model.ID, Status: state})
	}
	return rows, nil
}

func skillRows(raw json.RawMessage) ([]HubRow, error) {
	var catalog struct {
		Skills []struct {
			Name, Description, Scope    string
			Enabled                     bool
			SupportingFiles, TotalBytes uint64
		} `json:"skills"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return nil, err
	}
	rows := make([]HubRow, 0, len(catalog.Skills))
	for i, skill := range catalog.Skills {
		status := "Disabled"
		if skill.Enabled {
			status = "Enabled"
		}
		rows = append(rows, HubRow{Key: fmt.Sprintf("skill:%s:%d", skill.Name, i), Title: skill.Name, Detail: fmt.Sprintf("%s · %s · %d supporting files · %d bytes", skill.Scope, skill.Description, skill.SupportingFiles, skill.TotalBytes), Status: status})
	}
	return rows, nil
}
func usageRows(raw json.RawMessage) ([]HubRow, error) {
	var usage struct {
		SinceDay, UntilDay string
		TotalTokens        uint64  `json:"totalTokens"`
		CostUSD            float64 `json:"costUsd"`
		Records, Sessions  uint64
		Pricing            string
		Errors             []string
	}
	if err := json.Unmarshal(raw, &usage); err != nil {
		return nil, err
	}
	rows := []HubRow{{Key: "period", Title: "Period", Detail: usage.SinceDay + " – " + usage.UntilDay}, {Key: "tokens", Title: "Tokens", Detail: fmt.Sprintf("%d", usage.TotalTokens)}, {Key: "cost", Title: "Estimated cost", Detail: fmt.Sprintf("$%.2f · pricing %s", usage.CostUSD, usage.Pricing)}, {Key: "activity", Title: "Activity", Detail: fmt.Sprintf("%d records across %d sessions", usage.Records, usage.Sessions)}}
	for i, message := range usage.Errors {
		rows = append(rows, HubRow{Key: fmt.Sprintf("error:%d", i), Title: "Scan warning", Detail: message, Status: "Warning"})
	}
	return rows, nil
}

func settingsRows(raw json.RawMessage) ([]HubRow, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	rows := make([]HubRow, 0, 5)
	if value, ok := object["computer_use_enabled"]; ok {
		var enabled bool
		if err := json.Unmarshal(value, &enabled); err != nil {
			return nil, fmt.Errorf("computer_use_enabled: %w", err)
		}
		rows = append(rows, HubRow{Key: "computer-use", Title: "Computer use", Detail: map[bool]string{true: "Enabled", false: "Disabled"}[enabled]})
	}
	if value, ok := object["computer_use_allowed_apps"]; ok {
		var apps []json.RawMessage
		if err := json.Unmarshal(value, &apps); err != nil {
			return nil, fmt.Errorf("computer_use_allowed_apps: %w", err)
		}
		rows = append(rows, HubRow{Key: "allowed-apps", Title: "Allowed computer-use apps", Detail: fmt.Sprintf("%d configured", len(apps))})
	}
	if value, ok := object["disabled_providers"]; ok {
		var providers []string
		if err := json.Unmarshal(value, &providers); err != nil {
			return nil, fmt.Errorf("disabled_providers: %w", err)
		}
		rows = append(rows, HubRow{Key: "disabled-providers", Title: "Disabled providers", Detail: fmt.Sprintf("%d disabled", len(providers))})
	}
	if value, ok := object["provider_binary_overrides"]; ok {
		var overrides map[string]json.RawMessage
		if err := json.Unmarshal(value, &overrides); err != nil {
			return nil, fmt.Errorf("provider_binary_overrides: %w", err)
		}
		rows = append(rows, HubRow{Key: "provider-overrides", Title: "Provider binary overrides", Detail: fmt.Sprintf("%d configured (paths hidden)", len(overrides))})
	}
	known := map[string]bool{"computer_use_enabled": true, "computer_use_allowed_apps": true, "disabled_providers": true, "provider_binary_overrides": true}
	unknown := 0
	for key := range object {
		if !known[key] {
			unknown++
		}
	}
	if unknown > 0 {
		rows = append(rows, HubRow{Key: "other-settings", Title: "Other settings", Detail: fmt.Sprintf("%d preserved setting fields hidden from this read-only view", unknown)})
	}
	return rows, nil
}

func (c *Controller) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.isClosed.Store(true)
		close(c.closed)
		c.generation++
		c.transport = nil
		if c.taskCancel != nil {
			c.taskCancel()
			c.taskCancel = nil
		}
		for id, stop := range c.subCancels {
			stop()
			delete(c.subCancels, id)
		}
		c.runtimes = make(map[string]protocol.UUID)
		c.startOptions = make(map[string]protocol.WireDriverStartOptions)
		c.runtimeFlights = make(map[string]*runtimeFlight)
		c.pendingBySession = make(map[string]PendingInteraction)
		terminals := make([]*terminalui.Pane, 0, len(c.terminalPanes))
		for pane := range c.terminalPanes {
			terminals = append(terminals, pane)
		}
		c.terminalPanes = make(map[*terminalui.Pane]struct{})
		c.mu.Unlock()
		for _, pane := range terminals {
			_ = pane.Close()
		}
	})
}

func (c *Controller) current() (Transport, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.transport, c.generation
}

func (c *Controller) currentGeneration(gen uint64, transport Transport) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation == gen && c.transport == transport
}

func (c *Controller) runCommands() {
	for {
		select {
		case <-c.closed:
			// Controller Close is an explicit presentation invalidation boundary:
			// queued native commits must not outlive the window. Normal transport
			// and subscription closure drains staged provider bytes above.
			return
		case command := <-c.queue:
			if c.isClosed.Load() {
				continue
			}
			transport, gen := c.current()
			if c.isClosed.Load() {
				continue
			}
			if transport == nil {
				c.complete(command, SessionFailed, "Disconnected", errors.New("daemon is disconnected"), gen, transport)
				continue
			}
			go c.execute(gen, transport, command)
		}
	}
}

func (c *Controller) loadSummaries(gen uint64, transport Transport) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandLoadTaskState)})
	if err != nil {
		c.connectionError(gen, transport, err)
		return
	}
	decoded, err := protocol.DecodeResponsePayloadAny(raw)
	if err != nil {
		c.connectionError(gen, transport, err)
		return
	}
	state, ok := decoded.(*protocol.TaskStatePayload)
	if !ok {
		c.connectionError(gen, transport, fmt.Errorf("load task state returned %T", decoded))
		return
	}
	projects, sessions, err := summarizeTaskState(*state)
	if err != nil {
		c.connectionError(gen, transport, err)
		return
	}
	if !c.rememberStartOptions(gen, transport, sessions) {
		return
	}
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		c.Model.ReplaceSummaries(projects, sessions)
		if selected := c.Model.CurrentSession(); selected != nil {
			if selected.Hydrated {
				go c.attachForReplay(gen, transport, selected.ID)
			} else if !selected.Hydrating {
				selected.Hydrating = true
				selected.Activity = "Loading conversation…"
				go c.hydrate(gen, transport, selected.ID)
			}
		}
	})
	// Task-state notifications are hints; reload the complete snapshot so a
	// dropped/coalesced revision cannot produce partial hierarchy state.
	updates, stop := transport.SubscribeTaskState()
	c.mu.Lock()
	if c.generation != gen || c.transport != transport {
		c.mu.Unlock()
		stop()
		return
	}
	if c.taskCancel != nil {
		c.taskCancel()
	}
	c.taskCancel = stop
	c.mu.Unlock()
	go func() {
		for {
			select {
			case _, ok := <-updates:
				if !ok || !c.currentGeneration(gen, transport) {
					return
				}
				go c.loadSummariesOnce(gen, transport)
			case <-transport.Done():
				c.connectionError(gen, transport, transport.Err())
				return
			case <-c.closed:
				return
			}
		}
	}()
}

func (c *Controller) loadSummariesOnce(gen uint64, transport Transport) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := transport.Request(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandLoadTaskState)})
	if err != nil {
		return
	}
	decoded, err := protocol.DecodeResponsePayloadAny(raw)
	state, ok := decoded.(*protocol.TaskStatePayload)
	if err != nil || !ok {
		return
	}
	projects, sessions, err := summarizeTaskState(*state)
	if err != nil {
		return
	}
	if !c.rememberStartOptions(gen, transport, sessions) {
		return
	}
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			c.Model.ReplaceSummaries(projects, sessions)
		}
	})
}

func (c *Controller) hydrate(gen uint64, transport Transport, id string) {
	sessionID, err := protocol.ParseUUID(id)
	if err != nil {
		c.hydrationError(gen, transport, id, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := transport.Request(ctx, sessionID, protocol.NilUUID, protocol.HydrateSessionCommand{Type: protocol.CommandHydrateSession, SessionID: sessionID})
	if err != nil {
		c.hydrationError(gen, transport, id, err)
		return
	}
	decoded, err := protocol.DecodeResponsePayloadAny(raw)
	payload, ok := decoded.(*protocol.SessionPayload)
	if err != nil || !ok {
		if err == nil {
			err = fmt.Errorf("hydrate returned %T", decoded)
		}
		c.hydrationError(gen, transport, id, err)
		return
	}
	detail, err := decodeSession(payload.Session)
	if err != nil {
		c.hydrationError(gen, transport, id, err)
		return
	}
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		if session := c.Model.SessionByID(id); session != nil {
			session.Transcript = detail.Transcript
			session.CanSteer = detail.CanSteer
			session.Hydrated, session.Hydrating = true, false
			session.Activity = "Ready"
		}
	})
	// A persisted cursor is replay metadata only. Liveness is established by
	// attach (and start fallback) on the current transport generation.
	if c.currentGeneration(gen, transport) && !c.isClosed.Load() {
		go c.attachForReplay(gen, transport, id)
	}
}

func (c *Controller) execute(gen uint64, transport Transport, command Command) {
	sid, err := protocol.ParseUUID(command.SessionID)
	if err != nil {
		c.complete(command, SessionFailed, "Invalid session", err, gen, transport)
		return
	}
	runtimeID, err := c.ensureRuntime(gen, transport, command.SessionID, sid)
	if err != nil {
		c.complete(command, SessionFailed, "Could not attach session", err, gen, transport)
		return
	}
	if !c.currentGeneration(gen, transport) {
		return
	}
	var wire any
	switch command.Kind {
	case CommandSend, CommandRetry:
		text := command.Text
		wire = protocol.Prompt(text, nil, nil)
	case CommandSteer:
		wire = protocol.SteerCommand{Type: protocol.CommandSteer, Prompt: command.Text}
	case CommandStop:
		wire = protocol.Command{Type: string(protocol.CommandCancel)}
	case CommandApprove, CommandReject:
		pending := c.pending(command.SessionID)
		if command.RequestID == "" || command.RequestID != pending.RequestID {
			c.cancelSupersededInteraction(command, gen, transport)
			return
		}
		if pending.Kind == InteractionQuestion {
			answers := make([]protocol.UserInputAnswer, 0, len(command.Answers))
			for _, answer := range command.Answers {
				answers = append(answers, protocol.UserInputAnswer{QuestionID: answer.QuestionID, Answers: append([]string(nil), answer.Answers...)})
			}
			if len(answers) == 0 && pending.QuestionID != "" && strings.TrimSpace(command.Text) != "" {
				answers = append(answers, protocol.UserInputAnswer{QuestionID: pending.QuestionID, Answers: []string{strings.TrimSpace(command.Text)}})
			}
			wire = protocol.RespondUserInputCommand{Type: protocol.CommandRespondUserInput, RequestID: pending.RequestID, Answers: answers}
		} else {
			option := command.OptionID
			if option == "" {
				option = pending.ApproveOption
				if command.Kind == CommandReject {
					option = pending.RejectOption
				}
			}
			if option == "" {
				err = errors.New("provider did not supply an approval option identifier")
				break
			}
			matched := false
			for _, candidate := range pending.Options {
				if candidate.ID == option && candidate.Allow == (command.Kind == CommandApprove) {
					matched = true
					break
				}
			}
			if !matched {
				c.cancelSupersededInteraction(command, gen, transport)
				return
			}
			wire = protocol.RespondCommand{Type: protocol.CommandRespond, RequestID: pending.RequestID, OptionID: option}
		}
	default:
		err = fmt.Errorf("unsupported presentation command %d", command.Kind)
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err = transport.Request(ctx, sid, runtimeID, wire)
		cancel()
	}
	status, activity := SessionRunning, "Working"
	if command.Kind == CommandStop {
		status, activity = SessionPaused, "Stopped"
	}
	if command.Kind == CommandSteer {
		status, activity = SessionRunning, "Follow-up accepted"
	}
	c.complete(command, status, activity, err, gen, transport)
}

// cancelSupersededInteraction releases only the old duplicate-activation gate.
// A newer provider interaction owns the visible status and must remain
// actionable; treating this local race as a provider failure would hide it.
func (c *Controller) cancelSupersededInteraction(command Command, gen uint64, transport Transport) {
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			c.Model.CancelCommand(command)
		}
	})
}

func (c *Controller) ensureRuntime(gen uint64, transport Transport, id string, sid protocol.UUID) (protocol.UUID, error) {
	c.mu.Lock()
	if c.generation != gen || c.transport != transport || c.isClosed.Load() {
		c.mu.Unlock()
		return "", errors.New("runtime request belongs to a stale transport generation")
	}
	existing, ok := c.runtimes[id]
	if ok && existing.Valid() {
		c.mu.Unlock()
		return existing, nil
	}
	if flight := c.runtimeFlights[id]; flight != nil {
		c.mu.Unlock()
		select {
		case <-flight.done:
			return flight.runtime, flight.err
		case <-c.closed:
			return "", errors.New("controller is closed")
		}
	}
	flight := &runtimeFlight{done: make(chan struct{})}
	c.runtimeFlights[id] = flight
	c.mu.Unlock()
	runtimeID, err := c.establishRuntime(gen, transport, id, sid)
	c.mu.Lock()
	if c.runtimeFlights[id] == flight {
		delete(c.runtimeFlights, id)
	}
	flight.runtime, flight.err = runtimeID, err
	close(flight.done)
	c.mu.Unlock()
	return runtimeID, err
}

func (c *Controller) establishRuntime(gen uint64, transport Transport, id string, sid protocol.UUID) (protocol.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := transport.Request(ctx, sid, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandAttachSession)})
	if err != nil {
		return "", err
	}
	decoded, err := protocol.DecodeResponsePayloadAny(raw)
	payload, ok := decoded.(*protocol.SessionRuntimePayload)
	if err != nil || !ok {
		return "", fmt.Errorf("attach session returned %T: %w", decoded, err)
	}
	if payload.RuntimeID == nil {
		return c.startRuntime(gen, transport, id, sid)
	}
	runtimeID := *payload.RuntimeID
	c.mu.Lock()
	if c.generation != gen || c.transport != transport {
		c.mu.Unlock()
		return "", errors.New("attach result belongs to a stale transport generation")
	}
	c.runtimes[id] = runtimeID
	c.mu.Unlock()
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			if s := c.Model.SessionByID(id); s != nil {
				s.RuntimeID, s.CanSteer = string(runtimeID), payload.SupportsSteer
			}
		}
	})
	c.subscribe(gen, transport, id, string(runtimeID))
	return runtimeID, nil
}

func (c *Controller) attachForReplay(gen uint64, transport Transport, id string) {
	sid, err := protocol.ParseUUID(id)
	if err != nil {
		return
	}
	_, _ = c.ensureRuntime(gen, transport, id, sid)
}

func (c *Controller) startRuntime(gen uint64, transport Transport, id string, sid protocol.UUID) (protocol.UUID, error) {
	c.mu.Lock()
	options, ok := c.startOptions[id]
	c.mu.Unlock()
	if !ok || options.Provider == "" || options.CWD == "" {
		return "", errors.New("session is inactive and has no complete provider start options")
	}
	runtimeID, err := randomProtocolUUID()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := transport.Request(ctx, sid, runtimeID, protocol.Start(options))
	if err != nil {
		return "", err
	}
	decoded, err := protocol.DecodeResponsePayloadAny(raw)
	payload, ok := decoded.(*protocol.StartedPayload)
	if err != nil || !ok {
		return "", fmt.Errorf("start returned %T: %w", decoded, err)
	}
	c.mu.Lock()
	if c.generation != gen || c.transport != transport {
		c.mu.Unlock()
		return "", errors.New("start result belongs to a stale transport generation")
	}
	c.runtimes[id] = runtimeID
	c.mu.Unlock()
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			if s := c.Model.SessionByID(id); s != nil {
				s.RuntimeID, s.CanSteer = string(runtimeID), payload.SupportsSteer
			}
		}
	})
	c.subscribe(gen, transport, id, string(runtimeID))
	return runtimeID, nil
}

func randomProtocolUUID() (protocol.UUID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return protocol.ParseUUID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]))
}

func (c *Controller) rememberStartOptions(gen uint64, transport Transport, sessions []Session) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != gen || c.transport != transport {
		return false
	}
	for i := range sessions {
		c.startOptions[sessions[i].ID] = sessions[i].StartOptions
	}
	return true
}

func (c *Controller) subscribe(gen uint64, transport Transport, session, runtime string) {
	sid, sidErr := protocol.ParseUUID(session)
	rid, ridErr := protocol.ParseUUID(runtime)
	if sidErr != nil || ridErr != nil || !c.currentGeneration(gen, transport) {
		return
	}
	events, stop := transport.Subscribe(sid, rid)
	c.mu.Lock()
	if c.generation != gen || c.transport != transport {
		c.mu.Unlock()
		stop()
		return
	}
	if old := c.subCancels[session]; old != nil {
		old()
	}
	c.subCancels[session] = stop
	c.mu.Unlock()
	go c.consumeEvents(gen, transport, session, runtime, events)
}

func (c *Controller) consumeEvents(gen uint64, transport Transport, session, runtime string, events <-chan protocol.SequencedEvent) {
	stager := NewStreamStager(time.Now())
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: StreamApproval}); ready {
					c.applyBatch(gen, transport, session, runtime, 0, batch)
				}
				return
			}
			kind, staged, known := classifyEvent(protocol.EventKind(event.Event.Kind))
			if !known {
				if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: StreamApproval}); ready {
					c.applyBatch(gen, transport, session, runtime, event.Sequence, batch)
				}
				c.applyDecodeFailure(gen, transport, session, event, fmt.Errorf("unknown provider event %q", event.Event.Kind))
				continue
			}
			if staged {
				value, err := protocol.DecodeEventPayloadAny(event.Event)
				if err != nil {
					if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: StreamApproval}); ready {
						c.applyBatch(gen, transport, session, runtime, event.Sequence, batch)
					}
					c.applyDecodeFailure(gen, transport, session, event, err)
					continue
				}
				text, _ := value.(*string)
				if text == nil {
					if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: StreamApproval}); ready {
						c.applyBatch(gen, transport, session, runtime, event.Sequence, batch)
					}
					c.applyDecodeFailure(gen, transport, session, event, fmt.Errorf("%s payload is not text", event.Event.Kind))
					continue
				}
				if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: kind, Data: *text}); ready {
					c.applyBatch(gen, transport, session, runtime, event.Sequence, batch)
				}
			} else {
				if batch, ready := stager.Push(time.Now(), StreamDelta{Kind: kind}); ready {
					c.applyBatch(gen, transport, session, runtime, event.Sequence, batch)
				}
				c.applyEvent(gen, transport, session, event)
			}
		case now := <-ticker.C:
			if batch, ready := stager.DrainDue(now); ready {
				c.applyBatch(gen, transport, session, runtime, 0, batch)
			}
		case <-c.closed:
			return
		}
	}
}

// classifyEvent is exhaustive for protocol v7. Every non-delta event takes an
// immediate path after forcing staged bytes ahead of it to drain.
func classifyEvent(kind protocol.EventKind) (StreamDeltaKind, bool, bool) {
	switch kind {
	case protocol.EventTextDelta:
		return StreamText, true, true
	case protocol.EventReasoningDelta:
		return StreamReasoning, true, true
	case protocol.EventPermission, protocol.EventUserInputRequested:
		return StreamApproval, false, true
	case protocol.EventTurnFinished, protocol.EventError, protocol.EventProcessExited, protocol.EventTerminalError:
		return StreamCancellation, false, true
	case protocol.EventConnected, protocol.EventAgentPresetSelected, protocol.EventAutoTitleUpdated,
		protocol.EventAvailableCommands, protocol.EventTurnStarted, protocol.EventTurnParked,
		protocol.EventActivity, protocol.EventRichActivity, protocol.EventBackgroundWork,
		protocol.EventComputerUseUpdated, protocol.EventPromptSubmitted, protocol.EventSteerAccepted,
		protocol.EventSteerRejected, protocol.EventUsageUpdated, protocol.EventPlanUsageUpdated,
		protocol.EventGoalUpdated, protocol.EventTerminalOutput, protocol.EventTerminalExited:
		return StreamApproval, false, true
	default:
		return 0, false, false
	}
}

func (c *Controller) applyDecodeFailure(gen uint64, transport Transport, session string, event protocol.SequencedEvent, err error) {
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		if s := c.Model.SessionByID(session); s != nil {
			s.Failure = fmt.Sprintf("event %s could not be decoded: %v", event.Event.Kind, err)
			s.Transcript = append(s.Transcript, TranscriptItem{ID: fmt.Sprintf("event-error:%s:%d", event.Epoch, event.Sequence), Role: RoleNotice, Title: "Provider event error", Body: s.Failure})
		}
	})
}

func (c *Controller) applyBatch(gen uint64, transport Transport, session, runtime string, sequence uint64, batch StreamBatch) {
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		s := c.Model.SessionByID(session)
		if s == nil {
			return
		}
		for i, delta := range batch.Deltas {
			if delta.Kind != StreamText && delta.Kind != StreamReasoning {
				continue
			}
			role, title := RoleAssistant, "Response"
			if delta.Kind == StreamReasoning {
				role, title = RoleReasoning, "Reasoning"
			}
			if len(s.Transcript) > 0 && s.Transcript[len(s.Transcript)-1].Streaming && s.Transcript[len(s.Transcript)-1].Role == role {
				s.Transcript[len(s.Transcript)-1].Body += delta.Data
			} else {
				id := fmt.Sprintf("stream:%s:%d:%d", runtime, len(s.Transcript), i)
				s.Transcript = append(s.Transcript, TranscriptItem{ID: id, Role: role, Title: title, Body: delta.Data, Streaming: true})
			}
		}
	})
}

func (c *Controller) applyEvent(gen uint64, transport Transport, session string, event protocol.SequencedEvent) {
	value, err := protocol.DecodeEventPayloadAny(event.Event)
	if err != nil {
		c.applyDecodeFailure(gen, transport, session, event, err)
		return
	}
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		s := c.Model.SessionByID(session)
		if s == nil {
			return
		}
		if s.ProviderEventPayloads == nil {
			s.ProviderEventPayloads = make(map[string]json.RawMessage)
		}
		s.ProviderEventPayloads[event.Event.Kind] = bytes.Clone(event.Event.Payload)
		switch protocol.EventKind(event.Event.Kind) {
		case protocol.EventTurnStarted:
			s.Status, s.Activity = SessionRunning, "Working"
		case protocol.EventConnected:
			s.Activity = "Connected"
		case protocol.EventAgentPresetSelected:
			if p := value.(*string); p != nil {
				s.Activity = "Preset: " + *p
			}
		case protocol.EventAutoTitleUpdated:
			if p := value.(*string); p != nil && *p != "" {
				s.Title = *p
			}
		case protocol.EventAvailableCommands:
			p := value.(*[]protocol.ReportedCommandPayload)
			s.Transcript = append(s.Transcript, eventNotice(event, "Commands available", commandSummary(*p)))
		case protocol.EventTurnParked:
			s.Status, s.Activity = SessionPaused, "Turn parked"
		case protocol.EventTurnFinished:
			finished := value.(*protocol.TurnFinishedPayload)
			if finished.Success {
				s.Status, s.Activity = SessionIdle, "Ready"
			} else {
				s.Status, s.Activity = SessionFailed, "Provider failed"
			}
			for i := range s.Transcript {
				s.Transcript[i].Streaming = false
			}
		case protocol.EventPermission:
			p := value.(*protocol.PermissionPayload)
			s.Status, s.Activity = SessionWaitingApproval, p.Title
			s.Pending = PendingInteraction{Kind: InteractionApproval, RequestID: p.RequestID, Title: p.Title, Detail: p.Detail}
			for _, option := range p.Options {
				s.Pending.Options = append(s.Pending.Options, PendingPermissionOption{ID: option.ID, Label: option.Label, Allow: option.Allow})
				if option.Allow && s.Pending.ApproveOption == "" {
					s.Pending.ApproveOption = option.ID
				}
				if !option.Allow && s.Pending.RejectOption == "" {
					s.Pending.RejectOption = option.ID
				}
			}
			c.mu.Lock()
			c.pendingBySession[session] = s.Pending
			c.mu.Unlock()
		case protocol.EventUserInputRequested:
			p := value.(*protocol.UserInputRequestedPayload)
			if len(p.Questions) > 0 {
				q := p.Questions[0]
				s.Status, s.Activity = SessionWaitingApproval, q.Header
				pending := PendingInteraction{Kind: InteractionQuestion, RequestID: p.RequestID, Title: q.Header, Detail: q.Question, QuestionID: q.ID, Questions: make([]PendingQuestion, 0, len(p.Questions))}
				for _, question := range p.Questions {
					item := PendingQuestion{ID: question.ID, Header: question.Header, Question: question.Question, MultiSelect: question.MultiSelect, OptionDescriptions: make(map[string]string)}
					for _, option := range question.Options {
						item.Options = append(item.Options, option.Label)
						if option.Description != nil {
							item.OptionDescriptions[option.Label] = *option.Description
						}
					}
					pending.Questions = append(pending.Questions, item)
				}
				s.Pending = pending
				c.mu.Lock()
				c.pendingBySession[session] = s.Pending
				c.mu.Unlock()
			}
		case protocol.EventPromptSubmitted:
			p := value.(*protocol.PromptSubmittedPayload)
			s.LastPrompt = p.Message
			s.Transcript = append(s.Transcript, TranscriptItem{ID: "prompt:" + string(p.MessageID), Role: RoleUser, Body: p.Message})
		case protocol.EventActivity, protocol.EventRichActivity:
			title, detail := "Tool activity", ""
			role := RoleTool
			switch p := value.(type) {
			case *protocol.ActivityPayload:
				title = p.Title
				if p.Kind == "reasoning" {
					role = RoleReasoning
				}
				if p.Detail != nil {
					detail = *p.Detail
				}
			case *protocol.RichActivityPayload:
				title = p.Title
				if p.Kind == "reasoning" {
					role = RoleReasoning
				}
				if p.Detail != nil {
					detail = *p.Detail
				}
			}
			s.Transcript = append(s.Transcript, TranscriptItem{ID: fmt.Sprintf("event:%s:%d", event.Epoch, event.Sequence), Role: role, Title: title, Body: detail})
		case protocol.EventBackgroundWork:
			s.Transcript = append(s.Transcript, eventNotice(event, "Background work updated", "Provider background-work state changed. Open Background work for prepared details."))
		case protocol.EventComputerUseUpdated:
			p := value.(*protocol.ComputerUsePayload)
			s.Activity = "Computer use: " + p.Phase
		case protocol.EventSteerAccepted:
			s.Activity = "Follow-up accepted"
		case protocol.EventSteerRejected:
			p := value.(*protocol.SteerRejectedPayload)
			s.Failure, s.Activity = p.Reason, "Follow-up rejected"
		case protocol.EventUsageUpdated:
			p := value.(*protocol.UsageUpdatedPayload)
			detail := "Usage counters changed"
			if p.ContextTokens != nil && p.ContextWindow != nil {
				detail = fmt.Sprintf("%d of %d context tokens", *p.ContextTokens, *p.ContextWindow)
			}
			s.Transcript = append(s.Transcript, eventNotice(event, "Usage updated", detail))
		case protocol.EventPlanUsageUpdated:
			p := value.(*protocol.PlanUsageValue)
			parts := make([]string, 0, len(p.Windows))
			for _, window := range p.Windows {
				parts = append(parts, fmt.Sprintf("%s %.1f%%", window.Label, window.Percent))
			}
			s.Transcript = append(s.Transcript, eventNotice(event, "Plan usage updated", strings.Join(parts, " · ")))
		case protocol.EventGoalUpdated:
			body := "Goal cleared"
			if p, ok := value.(*protocol.ThreadGoalPayload); ok {
				body = p.Objective + " — " + p.Status
			}
			s.Transcript = append(s.Transcript, eventNotice(event, "Goal updated", body))
		case protocol.EventTerminalOutput:
			p := value.(*protocol.TerminalOutputPayload)
			s.Pane.Kind, s.Pane.Title = PaneTerminal, "Terminal"
			appendTerminalOutput(s, p.Data)
		case protocol.EventTerminalExited:
			s.Pane.Kind, s.Pane.Title = PaneTerminal, "Terminal"
			s.Pane.Summary += "\n[terminal exited]"
		case protocol.EventError:
			message := value.(*string)
			s.Status, s.Activity, s.Failure = SessionFailed, "Failed", *message
		case protocol.EventTerminalError:
			message := value.(*string)
			s.Pane.Kind, s.Pane.Title, s.Pane.Err = PaneTerminal, "Terminal", *message
		case protocol.EventProcessExited:
			s.Status, s.Activity = SessionPaused, "Provider exited"
		}
	})
}

func eventNotice(event protocol.SequencedEvent, title, body string) TranscriptItem {
	return TranscriptItem{ID: fmt.Sprintf("event:%s:%d", event.Epoch, event.Sequence), Role: RoleNotice, Title: title, Body: body}
}

func commandSummary(commands []protocol.ReportedCommandPayload) string {
	parts := make([]string, 0, len(commands))
	for _, command := range commands {
		label := command.Name
		if command.Description != "" {
			label += " — " + command.Description
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, "\n")
}

func appendTerminalOutput(session *Session, data []byte) {
	session.TerminalOutput = append(session.TerminalOutput, data...)
	if extra := len(session.TerminalOutput) - maxTerminalPresentationBytes; extra > 0 {
		session.TerminalDroppedBytes += int64(extra)
		session.TerminalOutput = append([]byte(nil), session.TerminalOutput[extra:]...)
	}
	session.Pane.Summary = string(session.TerminalOutput)
	if session.TerminalDroppedBytes > 0 {
		session.Pane.Summary = fmt.Sprintf("[terminal view omitted %d earlier bytes]\n", session.TerminalDroppedBytes) + session.Pane.Summary
	}
}

func (c *Controller) pending(id string) PendingInteraction {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingBySession[id]
}

func (c *Controller) complete(command Command, status SessionStatus, activity string, err error, gen uint64, transport Transport) {
	c.post(func() {
		if !c.currentGeneration(gen, transport) {
			return
		}
		if err != nil {
			status, activity = SessionFailed, err.Error()
		}
		if c.Model.CompleteCommand(command, status, activity) {
			if s := c.Model.SessionByID(command.SessionID); s != nil {
				if err != nil {
					s.Failure = err.Error()
				} else {
					s.Failure = ""
					if command.Kind == CommandSend || command.Kind == CommandRetry {
						s.LastPrompt = command.Text
					}
					if command.Kind == CommandApprove || command.Kind == CommandReject {
						s.Pending = PendingInteraction{}
						c.mu.Lock()
						delete(c.pendingBySession, command.SessionID)
						c.mu.Unlock()
					}
				}
			}
		}
	})
}

func (c *Controller) connectionError(gen uint64, transport Transport, err error) {
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			for i := range c.Model.Sessions {
				c.Model.Sessions[i].Activity = "Disconnected: " + err.Error()
			}
		}
	})
}
func (c *Controller) hydrationError(gen uint64, transport Transport, id string, err error) {
	c.post(func() {
		if c.currentGeneration(gen, transport) {
			if s := c.Model.SessionByID(id); s != nil {
				s.Hydrating = false
				s.Failure = err.Error()
				s.Activity = "Could not load conversation"
			}
		}
	})
}

type decodedSession struct {
	Summary    Session
	RuntimeID  string
	CanSteer   bool
	Transcript []TranscriptItem
}

func summarizeTaskState(state protocol.TaskStatePayload) ([]Project, []Session, error) {
	projects := make([]Project, 0, len(state.Projects))
	projectPaths := make(map[string]string, len(state.Projects))
	for _, raw := range state.Projects {
		var p struct{ ID, Name, Title, Path, Root string }
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, nil, err
		}
		name := p.Name
		if name == "" {
			name = p.Title
		}
		path := p.Path
		if path == "" {
			path = p.Root
		}
		projects = append(projects, Project{ID: p.ID, Name: name, PathHint: path})
		projectPaths[p.ID] = path
	}
	sessions := make([]Session, 0, len(state.Sessions))
	for _, raw := range state.Sessions {
		decoded, err := decodeSession(raw)
		if err != nil {
			return nil, nil, err
		}
		if decoded.Summary.StartOptions.CWD == "" {
			decoded.Summary.StartOptions.CWD = projectPaths[decoded.Summary.ProjectID]
		}
		sessions = append(sessions, decoded.Summary)
	}
	return projects, sessions, nil
}

func decodeSession(raw json.RawMessage) (decodedSession, error) {
	if len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return decodedSession{}, errors.New("session payload is null")
	}
	var value struct {
		ID, Title, ProjectID, Status                                    string
		Provider, RuntimeMode, CWD, Binary                              string
		Model, ReasoningEffort, ServiceTier, ContextWindow, AgentPreset *string
		ProviderCursor                                                  json.RawMessage `json:"provider_cursor"`
		UpdatedAt                                                       uint64          `json:"updated_at"`
		RuntimeCursor                                                   *struct {
			RuntimeID protocol.UUID `json:"runtime_id"`
		} `json:"runtime_event_cursor"`
		Messages      []json.RawMessage `json:"messages"`
		Transcript    []json.RawMessage `json:"transcript_blocks"`
		SupportsSteer bool              `json:"supports_steer"`
	}
	// snake-case fields not covered by default matching need explicit aliases.
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return decodedSession{}, err
	}
	_ = json.Unmarshal(raw, &value)
	_ = json.Unmarshal(envelope["id"], &value.ID)
	_ = json.Unmarshal(envelope["title"], &value.Title)
	_ = json.Unmarshal(envelope["project_id"], &value.ProjectID)
	_ = json.Unmarshal(envelope["status"], &value.Status)
	_ = json.Unmarshal(envelope["provider"], &value.Provider)
	_ = json.Unmarshal(envelope["runtime_mode"], &value.RuntimeMode)
	_ = json.Unmarshal(envelope["cwd"], &value.CWD)
	_ = json.Unmarshal(envelope["binary"], &value.Binary)
	_ = json.Unmarshal(envelope["model"], &value.Model)
	_ = json.Unmarshal(envelope["reasoning_effort"], &value.ReasoningEffort)
	_ = json.Unmarshal(envelope["service_tier"], &value.ServiceTier)
	_ = json.Unmarshal(envelope["context_window"], &value.ContextWindow)
	_ = json.Unmarshal(envelope["agent_preset"], &value.AgentPreset)
	if value.CWD == "" {
		_ = json.Unmarshal(envelope["working_directory"], &value.CWD)
	}
	status := SessionIdle
	switch value.Status {
	case "connecting":
		status = SessionPreparing
	case "working":
		status = SessionRunning
	case "waiting":
		status = SessionWaitingApproval
	case "background":
		status = SessionPaused
	case "failed":
		status = SessionFailed
	}
	cursor := value.ProviderCursor
	if len(cursor) == 0 {
		cursor = json.RawMessage("null")
	}
	options := protocol.WireDriverStartOptions{Provider: value.Provider, Binary: value.Binary, CWD: value.CWD, Mode: value.RuntimeMode, Model: value.Model, ReasoningEffort: value.ReasoningEffort, ServiceTier: value.ServiceTier, ContextWindow: value.ContextWindow, AgentPreset: value.AgentPreset, ProviderCursor: cursor}
	result := decodedSession{Summary: Session{ID: value.ID, ProjectID: value.ProjectID, Title: value.Title, Status: status, UpdatedAt: time.Unix(int64(value.UpdatedAt), 0), Activity: status.Label(), StartOptions: options}, CanSteer: value.SupportsSteer}
	if value.RuntimeCursor != nil {
		result.RuntimeID = string(value.RuntimeCursor.RuntimeID)
	}
	result.Summary.CanSteer = value.SupportsSteer
	messages := make([]TranscriptItem, 0, len(value.Messages))
	for i, row := range value.Messages {
		var m struct{ ID, Role, Title, Content, Text string }
		if json.Unmarshal(row, &m) != nil {
			continue
		}
		body := m.Content
		if body == "" {
			body = m.Text
		}
		role := RoleAssistant
		switch strings.ToLower(m.Role) {
		case "user":
			role = RoleUser
		case "reasoning":
			role = RoleReasoning
		case "tool":
			role = RoleTool
		case "notice", "system":
			role = RoleNotice
		}
		id := m.ID
		if id == "" {
			id = fmt.Sprintf("history:%d", i)
		}
		messages = append(messages, TranscriptItem{ID: id, Role: role, Title: m.Title, Body: body})
	}
	blocks := make(map[int][]TranscriptItem)
	for i, row := range value.Transcript {
		var position struct {
			After uint64 `json:"after_message"`
		}
		if err := json.Unmarshal(row, &position); err != nil {
			return decodedSession{}, fmt.Errorf("transcript_blocks[%d]: %w", i, err)
		}
		items, err := decodeTranscriptBlock(row, i)
		if err != nil {
			return decodedSession{}, fmt.Errorf("transcript_blocks[%d]: %w", i, err)
		}
		after := int(position.After)
		if after > len(messages) {
			after = len(messages)
		}
		blocks[after] = append(blocks[after], items...)
	}
	result.Transcript = append(result.Transcript, blocks[0]...)
	for i, message := range messages {
		result.Transcript = append(result.Transcript, message)
		result.Transcript = append(result.Transcript, blocks[i+1]...)
	}
	return result, nil
}

func decodeTranscriptBlock(raw json.RawMessage, index int) ([]TranscriptItem, error) {
	var block struct {
		After   uint64 `json:"after_message"`
		Content struct {
			Kind string          `json:"kind"`
			Data json.RawMessage `json:"data"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("block:%d:%d", block.After, index)
	switch block.Content.Kind {
	case "reasoning":
		var data struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(block.Content.Data, &data); err != nil {
			return nil, err
		}
		return []TranscriptItem{{ID: prefix, Role: RoleReasoning, Title: "Reasoning", Body: data.Content}}, nil
	case "activities":
		var rows []protocol.RichActivityPayload
		if err := json.Unmarshal(block.Content.Data, &rows); err != nil {
			return nil, err
		}
		items := make([]TranscriptItem, 0, len(rows))
		for i, row := range rows {
			body := ""
			role := RoleTool
			if row.Detail != nil {
				body = *row.Detail
			}
			if row.Output != nil && body == "" {
				body = *row.Output
			}
			if row.Kind == "reasoning" {
				role = RoleReasoning
				var reasoning struct {
					Content string `json:"content"`
				}
				if len(row.Reasoning) != 0 && string(bytes.TrimSpace(row.Reasoning)) != "null" && json.Unmarshal(row.Reasoning, &reasoning) == nil {
					body = reasoning.Content
				}
			}
			items = append(items, TranscriptItem{ID: fmt.Sprintf("%s:%s:%d", prefix, row.ID, i), Role: role, Title: row.Title, Body: body})
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unknown content kind %q", block.Content.Kind)
	}
}
