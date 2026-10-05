// Package presentation owns the desktop's durable-in-memory presentation
// state. It deliberately contains no MyGo types: native views read this state
// and dispatch small commands, while protocol and storage adapters can be
// supplied later without changing view code.
package presentation

import (
	"encoding/json"
	"strings"
	"time"

	codecomponent "github.com/roshankarande/waku/apps/desktop-go/internal/components/code"
	"github.com/roshankarande/waku/apps/desktop-go/internal/components/terminalui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

const (
	DefaultStreamInterval = 120 * time.Millisecond
	DefaultStreamMaxCount = 256
	DefaultStreamMaxBytes = 256 * 1024
)

type Project struct {
	ID       string
	Name     string
	PathHint string
}

type SessionStatus uint8

const (
	SessionIdle SessionStatus = iota
	SessionPreparing
	SessionRunning
	SessionWaitingApproval
	SessionPaused
	SessionFailed
	SessionStopping
	SessionResolvingApproval
	SessionRetrying
)

func (s SessionStatus) Label() string {
	switch s {
	case SessionPreparing:
		return "Preparing"
	case SessionRunning:
		return "Running"
	case SessionWaitingApproval:
		return "Approval needed"
	case SessionPaused:
		return "Paused"
	case SessionFailed:
		return "Failed"
	case SessionStopping:
		return "Stopping"
	case SessionResolvingApproval:
		return "Resolving approval"
	case SessionRetrying:
		return "Retrying"
	default:
		return "Ready"
	}
}

type Session struct {
	ID                    string
	ProjectID             string
	Title                 string
	Activity              string
	Status                SessionStatus
	Unread                bool
	UpdatedAt             time.Time
	Failure               string
	Transcript            []TranscriptItem
	Draft                 string
	SelectedPane          PaneKind
	Pane                  PaneState
	CommandBusy           bool
	CanSteer              bool
	RuntimeID             string
	Hydrated              bool
	Hydrating             bool
	Pending               PendingInteraction
	LastPrompt            string
	StartOptions          protocol.WireDriverStartOptions
	ProviderEventPayloads map[string]json.RawMessage
	TerminalOutput        []byte
	TerminalDroppedBytes  int64
	commandTokens         map[CommandKind]uint64
	commandStateToken     uint64
}

type InteractionKind uint8

const (
	InteractionNone InteractionKind = iota
	InteractionApproval
	InteractionQuestion
)

// PendingInteraction contains only provider-owned identifiers and prepared
// display text. The controller translates it back to protocol commands.
type PendingInteraction struct {
	Kind          InteractionKind
	RequestID     string
	Title         string
	Detail        string
	ApproveOption string
	RejectOption  string
	QuestionID    string
	Questions     []PendingQuestion
	Options       []PendingPermissionOption
}

type PendingPermissionOption struct {
	ID    string
	Label string
	Allow bool
}

type PendingQuestion struct {
	ID                 string
	Header             string
	Question           string
	Options            []string
	OptionDescriptions map[string]string
	MultiSelect        bool
	Answers            []string
	Custom             string
}

type QuestionAnswer struct {
	QuestionID string
	Answers    []string
}

type TranscriptRole uint8

const (
	RoleUser TranscriptRole = iota
	RoleAssistant
	RoleReasoning
	RoleTool
	RoleNotice
)

type TranscriptItem struct {
	ID    string
	Role  TranscriptRole
	Title string
	Body  string
	// DocumentKey identifies an immutable, already prepared document model.
	// Views may render it through their narrow document renderer seam. Body is
	// a bounded selectable fallback, not an invitation to parse in a frame.
	DocumentKey string
	Streaming   bool
}

type PaneKind uint8

const (
	PaneNone PaneKind = iota
	PaneFiles
	PaneReview
	PaneTerminal
	PaneBrowser
	PaneBackground
)

func (p PaneKind) Label() string {
	switch p {
	case PaneFiles:
		return "Files"
	case PaneReview:
		return "Review"
	case PaneTerminal:
		return "Terminal"
	case PaneBrowser:
		return "Browser"
	case PaneBackground:
		return "Background work"
	default:
		return ""
	}
}

type PaneState struct {
	Kind             PaneKind
	Title            string
	Summary          string
	Generation       uint64
	Pending          bool
	Err              string
	Editor           *codecomponent.EditorSession
	ReviewSnapshot   *codecomponent.Snapshot
	Review           *codecomponent.ReviewModel
	ReviewView       *codecomponent.ViewState
	Terminal         *terminalui.Pane
	CapabilityNote   string
	WorkspaceRoot    string
	WorkspaceParent  *string
	WorkspaceEntries []WorkspaceEntry
	SelectedPath     string
	ReviewSource     string
	Git              GitState
}

type WorkspaceEntry struct {
	Path, AbsolutePath, Name string
	IsDir                    bool
	Depth                    uint64
}

type GitState struct {
	Branch                           string
	Additions, Deletions             uint64
	StagedAdditions, StagedDeletions uint64
	HasStaged, HasUnstaged, CanPush  bool
}

type HubKind uint8

const (
	HubNone HubKind = iota
	HubSettings
	HubProviders
	HubSkills
	HubUsage
)

func (k HubKind) Label() string {
	switch k {
	case HubSettings:
		return "Settings"
	case HubProviders:
		return "Providers & models"
	case HubSkills:
		return "Skills"
	case HubUsage:
		return "Usage"
	default:
		return ""
	}
}

type HubRow struct{ Key, Title, Detail, Status string }
type HubState struct {
	Kind             HubKind
	Generation       uint64
	Pending          bool
	Err, Unavailable string
	Rows             []HubRow
	Settings         *protocol.DaemonSettingsValue
}

// Model is mutated only on MyGo's main thread. Async workers return through
// Window.Update and must use BeginPaneWork/CommitPaneWork so stale work from a
// previously selected session or pane cannot overwrite current state.
type Model struct {
	Projects          []Project
	Sessions          []Session
	SelectedProject   string
	SelectedSession   string
	SidebarOpen       bool
	RightPaneOpen     bool
	HubOpen           bool
	Hub               HubState
	HierarchyVersion  uint64
	generation        uint64
	commandGeneration uint64
	projectByID       map[string]int
	sessionByID       map[string]int
	sessionsByProject map[string][]int
}

func (m *Model) BeginHubWork(kind HubKind) uint64 {
	m.generation++
	m.HubOpen = true
	m.Hub = HubState{Kind: kind, Generation: m.generation, Pending: true}
	return m.generation
}
func (m *Model) CommitHubWork(kind HubKind, generation uint64, rows []HubRow, unavailable string, err error) bool {
	if !m.HubOpen || m.Hub.Kind != kind || m.Hub.Generation != generation {
		return false
	}
	m.Hub.Pending = false
	m.Hub.Rows = append([]HubRow(nil), rows...)
	m.Hub.Unavailable = unavailable
	if err != nil {
		m.Hub.Err = err.Error()
	}
	return true
}

func NewModel(projects []Project, sessions []Session) *Model {
	m := &Model{Projects: projects, Sessions: sessions, SidebarOpen: true}
	m.Reindex()
	if len(projects) > 0 {
		m.SelectedProject = projects[0].ID
	}
	for i := range sessions {
		if sessions[i].ProjectID == m.SelectedProject {
			m.SelectedSession = sessions[i].ID
			break
		}
	}
	return m
}

// Reindex refreshes lookup tables after callers replace or reorder the public
// project/session slices. It is linear in the combined hierarchy size.
func (m *Model) Reindex() {
	m.projectByID = make(map[string]int, len(m.Projects))
	m.sessionByID = make(map[string]int, len(m.Sessions))
	m.sessionsByProject = make(map[string][]int, len(m.Projects))
	for i := range m.Projects {
		m.projectByID[m.Projects[i].ID] = i
	}
	for i := range m.Sessions {
		s := &m.Sessions[i]
		m.sessionByID[s.ID] = i
		m.sessionsByProject[s.ProjectID] = append(m.sessionsByProject[s.ProjectID], i)
	}
}

// SessionIndices returns session positions in source order for a project.
// The returned slice is read-only and remains valid until Reindex.
func (m *Model) SessionIndices(projectID string) []int { return m.sessionsByProject[projectID] }

func (m *Model) CurrentSession() *Session {
	if i, ok := m.sessionByID[m.SelectedSession]; ok && i >= 0 && i < len(m.Sessions) && m.Sessions[i].ID == m.SelectedSession {
		return &m.Sessions[i]
	}
	return nil
}

func (m *Model) SessionByID(id string) *Session { return m.session(id) }

// ReplaceSummaries merges daemon summaries without discarding per-session UI
// state such as drafts, pane caches, transcript hydration, or in-flight gates.
func (m *Model) ReplaceSummaries(projects []Project, summaries []Session) {
	previous := make(map[string]Session, len(m.Sessions))
	oldRoots := make(map[string]string, len(m.Projects))
	newRoots := make(map[string]string, len(projects))
	for i := range m.Projects {
		oldRoots[m.Projects[i].ID] = m.Projects[i].PathHint
	}
	for i := range projects {
		newRoots[projects[i].ID] = projects[i].PathHint
	}
	for i := range m.Sessions {
		previous[m.Sessions[i].ID] = m.Sessions[i]
	}
	for i := range summaries {
		if old, ok := previous[summaries[i].ID]; ok {
			delete(previous, summaries[i].ID)
			summaries[i].Draft = old.Draft
			if old.ProjectID == summaries[i].ProjectID && oldRoots[old.ProjectID] == newRoots[summaries[i].ProjectID] {
				summaries[i].SelectedPane = old.SelectedPane
				summaries[i].Pane = old.Pane
			} else {
				closePaneContent(old.Pane)
				summaries[i].SelectedPane = PaneNone
				summaries[i].Pane = PaneState{}
			}
			summaries[i].Transcript = old.Transcript
			summaries[i].Hydrated = old.Hydrated
			summaries[i].Hydrating = old.Hydrating
			summaries[i].RuntimeID = old.RuntimeID
			summaries[i].Pending = old.Pending
			summaries[i].LastPrompt = old.LastPrompt
			summaries[i].ProviderEventPayloads = old.ProviderEventPayloads
			summaries[i].TerminalOutput = old.TerminalOutput
			summaries[i].TerminalDroppedBytes = old.TerminalDroppedBytes
			summaries[i].commandTokens = old.commandTokens
			summaries[i].commandStateToken = old.commandStateToken
			summaries[i].CommandBusy = old.CommandBusy
		}
	}
	for _, dropped := range previous {
		closePaneContent(dropped.Pane)
	}
	selected := m.SelectedSession
	m.Projects, m.Sessions = projects, summaries
	m.HierarchyVersion++
	m.Reindex()
	if !m.SelectSession(selected) {
		m.SelectedSession = ""
		if len(summaries) > 0 {
			m.SelectSession(summaries[0].ID)
		} else if len(projects) > 0 {
			m.SelectedProject = projects[0].ID
		}
	}
}

func (m *Model) SelectSession(id string) bool {
	i, ok := m.sessionByID[id]
	if ok && i >= 0 && i < len(m.Sessions) && m.Sessions[i].ID == id {
		m.SelectedSession = id
		m.SelectedProject = m.Sessions[i].ProjectID
		m.Sessions[i].Unread = false
		m.RightPaneOpen = m.Sessions[i].SelectedPane != PaneNone
		return true
	}
	return false
}

func (m *Model) SelectProject(id string) bool {
	if _, ok := m.projectByID[id]; !ok {
		return false
	}
	m.SelectedProject = id
	if indices := m.sessionsByProject[id]; len(indices) > 0 {
		return m.SelectSession(m.Sessions[indices[0]].ID)
	}
	m.SelectedSession = ""
	return true
}

func (m *Model) SetPane(kind PaneKind) {
	s := m.CurrentSession()
	if s == nil {
		return
	}
	if s.SelectedPane == kind {
		m.RightPaneOpen = !m.RightPaneOpen
		return
	}
	s.SelectedPane = kind
	m.RightPaneOpen = kind != PaneNone
	m.invalidatePane(s)
	s.Pane.Kind, s.Pane.Title = kind, kind.Label()
}

func (m *Model) invalidatePane(session *Session) {
	closePaneContent(session.Pane)
	m.generation++
	session.Pane = PaneState{Generation: m.generation}
}

func closePaneContent(pane PaneState) {
	if pane.Editor != nil {
		go pane.Editor.Close()
	}
	if pane.Terminal != nil {
		go pane.Terminal.Close()
	}
}

type PaneWork struct {
	SessionID  string
	Kind       PaneKind
	Generation uint64
}

func (m *Model) BeginPaneWork(kind PaneKind) PaneWork {
	m.generation++
	if session := m.CurrentSession(); session != nil {
		closePaneContent(session.Pane)
		session.Pane = PaneState{Kind: kind, Title: kind.Label(), Generation: m.generation, Pending: true}
	}
	return PaneWork{SessionID: m.SelectedSession, Kind: kind, Generation: m.generation}
}

func (m *Model) BeginPaneRefresh(sessionID string, kind PaneKind) (PaneWork, bool) {
	session := m.session(sessionID)
	if session == nil || session.Pane.Kind != kind {
		return PaneWork{}, false
	}
	m.generation++
	session.Pane.Generation = m.generation
	session.Pane.Pending = true
	session.Pane.Err = ""
	return PaneWork{SessionID: sessionID, Kind: kind, Generation: m.generation}, true
}

// CommitPaneWork returns false without changing state when work is obsolete.
func (m *Model) CommitPaneWork(work PaneWork, summary string, err error) bool {
	session := m.session(work.SessionID)
	if session == nil || work.Generation != session.Pane.Generation || work.Kind != session.Pane.Kind {
		return false
	}
	session.Pane.Pending = false
	session.Pane.Summary = summary
	if err != nil {
		session.Pane.Err = err.Error()
	} else {
		session.Pane.Err = ""
	}
	return true
}

func (m *Model) CommitPaneSummary(work PaneWork, summary string, git GitState) bool {
	session := m.session(work.SessionID)
	if session == nil || work.Generation != session.Pane.Generation || work.Kind != session.Pane.Kind {
		return false
	}
	session.Pane.Summary = summary
	session.Pane.Git = git
	return true
}

func (m *Model) AppendTranscript(sessionID string, item TranscriptItem) bool {
	if session := m.session(sessionID); session != nil {
		session.Transcript = append(session.Transcript, item)
		return true
	}
	return false
}

// BeginCommand allocates a generation token and gates duplicate activations in
// the command's class. Stop is intentionally independent of steer.
func (m *Model) BeginCommand(sessionID string, kind CommandKind, text string) (Command, bool) {
	s := m.session(sessionID)
	if s == nil || CommandPending(s, kind) {
		return Command{}, false
	}
	if s.commandTokens == nil {
		s.commandTokens = make(map[CommandKind]uint64)
	}
	m.commandGeneration++
	command := Command{Kind: kind, SessionID: sessionID, Text: text, Token: m.commandGeneration}
	for _, member := range commandClass(kind) {
		s.commandTokens[member] = command.Token
	}
	s.commandStateToken = command.Token
	s.CommandBusy = true
	return command, true
}

// CancelCommand rolls back a command which the dispatcher refused to enqueue.
func (m *Model) CancelCommand(command Command) bool {
	s := m.session(command.SessionID)
	if !commandCurrent(s, command) {
		return false
	}
	clearCommand(s, command)
	recomputeCommandStateToken(s)
	return true
}

// CompleteCommand applies an acknowledgement only when its token is current.
// Delayed completions from an older activation are rejected without mutation.
func (m *Model) CompleteCommand(command Command, status SessionStatus, activity string) bool {
	s := m.session(command.SessionID)
	if !commandCurrent(s, command) {
		return false
	}
	clearCommand(s, command)
	if command.Token < s.commandStateToken {
		return false
	}
	s.Status = status
	s.Activity = activity
	return true
}

// AppendStreamTail expands only the identified tail row. Stable IDs let the
// virtual list retain its anchor while its measured height changes.
func (m *Model) AppendStreamTail(sessionID, itemID, delta string) bool {
	if session := m.session(sessionID); session != nil {
		items := session.Transcript
		if len(items) == 0 || items[len(items)-1].ID != itemID || !items[len(items)-1].Streaming {
			return false
		}
		session.Transcript[len(items)-1].Body += delta
		return true
	}
	return false
}

func (m *Model) session(id string) *Session {
	i, ok := m.sessionByID[id]
	if !ok || i < 0 || i >= len(m.Sessions) || m.Sessions[i].ID != id {
		return nil
	}
	return &m.Sessions[i]
}

func CanSend(s *Session) bool {
	if s == nil || strings.TrimSpace(s.Draft) == "" {
		return false
	}
	return !CommandPending(s, CommandSend) && (s.Status == SessionIdle || s.Status == SessionPaused || s.Status == SessionFailed)
}

func CanStop(s *Session) bool {
	return s != nil && !CommandPending(s, CommandStop) && (s.Status == SessionPreparing || s.Status == SessionRunning || s.Status == SessionWaitingApproval || s.Status == SessionResolvingApproval || s.Status == SessionRetrying)
}

func PrimaryAction(s *Session) string {
	if CanStop(s) {
		return "Stop"
	}
	if s != nil && s.Status == SessionStopping {
		return "Stopping…"
	}
	if s != nil && s.Status == SessionFailed {
		return "Retry"
	}
	return "Send"
}

func SubmitAction(s *Session) (CommandKind, bool) {
	if s == nil || strings.TrimSpace(s.Draft) == "" {
		return CommandSend, false
	}
	switch s.Status {
	case SessionIdle, SessionPaused, SessionFailed:
		return CommandSend, !CommandPending(s, CommandSend)
	case SessionRunning:
		if s.CanSteer {
			return CommandSteer, !CommandPending(s, CommandSteer)
		}
		return CommandSend, false
	case SessionPreparing, SessionWaitingApproval:
		return CommandSend, false
	default:
		return CommandSend, false
	}
}

type CommandKind uint8

const (
	CommandSend CommandKind = iota
	CommandStop
	CommandRetry
	CommandApprove
	CommandReject
	CommandSteer
)

type Command struct {
	Kind      CommandKind
	SessionID string
	RequestID string
	Text      string
	Token     uint64
	Answers   []QuestionAnswer
	OptionID  string
}

func CommandPending(s *Session, kind CommandKind) bool {
	if s == nil {
		return false
	}
	if len(s.commandTokens) == 0 {
		return s.CommandBusy
	}
	_, ok := s.commandTokens[kind]
	return ok
}

func commandClass(kind CommandKind) []CommandKind {
	if kind == CommandApprove || kind == CommandReject {
		return []CommandKind{CommandApprove, CommandReject}
	}
	return []CommandKind{kind}
}

func commandCurrent(s *Session, command Command) bool {
	return s != nil && command.Token != 0 && s.commandTokens[command.Kind] == command.Token
}

func clearCommand(s *Session, command Command) {
	for _, member := range commandClass(command.Kind) {
		if s.commandTokens[member] == command.Token {
			delete(s.commandTokens, member)
		}
	}
	s.CommandBusy = len(s.commandTokens) != 0
}

func recomputeCommandStateToken(s *Session) {
	s.commandStateToken = 0
	for _, token := range s.commandTokens {
		if token > s.commandStateToken {
			s.commandStateToken = token
		}
	}
}

// Dispatcher must enqueue and return immediately; views call it on the UI
// thread. Network, process, filesystem and parsing work belong behind it.
type Dispatcher interface {
	Dispatch(Command) bool
}

type StreamDeltaKind uint8

const (
	StreamText StreamDeltaKind = iota
	StreamReasoning
	StreamCancellation
	StreamApproval
)

type StreamDelta struct {
	Kind StreamDeltaKind
	Data string
}

type StreamBatch struct {
	Deltas []StreamDelta
	Bytes  int
}

// StreamStager coalesces ordinary text/reasoning deltas to the established
// ~120ms cadence. Urgent cancellation and approval events bypass the queue.
// Count and byte limits force a bounded early drain under hostile producers.
type StreamStager struct {
	Interval time.Duration
	MaxCount int
	MaxBytes int
	last     time.Time
	pending  []StreamDelta
	bytes    int
}

func NewStreamStager(now time.Time) *StreamStager {
	return &StreamStager{Interval: DefaultStreamInterval, MaxCount: DefaultStreamMaxCount, MaxBytes: DefaultStreamMaxBytes, last: now}
}

func (s *StreamStager) Push(now time.Time, delta StreamDelta) (StreamBatch, bool) {
	if delta.Kind == StreamCancellation || delta.Kind == StreamApproval {
		// Preserve provider order: staged stream data is published immediately
		// before the urgent transition which caused this drain. Urgent payloads
		// are never retained by the stager, so their authoritative content can be
		// handed through exactly without weakening retained-data bounds.
		batch, _ := s.drain(now)
		batch.Deltas = append(batch.Deltas, delta)
		batch.Bytes += len(delta.Data)
		return batch, true
	}
	if s.Interval <= 0 {
		s.Interval = DefaultStreamInterval
	}
	if s.MaxCount <= 0 {
		s.MaxCount = DefaultStreamMaxCount
	}
	if s.MaxBytes <= 0 {
		s.MaxBytes = DefaultStreamMaxBytes
	}
	// An oversized provider delta is delivered immediately and exactly; it is
	// never retained. MaxBytes bounds staged ownership, not provider payloads.
	if len(delta.Data) > s.MaxBytes {
		batch, _ := s.drain(now)
		batch.Deltas = append(batch.Deltas, delta)
		batch.Bytes += len(delta.Data)
		return batch, true
	}
	if len(s.pending) > 0 && (len(s.pending) >= s.MaxCount || s.bytes+len(delta.Data) > s.MaxBytes) {
		batch, _ := s.drain(now)
		s.pending = append(s.pending, delta)
		s.bytes = len(delta.Data)
		return batch, true
	}
	s.pending = append(s.pending, delta)
	s.bytes += len(delta.Data)
	if len(s.pending) >= s.MaxCount || s.bytes >= s.MaxBytes || now.Sub(s.last) >= s.Interval {
		return s.drain(now)
	}
	return StreamBatch{}, false
}

func (s *StreamStager) DrainDue(now time.Time) (StreamBatch, bool) {
	if len(s.pending) == 0 || now.Sub(s.last) < s.Interval {
		return StreamBatch{}, false
	}
	return s.drain(now)
}

func (s *StreamStager) drain(now time.Time) (StreamBatch, bool) {
	batch := StreamBatch{Deltas: append([]StreamDelta(nil), s.pending...), Bytes: s.bytes}
	s.pending = s.pending[:0]
	s.bytes = 0
	s.last = now
	return batch, len(batch.Deltas) > 0
}
