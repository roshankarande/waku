package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type CommandType string

const (
	CommandAttachSession             CommandType = "attachSession"
	CommandStart                     CommandType = "start"
	CommandPrompt                    CommandType = "prompt"
	CommandSteer                     CommandType = "steer"
	CommandCancel                    CommandType = "cancel"
	CommandCancelComputerUse         CommandType = "cancelComputerUse"
	CommandRefreshBackgroundWork     CommandType = "refreshBackgroundWork"
	CommandStopBackgroundWork        CommandType = "stopBackgroundWork"
	CommandRespond                   CommandType = "respond"
	CommandRespondUserInput          CommandType = "respondUserInput"
	CommandGoal                      CommandType = "goal"
	CommandRunComputerTool           CommandType = "runComputerTool"
	CommandRejectComputerTool        CommandType = "rejectComputerTool"
	CommandApplyOptions              CommandType = "applyOptions"
	CommandRollback                  CommandType = "rollback"
	CommandFork                      CommandType = "fork"
	CommandGetSettings               CommandType = "getSettings"
	CommandUpdateSettings            CommandType = "updateSettings"
	CommandProbeProvider             CommandType = "probeProvider"
	CommandFetchPlanUsage            CommandType = "fetchPlanUsage"
	CommandProbeComputerPermissions  CommandType = "probeComputerPermissions"
	CommandLoadUsageHistory          CommandType = "loadUsageHistory"
	CommandLoadSkills                CommandType = "loadSkills"
	CommandSetSkillsEnabled          CommandType = "setSkillsEnabled"
	CommandTrashSkills               CommandType = "trashSkills"
	CommandLoadTaskState             CommandType = "loadTaskState"
	CommandSaveTaskState             CommandType = "saveTaskState"
	CommandRemoveSession             CommandType = "removeSession"
	CommandHydrateSession            CommandType = "hydrateSession"
	CommandSearchSessionMessages     CommandType = "searchSessionMessages"
	CommandListProviderSessions      CommandType = "listProviderSessions"
	CommandLoadProviderSession       CommandType = "loadProviderSession"
	CommandLoadComposerDrafts        CommandType = "loadComposerDrafts"
	CommandSaveComposerDrafts        CommandType = "saveComposerDrafts"
	CommandApplyComposerDraftChanges CommandType = "applyComposerDraftChanges"
	CommandStoreBlob                 CommandType = "storeBlob"
	CommandImportAttachment          CommandType = "importAttachment"
	CommandImportPathAttachment      CommandType = "importPathAttachment"
	CommandReadBlob                  CommandType = "readBlob"
	CommandReadAttachment            CommandType = "readAttachment"
	CommandSweepBlobs                CommandType = "sweepBlobs"
	CommandForkSessionFromResponse   CommandType = "forkSessionFromResponse"
	CommandRewindSessionToMessage    CommandType = "rewindSessionToMessage"
	CommandForkProviderSession       CommandType = "forkProviderSession"
	CommandWorkspace                 CommandType = "workspace"
	CommandOpenTerminal              CommandType = "openTerminal"
	CommandWriteTerminal             CommandType = "writeTerminal"
	CommandResizeTerminal            CommandType = "resizeTerminal"
	CommandCloseTerminal             CommandType = "closeTerminal"
	CommandCloseSession              CommandType = "closeSession"
)

type AckPayload struct {
	Type ResponsePayloadType `json:"type"`
}
type SessionRuntimePayload struct {
	Type          ResponsePayloadType `json:"type"`
	RuntimeID     *UUID               `json:"runtimeId"`
	SupportsSteer bool                `json:"supportsSteer"`
}
type StartedPayload struct {
	Type          ResponsePayloadType `json:"type"`
	SupportsSteer bool                `json:"supportsSteer"`
}
type OptionsAppliedPayload struct {
	Type    ResponsePayloadType `json:"type"`
	Applied bool                `json:"applied"`
}
type CursorPayload struct {
	Type   ResponsePayloadType `json:"type"`
	Cursor json.RawMessage     `json:"cursor"`
}
type SettingsPayload struct {
	Type     ResponsePayloadType `json:"type"`
	Settings json.RawMessage     `json:"settings"`
}
type ProviderProbePayload struct {
	Type    ResponsePayloadType `json:"type"`
	Probe   json.RawMessage     `json:"probe"`
	Version *string             `json:"version"`
}
type PlanUsagePayload struct {
	Type  ResponsePayloadType `json:"type"`
	Usage json.RawMessage     `json:"usage"`
}
type ComputerPermissionsPayload struct {
	Type        ResponsePayloadType `json:"type"`
	Permissions json.RawMessage     `json:"permissions"`
}
type UsageHistoryPayload struct {
	Type    ResponsePayloadType `json:"type"`
	History json.RawMessage     `json:"history"`
}
type SkillsCatalogPayload struct {
	Type    ResponsePayloadType `json:"type"`
	Catalog json.RawMessage     `json:"catalog"`
}
type TaskStatePayload struct {
	Type            ResponsePayloadType `json:"type"`
	Projects        []json.RawMessage   `json:"projects"`
	Sessions        []json.RawMessage   `json:"sessions"`
	DefaultCWD      string              `json:"defaultCwd"`
	ProjectlessRoot *string             `json:"projectlessRoot"`
}
type TaskStateSavedPayload struct {
	Type     ResponsePayloadType `json:"type"`
	Sessions []json.RawMessage   `json:"sessions"`
}
type SessionPayload struct {
	Type    ResponsePayloadType `json:"type"`
	Session json.RawMessage     `json:"session"`
}
type SessionMessageMatchesPayload struct {
	Type    ResponsePayloadType `json:"type"`
	Matches []json.RawMessage   `json:"matches"`
}
type ProviderSessionsPayload struct {
	Type     ResponsePayloadType `json:"type"`
	Sessions []json.RawMessage   `json:"sessions"`
}
type ProviderSessionHistoryPayload struct {
	Type    ResponsePayloadType `json:"type"`
	History json.RawMessage     `json:"history"`
}
type ComposerDraftsPayload struct {
	Type   ResponsePayloadType `json:"type"`
	Drafts json.RawMessage     `json:"drafts"`
}
type BlobStoredPayload struct {
	Type      ResponsePayloadType `json:"type"`
	Reference string              `json:"reference"`
	Path      string              `json:"path"`
}
type AttachmentStoredPayload struct {
	Type       ResponsePayloadType `json:"type"`
	Attachment json.RawMessage     `json:"attachment"`
}
type BlobDataPayload struct {
	Type  ResponsePayloadType `json:"type"`
	Bytes Base64Bytes         `json:"bytes"`
}
type ProviderSessionForkedPayload struct {
	Type   ResponsePayloadType `json:"type"`
	Result json.RawMessage     `json:"result"`
}
type SessionForkedPayload struct {
	Type              ResponsePayloadType `json:"type"`
	Session           json.RawMessage     `json:"session"`
	CheckpointWarning *string             `json:"checkpointWarning"`
}
type SessionRewoundPayload struct {
	Type           ResponsePayloadType `json:"type"`
	Session        json.RawMessage     `json:"session"`
	CleanupWarning *string             `json:"cleanupWarning"`
}
type WorkspacePayload struct {
	Type   ResponsePayloadType `json:"type"`
	Result json.RawMessage     `json:"result"`
}

// DecodeResponsePayloadAny rejects unknown variants and validates all required
// protocol-owned fields before returning a concrete payload type.
func DecodeResponsePayloadAny(raw json.RawMessage) (any, error) {
	var envelope ResponsePayloadEnvelope
	if err := Decode(raw, &envelope); err != nil {
		return nil, err
	}
	kind := ResponsePayloadType(envelope.Type)
	requiredFields := map[ResponsePayloadType][]string{
		ResponseSessionRuntime: {"supportsSteer"}, ResponseStarted: {"supportsSteer"}, ResponseOptionsApplied: {"applied"},
		ResponseSettings: {"settings"}, ResponseProviderProbe: {"probe"}, ResponseComputerPermissions: {"permissions"},
		ResponseUsageHistory: {"history"}, ResponseSkillsCatalog: {"catalog"}, ResponseTaskState: {"projects", "sessions", "defaultCwd"},
		ResponseTaskStateSaved: {"sessions"}, ResponseSessionMessageMatches: {"matches"}, ResponseProviderSessions: {"sessions"},
		ResponseProviderSessionHistory: {"history"}, ResponseComposerDrafts: {"drafts"}, ResponseBlobStored: {"reference", "path"},
		ResponseAttachmentStored: {"attachment"}, ResponseBlobData: {"bytes"}, ResponseProviderSessionForked: {"result"},
		ResponseSessionForked: {"session"}, ResponseSessionRewound: {"session"}, ResponseWorkspace: {"result"},
	}
	if err := requireJSONFields(raw, requiredFields[kind]...); err != nil {
		return nil, fmt.Errorf("%s payload: %w", kind, err)
	}
	decodeInto := func(dst any) (any, error) {
		if err := Decode(raw, dst); err != nil {
			return nil, err
		}
		return dst, nil
	}
	switch kind {
	case ResponseAck:
		var v AckPayload
		return decodeInto(&v)
	case ResponseSessionRuntime:
		var v SessionRuntimePayload
		return decodeInto(&v)
	case ResponseStarted:
		var v StartedPayload
		return decodeInto(&v)
	case ResponseOptionsApplied:
		var v OptionsAppliedPayload
		return decodeInto(&v)
	case ResponseCursor:
		var v CursorPayload
		return decodeInto(&v)
	case ResponseSettings:
		var v SettingsPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateDaemonSettings(v.Settings)
		}
		return value, e
	case ResponseProviderProbe:
		var v ProviderProbePayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderProbe(v.Probe)
		}
		return value, e
	case ResponsePlanUsage:
		var v PlanUsagePayload
		value, e := decodeInto(&v)
		if e == nil && len(v.Usage) != 0 && string(bytes.TrimSpace(v.Usage)) != "null" {
			var usage PlanUsageValue
			e = Decode(v.Usage, &usage)
		}
		return value, e
	case ResponseComputerPermissions:
		var v ComputerPermissionsPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateComputerPermissions(v.Permissions)
		}
		return value, e
	case ResponseUsageHistory:
		var v UsageHistoryPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateUsageHistory(v.History)
		}
		return value, e
	case ResponseSkillsCatalog:
		var v SkillsCatalogPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateSkillsCatalog(v.Catalog)
		}
		return value, e
	case ResponseTaskState:
		var v TaskStatePayload
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Projects, "projects")
		}
		if e == nil {
			for i, project := range v.Projects {
				if e = validateProject(project); e != nil {
					e = fmt.Errorf("projects[%d]: %w", i, e)
					break
				}
			}
		}
		if e == nil {
			e = requireObjectElements(v.Sessions, "sessions")
		}
		if e == nil {
			for i, session := range v.Sessions {
				if e = validateAgentSession(session); e != nil {
					e = fmt.Errorf("sessions[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case ResponseTaskStateSaved:
		var v TaskStateSavedPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Sessions, "sessions")
		}
		if e == nil {
			for i, session := range v.Sessions {
				if e = validateAgentSession(session); e != nil {
					e = fmt.Errorf("sessions[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case ResponseSession:
		var v SessionPayload
		value, e := decodeInto(&v)
		if e == nil && len(v.Session) != 0 && string(bytes.TrimSpace(v.Session)) != "null" {
			e = validateAgentSession(v.Session)
		}
		return value, e
	case ResponseSessionMessageMatches:
		var v SessionMessageMatchesPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Matches, "matches")
		}
		if e == nil {
			for i, match := range v.Matches {
				if e = validateSessionMessageMatch(match); e != nil {
					e = fmt.Errorf("matches[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case ResponseProviderSessions:
		var v ProviderSessionsPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Sessions, "sessions")
		}
		if e == nil {
			for i, session := range v.Sessions {
				if e = validateProviderSessionSummary(session); e != nil {
					e = fmt.Errorf("sessions[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case ResponseProviderSessionHistory:
		var v ProviderSessionHistoryPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderSessionHistory(v.History)
		}
		return value, e
	case ResponseComposerDrafts:
		var v ComposerDraftsPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateComposerDrafts(v.Drafts)
		}
		return value, e
	case ResponseBlobStored:
		var v BlobStoredPayload
		return decodeInto(&v)
	case ResponseAttachmentStored:
		var v AttachmentStoredPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateStoredAttachment(v.Attachment)
		}
		return value, e
	case ResponseBlobData:
		var v BlobDataPayload
		return decodeInto(&v)
	case ResponseProviderSessionForked:
		var v ProviderSessionForkedPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = requireJSONFields(v.Result, "cursor")
			if e == nil {
				var result struct {
					Cursor         json.RawMessage    `json:"cursor"`
					MessageIDs     *map[string]string `json:"messageIds"`
					SourceResumeAt *string            `json:"sourceResumeAt"`
				}
				e = Decode(v.Result, &result)
				if e == nil {
					e = validateProviderCursor(result.Cursor)
				}
				if e == nil {
					var object map[string]json.RawMessage
					e = Decode(v.Result, &object)
					for _, field := range []string{"messageIds"} {
						if raw, present := object[field]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
							e = fmt.Errorf("field %q is null", field)
							break
						}
					}
				}
			}
		}
		return value, e
	case ResponseSessionForked:
		var v SessionForkedPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateAgentSession(v.Session)
		}
		return value, e
	case ResponseSessionRewound:
		var v SessionRewoundPayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateAgentSession(v.Session)
		}
		return value, e
	case ResponseWorkspace:
		var v WorkspacePayload
		value, e := decodeInto(&v)
		if e == nil {
			e = validateWorkspaceResult(v.Result)
		}
		return value, e
	default:
		return nil, fmt.Errorf("unknown response payload type %q", envelope.Type)
	}
}

// RawCommand covers protocol commands whose domain payload belongs to another
// package while retaining a checked tag and exact raw JSON fields.
type RawCommand struct {
	Type   CommandType
	Fields map[string]json.RawMessage
}

func CommandTypeOf(raw json.RawMessage) (CommandType, error) {
	var header struct {
		Type CommandType `json:"type"`
	}
	if err := Decode(raw, &header); err != nil {
		return "", err
	}
	if header.Type == "" {
		return "", fmt.Errorf("command type is empty")
	}
	if !knownCommandTypes[header.Type] {
		return "", fmt.Errorf("unknown command type %q", header.Type)
	}
	return header.Type, nil
}

var knownCommandTypes = map[CommandType]bool{
	CommandAttachSession: true, CommandStart: true, CommandPrompt: true, CommandSteer: true,
	CommandCancel: true, CommandCancelComputerUse: true, CommandRefreshBackgroundWork: true,
	CommandStopBackgroundWork: true, CommandRespond: true, CommandRespondUserInput: true,
	CommandGoal: true, CommandRunComputerTool: true, CommandRejectComputerTool: true,
	CommandApplyOptions: true, CommandRollback: true, CommandFork: true, CommandGetSettings: true,
	CommandUpdateSettings: true, CommandProbeProvider: true, CommandFetchPlanUsage: true,
	CommandProbeComputerPermissions: true, CommandLoadUsageHistory: true, CommandLoadSkills: true,
	CommandSetSkillsEnabled: true, CommandTrashSkills: true, CommandLoadTaskState: true,
	CommandSaveTaskState: true, CommandRemoveSession: true, CommandHydrateSession: true,
	CommandSearchSessionMessages: true, CommandListProviderSessions: true,
	CommandLoadProviderSession: true, CommandLoadComposerDrafts: true,
	CommandSaveComposerDrafts: true, CommandApplyComposerDraftChanges: true,
	CommandStoreBlob: true, CommandImportAttachment: true, CommandImportPathAttachment: true,
	CommandReadBlob: true, CommandReadAttachment: true, CommandSweepBlobs: true,
	CommandForkSessionFromResponse: true, CommandRewindSessionToMessage: true,
	CommandForkProviderSession: true, CommandWorkspace: true, CommandOpenTerminal: true,
	CommandWriteTerminal: true, CommandResizeTerminal: true, CommandCloseTerminal: true,
	CommandCloseSession: true,
}

func NewRawCommand(kind CommandType, fields map[string]any) (RawCommand, error) {
	if !knownCommandTypes[kind] {
		return RawCommand{}, fmt.Errorf("unknown command type %q", kind)
	}
	r := RawCommand{Type: kind, Fields: make(map[string]json.RawMessage, len(fields))}
	for key, value := range fields {
		b, err := json.Marshal(value)
		if err != nil {
			return RawCommand{}, err
		}
		r.Fields[key] = b
	}
	return r, nil
}
func (c RawCommand) MarshalJSON() ([]byte, error) {
	if !knownCommandTypes[c.Type] {
		return nil, fmt.Errorf("unknown command type %q", c.Type)
	}
	object := map[string]json.RawMessage{"type": json.RawMessage(strconvQuote(string(c.Type)))}
	for key, value := range c.Fields {
		if key == "type" {
			return nil, fmt.Errorf("command field type is reserved")
		}
		object[key] = value
	}
	return json.Marshal(object)
}
func strconvQuote(s string) []byte { b, _ := json.Marshal(s); return b }

type WireDriverStartOptions struct {
	Provider           string          `json:"provider"`
	Binary             string          `json:"binary"`
	CWD                string          `json:"cwd"`
	Mode               string          `json:"mode"`
	Model              *string         `json:"model"`
	ReasoningEffort    *string         `json:"reasoningEffort"`
	ServiceTier        *string         `json:"serviceTier"`
	ContextWindow      *string         `json:"contextWindow"`
	AgentPreset        *string         `json:"agentPreset"`
	ComputerUseEnabled bool            `json:"computerUseEnabled"`
	ProviderCursor     json.RawMessage `json:"providerCursor"`
}
type StartCommand struct {
	Type    CommandType            `json:"type"`
	Options WireDriverStartOptions `json:"options"`
}

func Start(options WireDriverStartOptions) StartCommand { return StartCommand{CommandStart, options} }

type WireSessionOptions struct {
	Mode            string  `json:"mode"`
	Model           *string `json:"model"`
	ReasoningEffort *string `json:"reasoningEffort"`
	ServiceTier     *string `json:"serviceTier"`
	ContextWindow   *string `json:"contextWindow"`
}
type ApplyOptionsCommand struct {
	Type    CommandType        `json:"type"`
	Options WireSessionOptions `json:"options"`
}

type SteerCommand struct {
	Type   CommandType `json:"type"`
	Prompt string      `json:"prompt"`
}
type RespondCommand struct {
	Type      CommandType `json:"type"`
	RequestID string      `json:"requestId"`
	OptionID  string      `json:"optionId"`
}
type UserInputAnswer struct {
	QuestionID string   `json:"questionId"`
	Answers    []string `json:"answers"`
}

func (a *UserInputAnswer) UnmarshalJSON(data []byte) error {
	type alias UserInputAnswer
	if err := requireJSONFields(data, "questionId", "answers"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*a = UserInputAnswer(v)
	return nil
}

type RespondUserInputCommand struct {
	Type      CommandType       `json:"type"`
	RequestID string            `json:"requestId"`
	Answers   []UserInputAnswer `json:"answers"`
}
type TurnsCommand struct {
	Type  CommandType `json:"type"`
	Turns uint64      `json:"turns"`
}
type ForkCommand struct {
	Type          CommandType `json:"type"`
	TurnsToRemove uint64      `json:"turnsToRemove"`
}
type TurnCountCommand struct {
	Type      CommandType `json:"type"`
	TurnCount uint64      `json:"turnCount"`
}
type OpenTerminalCommand struct {
	Type CommandType `json:"type"`
	CWD  string      `json:"cwd"`
	Cols uint16      `json:"cols"`
	Rows uint16      `json:"rows"`
}
type ResizeTerminalCommand struct {
	Type CommandType `json:"type"`
	Cols uint16      `json:"cols"`
	Rows uint16      `json:"rows"`
}
type StoreBlobCommand struct {
	Type     CommandType `json:"type"`
	MIMEType string      `json:"mimeType"`
	Bytes    Base64Bytes `json:"bytes"`
}

type StopBackgroundWorkCommand struct {
	Type      CommandType     `json:"type"`
	Key       json.RawMessage `json:"key"`
	ControlID string          `json:"controlId"`
}
type GoalCommand struct {
	Type      CommandType     `json:"type"`
	Operation json.RawMessage `json:"operation"`
}
type WireComputerToolRequest struct {
	CallID    string          `json:"callId"`
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}
type RunComputerToolCommand struct {
	Type    CommandType             `json:"type"`
	Request WireComputerToolRequest `json:"request"`
}
type RejectComputerToolCommand struct {
	Type    CommandType             `json:"type"`
	Request WireComputerToolRequest `json:"request"`
	Reason  string                  `json:"reason"`
}
type UpdateSettingsCommand struct {
	Type     CommandType     `json:"type"`
	Settings json.RawMessage `json:"settings"`
}
type ProbeProviderCommand struct {
	Type           CommandType `json:"type"`
	Provider       string      `json:"provider"`
	BinaryOverride *string     `json:"binaryOverride"`
	DiscoverModels bool        `json:"discoverModels"`
	ProbeVersion   bool        `json:"probeVersion"`
}
type FetchPlanUsageCommand struct {
	Type           CommandType `json:"type"`
	Provider       string      `json:"provider"`
	BinaryOverride *string     `json:"binaryOverride"`
	CLIVersion     *string     `json:"cliVersion"`
}
type ProbeComputerPermissionsCommand struct {
	Type   CommandType `json:"type"`
	Prompt bool        `json:"prompt"`
}
type LoadUsageHistoryCommand struct {
	Type         CommandType     `json:"type"`
	Window       json.RawMessage `json:"window"`
	ProjectRoots []string        `json:"projectRoots"`
}
type SkillProject [2]string

func (p *SkillProject) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("skill project is null")
	}
	var v []string
	if err := Decode(data, &v); err != nil {
		return err
	}
	if len(v) != 2 {
		return fmt.Errorf("skill project has %d fields", len(v))
	}
	p[0], p[1] = v[0], v[1]
	return nil
}

type LoadSkillsCommand struct {
	Type     CommandType    `json:"type"`
	Projects []SkillProject `json:"projects"`
}
type SetSkillsEnabledCommand struct {
	Type    CommandType `json:"type"`
	Dirs    []string    `json:"dirs"`
	Enabled bool        `json:"enabled"`
}
type TrashSkillsCommand struct {
	Type CommandType `json:"type"`
	Dirs []string    `json:"dirs"`
}
type SaveTaskStateCommand struct {
	Type           CommandType       `json:"type"`
	Projects       []json.RawMessage `json:"projects"`
	LiveSessionIDs []UUID            `json:"liveSessionIds"`
	Sessions       []json.RawMessage `json:"sessions"`
}
type HydrateSessionCommand struct {
	Type      CommandType `json:"type"`
	SessionID UUID        `json:"sessionId"`
}
type SearchSessionMessagesCommand struct {
	Type  CommandType `json:"type"`
	Query string      `json:"query"`
	Limit uint64      `json:"limit"`
}
type ListProviderSessionsCommand struct {
	Type     CommandType `json:"type"`
	Provider string      `json:"provider"`
	Limit    uint64      `json:"limit"`
}
type LoadProviderSessionCommand struct {
	Type   CommandType     `json:"type"`
	Cursor json.RawMessage `json:"cursor"`
	CWD    string          `json:"cwd"`
}
type SaveComposerDraftsCommand struct {
	Type       CommandType     `json:"type"`
	Drafts     json.RawMessage `json:"drafts"`
	Generation uint64          `json:"generation"`
}
type ApplyComposerDraftChangesCommand struct {
	Type    CommandType       `json:"type"`
	Changes []json.RawMessage `json:"changes"`
}
type ImportAttachmentCommand struct {
	Type   CommandType     `json:"type"`
	Name   string          `json:"name"`
	Upload json.RawMessage `json:"upload"`
}
type ImportPathAttachmentCommand struct {
	Type CommandType `json:"type"`
	Path string      `json:"path"`
}
type ReadBlobCommand struct {
	Type      CommandType `json:"type"`
	Reference string      `json:"reference"`
}
type ReadAttachmentCommand struct {
	Type      CommandType `json:"type"`
	Reference string      `json:"reference"`
	Path      string      `json:"path"`
}
type ForkProviderSessionCommand struct {
	Type    CommandType     `json:"type"`
	Request json.RawMessage `json:"request"`
}
type WorkspaceCommand struct {
	Type      CommandType     `json:"type"`
	Operation json.RawMessage `json:"operation"`
}

// DecodeCommand validates the complete protocol-v7 command discriminant and
// decodes every variant into a concrete Go type. Domain objects that belong to
// storage/provider packages remain RawMessage, but their presence and JSON
// shape are still checked here.
func DecodeCommand(raw json.RawMessage) (any, error) {
	kind, err := CommandTypeOf(raw)
	if err != nil {
		return nil, err
	}
	requiredFields := map[CommandType][]string{
		CommandStart: {"options"}, CommandPrompt: {"prompt"}, CommandSteer: {"prompt"}, CommandStopBackgroundWork: {"key", "controlId"},
		CommandRespond: {"requestId", "optionId"}, CommandRespondUserInput: {"requestId", "answers"}, CommandGoal: {"operation"},
		CommandRunComputerTool: {"request"}, CommandRejectComputerTool: {"request", "reason"}, CommandApplyOptions: {"options"},
		CommandRollback: {"turns"}, CommandFork: {"turnsToRemove"}, CommandUpdateSettings: {"settings"},
		CommandProbeProvider: {"provider", "discoverModels", "probeVersion"}, CommandFetchPlanUsage: {"provider"},
		CommandProbeComputerPermissions: {"prompt"}, CommandLoadUsageHistory: {"window", "projectRoots"}, CommandLoadSkills: {"projects"},
		CommandSetSkillsEnabled: {"dirs", "enabled"}, CommandTrashSkills: {"dirs"}, CommandSaveTaskState: {"projects", "liveSessionIds", "sessions"},
		CommandHydrateSession: {"sessionId"}, CommandSearchSessionMessages: {"query", "limit"}, CommandListProviderSessions: {"provider", "limit"},
		CommandLoadProviderSession: {"cursor", "cwd"}, CommandSaveComposerDrafts: {"drafts", "generation"}, CommandApplyComposerDraftChanges: {"changes"},
		CommandStoreBlob: {"mimeType", "bytes"}, CommandImportAttachment: {"name", "upload"}, CommandImportPathAttachment: {"path"},
		CommandReadBlob: {"reference"}, CommandReadAttachment: {"reference", "path"}, CommandForkSessionFromResponse: {"turnCount"},
		CommandRewindSessionToMessage: {"turnCount"}, CommandForkProviderSession: {"request"}, CommandWorkspace: {"operation"},
		CommandOpenTerminal: {"cwd", "cols", "rows"}, CommandWriteTerminal: {"data"}, CommandResizeTerminal: {"cols", "rows"},
	}
	if err := requireJSONFields(raw, requiredFields[kind]...); err != nil {
		return nil, fmt.Errorf("%s command: %w", kind, err)
	}
	if kind == CommandStart {
		if err := requireNestedJSONFields(raw, "options", "provider", "binary", "cwd", "mode", "computerUseEnabled"); err != nil {
			return nil, fmt.Errorf("start command: %w", err)
		}
	}
	if kind == CommandApplyOptions {
		if err := requireNestedJSONFields(raw, "options", "mode"); err != nil {
			return nil, fmt.Errorf("applyOptions command: %w", err)
		}
	}
	if kind == CommandRunComputerTool || kind == CommandRejectComputerTool {
		if err := requireNestedJSONFields(raw, "request", "callId", "tool", "arguments"); err != nil {
			return nil, fmt.Errorf("%s command: %w", kind, err)
		}
	}
	decodeInto := func(dst any) (any, error) {
		if err := Decode(raw, dst); err != nil {
			return nil, err
		}
		return dst, nil
	}
	required := func(name string, value json.RawMessage) error {
		if len(value) == 0 || string(value) == "null" {
			return fmt.Errorf("%s command has no %s", kind, name)
		}
		return nil
	}
	switch kind {
	case CommandAttachSession, CommandCancel, CommandCancelComputerUse, CommandRefreshBackgroundWork,
		CommandGetSettings, CommandLoadTaskState, CommandRemoveSession, CommandLoadComposerDrafts,
		CommandSweepBlobs, CommandCloseTerminal, CommandCloseSession:
		var v Command
		return decodeInto(&v)
	case CommandStart:
		var v StartCommand
		value, e := decodeInto(&v)
		if e == nil && v.Options.Provider == "" {
			e = fmt.Errorf("start command has no provider")
		}
		if e == nil && len(v.Options.ProviderCursor) != 0 && string(bytes.TrimSpace(v.Options.ProviderCursor)) != "null" {
			e = validateProviderCursor(v.Options.ProviderCursor)
		}
		return value, e
	case CommandPrompt:
		var v PromptCommand
		return decodeInto(&v)
	case CommandSteer:
		var v SteerCommand
		return decodeInto(&v)
	case CommandStopBackgroundWork:
		var v StopBackgroundWorkCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = requireJSONObject(v.Key, "stopBackgroundWork key")
		}
		return value, e
	case CommandRespond:
		var v RespondCommand
		value, e := decodeInto(&v)
		if e == nil && v.RequestID == "" {
			e = fmt.Errorf("respond command has no requestId")
		}
		return value, e
	case CommandRespondUserInput:
		var v RespondUserInputCommand
		value, e := decodeInto(&v)
		if e == nil && v.RequestID == "" {
			e = fmt.Errorf("respondUserInput command has no requestId")
		}
		return value, e
	case CommandGoal:
		var v GoalCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateGoalOperation(v.Operation)
		}
		return value, e
	case CommandRunComputerTool:
		var v RunComputerToolCommand
		value, e := decodeInto(&v)
		if e == nil && (v.Request.CallID == "" || v.Request.Tool == "") {
			e = fmt.Errorf("runComputerTool command has an incomplete request")
		}
		if e == nil {
			e = required("request.arguments", v.Request.Arguments)
		}
		return value, e
	case CommandRejectComputerTool:
		var v RejectComputerToolCommand
		value, e := decodeInto(&v)
		if e == nil && (v.Request.CallID == "" || v.Request.Tool == "") {
			e = fmt.Errorf("rejectComputerTool command has an incomplete request")
		}
		if e == nil {
			e = required("request.arguments", v.Request.Arguments)
		}
		return value, e
	case CommandApplyOptions:
		var v ApplyOptionsCommand
		return decodeInto(&v)
	case CommandRollback:
		var v TurnsCommand
		return decodeInto(&v)
	case CommandFork:
		var v ForkCommand
		return decodeInto(&v)
	case CommandUpdateSettings:
		var v UpdateSettingsCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateDaemonSettings(v.Settings)
		}
		return value, e
	case CommandProbeProvider:
		var v ProbeProviderCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderKind(v.Provider)
		}
		return value, e
	case CommandFetchPlanUsage:
		var v FetchPlanUsageCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderKind(v.Provider)
		}
		return value, e
	case CommandProbeComputerPermissions:
		var v ProbeComputerPermissionsCommand
		return decodeInto(&v)
	case CommandLoadUsageHistory:
		var v LoadUsageHistoryCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateUsageWindow(v.Window)
		}
		return value, e
	case CommandLoadSkills:
		var v LoadSkillsCommand
		return decodeInto(&v)
	case CommandSetSkillsEnabled:
		var v SetSkillsEnabledCommand
		return decodeInto(&v)
	case CommandTrashSkills:
		var v TrashSkillsCommand
		return decodeInto(&v)
	case CommandSaveTaskState:
		var v SaveTaskStateCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Projects, "projects")
		}
		if e == nil {
			for i, project := range v.Projects {
				if e = validateProject(project); e != nil {
					e = fmt.Errorf("projects[%d]: %w", i, e)
					break
				}
			}
		}
		if e == nil {
			e = requireObjectElements(v.Sessions, "sessions")
		}
		if e == nil {
			for i, session := range v.Sessions {
				if e = validateAgentSession(session); e != nil {
					e = fmt.Errorf("sessions[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case CommandHydrateSession:
		var v HydrateSessionCommand
		return decodeInto(&v)
	case CommandSearchSessionMessages:
		var v SearchSessionMessagesCommand
		return decodeInto(&v)
	case CommandListProviderSessions:
		var v ListProviderSessionsCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderKind(v.Provider)
		}
		return value, e
	case CommandLoadProviderSession:
		var v LoadProviderSessionCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderCursor(v.Cursor)
		}
		return value, e
	case CommandSaveComposerDrafts:
		var v SaveComposerDraftsCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateComposerDrafts(v.Drafts)
		}
		return value, e
	case CommandApplyComposerDraftChanges:
		var v ApplyComposerDraftChangesCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = requireObjectElements(v.Changes, "changes")
		}
		if e == nil {
			for i, change := range v.Changes {
				if e = validateComposerDraftChange(change); e != nil {
					e = fmt.Errorf("changes[%d]: %w", i, e)
					break
				}
			}
		}
		return value, e
	case CommandStoreBlob:
		var v StoreBlobCommand
		return decodeInto(&v)
	case CommandImportAttachment:
		var v ImportAttachmentCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateAttachmentUpload(v.Upload)
		}
		return value, e
	case CommandImportPathAttachment:
		var v ImportPathAttachmentCommand
		return decodeInto(&v)
	case CommandReadBlob:
		var v ReadBlobCommand
		return decodeInto(&v)
	case CommandReadAttachment:
		var v ReadAttachmentCommand
		return decodeInto(&v)
	case CommandForkSessionFromResponse, CommandRewindSessionToMessage:
		var v TurnCountCommand
		return decodeInto(&v)
	case CommandForkProviderSession:
		var v ForkProviderSessionCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateProviderForkRequest(v.Request)
		}
		return value, e
	case CommandWorkspace:
		var v WorkspaceCommand
		value, e := decodeInto(&v)
		if e == nil {
			e = validateWorkspaceOperation(v.Operation)
		}
		return value, e
	case CommandOpenTerminal:
		var v OpenTerminalCommand
		return decodeInto(&v)
	case CommandWriteTerminal:
		var v WriteTerminalCommand
		return decodeInto(&v)
	case CommandResizeTerminal:
		var v ResizeTerminalCommand
		return decodeInto(&v)
	default:
		return nil, fmt.Errorf("unknown command type %q", kind)
	}
}

func validateProviderKind(value string) error {
	switch value {
	case "amp", "claude", "codex", "cursor", "deepSeek", "fx", "openCode", "openCode2", "grok", "kimi", "ohMyPi", "pi":
		return nil
	}
	return fmt.Errorf("unknown provider %q", value)
}

func requireJSONFields(raw []byte, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	for _, field := range fields {
		value, ok := object[field]
		if !ok {
			return fmt.Errorf("missing field %q", field)
		}
		if string(bytes.TrimSpace(value)) == "null" {
			return fmt.Errorf("field %q is null", field)
		}
	}
	return nil
}

func requireNestedJSONFields(raw []byte, parent string, fields ...string) error {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	value, ok := object[parent]
	if !ok {
		return fmt.Errorf("missing field %q", parent)
	}
	if err := requireJSONFields(value, fields...); err != nil {
		return fmt.Errorf("field %q: %w", parent, err)
	}
	return nil
}

func requireJSONObject(raw []byte, label string) error {
	var v map[string]json.RawMessage
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%s is null", label)
	}
	if err := Decode(raw, &v); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if v == nil {
		return fmt.Errorf("%s is not an object", label)
	}
	return nil
}
func requireObjectElements(values []json.RawMessage, label string) error {
	for i, v := range values {
		if err := requireJSONObject(v, fmt.Sprintf("%s[%d]", label, i)); err != nil {
			return err
		}
	}
	return nil
}
func decodeJSONField(raw []byte, field string, dst any) error {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	value, ok := object[field]
	if !ok {
		return fmt.Errorf("missing field %q", field)
	}
	if err := Decode(value, dst); err != nil {
		return fmt.Errorf("field %q: %w", field, err)
	}
	return nil
}
func rejectNullFields(object map[string]json.RawMessage, fields ...string) error {
	for _, field := range fields {
		if raw, present := object[field]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("field %q is null", field)
		}
	}
	return nil
}
func validateUsageWindow(raw []byte) error {
	var unit string
	if Decode(raw, &unit) == nil {
		if unit == "thisMonth" || unit == "lastMonth" {
			return nil
		}
		return fmt.Errorf("unknown usage window %q", unit)
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	if len(object) != 1 {
		return fmt.Errorf("usage window must contain one variant")
	}
	for key, value := range object {
		if key != "trailingDays" && key != "months" {
			return fmt.Errorf("unknown usage window %q", key)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("usage window %q is null", key)
		}
		var count uint32
		if err := Decode(value, &count); err != nil {
			return err
		}
	}
	return nil
}
func validateGoalOperation(raw []byte) error {
	if err := requireJSONFields(raw, "kind"); err != nil {
		return err
	}
	var v struct {
		Kind      string  `json:"kind"`
		Objective *string `json:"objective"`
		Status    *string `json:"status"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	switch v.Kind {
	case "refresh", "clear":
		return nil
	case "set":
		if err := requireJSONFields(raw, "replace"); err != nil {
			return err
		}
		if v.Status != nil {
			switch *v.Status {
			case "active", "paused", "blocked", "usageLimited", "budgetLimited", "complete":
			default:
				return fmt.Errorf("unknown goal status %q", *v.Status)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown goal operation %q", v.Kind)
	}
}
func validateProviderCursor(raw []byte) error {
	if err := requireJSONFields(raw, "provider"); err != nil {
		return err
	}
	var v struct {
		Provider string `json:"provider"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	if err := validateProviderKind(v.Provider); err != nil {
		return err
	}
	field := "sessionId"
	if v.Provider == "codex" || v.Provider == "amp" {
		field = "threadId"
	}
	return requireJSONFields(raw, field)
}
func validateAttachmentUpload(raw []byte) error {
	if err := requireJSONFields(raw, "kind"); err != nil {
		return err
	}
	var v struct {
		Kind       string            `json:"kind"`
		DataBase64 string            `json:"data_base64"`
		Entries    []json.RawMessage `json:"entries"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	switch v.Kind {
	case "file":
		return requireJSONFields(raw, "data_base64")
	case "directory":
		if err := requireJSONFields(raw, "entries"); err != nil {
			return err
		}
		for i, e := range v.Entries {
			if err := requireJSONFields(e, "relativePath", "dataBase64"); err != nil {
				return fmt.Errorf("entry[%d]: %w", i, err)
			}
			var entry struct {
				RelativePath string `json:"relativePath"`
				DataBase64   string `json:"dataBase64"`
			}
			if err := Decode(e, &entry); err != nil {
				return fmt.Errorf("entry[%d]: %w", i, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown attachment kind %q", v.Kind)
	}
}

func validateProviderProbe(raw []byte) error {
	if err := requireJSONFields(raw, "provider", "installed"); err != nil {
		return err
	}
	var v struct {
		Provider     string            `json:"provider"`
		Installed    bool              `json:"installed"`
		Path         *string           `json:"path"`
		Models       []json.RawMessage `json:"models"`
		AgentPresets []json.RawMessage `json:"agent_presets"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	if err := validateProviderKind(v.Provider); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	for _, field := range []string{"models", "agent_presets"} {
		if value, present := object[field]; present {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return fmt.Errorf("field %q is null", field)
			}
			var entries []json.RawMessage
			if err := Decode(value, &entries); err != nil {
				return fmt.Errorf("field %q: %w", field, err)
			}
			if err := requireObjectElements(entries, field); err != nil {
				return err
			}
			for i, entry := range entries {
				var err error
				if field == "models" {
					err = validateProviderModel(entry)
				} else {
					err = validateProviderAgentPreset(entry)
				}
				if err != nil {
					return fmt.Errorf("%s[%d]: %w", field, i, err)
				}
			}
		}
	}
	return nil
}

func validateProviderModelOption(raw []byte) error {
	if err := requireJSONFields(raw, "id", "label"); err != nil {
		return err
	}
	var value struct {
		ID          string  `json:"id"`
		Label       string  `json:"label"`
		Description *string `json:"description"`
	}
	return Decode(raw, &value)
}

func validateProviderModel(raw []byte) error {
	if err := requireJSONFields(raw, "id", "name"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	var value struct {
		ID                     string  `json:"id"`
		Name                   string  `json:"name"`
		SubProvider            *string `json:"sub_provider"`
		IsDefault              bool    `json:"is_default"`
		DefaultReasoningEffort *string `json:"default_reasoning_effort"`
		DefaultServiceTier     *string `json:"default_service_tier"`
		DefaultContextWindow   *string `json:"default_context_window"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := rejectNullFields(object, "is_default"); err != nil {
		return err
	}
	for _, field := range []string{"reasoning_efforts", "service_tiers", "context_windows"} {
		rawEntries, present := object[field]
		if !present {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(rawEntries), []byte("null")) {
			return fmt.Errorf("field %q is null", field)
		}
		var entries []json.RawMessage
		if err := Decode(rawEntries, &entries); err != nil {
			return fmt.Errorf("field %q: %w", field, err)
		}
		for i, entry := range entries {
			if err := validateProviderModelOption(entry); err != nil {
				return fmt.Errorf("%s[%d]: %w", field, i, err)
			}
		}
	}
	return nil
}

func validateProviderAgentPreset(raw []byte) error {
	if err := requireJSONFields(raw, "id", "name"); err != nil {
		return err
	}
	var value struct {
		ID          string  `json:"id"`
		Name        string  `json:"name"`
		Description *string `json:"description"`
		IsDefault   bool    `json:"is_default"`
		IsCustom    bool    `json:"is_custom"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	return rejectNullFields(object, "is_default", "is_custom")
}

func validateComputerPermissions(raw []byte) error {
	if err := requireJSONFields(raw, "screenRecording", "accessibility"); err != nil {
		return err
	}
	var value struct {
		ScreenRecording bool `json:"screenRecording"`
		Accessibility   bool `json:"accessibility"`
	}
	return Decode(raw, &value)
}

func validateSkillsCatalog(raw []byte) error {
	if err := requireJSONFields(raw, "skills"); err != nil {
		return err
	}
	var value struct {
		Skills []json.RawMessage `json:"skills"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := requireObjectElements(value.Skills, "skills"); err != nil {
		return err
	}
	for i, skill := range value.Skills {
		if err := validateSkillEntry(skill); err != nil {
			return fmt.Errorf("skills[%d]: %w", i, err)
		}
	}
	return nil
}

func validateSkillEntry(raw []byte) error {
	fields := []string{"name", "description", "scope", "installs", "enabled", "body", "supportingFiles", "totalBytes", "duplicates", "rowKey"}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	var value struct {
		Name            string            `json:"name"`
		Description     string            `json:"description"`
		Scope           string            `json:"scope"`
		Installs        []json.RawMessage `json:"installs"`
		Enabled         bool              `json:"enabled"`
		Body            string            `json:"body"`
		SupportingFiles uint64            `json:"supportingFiles"`
		TotalBytes      uint64            `json:"totalBytes"`
		Duplicates      uint64            `json:"duplicates"`
		RowKey          uint64            `json:"rowKey"`
		Project         *string           `json:"project"`
		AllowedTools    *string           `json:"allowedTools"`
		ModifiedAt      *uint64           `json:"modifiedAt"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Scope != "user" && value.Scope != "project" {
		return fmt.Errorf("unknown skill scope %q", value.Scope)
	}
	for i, install := range value.Installs {
		if err := requireJSONFields(install, "source", "dir", "skillFile", "enabled"); err != nil {
			return fmt.Errorf("installs[%d]: %w", i, err)
		}
		var typed struct {
			Source    json.RawMessage `json:"source"`
			Dir       string          `json:"dir"`
			SkillFile string          `json:"skillFile"`
			Enabled   bool            `json:"enabled"`
		}
		if err := Decode(install, &typed); err != nil {
			return err
		}
		if err := validateSkillSource(typed.Source); err != nil {
			return fmt.Errorf("installs[%d].source: %w", i, err)
		}
	}
	return nil
}

func validateSkillSource(raw []byte) error {
	var unit string
	if err := Decode(raw, &unit); err == nil {
		if unit == "shared" {
			return nil
		}
		return fmt.Errorf("unknown skill source %q", unit)
	}
	var object struct {
		Provider string `json:"provider"`
	}
	if err := requireJSONFields(raw, "provider"); err != nil {
		return err
	}
	if err := Decode(raw, &object); err != nil {
		return err
	}
	return validateProviderKind(object.Provider)
}

func validateStoredAttachment(raw []byte) error {
	if err := requireJSONFields(raw, "reference", "path", "name", "isDir"); err != nil {
		return err
	}
	var value struct {
		Reference string `json:"reference"`
		Path      string `json:"path"`
		Name      string `json:"name"`
		IsDir     bool   `json:"isDir"`
	}
	return Decode(raw, &value)
}

func validateProject(raw []byte) error {
	if err := requireJSONFields(raw, "id", "name", "path"); err != nil {
		return err
	}
	var value struct {
		ID        UUID   `json:"id"`
		Name      string `json:"name"`
		Path      string `json:"path"`
		CreatedAt uint64 `json:"created_at"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	_ = Decode(raw, &object)
	return rejectNullFields(object, "created_at")
}

func validateSessionMessageMatch(raw []byte) error {
	if err := requireJSONFields(raw, "session_id", "source", "snippet"); err != nil {
		return err
	}
	var value struct {
		SessionID UUID   `json:"session_id"`
		Source    string `json:"source"`
		Snippet   string `json:"snippet"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Source != "user" && value.Source != "assistant" && value.Source != "system" {
		return fmt.Errorf("unknown message role %q", value.Source)
	}
	return nil
}

func validateProviderSessionSummary(raw []byte) error {
	if err := requireJSONFields(raw, "cursor", "title", "cwd", "created_at", "updated_at"); err != nil {
		return err
	}
	var value struct {
		Cursor    json.RawMessage `json:"cursor"`
		Title     string          `json:"title"`
		CWD       string          `json:"cwd"`
		CreatedAt uint64          `json:"created_at"`
		UpdatedAt uint64          `json:"updated_at"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	return validateProviderCursor(value.Cursor)
}

func validateDaemonSettings(raw []byte) error {
	if err := requireJSONObject(raw, "settings"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	if err := rejectNullFields(object, "computer_use_enabled", "computer_use_allowed_apps", "disabled_providers", "provider_binary_overrides"); err != nil {
		return err
	}
	if value, present := object["computer_use_enabled"]; present {
		var typed bool
		if err := Decode(value, &typed); err != nil {
			return err
		}
	}
	if value, present := object["computer_use_allowed_apps"]; present {
		var grants []json.RawMessage
		if err := Decode(value, &grants); err != nil {
			return err
		}
		for i, grant := range grants {
			if err := requireJSONFields(grant, "bundleId", "appName"); err != nil {
				return fmt.Errorf("computer_use_allowed_apps[%d]: %w", i, err)
			}
			var typed struct {
				BundleID string `json:"bundleId"`
				AppName  string `json:"appName"`
			}
			if err := Decode(grant, &typed); err != nil {
				return err
			}
		}
	}
	if value, present := object["disabled_providers"]; present {
		var providers []string
		if err := Decode(value, &providers); err != nil {
			return err
		}
		for _, provider := range providers {
			if err := validateProviderKind(provider); err != nil {
				return err
			}
		}
	}
	if value, present := object["provider_binary_overrides"]; present {
		var overrides map[string]string
		if err := Decode(value, &overrides); err != nil {
			return err
		}
		for provider := range overrides {
			if err := validateProviderKind(provider); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateComposerDrafts(raw []byte) error {
	if err := requireJSONObject(raw, "drafts"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	for _, field := range []string{"new_sessions", "sessions"} {
		value, present := object[field]
		if !present {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("field %q is null", field)
		}
		var drafts map[string]json.RawMessage
		if err := Decode(value, &drafts); err != nil {
			return err
		}
		for id, draft := range drafts {
			if !UUID(id).Valid() {
				return fmt.Errorf("%s key %q is not a UUID", field, id)
			}
			if err := validateComposerDraft(draft); err != nil {
				return fmt.Errorf("%s[%s]: %w", field, id, err)
			}
		}
	}
	return nil
}

func validateComposerDraft(raw []byte) error {
	if err := requireJSONObject(raw, "draft"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	if err := rejectNullFields(object, "text", "attachments"); err != nil {
		return err
	}
	if value, present := object["text"]; present {
		var text string
		if err := Decode(value, &text); err != nil {
			return err
		}
	}
	if value, present := object["attachments"]; present {
		var attachments []json.RawMessage
		if err := Decode(value, &attachments); err != nil {
			return err
		}
		for i, attachment := range attachments {
			if err := validateMessageAttachment(attachment); err != nil {
				return fmt.Errorf("attachments[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func validateComposerDraftChange(raw []byte) error {
	if err := requireJSONFields(raw, "target"); err != nil {
		return err
	}
	var value struct {
		Target json.RawMessage `json:"target"`
		Draft  json.RawMessage `json:"draft"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := requireJSONFields(value.Target, "type"); err != nil {
		return err
	}
	var target struct {
		Type      string `json:"type"`
		ProjectID UUID   `json:"projectId"`
		SessionID UUID   `json:"sessionId"`
	}
	if err := Decode(value.Target, &target); err != nil {
		return err
	}
	switch target.Type {
	case "newSession":
		if err := requireJSONFields(value.Target, "projectId"); err != nil {
			return err
		}
	case "session":
		if err := requireJSONFields(value.Target, "sessionId"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown composer draft target %q", target.Type)
	}
	var object map[string]json.RawMessage
	_ = Decode(raw, &object)
	if draft, present := object["draft"]; present && !bytes.Equal(bytes.TrimSpace(draft), []byte("null")) {
		return validateComposerDraft(draft)
	}
	return nil
}

func validateAgentSession(raw []byte) error {
	fields := []string{"id", "title", "project_id", "provider", "runtime_mode", "status", "created_at", "updated_at"}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	var value struct {
		ID          UUID   `json:"id"`
		Title       string `json:"title"`
		ProjectID   UUID   `json:"project_id"`
		Provider    string `json:"provider"`
		RuntimeMode string `json:"runtime_mode"`
		Status      string `json:"status"`
		CreatedAt   uint64 `json:"created_at"`
		UpdatedAt   uint64 `json:"updated_at"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := validateProviderKind(value.Provider); err != nil {
		return err
	}
	switch value.RuntimeMode {
	case "plan", "ask", "autoAcceptEdits", "auto", "fullAccess":
	default:
		return fmt.Errorf("unknown runtime mode %q", value.RuntimeMode)
	}
	switch value.Status {
	case "idle", "connecting", "working", "waiting", "background", "failed":
	default:
		return fmt.Errorf("unknown session status %q", value.Status)
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	if workspace, present := object["workspace"]; present {
		if err := validateSessionWorkspace(workspace); err != nil {
			return fmt.Errorf("workspace: %w", err)
		}
	}
	if cursor, present := object["provider_cursor"]; present && !bytes.Equal(bytes.TrimSpace(cursor), []byte("null")) {
		if err := validateProviderCursor(cursor); err != nil {
			return fmt.Errorf("provider_cursor: %w", err)
		}
	}
	if goal, present := object["thread_goal"]; present && !bytes.Equal(bytes.TrimSpace(goal), []byte("null")) {
		var value ThreadGoalPayload
		if err := Decode(goal, &value); err != nil {
			return fmt.Errorf("thread_goal: %w", err)
		}
	}
	if usage, present := object["context_usage"]; present && !bytes.Equal(bytes.TrimSpace(usage), []byte("null")) {
		if err := requireJSONFields(usage, "tokens"); err != nil {
			return fmt.Errorf("context_usage: %w", err)
		}
		var value struct {
			Tokens uint64  `json:"tokens"`
			Window *uint64 `json:"window"`
		}
		if err := Decode(usage, &value); err != nil {
			return fmt.Errorf("context_usage: %w", err)
		}
	}
	if cursor, present := object["runtime_event_cursor"]; present && !bytes.Equal(bytes.TrimSpace(cursor), []byte("null")) {
		if err := requireJSONFields(cursor, "runtime_id", "epoch", "sequence"); err != nil {
			return fmt.Errorf("runtime_event_cursor: %w", err)
		}
		var value struct {
			RuntimeID UUID   `json:"runtime_id"`
			Epoch     UUID   `json:"epoch"`
			Sequence  uint64 `json:"sequence"`
		}
		if err := Decode(cursor, &value); err != nil {
			return fmt.Errorf("runtime_event_cursor: %w", err)
		}
	}
	vectorValidators := map[string]func([]byte) error{
		"available_commands": validateReportedCommand,
		"messages":           validateMessage,
		"transcript_blocks":  validateTranscriptBlock,
		"turns":              validateAgentTurn,
		"queued_messages":    validateQueuedMessage,
	}
	for field, validate := range vectorValidators {
		item, present := object[field]
		if !present {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(item), []byte("null")) {
			return fmt.Errorf("field %q is null", field)
		}
		var entries []json.RawMessage
		if err := Decode(item, &entries); err != nil {
			return fmt.Errorf("field %q: %w", field, err)
		}
		for i, entry := range entries {
			if err := validate(entry); err != nil {
				return fmt.Errorf("%s[%d]: %w", field, i, err)
			}
		}
	}
	return nil
}

func validateSessionWorkspace(raw []byte) error {
	if err := requireJSONFields(raw, "kind"); err != nil {
		return err
	}
	var value struct {
		Kind       string  `json:"kind"`
		BaseBranch *string `json:"baseBranch"`
		Path       string  `json:"path"`
		Branch     string  `json:"branch"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	switch value.Kind {
	case "local", "newWorktree":
		return nil
	case "worktree":
		return requireJSONFields(raw, "path", "branch")
	default:
		return fmt.Errorf("unknown session workspace %q", value.Kind)
	}
}

func validateReportedCommand(raw []byte) error {
	var value ReportedCommandPayload
	return Decode(raw, &value)
}

func validateMessageAttachment(raw []byte) error {
	if err := requireJSONFields(raw, "path", "mention", "name", "is_dir", "is_image"); err != nil {
		return err
	}
	var value struct {
		Path    string `json:"path"`
		Mention string `json:"mention"`
		Name    string `json:"name"`
		IsDir   bool   `json:"is_dir"`
		IsImage bool   `json:"is_image"`
	}
	return Decode(raw, &value)
}

func validateAttachmentsField(object map[string]json.RawMessage) error {
	raw, present := object["attachments"]
	if !present {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("attachments is null")
	}
	var entries []json.RawMessage
	if err := Decode(raw, &entries); err != nil {
		return err
	}
	for i, entry := range entries {
		if err := validateMessageAttachment(entry); err != nil {
			return fmt.Errorf("attachments[%d]: %w", i, err)
		}
	}
	return nil
}

func validateMessage(raw []byte) error {
	if err := requireJSONFields(raw, "id", "role", "content", "created_at", "streaming"); err != nil {
		return err
	}
	var value struct {
		ID        UUID   `json:"id"`
		Role      string `json:"role"`
		Content   string `json:"content"`
		CreatedAt uint64 `json:"created_at"`
		Streaming bool   `json:"streaming"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Role != "user" && value.Role != "assistant" && value.Role != "system" {
		return fmt.Errorf("unknown message role %q", value.Role)
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	if turnID, present := object["turn_id"]; present && !bytes.Equal(bytes.TrimSpace(turnID), []byte("null")) {
		var id UUID
		if err := Decode(turnID, &id); err != nil {
			return fmt.Errorf("turn_id: %w", err)
		}
	}
	return validateAttachmentsField(object)
}

func validateQueuedMessage(raw []byte) error {
	if err := requireJSONFields(raw, "id", "content", "created_at"); err != nil {
		return err
	}
	var value struct {
		ID        UUID   `json:"id"`
		Content   string `json:"content"`
		CreatedAt uint64 `json:"created_at"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	return validateAttachmentsField(object)
}

func validateCheckpoint(raw []byte) error {
	if err := requireJSONFields(raw, "turn_count", "git_ref", "status", "created_at"); err != nil {
		return err
	}
	var value struct {
		TurnCount uint64            `json:"turn_count"`
		GitRef    string            `json:"git_ref"`
		Status    string            `json:"status"`
		CreatedAt uint64            `json:"created_at"`
		Files     []json.RawMessage `json:"files"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Status != "ready" && value.Status != "unavailable" && value.Status != "error" {
		return fmt.Errorf("unknown checkpoint status %q", value.Status)
	}
	var object map[string]json.RawMessage
	_ = Decode(raw, &object)
	if err := rejectNullFields(object, "files", "additions", "deletions"); err != nil {
		return err
	}
	if files, present := object["files"]; present {
		if bytes.Equal(bytes.TrimSpace(files), []byte("null")) {
			return fmt.Errorf("files is null")
		}
		for i, file := range value.Files {
			if err := requireJSONFields(file, "path", "additions", "deletions"); err != nil {
				return fmt.Errorf("files[%d]: %w", i, err)
			}
			var typed struct {
				Path      string `json:"path"`
				Additions uint64 `json:"additions"`
				Deletions uint64 `json:"deletions"`
			}
			if err := Decode(file, &typed); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAgentTurn(raw []byte) error {
	if err := requireJSONFields(raw, "id", "turn_count", "status", "started_at"); err != nil {
		return err
	}
	var value struct {
		ID          UUID    `json:"id"`
		TurnCount   uint64  `json:"turn_count"`
		Status      string  `json:"status"`
		StartedAt   uint64  `json:"started_at"`
		CompletedAt *uint64 `json:"completed_at"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Status != "running" && value.Status != "completed" && value.Status != "failed" && value.Status != "interrupted" {
		return fmt.Errorf("unknown turn status %q", value.Status)
	}
	var object map[string]json.RawMessage
	_ = Decode(raw, &object)
	if err := rejectNullFields(object, "provider_turn_started"); err != nil {
		return err
	}
	if checkpoint, present := object["checkpoint"]; present && !bytes.Equal(bytes.TrimSpace(checkpoint), []byte("null")) {
		return validateCheckpoint(checkpoint)
	}
	return nil
}

func validateTranscriptBlock(raw []byte) error {
	if err := requireJSONFields(raw, "after_message", "content"); err != nil {
		return err
	}
	var value struct {
		AfterMessage uint64          `json:"after_message"`
		TurnID       *UUID           `json:"turn_id"`
		Content      json.RawMessage `json:"content"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := requireJSONFields(value.Content, "kind", "data"); err != nil {
		return fmt.Errorf("content: %w", err)
	}
	var content struct {
		Kind string          `json:"kind"`
		Data json.RawMessage `json:"data"`
	}
	if err := Decode(value.Content, &content); err != nil {
		return err
	}
	switch content.Kind {
	case "reasoning":
		if err := requireJSONFields(content.Data, "content", "started_at_ms", "finished_at_ms"); err != nil {
			return err
		}
		var reasoning struct {
			Content      string `json:"content"`
			StartedAtMS  uint64 `json:"started_at_ms"`
			FinishedAtMS uint64 `json:"finished_at_ms"`
		}
		return Decode(content.Data, &reasoning)
	case "activities":
		var activities []json.RawMessage
		if err := Decode(content.Data, &activities); err != nil {
			return err
		}
		for i, activity := range activities {
			var typed RichActivityPayload
			if err := Decode(activity, &typed); err != nil {
				return fmt.Errorf("activities[%d]: %w", i, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown transcript content kind %q", content.Kind)
	}
}

func validateProviderSessionHistory(raw []byte) error {
	if err := requireJSONObject(raw, "history"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	for field, validate := range map[string]func([]byte) error{"messages": validateMessage, "turns": validateAgentTurn} {
		value, present := object[field]
		if !present {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("field %q is null", field)
		}
		var entries []json.RawMessage
		if err := Decode(value, &entries); err != nil {
			return fmt.Errorf("field %q: %w", field, err)
		}
		for i, entry := range entries {
			if err := validate(entry); err != nil {
				return fmt.Errorf("%s[%d]: %w", field, i, err)
			}
		}
	}
	return nil
}

func validateUsageHistory(raw []byte) error {
	fields := []string{"window", "sinceDay", "untilDay", "totals", "totalTokens", "costUsd", "records", "sessions", "providers", "models", "daily", "months", "projects", "quality", "pricing", "scannedFiles", "skippedFiles", "errors", "scanDuration"}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	var value struct {
		Window       json.RawMessage   `json:"window"`
		SinceDay     string            `json:"sinceDay"`
		UntilDay     string            `json:"untilDay"`
		Totals       json.RawMessage   `json:"totals"`
		TotalTokens  uint64            `json:"totalTokens"`
		CostUSD      float64           `json:"costUsd"`
		Records      uint64            `json:"records"`
		Sessions     uint64            `json:"sessions"`
		Providers    []json.RawMessage `json:"providers"`
		Models       []json.RawMessage `json:"models"`
		Daily        []json.RawMessage `json:"daily"`
		Months       []json.RawMessage `json:"months"`
		Projects     []json.RawMessage `json:"projects"`
		Quality      json.RawMessage   `json:"quality"`
		Pricing      string            `json:"pricing"`
		ScannedFiles uint64            `json:"scannedFiles"`
		SkippedFiles uint64            `json:"skippedFiles"`
		Errors       []string          `json:"errors"`
		ScanDuration json.RawMessage   `json:"scanDuration"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	for field, date := range map[string]string{"sinceDay": value.SinceDay, "untilDay": value.UntilDay} {
		if _, err := time.Parse("2006-01-02", date); err != nil {
			return fmt.Errorf("invalid %s %q", field, date)
		}
	}
	if err := validateUsageWindow(value.Window); err != nil {
		return fmt.Errorf("window: %w", err)
	}
	if value.Pricing != "fresh" && value.Pricing != "cached" && value.Pricing != "unavailable" {
		return fmt.Errorf("unknown pricing status %q", value.Pricing)
	}
	if err := requireJSONFields(value.Totals, "uncachedInput", "cachedInput", "cacheCreation", "output", "reasoning"); err != nil {
		return fmt.Errorf("totals: %w", err)
	}
	var totals struct {
		UncachedInput uint64 `json:"uncachedInput"`
		CachedInput   uint64 `json:"cachedInput"`
		CacheCreation uint64 `json:"cacheCreation"`
		Output        uint64 `json:"output"`
		Reasoning     uint64 `json:"reasoning"`
	}
	if err := Decode(value.Totals, &totals); err != nil {
		return err
	}
	if err := requireJSONFields(value.Quality, "providerReportedShare", "modelPricedShare", "unpricedShare", "cacheSavingsUsd"); err != nil {
		return fmt.Errorf("quality: %w", err)
	}
	var quality struct {
		ProviderReportedShare float64 `json:"providerReportedShare"`
		ModelPricedShare      float64 `json:"modelPricedShare"`
		UnpricedShare         float64 `json:"unpricedShare"`
		CacheSavingsUSD       float64 `json:"cacheSavingsUsd"`
	}
	if err := Decode(value.Quality, &quality); err != nil {
		return err
	}
	if err := requireJSONFields(value.ScanDuration, "secs", "nanos"); err != nil {
		return fmt.Errorf("scanDuration: %w", err)
	}
	var duration struct {
		Secs  uint64 `json:"secs"`
		Nanos uint32 `json:"nanos"`
	}
	if err := Decode(value.ScanDuration, &duration); err != nil {
		return err
	}
	for name, entries := range map[string][]json.RawMessage{"providers": value.Providers, "models": value.Models, "daily": value.Daily, "months": value.Months, "projects": value.Projects} {
		if err := requireObjectElements(entries, name); err != nil {
			return err
		}
		for i, entry := range entries {
			if err := validateUsageSlice(name, entry); err != nil {
				return fmt.Errorf("%s[%d]: %w", name, i, err)
			}
		}
	}
	return nil
}

func validateUsageSlice(kind string, raw []byte) error {
	required := map[string][]string{
		"providers": {"provider", "costUsd", "totalTokens", "costShare", "tokenShare"},
		"models":    {"provider", "model", "costUsd", "totalTokens", "costShare"},
		"daily":     {"day", "costUsd", "totalTokens", "byProvider"},
		"months":    {"firstDay", "costUsd", "totalTokens", "byProvider", "sessions", "activeDays", "topModels"},
		"projects":  {"path", "costUsd", "totalTokens", "byProvider", "sessions", "costShare", "topModels"},
	}[kind]
	if err := requireJSONFields(raw, required...); err != nil {
		return err
	}
	var value struct {
		Provider    string            `json:"provider"`
		Model       string            `json:"model"`
		Path        string            `json:"path"`
		Day         string            `json:"day"`
		FirstDay    string            `json:"firstDay"`
		CostUSD     float64           `json:"costUsd"`
		TotalTokens uint64            `json:"totalTokens"`
		CostShare   float64           `json:"costShare"`
		TokenShare  float64           `json:"tokenShare"`
		ByProvider  []json.RawMessage `json:"byProvider"`
		Sessions    uint64            `json:"sessions"`
		ActiveDays  uint32            `json:"activeDays"`
		TopModels   []json.RawMessage `json:"topModels"`
		LastDay     *string           `json:"lastDay"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if kind == "providers" || kind == "models" {
		if value.Provider != "claude" && value.Provider != "codex" {
			return fmt.Errorf("unknown usage provider %q", value.Provider)
		}
	}
	date := value.Day
	if kind == "months" {
		date = value.FirstDay
	}
	if date != "" {
		if _, err := time.Parse("2006-01-02", date); err != nil {
			return fmt.Errorf("invalid date %q", date)
		}
	}
	if value.LastDay != nil {
		if _, err := time.Parse("2006-01-02", *value.LastDay); err != nil {
			return fmt.Errorf("invalid lastDay %q", *value.LastDay)
		}
	}
	if kind == "daily" || kind == "months" || kind == "projects" {
		if len(value.ByProvider) != 2 {
			return fmt.Errorf("byProvider has %d entries, want 2", len(value.ByProvider))
		}
		for i, day := range value.ByProvider {
			if err := requireJSONFields(day, "costUsd", "totalTokens"); err != nil {
				return fmt.Errorf("byProvider[%d]: %w", i, err)
			}
			var typed struct {
				CostUSD     float64 `json:"costUsd"`
				TotalTokens uint64  `json:"totalTokens"`
			}
			if err := Decode(day, &typed); err != nil {
				return err
			}
		}
	}
	if kind == "months" || kind == "projects" {
		for i, tuple := range value.TopModels {
			var parts []json.RawMessage
			if err := Decode(tuple, &parts); err != nil || len(parts) != 2 {
				return fmt.Errorf("topModels[%d] is not a pair", i)
			}
			var model string
			var cost float64
			if err := Decode(parts[0], &model); err != nil {
				return err
			}
			if err := Decode(parts[1], &cost); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateWorkspaceOperation(raw []byte) error {
	if err := requireJSONFields(raw, "type"); err != nil {
		return err
	}
	var v struct {
		Type     string `json:"type"`
		Provider string `json:"provider"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	required := map[string][]string{"listTree": {"root", "expanded_paths"}, "browseDirectory": {}, "readTextFile": {"root", "relative_path"}, "writeTextFile": {"root", "relative_path", "content"}, "listProjectFiles": {"root", "cap"}, "discoverSlashCommands": {"provider", "project_root"}, "createProjectlessWorkspace": {}, "migrateProjectlessWorkspace": {"path"}, "inspectBranches": {"cwd"}, "checkoutBranch": {"cwd", "branch", "create"}, "createWorktree": {"project_path", "project_id", "session_id", "prompt"}, "inspectCommit": {"cwd"}, "generateCommitMessage": {"cwd", "include_unstaged", "invocation"}, "commit": {"cwd", "message", "include_unstaged", "push"}, "push": {"cwd"}, "captureTurnStart": {"cwd", "session_id", "turn_count"}, "captureTurn": {"cwd", "session_id", "turn_count"}, "captureRef": {"cwd", "git_ref"}, "restoreRef": {"cwd", "git_ref"}, "hasRef": {"cwd", "git_ref"}, "sessionTurnRefs": {"cwd", "session_id"}, "deleteRef": {"cwd", "git_ref"}, "deleteTurnRefsAfter": {"cwd", "session_id", "retained_turn_count", "previous_turn_count"}, "deleteSessionRefs": {"cwd", "session_id"}, "copySessionRefs": {"cwd", "source_session_id", "target_session_id", "through_turn_count"}, "collectReviewDiff": {"cwd", "source"}}
	fields, ok := required[v.Type]
	if !ok {
		return fmt.Errorf("unknown workspace operation %q", v.Type)
	}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	stringFields := map[string]bool{"root": true, "relative_path": true, "content": true, "project_root": true, "path": true, "cwd": true, "branch": true, "project_path": true, "prompt": true, "message": true, "git_ref": true}
	boolFields := map[string]bool{"create": true, "include_unstaged": true, "push": true}
	uintFields := map[string]bool{"cap": true, "turn_count": true, "retained_turn_count": true, "previous_turn_count": true, "through_turn_count": true}
	uuidFields := map[string]bool{"project_id": true, "session_id": true, "source_session_id": true, "target_session_id": true}
	for _, field := range fields {
		switch {
		case stringFields[field]:
			var x string
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case boolFields[field]:
			var x bool
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case uintFields[field]:
			var x uint64
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case uuidFields[field]:
			var x UUID
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case field == "expanded_paths":
			var x []string
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case field == "invocation":
			var x json.RawMessage
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
			if err := validateAgentInvocation(x); err != nil {
				return fmt.Errorf("invocation: %w", err)
			}
		case field == "source":
			var x json.RawMessage
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
			if err := validateReviewDiffSource(x); err != nil {
				return fmt.Errorf("source: %w", err)
			}
		}
	}
	if v.Type == "discoverSlashCommands" {
		if err := validateProviderKind(v.Provider); err != nil {
			return err
		}
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	for _, field := range []string{"path", "prompt", "binary_override", "base_branch"} {
		if value, present := object[field]; present && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			var text string
			if err := Decode(value, &text); err != nil {
				return fmt.Errorf("field %q: %w", field, err)
			}
		}
	}
	return nil
}

func validateAgentInvocation(raw []byte) error {
	if err := requireJSONFields(raw, "provider", "binary"); err != nil {
		return err
	}
	var value struct {
		Provider        string  `json:"provider"`
		Binary          string  `json:"binary"`
		Model           *string `json:"model"`
		ReasoningEffort *string `json:"reasoning_effort"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	return validateProviderKind(value.Provider)
}

func validateReviewDiffSource(raw []byte) error {
	var unit string
	if err := Decode(raw, &unit); err == nil {
		switch unit {
		case "uncommitted", "unstaged", "staged", "committed", "branch":
			return nil
		default:
			return fmt.Errorf("unknown review diff source %q", unit)
		}
	}
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	lastTurn, ok := object["lastTurn"]
	if !ok || len(object) != 1 {
		return fmt.Errorf("invalid review diff source")
	}
	if err := requireJSONFields(lastTurn, "session_id", "turn_id", "turn_count"); err != nil {
		return err
	}
	var value struct {
		SessionID UUID   `json:"session_id"`
		TurnID    UUID   `json:"turn_id"`
		TurnCount uint64 `json:"turn_count"`
	}
	return Decode(lastTurn, &value)
}

func validateWorkspaceResult(raw []byte) error {
	if err := requireJSONFields(raw, "type"); err != nil {
		return err
	}
	var v struct {
		Type string `json:"type"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	required := map[string][]string{"ack": {}, "workingTree": {"entries"}, "directory": {"path", "home", "filesystem_root", "entries"}, "textFile": {"content"}, "projectFiles": {"entries"}, "slashCommands": {"commands"}, "projectlessWorkspace": {"cwd"}, "branches": {}, "branchChanged": {"snapshot"}, "worktreeCreated": {"worktree"}, "commitSnapshot": {"snapshot"}, "commitMessage": {"message"}, "checkpoint": {"checkpoint"}, "bool": {"value"}, "turnRefs": {"turn_counts"}, "reviewDiff": {"data"}}
	fields, ok := required[v.Type]
	if !ok {
		return fmt.Errorf("unknown workspace result %q", v.Type)
	}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	switch v.Type {
	case "ack":
		return nil
	case "workingTree":
		return validateRawArrayField(raw, "entries", validateWorkingTreeEntry)
	case "directory":
		var value struct {
			Path           string  `json:"path"`
			Parent         *string `json:"parent"`
			Home           string  `json:"home"`
			FilesystemRoot string  `json:"filesystem_root"`
		}
		if err := Decode(raw, &value); err != nil {
			return err
		}
		return validateRawArrayField(raw, "entries", validateWorkingTreeEntry)
	case "textFile":
		var value struct {
			Content string `json:"content"`
		}
		return Decode(raw, &value)
	case "projectFiles":
		return validateRawArrayField(raw, "entries", validateFileEntry)
	case "slashCommands":
		return validateRawArrayField(raw, "commands", validateSlashCommand)
	case "projectlessWorkspace":
		var value struct {
			CWD string `json:"cwd"`
		}
		return Decode(raw, &value)
	case "branches":
		return validateOptionalObjectField(raw, "snapshot", validateBranchSnapshot)
	case "branchChanged":
		return validateObjectField(raw, "snapshot", validateBranchSnapshot)
	case "worktreeCreated":
		return validateObjectField(raw, "worktree", validateCreatedWorktree)
	case "commitSnapshot":
		return validateObjectField(raw, "snapshot", validateCommitSnapshot)
	case "commitMessage":
		var value struct {
			Message string `json:"message"`
		}
		return Decode(raw, &value)
	case "checkpoint":
		return validateObjectField(raw, "checkpoint", validateCheckpoint)
	case "bool":
		var value struct {
			Value bool `json:"value"`
		}
		return Decode(raw, &value)
	case "turnRefs":
		var value struct {
			TurnCounts []uint64 `json:"turn_counts"`
		}
		return Decode(raw, &value)
	case "reviewDiff":
		return validateObjectField(raw, "data", validateReviewDiffData)
	default:
		panic("validated workspace result missing decoder")
	}
}

func validateRawArrayField(raw []byte, field string, validate func([]byte) error) error {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	var entries []json.RawMessage
	if err := Decode(object[field], &entries); err != nil {
		return fmt.Errorf("field %q: %w", field, err)
	}
	for i, entry := range entries {
		if err := validate(entry); err != nil {
			return fmt.Errorf("%s[%d]: %w", field, i, err)
		}
	}
	return nil
}

func validateObjectField(raw []byte, field string, validate func([]byte) error) error {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	return validate(object[field])
}

func validateOptionalObjectField(raw []byte, field string, validate func([]byte) error) error {
	var object map[string]json.RawMessage
	if err := Decode(raw, &object); err != nil {
		return err
	}
	value, present := object[field]
	if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return nil
	}
	return validate(value)
}

func validateWorkingTreeEntry(raw []byte) error {
	if err := requireJSONFields(raw, "relativePath", "absolutePath", "name", "isDir", "expanded", "depth"); err != nil {
		return err
	}
	var value struct {
		RelativePath string `json:"relativePath"`
		AbsolutePath string `json:"absolutePath"`
		Name         string `json:"name"`
		IsDir        bool   `json:"isDir"`
		Expanded     bool   `json:"expanded"`
		Depth        uint64 `json:"depth"`
	}
	return Decode(raw, &value)
}

func validateFileEntry(raw []byte) error {
	if err := requireJSONFields(raw, "path", "is_dir"); err != nil {
		return err
	}
	var value struct {
		Path  string `json:"path"`
		IsDir bool   `json:"is_dir"`
	}
	return Decode(raw, &value)
}

func validateSlashCommand(raw []byte) error {
	if err := requireJSONFields(raw, "name", "description", "scope"); err != nil {
		return err
	}
	var value struct {
		Name         string  `json:"name"`
		Description  string  `json:"description"`
		Scope        string  `json:"scope"`
		ArgumentHint *string `json:"argument_hint"`
		Template     *string `json:"template"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	switch value.Scope {
	case "Project", "User", "Skill", "Builtin", "Waku":
		return nil
	default:
		return fmt.Errorf("unknown command scope %q", value.Scope)
	}
}

func validateBranchSnapshot(raw []byte) error {
	if err := requireJSONFields(raw, "repository", "branches", "additions", "deletions"); err != nil {
		return err
	}
	var value struct {
		Repository string            `json:"repository"`
		Branches   []json.RawMessage `json:"branches"`
		Additions  uint64            `json:"additions"`
		Deletions  uint64            `json:"deletions"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	for i, branch := range value.Branches {
		if err := requireJSONFields(branch, "name", "checked_out_elsewhere"); err != nil {
			return fmt.Errorf("branches[%d]: %w", i, err)
		}
		var typed struct {
			Name                string `json:"name"`
			CheckedOutElsewhere bool   `json:"checked_out_elsewhere"`
		}
		if err := Decode(branch, &typed); err != nil {
			return err
		}
	}
	return nil
}

func validateCreatedWorktree(raw []byte) error {
	if err := requireJSONFields(raw, "path", "branch"); err != nil {
		return err
	}
	var value struct {
		Path   string `json:"path"`
		Branch string `json:"branch"`
	}
	return Decode(raw, &value)
}

func validateCommitSnapshot(raw []byte) error {
	fields := []string{"branch", "additions", "deletions", "staged_additions", "staged_deletions", "has_staged", "has_unstaged", "can_push"}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	var value struct {
		Branch          string `json:"branch"`
		Additions       uint64 `json:"additions"`
		Deletions       uint64 `json:"deletions"`
		StagedAdditions uint64 `json:"staged_additions"`
		StagedDeletions uint64 `json:"staged_deletions"`
		HasStaged       bool   `json:"has_staged"`
		HasUnstaged     bool   `json:"has_unstaged"`
		CanPush         bool   `json:"can_push"`
	}
	return Decode(raw, &value)
}

func validateReviewDiffData(raw []byte) error {
	if err := requireJSONFields(raw, "source", "numstat", "patch", "completeContext"); err != nil {
		return err
	}
	var value struct {
		Source          json.RawMessage `json:"source"`
		Numstat         string          `json:"numstat"`
		Patch           string          `json:"patch"`
		CompleteContext bool            `json:"completeContext"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	return validateReviewDiffSource(value.Source)
}

func validateBackgroundKey(raw []byte) error {
	if err := requireJSONFields(raw, "kind", "providerId"); err != nil {
		return err
	}
	var value struct {
		Kind       string `json:"kind"`
		ProviderID string `json:"providerId"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Kind != "process" && value.Kind != "monitor" && value.Kind != "subagent" {
		return fmt.Errorf("unknown background work kind %q", value.Kind)
	}
	return nil
}

func validateBackgroundItem(raw []byte) error {
	fields := []string{"key", "title", "outputTruncated", "startedAtMs", "updatedAtMs", "background", "canStop", "status"}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	var value struct {
		Key             json.RawMessage `json:"key"`
		Title           string          `json:"title"`
		OutputTruncated bool            `json:"outputTruncated"`
		StartedAtMS     uint64          `json:"startedAtMs"`
		UpdatedAtMS     uint64          `json:"updatedAtMs"`
		Background      bool            `json:"background"`
		CanStop         bool            `json:"canStop"`
		Status          string          `json:"status"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if err := validateBackgroundKey(value.Key); err != nil {
		return err
	}
	switch value.Status {
	case "starting", "running", "monitoring", "stopping", "completed", "failed", "stopped", "lost":
		return nil
	default:
		return fmt.Errorf("unknown background status %q", value.Status)
	}
}
func validateProviderForkRequest(raw []byte) error {
	if err := requireJSONFields(raw, "provider"); err != nil {
		return err
	}
	var v struct {
		Provider string `json:"provider"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	required := map[string][]string{"claude": {"session_id", "turn_count", "title"}, "amp": {"binary", "cwd", "thread_id", "turn_count"}, "cursor": {"source", "turn_count"}, "openCode": {"binary", "session_id", "turn_count"}, "openCode2": {"binary", "session_id", "turn_count"}, "grok": {"binary", "cwd", "session_id", "turn_count"}}
	fields, ok := required[v.Provider]
	if !ok {
		return fmt.Errorf("unsupported fork provider %q", v.Provider)
	}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	for _, field := range fields {
		switch field {
		case "turn_count":
			var x uint64
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		case "source":
			var x map[string]json.RawMessage
			if err := decodeJSONField(raw, field, &x); err != nil || x == nil {
				if err != nil {
					return err
				}
				return fmt.Errorf("fork source is not an object")
			}
		default:
			var x string
			if err := decodeJSONField(raw, field, &x); err != nil {
				return err
			}
		}
	}
	return nil
}
func validateBackgroundWork(raw []byte) error {
	if err := requireJSONFields(raw, "type"); err != nil {
		return err
	}
	var v struct {
		Type string `json:"type"`
	}
	if err := Decode(raw, &v); err != nil {
		return err
	}
	required := map[string][]string{"upsert": {"key", "title", "outputTruncated", "startedAtMs", "updatedAtMs", "background", "canStop", "status"}, "outputDelta": {"key", "delta"}, "reconcileProcesses": {"items"}, "reconcileLive": {"items"}, "stopRequested": {"kind", "providerId"}, "stopFailed": {"key", "message"}}
	fields, ok := required[v.Type]
	if !ok {
		return fmt.Errorf("unknown background work event %q", v.Type)
	}
	if err := requireJSONFields(raw, fields...); err != nil {
		return err
	}
	switch v.Type {
	case "upsert":
		return validateBackgroundItem(raw)
	case "outputDelta", "stopFailed":
		var value struct {
			Key json.RawMessage `json:"key"`
		}
		if err := Decode(raw, &value); err != nil {
			return err
		}
		return validateBackgroundKey(value.Key)
	case "reconcileProcesses", "reconcileLive":
		var value struct {
			Items []json.RawMessage `json:"items"`
		}
		if err := Decode(raw, &value); err != nil {
			return err
		}
		for i, item := range value.Items {
			if err := validateBackgroundItem(item); err != nil {
				return fmt.Errorf("items[%d]: %w", i, err)
			}
		}
		return nil
	case "stopRequested":
		return validateBackgroundKey(raw)
	}
	return nil
}

type ResponsePayloadEnvelope struct {
	Type string `json:"type"`
}

type ResponsePayloadType string

const (
	ResponseAck                    ResponsePayloadType = "ack"
	ResponseSessionRuntime         ResponsePayloadType = "sessionRuntime"
	ResponseStarted                ResponsePayloadType = "started"
	ResponseOptionsApplied         ResponsePayloadType = "optionsApplied"
	ResponseCursor                 ResponsePayloadType = "cursor"
	ResponseSettings               ResponsePayloadType = "settings"
	ResponseProviderProbe          ResponsePayloadType = "providerProbe"
	ResponsePlanUsage              ResponsePayloadType = "planUsage"
	ResponseComputerPermissions    ResponsePayloadType = "computerPermissions"
	ResponseUsageHistory           ResponsePayloadType = "usageHistory"
	ResponseSkillsCatalog          ResponsePayloadType = "skillsCatalog"
	ResponseTaskState              ResponsePayloadType = "taskState"
	ResponseTaskStateSaved         ResponsePayloadType = "taskStateSaved"
	ResponseSession                ResponsePayloadType = "session"
	ResponseSessionMessageMatches  ResponsePayloadType = "sessionMessageMatches"
	ResponseProviderSessions       ResponsePayloadType = "providerSessions"
	ResponseProviderSessionHistory ResponsePayloadType = "providerSessionHistory"
	ResponseComposerDrafts         ResponsePayloadType = "composerDrafts"
	ResponseBlobStored             ResponsePayloadType = "blobStored"
	ResponseAttachmentStored       ResponsePayloadType = "attachmentStored"
	ResponseBlobData               ResponsePayloadType = "blobData"
	ResponseProviderSessionForked  ResponsePayloadType = "providerSessionForked"
	ResponseSessionForked          ResponsePayloadType = "sessionForked"
	ResponseSessionRewound         ResponsePayloadType = "sessionRewound"
	ResponseWorkspace              ResponsePayloadType = "workspace"
)

type EventKind string

const (
	EventConnected           EventKind = "connected"
	EventAgentPresetSelected EventKind = "agentPresetSelected"
	EventAutoTitleUpdated    EventKind = "autoTitleUpdated"
	EventAvailableCommands   EventKind = "availableCommands"
	EventTurnStarted         EventKind = "turnStarted"
	EventTurnParked          EventKind = "turnParked"
	EventTextDelta           EventKind = "textDelta"
	EventReasoningDelta      EventKind = "reasoningDelta"
	EventActivity            EventKind = "activity"
	EventRichActivity        EventKind = "richActivity"
	EventBackgroundWork      EventKind = "backgroundWork"
	EventPermission          EventKind = "permission"
	EventUserInputRequested  EventKind = "userInputRequested"
	EventComputerUseUpdated  EventKind = "computerUseUpdated"
	EventPromptSubmitted     EventKind = "promptSubmitted"
	EventSteerAccepted       EventKind = "steerAccepted"
	EventSteerRejected       EventKind = "steerRejected"
	EventUsageUpdated        EventKind = "usageUpdated"
	EventPlanUsageUpdated    EventKind = "planUsageUpdated"
	EventGoalUpdated         EventKind = "goalUpdated"
	EventTurnFinished        EventKind = "turnFinished"
	EventError               EventKind = "error"
	EventProcessExited       EventKind = "processExited"
	EventTerminalOutput      EventKind = "terminalOutput"
	EventTerminalExited      EventKind = "terminalExited"
	EventTerminalError       EventKind = "terminalError"
)

type ActivityPayload struct {
	ID       *string `json:"id"`
	Kind     string  `json:"kind"`
	Title    string  `json:"title"`
	Detail   *string `json:"detail"`
	Complete bool    `json:"complete"`
}
type RichActivityPayload struct {
	ID                 UUID              `json:"id"`
	SourceID           *string           `json:"source_id"`
	Kind               string            `json:"kind"`
	Title              string            `json:"title"`
	ToolName           *string           `json:"tool_name"`
	MCPServer          *string           `json:"mcp_server"`
	Detail             *string           `json:"detail"`
	Arguments          *string           `json:"arguments"`
	Output             *string           `json:"output"`
	ImageURLs          []string          `json:"image_urls"`
	Failed             bool              `json:"failed"`
	Complete           bool              `json:"complete"`
	FileChanges        []json.RawMessage `json:"file_changes"`
	DisplayTarget      *string           `json:"display_target"`
	DisplayDescription *string           `json:"display_description"`
	Reasoning          json.RawMessage   `json:"reasoning"`
}

func (a *RichActivityPayload) UnmarshalJSON(data []byte) error {
	type alias RichActivityPayload
	if err := requireJSONFields(data, "id", "kind", "title", "complete"); err != nil {
		return err
	}
	var value alias
	if err := Decode(data, &value); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(data, &object); err != nil {
		return err
	}
	if err := rejectNullFields(object, "image_urls", "failed", "file_changes"); err != nil {
		return err
	}
	switch value.Kind {
	case "reasoning", "command", "fileChange", "fileRead", "fileSearch", "fileList", "search", "plan", "tool":
	default:
		return fmt.Errorf("unknown activity kind %q", value.Kind)
	}
	for i, change := range value.FileChanges {
		if err := validateActivityFileChange(change); err != nil {
			return fmt.Errorf("file_changes[%d]: %w", i, err)
		}
	}
	if reasoning, present := object["reasoning"]; present && !bytes.Equal(bytes.TrimSpace(reasoning), []byte("null")) {
		if err := requireJSONFields(reasoning, "content", "started_at_ms", "finished_at_ms"); err != nil {
			return fmt.Errorf("reasoning: %w", err)
		}
		var typed struct {
			Content      string `json:"content"`
			StartedAtMS  uint64 `json:"started_at_ms"`
			FinishedAtMS uint64 `json:"finished_at_ms"`
		}
		if err := Decode(reasoning, &typed); err != nil {
			return err
		}
	}
	*a = RichActivityPayload(value)
	return nil
}

func validateActivityFileChange(raw []byte) error {
	if err := requireJSONFields(raw, "path"); err != nil {
		return err
	}
	var value struct {
		Path      string  `json:"path"`
		Additions *uint64 `json:"additions"`
		Deletions *uint64 `json:"deletions"`
		Status    *string `json:"status"`
		Diff      *string `json:"diff"`
	}
	if err := Decode(raw, &value); err != nil {
		return err
	}
	if value.Status != nil && *value.Status != "added" && *value.Status != "modified" && *value.Status != "deleted" {
		return fmt.Errorf("unknown file change status %q", *value.Status)
	}
	return nil
}

type PermissionPayload struct {
	RequestID string                    `json:"requestId"`
	Title     string                    `json:"title"`
	Detail    string                    `json:"detail"`
	Options   []PermissionOptionPayload `json:"options"`
}
type UserInputRequestedPayload struct {
	RequestID string                     `json:"requestId"`
	Questions []UserInputQuestionPayload `json:"questions"`
}
type PermissionOptionPayload struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Allow bool   `json:"allow"`
}

func (p *PermissionOptionPayload) UnmarshalJSON(data []byte) error {
	type alias PermissionOptionPayload
	if err := requireJSONFields(data, "id", "label", "allow"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*p = PermissionOptionPayload(v)
	return nil
}

type UserInputOptionPayload struct {
	Label       string  `json:"label"`
	Description *string `json:"description"`
}

func (o *UserInputOptionPayload) UnmarshalJSON(data []byte) error {
	type alias UserInputOptionPayload
	if err := requireJSONFields(data, "label"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*o = UserInputOptionPayload(v)
	return nil
}

type UserInputQuestionPayload struct {
	ID          string                   `json:"id"`
	Header      string                   `json:"header"`
	Question    string                   `json:"question"`
	Options     []UserInputOptionPayload `json:"options"`
	MultiSelect bool                     `json:"multiSelect"`
}

func (q *UserInputQuestionPayload) UnmarshalJSON(data []byte) error {
	type alias UserInputQuestionPayload
	if err := requireJSONFields(data, "id", "header", "question"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(data, &object); err != nil {
		return err
	}
	for _, field := range []string{"options", "multiSelect"} {
		if raw, ok := object[field]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("question field %q is null", field)
		}
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*q = UserInputQuestionPayload(v)
	return nil
}

type ReportedCommandPayload struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (c *ReportedCommandPayload) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("reported command is null")
	}
	var name string
	if Decode(data, &name) == nil {
		c.Name = name
		return nil
	}
	type alias ReportedCommandPayload
	if err := requireJSONFields(data, "name"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := Decode(data, &object); err != nil {
		return err
	}
	if raw, ok := object["description"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("reported command description is null")
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*c = ReportedCommandPayload(v)
	return nil
}

type PromptSubmittedPayload struct {
	Message   string `json:"message"`
	TurnID    UUID   `json:"turnId"`
	MessageID UUID   `json:"messageId"`
}
type SteerAcceptedPayload struct {
	Message string `json:"message"`
}
type SteerRejectedPayload struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
}
type UsageUpdatedPayload struct {
	ContextTokens *uint64 `json:"contextTokens"`
	ContextWindow *uint64 `json:"contextWindow"`
}
type TurnFinishedPayload struct {
	Success bool    `json:"success"`
	Summary *string `json:"summary"`
}
type TerminalOutputPayload struct {
	Data Base64Bytes `json:"data"`
}
type ComputerUsePayload struct {
	Target   *ComputerTargetPayload `json:"target"`
	Phase    string                 `json:"phase"`
	Visible  bool                   `json:"visible"`
	ImageURL *string                `json:"imageUrl"`
}
type ComputerTargetPayload struct {
	WindowID    uint64  `json:"windowId"`
	BundleID    string  `json:"bundleId"`
	TeamID      *string `json:"teamId"`
	AppName     string  `json:"appName"`
	WindowTitle string  `json:"windowTitle"`
	Width       uint32  `json:"width"`
	Height      uint32  `json:"height"`
}

func (t *ComputerTargetPayload) UnmarshalJSON(data []byte) error {
	type alias ComputerTargetPayload
	if err := requireJSONFields(data, "windowId", "bundleId", "appName", "windowTitle", "width", "height"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*t = ComputerTargetPayload(v)
	return nil
}

type PlanWindowPayload struct {
	Label    string  `json:"label"`
	Percent  float64 `json:"percent"`
	ResetsAt *int64  `json:"resetsAt"`
}

func (w *PlanWindowPayload) UnmarshalJSON(data []byte) error {
	type alias PlanWindowPayload
	if err := requireJSONFields(data, "label", "percent"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*w = PlanWindowPayload(v)
	return nil
}

type PlanUsageValue struct {
	PlanLabel *string             `json:"planLabel"`
	Windows   []PlanWindowPayload `json:"windows"`
}

func (p *PlanUsageValue) UnmarshalJSON(data []byte) error {
	type alias PlanUsageValue
	if err := requireJSONFields(data, "windows"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	*p = PlanUsageValue(v)
	return nil
}

type ThreadGoalPayload struct {
	Objective       string `json:"objective"`
	Status          string `json:"status"`
	TokenBudget     *int64 `json:"tokenBudget"`
	TokensUsed      int64  `json:"tokensUsed"`
	TimeUsedSeconds int64  `json:"timeUsedSeconds"`
}

func (g *ThreadGoalPayload) UnmarshalJSON(data []byte) error {
	type alias ThreadGoalPayload
	if err := requireJSONFields(data, "objective", "status"); err != nil {
		return err
	}
	var v alias
	if err := Decode(data, &v); err != nil {
		return err
	}
	switch v.Status {
	case "active", "paused", "blocked", "usageLimited", "budgetLimited", "complete":
	default:
		return fmt.Errorf("unknown goal status %q", v.Status)
	}
	*g = ThreadGoalPayload(v)
	return nil
}

func DecodeEventPayloadAny(event WireDriverEvent) (any, error) {
	raw := event.Payload
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	decodeInto := func(dst any) (any, error) {
		if err := Decode(raw, dst); err != nil {
			return nil, err
		}
		return dst, nil
	}
	require := func(fields ...string) error { return requireJSONFields(raw, fields...) }
	isNull := bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
	switch EventKind(event.Kind) {
	case EventTurnStarted, EventTurnParked, EventProcessExited, EventTerminalExited:
		var nullValue any
		if err := Decode(raw, &nullValue); err != nil {
			return nil, err
		}
		if nullValue != nil {
			return nil, fmt.Errorf("%s event payload is not null", event.Kind)
		}
		return struct{}{}, nil
	case EventTextDelta, EventReasoningDelta, EventError, EventTerminalError:
		if isNull {
			return nil, fmt.Errorf("%s event payload is null", event.Kind)
		}
		var v string
		return decodeInto(&v)
	case EventActivity:
		if err := require("kind", "title", "complete"); err != nil {
			return nil, err
		}
		var v ActivityPayload
		value, err := decodeInto(&v)
		if err == nil {
			switch v.Kind {
			case "reasoning", "command", "fileChange", "fileRead", "fileSearch", "fileList", "search", "plan", "tool":
			default:
				err = fmt.Errorf("unknown activity kind %q", v.Kind)
			}
		}
		return value, err
	case EventPermission:
		if err := require("requestId", "title", "detail", "options"); err != nil {
			return nil, err
		}
		var v PermissionPayload
		return decodeInto(&v)
	case EventUserInputRequested:
		if err := require("requestId", "questions"); err != nil {
			return nil, err
		}
		var v UserInputRequestedPayload
		return decodeInto(&v)
	case EventPromptSubmitted:
		if err := require("message", "turnId", "messageId"); err != nil {
			return nil, err
		}
		var v PromptSubmittedPayload
		return decodeInto(&v)
	case EventSteerAccepted:
		if err := require("message"); err != nil {
			return nil, err
		}
		var v SteerAcceptedPayload
		return decodeInto(&v)
	case EventSteerRejected:
		if err := require("message", "reason"); err != nil {
			return nil, err
		}
		var v SteerRejectedPayload
		return decodeInto(&v)
	case EventUsageUpdated:
		if isNull {
			return nil, fmt.Errorf("usageUpdated event payload is null")
		}
		var v UsageUpdatedPayload
		return decodeInto(&v)
	case EventTurnFinished:
		if err := require("success"); err != nil {
			return nil, err
		}
		var v TurnFinishedPayload
		return decodeInto(&v)
	case EventTerminalOutput:
		if err := require("data"); err != nil {
			return nil, err
		}
		var v TerminalOutputPayload
		return decodeInto(&v)
	case EventComputerUseUpdated:
		if err := require("phase", "visible"); err != nil {
			return nil, err
		}
		var v ComputerUsePayload
		value, err := decodeInto(&v)
		if err == nil {
			switch v.Phase {
			case "awaitingApproval", "running", "failed":
			default:
				err = fmt.Errorf("unknown computer use phase %q", v.Phase)
			}
		}
		return value, err
	case EventAgentPresetSelected, EventAutoTitleUpdated:
		var v *string
		return decodeInto(&v)
	case EventAvailableCommands:
		if isNull {
			return nil, fmt.Errorf("availableCommands event payload is null")
		}
		var v []ReportedCommandPayload
		return decodeInto(&v)
	case EventConnected:
		if isNull {
			var v any
			return decodeInto(&v)
		}
		if err := validateProviderCursor(raw); err != nil {
			return nil, err
		}
		return json.RawMessage(bytes.Clone(raw)), nil
	case EventRichActivity:
		var v RichActivityPayload
		return decodeInto(&v)
	case EventBackgroundWork:
		if err := validateBackgroundWork(raw); err != nil {
			return nil, err
		}
		return json.RawMessage(bytes.Clone(raw)), nil
	case EventPlanUsageUpdated:
		var v PlanUsageValue
		return decodeInto(&v)
	case EventGoalUpdated:
		if isNull {
			var v any
			return decodeInto(&v)
		}
		var v ThreadGoalPayload
		return decodeInto(&v)
	default:
		return nil, fmt.Errorf("unknown driver event kind %q", event.Kind)
	}
}

func DecodeResponsePayload[T any](raw json.RawMessage, expected string) (T, error) {
	var zero T
	var envelope ResponsePayloadEnvelope
	if err := Decode(raw, &envelope); err != nil {
		return zero, err
	}
	if envelope.Type != expected {
		return zero, fmt.Errorf("response payload %q, expected %q", envelope.Type, expected)
	}
	var result T
	if err := Decode(raw, &result); err != nil {
		return zero, err
	}
	return result, nil
}
func DecodeEventPayload[T any](event WireDriverEvent) (T, error) {
	var result T
	err := Decode(event.Payload, &result)
	return result, err
}
