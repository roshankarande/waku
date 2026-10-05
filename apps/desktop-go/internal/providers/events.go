package providers

import "encoding/json"

type EventKind string

const (
	EventConnected          EventKind = "connected"
	EventAvailableCommands  EventKind = "availableCommands"
	EventAutoTitleUpdated   EventKind = "autoTitleUpdated"
	EventTurnStarted        EventKind = "turnStarted"
	EventTextDelta          EventKind = "textDelta"
	EventReasoningDelta     EventKind = "reasoningDelta"
	EventActivity           EventKind = "activity"
	EventBackgroundWork     EventKind = "backgroundWork"
	EventPermission         EventKind = "permission"
	EventUserInputRequested EventKind = "userInputRequested"
	EventSteerAccepted      EventKind = "steerAccepted"
	EventSteerRejected      EventKind = "steerRejected"
	EventUsageUpdated       EventKind = "usageUpdated"
	EventPlanUsageUpdated   EventKind = "planUsageUpdated"
	EventGoalUpdated        EventKind = "goalUpdated"
	EventTurnFinished       EventKind = "turnFinished"
	EventError              EventKind = "error"
	EventProcessExited      EventKind = "processExited"
)

// Event keeps frequently consumed fields typed and retains the native payload
// verbatim for provider-specific details. Raw is never decoded through any.
type Event struct {
	Kind          EventKind
	Text          string
	RequestID     json.RawMessage
	Cursor        json.RawMessage
	Activity      *Activity
	Background    *BackgroundWork
	Permission    *Permission
	Questions     []UserInputQuestion
	Goal          json.RawMessage
	GoalPresent   bool
	Success       bool
	Summary       string
	ContextTokens *uint64
	ContextWindow *uint64
	Raw           json.RawMessage
}

type Activity struct {
	ID, Kind, Title, Detail string
	Tool, MCPServer         string
	Complete, Failed        bool
	Arguments, Output       json.RawMessage
}

type BackgroundWork struct {
	ID, Kind, Status, Title string
	ControlID, ParentID     string
	OutputDelta             string
	Raw                     json.RawMessage
}

type Permission struct {
	Title, Detail string
	Options       []PermissionOption
}

type PermissionOption struct {
	ID, Label string
	Allow     bool
}

type UserInputQuestion struct {
	ID, Header, Question string
	Options              []UserInputOption
	MultiSelect          bool
}

type UserInputOption struct{ Label, Description string }
