// Package providers owns provider process and network transports.  It does not
// know about presentation, persistence, or the daemon wire protocol.
package providers

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	ErrUnsupported = errors.New("provider operation is not supported")
	ErrClosed      = errors.New("provider session is closed")
)

type Provider string

const (
	ProviderCodex    Provider = "codex"
	ProviderCursor   Provider = "cursor"
	ProviderFx       Provider = "fx"
	ProviderGrok     Provider = "grok"
	ProviderKimi     Provider = "kimi"
	ProviderPi       Provider = "pi"
	ProviderOhMyPi   Provider = "ohMyPi"
	ProviderClaude   Provider = "claude"
	ProviderAmp      Provider = "amp"
	ProviderOpenCode Provider = "openCode"
	ProviderDeepSeek Provider = "deepSeek"
)

// Capabilities is intentionally explicit.  A false value is a product gate,
// not permission for callers to optimistically try an operation.
type Capabilities struct {
	Models, Reasoning, ServiceTier, Attachments bool
	Approvals, Questions, Steering              bool
	Resume, Fork, Rewind, ImportedHistory       bool
	BackgroundWork, Goals, ComputerUse          bool
	AccessModes                                 []AccessMode
	Cancellation                                Cancellation
}

type Cancellation string

const (
	CancellationProtocol Cancellation = "protocol"
	CancellationDrop     Cancellation = "dropSession"
)

type AccessMode string

const (
	AccessAsk             AccessMode = "ask"
	AccessAutoAcceptEdits AccessMode = "autoAcceptEdits"
	AccessAuto            AccessMode = "auto"
	AccessFull            AccessMode = "fullAccess"
)

type Descriptor struct {
	Provider     Provider
	Transport    string
	Binary       string
	Implemented  bool
	Capabilities Capabilities
}

type StartOptions struct {
	Binary, WorkingDirectory            string
	Access                              AccessMode
	Model, ReasoningEffort, ServiceTier string
	ResumeCursor                        json.RawMessage
	ComputerUse                         bool
	// Discovery-only hints. Callers leave these false for Start.
	DiscoverModels, ProbeVersion bool
}

type SessionOptions struct {
	Access                              AccessMode
	Model, ReasoningEffort, ServiceTier string
}

type Discovery struct {
	Available  bool
	Executable string
	Version    string
	Models     []Model
	Reason     string
}

type Model struct {
	ID, Label string
	Raw       json.RawMessage
}

// Sink must consume Emit synchronously.  Adapters call it in native arrival
// order and never fan events out through independently scheduled goroutines.
type Sink interface{ Emit(Event) }
type SinkFunc func(Event)

func (f SinkFunc) Emit(event Event) { f(event) }

type Adapter interface {
	Descriptor() Descriptor
	Discover(context.Context, StartOptions) (Discovery, error)
	Start(context.Context, StartOptions, Sink) (Session, error)
}

// SessionCatalogAdapter is an optional daemon-facing extension. Raw JSON is
// intentional: summaries, histories, and fork requests are protocol-owned
// shapes and must remain byte-faithful across the provider boundary.
type SessionCatalogAdapter interface {
	ListSessions(context.Context, uint64) ([]json.RawMessage, error)
	LoadSession(context.Context, json.RawMessage, string) (json.RawMessage, error)
	ForkSession(context.Context, json.RawMessage) (json.RawMessage, error)
}

// Session is session-lived, including idle time between turns.  Unsupported
// operations return ErrUnsupported instead of silently degrading behavior.
type Session interface {
	Prompt(context.Context, string) error
	Steer(context.Context, string) error
	Cancel(context.Context) error
	RespondPermission(context.Context, json.RawMessage, string) error
	RespondQuestions(context.Context, json.RawMessage, []UserInputAnswer) error
	Goal(context.Context, GoalOperation) error
	ApplyOptions(context.Context, SessionOptions) (bool, error)
	Rollback(context.Context, uint64) (json.RawMessage, error)
	Fork(context.Context, uint64) (json.RawMessage, error)
	Close() error
}

type UserInputAnswer struct {
	QuestionID string
	Answers    []string
}

type GoalOperation struct {
	Kind      string
	Objective *string
	Status    *string
	Replace   bool
}
