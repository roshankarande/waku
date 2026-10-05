package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

// The typed helpers below perform network I/O and wait for a correlated
// response. UI code should call them from its worker/task mechanism, passing a
// context that is cancelled when the owning view is discarded.

func (c *Client) GetSettings(ctx context.Context) (protocol.DaemonSettingsValue, error) {
	payload, err := c.requestPayload(ctx, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandGetSettings)})
	if err != nil {
		return protocol.DaemonSettingsValue{}, err
	}
	value, ok := payload.(*protocol.SettingsPayload)
	if !ok {
		return protocol.DaemonSettingsValue{}, unexpectedPayload(payload, protocol.ResponseSettings)
	}
	return protocol.DecodeDaemonSettings(value.Settings)
}

func (c *Client) UpdateSettings(ctx context.Context, settings protocol.DaemonSettingsValue) error {
	raw, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("encode daemon settings: %w", err)
	}
	return c.requestAck(ctx, protocol.NilUUID, protocol.NilUUID, protocol.UpdateSettingsCommand{
		Type: protocol.CommandUpdateSettings, Settings: raw,
	})
}

func (c *Client) FetchPlanUsage(ctx context.Context, provider string, binaryOverride, cliVersion *string) (*protocol.PlanUsageValue, error) {
	payload, err := c.requestPayload(ctx, protocol.NilUUID, protocol.NilUUID, protocol.FetchPlanUsageCommand{
		Type: protocol.CommandFetchPlanUsage, Provider: provider, BinaryOverride: binaryOverride, CLIVersion: cliVersion,
	})
	if err != nil {
		return nil, err
	}
	value, ok := payload.(*protocol.PlanUsagePayload)
	if !ok {
		return nil, unexpectedPayload(payload, protocol.ResponsePlanUsage)
	}
	if len(value.Usage) == 0 || bytes.Equal(bytes.TrimSpace(value.Usage), []byte("null")) {
		return nil, nil
	}
	var usage protocol.PlanUsageValue
	if err := protocol.Decode(value.Usage, &usage); err != nil {
		return nil, fmt.Errorf("decode plan usage: %w", err)
	}
	return &usage, nil
}

func (c *Client) LoadUsageHistory(ctx context.Context, window protocol.UsageWindowValue, projectRoots []string) (protocol.UsageHistoryValue, error) {
	raw, err := json.Marshal(window)
	if err != nil {
		return protocol.UsageHistoryValue{}, fmt.Errorf("encode usage window: %w", err)
	}
	payload, err := c.requestPayload(ctx, protocol.NilUUID, protocol.NilUUID, protocol.LoadUsageHistoryCommand{
		Type: protocol.CommandLoadUsageHistory, Window: raw, ProjectRoots: nonNilStrings(projectRoots),
	})
	if err != nil {
		return protocol.UsageHistoryValue{}, err
	}
	value, ok := payload.(*protocol.UsageHistoryPayload)
	if !ok {
		return protocol.UsageHistoryValue{}, unexpectedPayload(payload, protocol.ResponseUsageHistory)
	}
	return protocol.DecodeUsageHistoryValue(value.History)
}

func (c *Client) LoadSkills(ctx context.Context, projects []protocol.SkillProject) (protocol.SkillsCatalogValue, error) {
	if projects == nil {
		projects = []protocol.SkillProject{}
	}
	payload, err := c.requestPayload(ctx, protocol.NilUUID, protocol.NilUUID, protocol.LoadSkillsCommand{
		Type: protocol.CommandLoadSkills, Projects: projects,
	})
	if err != nil {
		return protocol.SkillsCatalogValue{}, err
	}
	value, ok := payload.(*protocol.SkillsCatalogPayload)
	if !ok {
		return protocol.SkillsCatalogValue{}, unexpectedPayload(payload, protocol.ResponseSkillsCatalog)
	}
	return protocol.DecodeSkillsCatalogValue(value.Catalog)
}

func (c *Client) SetSkillsEnabled(ctx context.Context, dirs []string, enabled bool) error {
	return c.requestAck(ctx, protocol.NilUUID, protocol.NilUUID, protocol.SetSkillsEnabledCommand{
		Type: protocol.CommandSetSkillsEnabled, Dirs: nonNilStrings(dirs), Enabled: enabled,
	})
}

func (c *Client) TrashSkills(ctx context.Context, dirs []string) error {
	return c.requestAck(ctx, protocol.NilUUID, protocol.NilUUID, protocol.TrashSkillsCommand{
		Type: protocol.CommandTrashSkills, Dirs: nonNilStrings(dirs),
	})
}

// Workspace sends an operation to the daemon host. Path strings in operation
// and result are deliberately passed through unchanged: they do not belong to
// the desktop host and must not be cleaned, joined, or made absolute here.
func (c *Client) Workspace(ctx context.Context, operation any) (any, error) {
	raw, err := json.Marshal(operation)
	if err != nil {
		return nil, fmt.Errorf("encode workspace operation: %w", err)
	}
	payload, err := c.requestPayload(ctx, protocol.NilUUID, protocol.NilUUID, protocol.WorkspaceCommand{
		Type: protocol.CommandWorkspace, Operation: raw,
	})
	if err != nil {
		return nil, err
	}
	value, ok := payload.(*protocol.WorkspacePayload)
	if !ok {
		return nil, unexpectedPayload(payload, protocol.ResponseWorkspace)
	}
	return protocol.DecodeWorkspaceResultAny(value.Result)
}

func (c *Client) OpenTerminal(ctx context.Context, sessionID, runtimeID protocol.UUID, cwd string, cols, rows uint16) error {
	return c.requestAck(ctx, sessionID, runtimeID, protocol.OpenTerminalCommand{
		Type: protocol.CommandOpenTerminal, CWD: cwd, Cols: cols, Rows: rows,
	})
}

func (c *Client) WriteTerminal(ctx context.Context, sessionID, runtimeID protocol.UUID, data []byte) error {
	return c.requestAck(ctx, sessionID, runtimeID, protocol.WriteTerminal(data))
}

func (c *Client) ResizeTerminal(ctx context.Context, sessionID, runtimeID protocol.UUID, cols, rows uint16) error {
	return c.requestAck(ctx, sessionID, runtimeID, protocol.ResizeTerminalCommand{
		Type: protocol.CommandResizeTerminal, Cols: cols, Rows: rows,
	})
}

func (c *Client) CloseTerminal(ctx context.Context, sessionID, runtimeID protocol.UUID) error {
	return c.requestAck(ctx, sessionID, runtimeID, protocol.Command{Type: string(protocol.CommandCloseTerminal)})
}

type TerminalEvent struct {
	Kind    protocol.EventKind
	Data    []byte
	Message string
}

// SubscribeTerminal exclusively consumes the runtime subscription and
// demultiplexes terminal events. Its output is intentionally unbuffered: a
// stalled consumer applies backpressure to the existing count-and-byte-bounded
// runtime queue, which fails the transport and enables replay rather than
// silently dropping terminal bytes.
func (c *Client) SubscribeTerminal(ctx context.Context, sessionID, runtimeID protocol.UUID) (<-chan TerminalEvent, func()) {
	sub := newTerminalEventSubscription(c.markDelivered)
	unsubscribe := c.installSubscription(sessionID, runtimeID, sub)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			unsubscribe()
		})
	}
	go func() {
		select {
		case <-ctx.Done():
			stop()
		case <-sub.stopped:
		}
	}()
	return sub.terminal, stop
}

func terminalEvent(event protocol.WireDriverEvent) (TerminalEvent, bool) {
	switch protocol.EventKind(event.Kind) {
	case protocol.EventTerminalOutput:
		var payload protocol.TerminalOutputPayload
		if err := protocol.Decode(event.Payload, &payload); err != nil {
			return TerminalEvent{}, false // transport validation normally makes this unreachable
		}
		return TerminalEvent{Kind: protocol.EventTerminalOutput, Data: append([]byte(nil), payload.Data...)}, true
	case protocol.EventTerminalExited:
		return TerminalEvent{Kind: protocol.EventTerminalExited}, true
	case protocol.EventTerminalError:
		var message string
		if err := protocol.Decode(event.Payload, &message); err != nil {
			return TerminalEvent{}, false
		}
		return TerminalEvent{Kind: protocol.EventTerminalError, Message: message}, true
	default:
		return TerminalEvent{}, false
	}
}

func (c *Client) requestPayload(ctx context.Context, sessionID, runtimeID protocol.UUID, command any) (any, error) {
	raw, err := c.Request(ctx, sessionID, runtimeID, command)
	if err != nil {
		return nil, err
	}
	payload, err := protocol.DecodeResponsePayloadAny(raw)
	if err != nil {
		return nil, fmt.Errorf("decode correlated daemon response: %w", err)
	}
	return payload, nil
}

func (c *Client) requestAck(ctx context.Context, sessionID, runtimeID protocol.UUID, command any) error {
	payload, err := c.requestPayload(ctx, sessionID, runtimeID, command)
	if err != nil {
		return err
	}
	if _, ok := payload.(*protocol.AckPayload); !ok {
		return unexpectedPayload(payload, protocol.ResponseAck)
	}
	return nil
}

func unexpectedPayload(payload any, expected protocol.ResponsePayloadType) error {
	return fmt.Errorf("daemon returned %T, expected %s response", payload, expected)
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
