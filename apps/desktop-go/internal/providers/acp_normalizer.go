package providers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errACPToolStateLimit = errors.New("ACP unfinished tool state limit exceeded")

const (
	maxACPTools     = 1024
	maxACPToolBytes = 4 << 20
)

// acpNormalizer translates only user-visible session/update payloads. Unknown
// notifications are agent-private extension traffic and are intentionally not
// surfaced.
type acpNormalizer struct {
	provider  Provider
	session   string
	tools     map[string]Activity
	toolBytes int
	produced  bool
}

func newACPNormalizer(provider Provider, session string) *acpNormalizer {
	return &acpNormalizer{provider: provider, session: session, tools: make(map[string]Activity)}
}

func (n *acpNormalizer) consume(params, raw json.RawMessage) ([]Event, error) {
	var envelope struct {
		SessionID string          `json:"sessionId"`
		Update    json.RawMessage `json:"update"`
	}
	if err := decodeUseNumber(params, &envelope); err != nil {
		return nil, fmt.Errorf("ACP session/update params: %w", err)
	}
	if envelope.SessionID == "" || envelope.SessionID != n.session {
		return nil, fmt.Errorf("ACP session/update has unexpected sessionId %q", envelope.SessionID)
	}
	var update map[string]json.RawMessage
	if err := decodeUseNumber(envelope.Update, &update); err != nil || update == nil {
		return nil, errors.New("ACP session/update has invalid update object")
	}
	kind := rawString(update["sessionUpdate"])
	event := func(eventKind EventKind) Event { return Event{Kind: eventKind, Raw: cloneRaw(raw)} }
	switch kind {
	case "user_message_chunk":
		return nil, nil
	case "agent_message_chunk", "agent_thought_chunk":
		text, err := acpContentText(update["content"])
		if err != nil {
			return nil, fmt.Errorf("ACP %s: %w", kind, err)
		}
		if text == "" {
			return nil, nil
		}
		if n.provider == ProviderFx && !n.produced && kind == "agent_message_chunk" && fxNotice(text) {
			return nil, nil
		}
		n.produced = true
		e := event(EventTextDelta)
		if kind == "agent_thought_chunk" {
			e.Kind = EventReasoningDelta
		}
		e.Text = stripPrivate(text)
		if e.Text == "" {
			return nil, nil
		}
		return []Event{e}, nil
	case "tool_call", "tool_call_update":
		activity, err := n.tool(update)
		if err != nil {
			return nil, err
		}
		n.produced = true
		e := event(EventActivity)
		e.Activity = activity
		return []Event{e}, nil
	case "plan":
		n.produced = true
		e := event(EventActivity)
		e.Activity = &Activity{ID: "acp-plan", Kind: "plan", Title: "Plan updated"}
		return []Event{e}, nil
	case "usage_update":
		used, err := optionalUint64(update["used"])
		if err != nil {
			return nil, fmt.Errorf("ACP usage used: %w", err)
		}
		var window *uint64
		for _, key := range []string{"max", "limit", "size", "contextWindow", "context_window"} {
			value, valueErr := optionalUint64(update[key])
			if valueErr != nil {
				return nil, fmt.Errorf("ACP usage %s: %w", key, valueErr)
			}
			if value != nil {
				window = value
				break
			}
		}
		if used == nil && window == nil {
			return nil, nil
		}
		e := event(EventUsageUpdated)
		e.ContextTokens, e.ContextWindow = used, window
		return []Event{e}, nil
	case "available_commands_update":
		if len(update["availableCommands"]) == 0 {
			return nil, errors.New("ACP available commands update is missing availableCommands")
		}
		var commands []json.RawMessage
		if err := decodeUseNumber(update["availableCommands"], &commands); err != nil {
			return nil, fmt.Errorf("ACP available commands: %w", err)
		}
		for _, command := range commands {
			var value struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			if err := decodeUseNumber(command, &value); err != nil || strings.TrimSpace(value.Name) == "" {
				return nil, errors.New("ACP available commands contains an invalid command")
			}
		}
		e := event(EventAvailableCommands)
		// Event.Raw is the existing provider→daemon payload seam for this event;
		// retain the exact validated array rather than the surrounding envelope.
		e.Raw = cloneRaw(update["availableCommands"])
		return []Event{e}, nil
	case "session_info_update":
		if titleRaw, ok := update["title"]; ok {
			if string(titleRaw) == "null" {
				return []Event{{Kind: EventAutoTitleUpdated, Raw: cloneRaw(raw)}}, nil
			}
			var title string
			if err := decodeUseNumber(titleRaw, &title); err != nil {
				return nil, fmt.Errorf("ACP session title: %w", err)
			}
			e := event(EventAutoTitleUpdated)
			e.Text = stripPrivate(title)
			return []Event{e}, nil
		}
		return nil, nil
	case "":
		return nil, errors.New("ACP session/update is missing sessionUpdate")
	default:
		return nil, nil
	}
}

func (n *acpNormalizer) tool(update map[string]json.RawMessage) (*Activity, error) {
	id := rawString(update["toolCallId"])
	if id == "" {
		return nil, errors.New("ACP tool update is missing toolCallId")
	}
	status := rawString(update["status"])
	if status == "" {
		status = "pending"
	}
	if status != "pending" && status != "in_progress" && status != "completed" && status != "failed" {
		return nil, fmt.Errorf("ACP tool update has invalid status %q", status)
	}
	complete := status == "completed" || status == "failed"
	stored, exists := n.tools[id]
	kind := acpToolKind(rawString(update["kind"]))
	if kind == "tool" && exists {
		kind = stored.Kind
	}
	title := rawString(update["title"])
	if title == "" && exists {
		title = stored.Title
	}
	if title == "" {
		title = "Tool"
	}
	activity := &Activity{ID: id, Kind: kind, Title: stripPrivate(title), Complete: complete, Failed: status == "failed", Arguments: cloneRaw(update["rawInput"])}
	if content := update["content"]; len(content) != 0 && string(content) != "null" {
		activity.Output = cloneRaw(content)
	} else {
		activity.Output = cloneRaw(update["rawOutput"])
	}
	if complete {
		if exists {
			n.toolBytes -= acpActivitySize(stored)
		}
		delete(n.tools, id)
	} else {
		newSize := acpActivitySize(*activity)
		previousSize := 0
		if exists {
			previousSize = acpActivitySize(stored)
		}
		if (!exists && len(n.tools) >= maxACPTools) || n.toolBytes-previousSize+newSize > maxACPToolBytes {
			return nil, errACPToolStateLimit
		}
		n.tools[id] = *activity
		n.toolBytes = n.toolBytes - previousSize + newSize
	}
	return activity, nil
}

func acpActivitySize(activity Activity) int {
	return len(activity.ID) + len(activity.Kind) + len(activity.Title) + len(activity.Detail) + len(activity.Arguments) + len(activity.Output) + 64
}

func acpContentText(raw json.RawMessage) (string, error) {
	var content struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := decodeUseNumber(raw, &content); err != nil {
		return "", err
	}
	if content.Type != "text" {
		return "", fmt.Errorf("unsupported content type %q", content.Type)
	}
	return content.Text, nil
}

func optionalUint64(raw json.RawMessage) (*uint64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func acpToolKind(kind string) string {
	switch kind {
	case "execute":
		return "command"
	case "edit", "delete", "move":
		return "fileChange"
	case "read":
		return "fileRead"
	case "search", "fetch":
		return "search"
	case "think":
		return "reasoning"
	default:
		return "tool"
	}
}

func fxNotice(text string) bool {
	return strings.HasPrefix(text, "[context] ") || strings.HasPrefix(text, "skill discovery warning: ")
}
