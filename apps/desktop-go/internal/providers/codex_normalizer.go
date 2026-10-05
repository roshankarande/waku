package providers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
)

// CodexNormalizer translates app-server messages one at a time. Its state is
// per native thread and must not be shared by sessions.
type CodexNormalizer struct {
	threadID       string
	turnID         string
	turnIDs        []string
	citations      map[string]citation
	nextCitation   int
	citationBuffer string
	reasoningPart  string
}

type citation struct {
	number int
	url    string
}

func NewCodexNormalizer() *CodexNormalizer {
	return &CodexNormalizer{citations: make(map[string]citation), nextCitation: 1}
}

func (n *CodexNormalizer) Consume(raw []byte) ([]Event, error) {
	envelope, err := decodeRPCMessage(raw)
	if err != nil {
		return nil, err
	}
	if envelope.Method == "" {
		return n.response(envelope, raw)
	}

	var params map[string]json.RawMessage
	if len(envelope.Params) != 0 && string(envelope.Params) != "null" {
		if err := decodeUseNumber(envelope.Params, &params); err != nil {
			return nil, fmt.Errorf("Codex %s params: %w", envelope.Method, err)
		}
	}
	event := func(kind EventKind) Event { return Event{Kind: kind, Raw: cloneRaw(raw)} }
	switch envelope.Method {
	case "turn/started":
		var p struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if err := decodeUseNumber(envelope.Params, &p); err != nil || p.Turn.ID == "" {
			return nil, fmt.Errorf("Codex turn/started missing valid turn.id")
		}
		n.citations = make(map[string]citation)
		n.nextCitation = 1
		n.citationBuffer = ""
		n.reasoningPart = ""
		n.turnID = p.Turn.ID
		if p.Turn.ID != "" && (len(n.turnIDs) == 0 || n.turnIDs[len(n.turnIDs)-1] != p.Turn.ID) {
			n.turnIDs = append(n.turnIDs, p.Turn.ID)
		}
		return []Event{event(EventTurnStarted)}, nil
	case "item/agentMessage/delta":
		var delta string
		if err := decodeUseNumber(params["delta"], &delta); err != nil {
			return nil, fmt.Errorf("Codex agent delta: %w", err)
		}
		if delta = n.rewriteCitations(delta); delta != "" {
			e := event(EventTextDelta)
			e.Text = delta
			return []Event{e}, nil
		}
	case "item/reasoning/summaryTextDelta", "item/reasoning/textDelta":
		var delta string
		if err := decodeUseNumber(params["delta"], &delta); err != nil {
			return nil, fmt.Errorf("Codex reasoning delta: %w", err)
		}
		if delta == "" {
			return nil, nil
		}
		part := rawString(params["itemId"]) + ":" + firstRawNumber(params["summaryIndex"], params["contentIndex"])
		var events []Event
		if n.reasoningPart != "" && part != ":" && part != n.reasoningPart {
			e := event(EventReasoningDelta)
			e.Text = "\n\n"
			events = append(events, e)
		}
		if part != ":" {
			n.reasoningPart = part
		}
		e := event(EventReasoningDelta)
		e.Text = stripPrivate(delta)
		return append(events, e), nil
	case "item/commandExecution/outputDelta":
		var id, outputDelta string
		if err := decodeUseNumber(params["itemId"], &id); err != nil {
			return nil, fmt.Errorf("Codex command output itemId: %w", err)
		}
		if err := decodeUseNumber(params["delta"], &outputDelta); err != nil {
			return nil, fmt.Errorf("Codex command output delta: %w", err)
		}
		if id != "" && outputDelta != "" {
			e := event(EventBackgroundWork)
			e.Background = &BackgroundWork{ID: id, Kind: "process", OutputDelta: outputDelta, Raw: cloneRaw(envelope.Params)}
			return []Event{e}, nil
		}
	case "item/started", "item/completed":
		itemRaw := params["item"]
		if len(itemRaw) == 0 {
			return nil, errors.New("Codex item event missing item")
		}
		var itemObject map[string]json.RawMessage
		if err := decodeUseNumber(itemRaw, &itemObject); err != nil {
			return nil, fmt.Errorf("Codex item event: %w", err)
		}
		n.captureCitations(itemRaw)
		var events []Event
		for _, work := range codexBackgroundWork(itemRaw, envelope.Method == "item/completed") {
			e := event(EventBackgroundWork)
			e.Background = work
			events = append(events, e)
		}
		if activity := codexActivity(itemRaw, envelope.Method == "item/completed"); activity != nil {
			e := event(EventActivity)
			e.Activity = activity
			events = append(events, e)
		}
		return events, nil
	case "turn/completed":
		var p struct {
			Turn struct {
				Status string `json:"status"`
				Error  struct {
					Message string `json:"message"`
				} `json:"error"`
			} `json:"turn"`
		}
		if err := decodeUseNumber(envelope.Params, &p); err != nil {
			return nil, fmt.Errorf("Codex turn/completed params: %w", err)
		}
		if p.Turn.Status != "completed" && p.Turn.Status != "failed" && p.Turn.Status != "interrupted" {
			return nil, fmt.Errorf("Codex turn/completed has invalid status %q", p.Turn.Status)
		}
		n.citationBuffer = ""
		n.turnID = ""
		e := event(EventTurnFinished)
		e.Success = p.Turn.Status == "" || p.Turn.Status == "completed"
		e.Summary = p.Turn.Error.Message
		return []Event{e}, nil
	case "thread/tokenUsage/updated":
		var p struct {
			TokenUsage struct {
				Last struct {
					TotalTokens uint64 `json:"totalTokens"`
				} `json:"last"`
				ModelContextWindow uint64 `json:"modelContextWindow"`
			} `json:"tokenUsage"`
		}
		if len(params["tokenUsage"]) == 0 {
			return nil, errors.New("Codex token usage update missing tokenUsage")
		}
		if err := decodeUseNumber(envelope.Params, &p); err != nil {
			return nil, fmt.Errorf("Codex token usage: %w", err)
		}
		if p.TokenUsage.Last.TotalTokens != 0 || p.TokenUsage.ModelContextWindow != 0 {
			e := event(EventUsageUpdated)
			if p.TokenUsage.Last.TotalTokens != 0 {
				v := p.TokenUsage.Last.TotalTokens
				e.ContextTokens = &v
			}
			if p.TokenUsage.ModelContextWindow != 0 {
				v := p.TokenUsage.ModelContextWindow
				e.ContextWindow = &v
			}
			return []Event{e}, nil
		}
	case "thread/name/updated":
		var notificationThread string
		if err := decodeUseNumber(params["threadId"], &notificationThread); err != nil {
			return nil, fmt.Errorf("Codex thread name threadId: %w", err)
		}
		if notificationThread == n.threadID {
			e := event(EventAutoTitleUpdated)
			e.Text = rawString(params["threadName"])
			return []Event{e}, nil
		}
	case "account/rateLimits/updated":
		if raw := params["rateLimits"]; len(raw) != 0 {
			var snapshot map[string]json.RawMessage
			if err := decodeUseNumber(raw, &snapshot); err != nil {
				return nil, fmt.Errorf("Codex rate limits: %w", err)
			}
			e := event(EventPlanUsageUpdated)
			e.Raw = cloneRaw(raw)
			return []Event{e}, nil
		}
		return nil, errors.New("Codex rate limit update missing rateLimits")
	case "thread/goal/updated":
		var goalThread string
		if err := decodeUseNumber(params["threadId"], &goalThread); err != nil {
			return nil, fmt.Errorf("Codex goal threadId: %w", err)
		}
		if goalThread == n.threadID && len(params["goal"]) != 0 {
			var goal map[string]json.RawMessage
			if err := decodeUseNumber(params["goal"], &goal); err != nil {
				return nil, fmt.Errorf("Codex goal: %w", err)
			}
			e := event(EventGoalUpdated)
			e.GoalPresent = true
			e.Goal = cloneRaw(params["goal"])
			return []Event{e}, nil
		}
		if goalThread == n.threadID {
			return nil, errors.New("Codex goal update missing goal")
		}
	case "thread/goal/cleared":
		var clearedThread string
		if err := decodeUseNumber(params["threadId"], &clearedThread); err != nil {
			return nil, fmt.Errorf("Codex cleared goal threadId: %w", err)
		}
		if clearedThread == n.threadID {
			return []Event{event(EventGoalUpdated)}, nil
		}
	case "item/tool/requestUserInput":
		if !validRPCID(envelope.ID) {
			return nil, errors.New("Codex user input request missing valid id")
		}
		questions, questionErr := codexQuestions(params["questions"])
		if questionErr != nil {
			return nil, questionErr
		}
		if len(questions) != 0 {
			e := event(EventUserInputRequested)
			e.RequestID = cloneRaw(envelope.ID)
			e.Questions = questions
			return []Event{e}, nil
		}
	case "error":
		if text := rawString(params["message"]); text != "" {
			e := event(EventError)
			e.Text = text
			return []Event{e}, nil
		}
		return nil, errors.New("Codex error notification missing message")
	case "mcpServer/startupStatus/updated":
		var mcpStatus string
		if err := decodeUseNumber(params["status"], &mcpStatus); err != nil {
			return nil, fmt.Errorf("Codex MCP startup status: %w", err)
		}
		switch mcpStatus {
		case "starting", "ready", "cancelled":
			return nil, nil
		case "failed":
			message := rawString(params["error"])
			if name := rawString(params["name"]); name != "" && message != "" {
				message = name + ": " + message
			}
			if message != "" {
				e := event(EventError)
				e.Text = message
				return []Event{e}, nil
			}
			return nil, errors.New("Codex MCP startup failure missing error")
		default:
			return nil, fmt.Errorf("Codex MCP startup status %q is invalid", mcpStatus)
		}
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		if len(envelope.Params) == 0 || string(envelope.Params) == "null" {
			return nil, errors.New("Codex approval request missing params")
		}
		if validRPCID(envelope.ID) {
			e := event(EventPermission)
			e.RequestID = cloneRaw(envelope.ID)
			e.Permission = codexPermission(envelope.Method, params)
			return []Event{e}, nil
		}
		return nil, errors.New("Codex approval request missing id")
	}
	return nil, nil
}

func (n *CodexNormalizer) response(envelope RPCMessage, raw []byte) ([]Event, error) {
	if string(envelope.ID) != "1" {
		if len(envelope.Error) != 0 {
			message, _ := rpcErrorMessage(envelope.Error)
			return []Event{{Kind: EventError, Text: message, Raw: cloneRaw(raw)}}, nil
		}
		return nil, nil
	}
	var result struct {
		Thread struct {
			ID    string `json:"id"`
			Turns []struct {
				ID string `json:"id"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if decodeUseNumber(envelope.Result, &result) == nil && result.Thread.ID != "" {
		n.threadID = result.Thread.ID
		n.turnIDs = n.turnIDs[:0]
		for _, turn := range result.Thread.Turns {
			if turn.ID != "" {
				n.turnIDs = append(n.turnIDs, turn.ID)
			}
		}
		cursor, _ := json.Marshal(map[string]string{"provider": "codex", "threadId": result.Thread.ID})
		return []Event{{Kind: EventConnected, Cursor: cursor, Raw: cloneRaw(raw)}}, nil
	}
	if len(envelope.Error) != 0 {
		message, _ := rpcErrorMessage(envelope.Error)
		return []Event{{Kind: EventError, Text: message, Raw: cloneRaw(raw)}}, nil
	}
	return nil, errors.New("Codex thread open response missing thread.id and error")
}

func (n *CodexNormalizer) captureCitations(itemRaw json.RawMessage) {
	var item struct {
		Type    string `json:"type"`
		Results []struct {
			RefID  string `json:"ref_id"`
			RefID2 string `json:"refId"`
			URL    string `json:"url"`
		} `json:"results"`
	}
	if decodeUseNumber(itemRaw, &item) != nil || item.Type != "webSearch" {
		return
	}
	for _, result := range item.Results {
		if result.RefID == "" {
			result.RefID = result.RefID2
		}
		if result.RefID != "" && result.URL != "" {
			if _, exists := n.citations[result.RefID]; !exists {
				n.citations[result.RefID] = citation{url: result.URL}
			}
		}
	}
}

func (n *CodexNormalizer) rewriteCitations(delta string) string {
	text := n.citationBuffer + delta
	n.citationBuffer = ""
	var out strings.Builder
	for {
		start := strings.IndexRune(text, '\ue200')
		if start < 0 {
			out.WriteString(stripPrivate(text))
			break
		}
		out.WriteString(stripPrivate(text[:start]))
		text = text[start:]
		end := strings.IndexRune(text, '\ue201')
		if end < 0 {
			n.citationBuffer = text
			break
		}
		marker := text[len("\ue200"):end]
		text = text[end+len("\ue201"):]
		refs := strings.Split(marker, "\ue202")
		seen := map[string]bool{}
		for _, ref := range refs {
			ref = strings.TrimPrefix(ref, "cite")
			if c, ok := n.citations[ref]; ok && !seen[ref] {
				if c.number == 0 {
					c.number = n.nextCitation
					n.nextCitation++
					n.citations[ref] = c
				}
				if out.Len() > 0 {
					out.WriteByte(' ')
				}
				fmt.Fprintf(&out, "[%d](%s)", c.number, markdownDestination(c.url))
				seen[ref] = true
			}
		}
	}
	return out.String()
}

func markdownDestination(value string) string {
	return strings.NewReplacer("\\", "\\\\", "(", "\\(", ")", "\\)").Replace(value)
}

func stripPrivate(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Co, r) {
			return -1
		}
		return r
	}, value)
}

func decodeUseNumber(raw []byte, value any) error {
	if len(raw) == 0 {
		return io.ErrUnexpectedEOF
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	return nil
}
func cloneRaw(raw []byte) json.RawMessage { return append(json.RawMessage(nil), raw...) }
func rawString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}
func firstRawNumber(values ...json.RawMessage) string {
	for _, value := range values {
		if len(value) != 0 && string(value) != "null" {
			return string(value)
		}
	}
	return ""
}
