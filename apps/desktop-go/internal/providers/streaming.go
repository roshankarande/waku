package providers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StreamingAdapter drives the two Anthropic-shaped, long-lived NDJSON
// transports. Claude and Amp deliberately share framing and normalization,
// but retain different launch, completion, approval, cancellation, and steer
// behavior.
type StreamingAdapter struct {
	Provider     Provider
	startProcess func(context.Context, ProcessSpec) (*StdioProcess, error)
}

func (a StreamingAdapter) Descriptor() Descriptor { return Descriptors()[a.Provider] }

func (a StreamingAdapter) Discover(ctx context.Context, options StartOptions) (Discovery, error) {
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	d, err := a.descriptor()
	if err != nil {
		return Discovery{}, err
	}
	binary := options.Binary
	if binary == "" {
		binary = d.Binary
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return Discovery{Reason: err.Error()}, nil
	}
	return Discovery{Available: true, Executable: path}, nil
}

func (a StreamingAdapter) descriptor() (Descriptor, error) {
	d, ok := Descriptors()[a.Provider]
	if !ok || !d.Implemented || (a.Provider != ProviderClaude && a.Provider != ProviderAmp) {
		return Descriptor{}, fmt.Errorf("%w: %q does not have a streaming adapter", ErrUnsupported, a.Provider)
	}
	return d, nil
}

func (a StreamingAdapter) Start(ctx context.Context, options StartOptions, sink Sink) (Session, error) {
	d, err := a.descriptor()
	if err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("provider event sink is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	access := normalizedAccess(options.Access)
	if a.Provider == ProviderAmp && access != AccessFull {
		return nil, errors.New("Amp currently supports Full access only")
	}
	if options.ComputerUse {
		return nil, fmt.Errorf("%w: computer use is not implemented for %s", ErrUnsupported, a.Provider)
	}
	if options.ServiceTier != "" && a.Provider == ProviderClaude {
		return nil, fmt.Errorf("%w: Claude service tier selection is not implemented", ErrUnsupported)
	}
	cursor, err := parseStreamingCursor(a.Provider, options.ResumeCursor)
	if err != nil {
		return nil, err
	}
	if a.Provider == ProviderClaude && cursor == "" {
		cursor, err = randomSessionID()
		if err != nil {
			return nil, err
		}
	}
	cwd := options.WorkingDirectory
	if cwd == "" {
		cwd, err = os.Getwd()
	} else {
		cwd, err = filepath.Abs(cwd)
	}
	if err != nil {
		return nil, err
	}
	binary := options.Binary
	if binary == "" {
		binary = d.Binary
	}
	spec := ProcessSpec{Path: binary, Directory: cwd, Args: streamArgs(a.Provider, options, cursor)}
	starter := StartStdio
	if a.startProcess != nil {
		starter = a.startProcess
	}
	process, err := starter(ctx, spec)
	if err != nil {
		return nil, err
	}
	s := &streamSession{
		provider: a.Provider, process: process, sink: sink, access: access, sessionID: cursor,
		events: make(chan Event, 256), tools: make(map[string]Activity), permissions: make(map[string]streamPermission),
		questions: make(map[string]streamQuestion), stderrDone: make(chan struct{}),
	}
	go func() { _, _ = io.Copy(io.Discard, process.Stderr); close(s.stderrDone) }()
	go s.dispatch()
	if a.Provider == ProviderClaude {
		s.emit(Event{Kind: EventConnected, Cursor: streamCursor(a.Provider, cursor)})
	}
	go s.readLoop()
	return s, nil
}

func randomSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("Claude session id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

func streamArgs(provider Provider, options StartOptions, cursor string) []string {
	if provider == ProviderClaude {
		args := []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--thinking-display", "summarized", "--replay-user-messages", "--permission-prompt-tool", "stdio", "--permission-mode", claudePermissionMode(normalizedAccess(options.Access))}
		if normalizedAccess(options.Access) == AccessFull {
			args = append(args, "--dangerously-skip-permissions")
		}
		if options.Model != "" {
			args = append(args, "--model", options.Model)
		}
		if options.ReasoningEffort != "" {
			args = append(args, "--effort", options.ReasoningEffort)
		}
		if len(options.ResumeCursor) != 0 && string(options.ResumeCursor) != "null" {
			return append(args, "--resume", cursor)
		}
		return append(args, "--session-id", cursor)
	}
	args := []string{"--execute", "--stream-json-thinking", "--stream-json-input", "--dangerously-allow-all"}
	if cursor != "" {
		args = append([]string{"threads", "continue", cursor}, args...)
	}
	if options.Model != "" {
		args = append(args, "--mode", options.Model)
	}
	if options.ReasoningEffort != "" {
		args = append(args, "--effort", options.ReasoningEffort)
	}
	if options.ServiceTier == "fast" {
		args = append(args, "--fast")
	}
	return args
}

func claudePermissionMode(mode AccessMode) string {
	switch mode {
	case AccessAutoAcceptEdits:
		return "acceptEdits"
	case AccessAuto:
		return "auto"
	case AccessFull:
		return "bypassPermissions"
	default:
		return "default"
	}
}

func parseStreamingCursor(provider Provider, raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var value struct {
		Provider  Provider `json:"provider"`
		SessionID string   `json:"sessionId"`
		ThreadID  string   `json:"threadId"`
	}
	if err := decodeUseNumber(raw, &value); err != nil {
		return "", fmt.Errorf("%s resume cursor: %w", provider, err)
	}
	if value.Provider != provider {
		return "", fmt.Errorf("%s adapter cannot resume %q cursor", provider, value.Provider)
	}
	id := value.SessionID
	if provider == ProviderAmp {
		id = value.ThreadID
	}
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("%s resume cursor has no native session id", provider)
	}
	return id, nil
}

func streamCursor(provider Provider, id string) json.RawMessage {
	if id == "" {
		return nil
	}
	var value any = map[string]any{"provider": provider, "sessionId": id}
	if provider == ProviderAmp {
		value = map[string]any{"provider": provider, "threadId": id}
	}
	raw, _ := json.Marshal(value)
	return raw
}

type streamPermission struct{ raw json.RawMessage }
type streamQuestion struct{ input json.RawMessage }

type streamSession struct {
	provider Provider
	process  *StdioProcess
	sink     Sink
	access   AccessMode

	mu          sync.Mutex
	sessionID   string
	active      bool
	finished    bool
	sawText     bool
	sawReason   bool
	tools       map[string]Activity
	toolBytes   int
	permissions map[string]streamPermission
	questions   map[string]streamQuestion
	next        uint64
	closed      atomic.Bool

	eventMu     sync.Mutex
	events      chan Event
	eventBytes  int
	eventsClose bool
	slowSink    atomic.Bool
	stderrDone  chan struct{}
}

func (s *streamSession) Descriptor() Descriptor { return Descriptors()[s.provider] }

func (s *streamSession) emit(event Event) bool {
	size := eventSize(event)
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.eventsClose {
		return false
	}
	if len(s.events) == cap(s.events) || s.eventBytes+size > maxQueuedEventBytes {
		s.slowSink.Store(true)
		go terminateProcess(s.process)
		return false
	}
	s.eventBytes += size
	s.events <- event
	return true
}

func (s *streamSession) dispatch() {
	for event := range s.events {
		s.eventMu.Lock()
		s.eventBytes -= eventSize(event)
		s.eventMu.Unlock()
		s.sink.Emit(event)
	}
	if s.slowSink.Load() {
		s.sink.Emit(Event{Kind: EventError, Text: "provider event sink exceeded bounded queue"})
	}
	s.sink.Emit(Event{Kind: EventProcessExited})
}

func (s *streamSession) closeEvents() {
	s.eventMu.Lock()
	if !s.eventsClose {
		s.eventsClose = true
		close(s.events)
	}
	s.eventMu.Unlock()
}

func (s *streamSession) readLoop() {
	var readErr error
	for {
		var raw json.RawMessage
		if err := s.process.Lines.Decode(&raw); err != nil {
			readErr = err
			break
		}
		if err := s.consume(raw); err != nil {
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
		}
	}
	if !s.closed.Load() && readErr != nil && !errors.Is(readErr, io.EOF) {
		s.emit(Event{Kind: EventError, Text: readErr.Error()})
	}
	s.failActive("provider stream closed before turn completion")
	_ = s.process.Close()
	_ = s.process.Wait()
	<-s.stderrDone
	s.closed.Store(true)
	s.closeEvents()
}

func (s *streamSession) consume(raw json.RawMessage) error {
	var value map[string]json.RawMessage
	if err := decodeUseNumber(raw, &value); err != nil || value == nil {
		return errors.New("provider stream message must be a JSON object")
	}
	typeName, err := requiredString(value, "type")
	if err != nil {
		return err
	}
	switch typeName {
	case "system":
		return s.consumeSystem(value, raw)
	case "stream_event":
		if s.provider != ProviderClaude {
			return nil
		}
		return s.consumeStreamEvent(value, raw)
	case "assistant":
		return s.consumeAssistant(value, raw)
	case "user":
		return s.consumeUser(value, raw)
	case "result":
		return s.consumeResult(value, raw)
	case "control_request":
		if s.provider == ProviderClaude {
			return s.consumeControlRequest(value, raw)
		}
		return nil
	case "control_response", "rate_limit_event":
		return nil
	default:
		return nil
	}
}

func requiredString(value map[string]json.RawMessage, key string) (string, error) {
	raw, ok := value[key]
	if !ok || string(raw) == "null" {
		return "", fmt.Errorf("provider message is missing %s", key)
	}
	var text string
	if err := decodeUseNumber(raw, &text); err != nil || text == "" {
		return "", fmt.Errorf("provider message has invalid %s", key)
	}
	return text, nil
}

func (s *streamSession) consumeSystem(value map[string]json.RawMessage, raw json.RawMessage) error {
	if rawString(value["subtype"]) == "init" {
		id := rawString(value["session_id"])
		if id == "" {
			return errors.New("provider init is missing session_id")
		}
		s.mu.Lock()
		if s.sessionID != "" && s.provider == ProviderAmp && s.sessionID != id {
			s.mu.Unlock()
			return errors.New("Amp resumed a different thread")
		}
		s.sessionID = id
		s.mu.Unlock()
		if s.provider == ProviderAmp {
			s.emit(Event{Kind: EventConnected, Cursor: streamCursor(s.provider, id), Raw: cloneRaw(raw)})
		}
		if commands, ok := value["slash_commands"]; ok && string(commands) != "null" {
			var names []string
			if err := decodeUseNumber(commands, &names); err != nil {
				return fmt.Errorf("Claude slash_commands: %w", err)
			}
			items := make([]map[string]string, 0, len(names))
			for _, name := range names {
				if strings.TrimSpace(name) != "" {
					items = append(items, map[string]string{"name": name, "description": ""})
				}
			}
			payload, _ := json.Marshal(items)
			s.emit(Event{Kind: EventAvailableCommands, Raw: payload})
		}
	}
	if message := rawString(value["error"]); message != "" {
		s.emit(Event{Kind: EventError, Text: stripPrivate(message), Raw: cloneRaw(raw)})
	}
	return nil
}

func (s *streamSession) beginTurn() {
	s.mu.Lock()
	if !s.active {
		s.active, s.finished = true, false
		s.mu.Unlock()
		s.emit(Event{Kind: EventTurnStarted})
		return
	}
	s.mu.Unlock()
}

func (s *streamSession) consumeStreamEvent(value map[string]json.RawMessage, raw json.RawMessage) error {
	if parent := rawString(value["parent_tool_use_id"]); parent != "" {
		return nil
	}
	var event map[string]json.RawMessage
	if err := decodeUseNumber(value["event"], &event); err != nil || event == nil {
		return errors.New("Claude stream_event has invalid event")
	}
	s.beginTurn()
	if rawString(event["type"]) == "message_start" {
		s.mu.Lock()
		s.sawText, s.sawReason = false, false
		s.mu.Unlock()
	}
	var delta map[string]json.RawMessage
	if len(event["delta"]) == 0 || decodeUseNumber(event["delta"], &delta) != nil {
		return nil
	}
	kind := rawString(delta["type"])
	text := rawString(delta["text"])
	if kind == "thinking_delta" {
		text = rawString(delta["thinking"])
	}
	text = stripPrivate(text)
	if text == "" {
		return nil
	}
	s.mu.Lock()
	if kind == "text_delta" {
		s.sawText = true
	}
	if kind == "thinking_delta" {
		s.sawReason = true
	}
	s.mu.Unlock()
	if kind == "text_delta" {
		s.emit(Event{Kind: EventTextDelta, Text: text, Raw: cloneRaw(raw)})
	}
	if kind == "thinking_delta" {
		s.emit(Event{Kind: EventReasoningDelta, Text: text, Raw: cloneRaw(raw)})
	}
	return nil
}

func (s *streamSession) consumeAssistant(value map[string]json.RawMessage, raw json.RawMessage) error {
	if rawString(value["parent_tool_use_id"]) != "" {
		return nil
	}
	var message map[string]json.RawMessage
	if err := decodeUseNumber(value["message"], &message); err != nil || message == nil {
		return errors.New("provider assistant message is invalid")
	}
	var blocks []map[string]json.RawMessage
	if err := decodeUseNumber(message["content"], &blocks); err != nil {
		return errors.New("provider assistant content must be an array")
	}
	s.beginTurn()
	s.mu.Lock()
	sawText, sawReason := s.sawText, s.sawReason
	s.mu.Unlock()
	for _, block := range blocks {
		switch rawString(block["type"]) {
		case "text":
			if s.provider == ProviderAmp || !sawText {
				if text := stripPrivate(rawString(block["text"])); text != "" {
					s.emit(Event{Kind: EventTextDelta, Text: text, Raw: cloneRaw(raw)})
				}
			}
		case "thinking":
			if s.provider == ProviderAmp || !sawReason {
				if text := stripPrivate(rawString(block["thinking"])); text != "" {
					s.emit(Event{Kind: EventReasoningDelta, Text: text, Raw: cloneRaw(raw)})
				}
			}
		case "tool_use":
			id, name := rawString(block["id"]), rawString(block["name"])
			if id == "" || name == "" {
				return errors.New("provider tool_use is missing id or name")
			}
			activity := Activity{ID: id, Kind: streamToolKind(name), Title: streamToolTitle(name, block["input"]), Tool: name, Arguments: cloneRaw(block["input"])}
			s.mu.Lock()
			activityBytes := streamActivityBytes(activity)
			if len(s.tools) >= 1024 || s.toolBytes+activityBytes > 4<<20 {
				s.mu.Unlock()
				return errors.New("provider unfinished tool state limit exceeded")
			}
			s.tools[id] = activity
			s.toolBytes += activityBytes
			s.mu.Unlock()
			a := activity
			s.emit(Event{Kind: EventActivity, Activity: &a, Raw: cloneRaw(raw)})
		}
	}
	if usage, ok := message["usage"]; ok {
		if used := claudeUsage(usage); used != nil {
			s.emit(Event{Kind: EventUsageUpdated, ContextTokens: used, Raw: cloneRaw(raw)})
		}
	}
	if s.provider == ProviderAmp && rawString(message["stop_reason"]) == "end_turn" {
		s.finish(true, "", raw)
	}
	return nil
}

func claudeUsage(raw json.RawMessage) *uint64 {
	var v struct {
		Input       uint64 `json:"input_tokens"`
		CacheCreate uint64 `json:"cache_creation_input_tokens"`
		CacheRead   uint64 `json:"cache_read_input_tokens"`
	}
	if decodeUseNumber(raw, &v) != nil {
		return nil
	}
	n := v.Input + v.CacheCreate + v.CacheRead
	return &n
}

func (s *streamSession) consumeUser(value map[string]json.RawMessage, raw json.RawMessage) error {
	var replay bool
	_ = decodeUseNumber(value["isReplay"], &replay)
	if replay {
		return nil
	}
	if rawString(value["parent_tool_use_id"]) != "" {
		return nil
	}
	var message map[string]json.RawMessage
	if err := decodeUseNumber(value["message"], &message); err != nil || message == nil {
		return errors.New("provider user message is invalid")
	}
	var blocks []map[string]json.RawMessage
	if err := decodeUseNumber(message["content"], &blocks); err != nil {
		return errors.New("provider user content must be an array")
	}
	for _, block := range blocks {
		if rawString(block["type"]) != "tool_result" {
			continue
		}
		id := rawString(block["tool_use_id"])
		if id == "" {
			return errors.New("provider tool_result is missing tool_use_id")
		}
		s.mu.Lock()
		activity, ok := s.tools[id]
		if ok {
			delete(s.tools, id)
			s.toolBytes -= streamActivityBytes(activity)
		}
		s.mu.Unlock()
		if !ok {
			activity = Activity{ID: id, Kind: "tool", Title: "Tool"}
		}
		activity.Complete = true
		_ = decodeUseNumber(block["is_error"], &activity.Failed)
		activity.Output = cloneRaw(block["content"])
		a := activity
		s.emit(Event{Kind: EventActivity, Activity: &a, Raw: cloneRaw(raw)})
	}
	return nil
}

func (s *streamSession) consumeResult(value map[string]json.RawMessage, raw json.RawMessage) error {
	if s.provider == ProviderAmp {
		var failed bool
		_ = decodeUseNumber(value["is_error"], &failed)
		if failed {
			s.emit(Event{Kind: EventError, Text: firstNonempty(rawString(value["error"]), rawString(value["result"]), "Amp reported an error"), Raw: cloneRaw(raw)})
		}
		return nil
	}
	var failed bool
	if err := decodeUseNumber(value["is_error"], &failed); err != nil {
		return errors.New("Claude result requires boolean is_error")
	}
	summary := ""
	if failed {
		summary = firstNonempty(rawString(value["result"]), "Claude reported an error")
		s.emit(Event{Kind: EventError, Text: summary, Raw: cloneRaw(raw)})
	}
	if rawString(value["stop_reason"]) == "" {
		return errors.New("Claude result is missing stop_reason")
	}
	s.finish(!failed, summary, raw)
	return nil
}

func (s *streamSession) consumeControlRequest(value map[string]json.RawMessage, raw json.RawMessage) error {
	id := rawString(value["request_id"])
	var request map[string]json.RawMessage
	if id == "" || decodeUseNumber(value["request"], &request) != nil || request == nil {
		return errors.New("Claude control_request is malformed")
	}
	if rawString(request["subtype"]) != "can_use_tool" {
		return nil
	}
	s.beginTurn()
	tool := rawString(request["tool_name"])
	if tool == "AskUserQuestion" {
		return s.emitClaudeQuestions(id, request, raw)
	}
	if s.access != AccessAsk {
		return s.writePermission(id, "allow")
	}
	s.mu.Lock()
	s.permissions[id] = streamPermission{raw: cloneRaw(raw)}
	s.mu.Unlock()
	p := &Permission{Title: firstNonempty(rawString(request["display_name"]), tool, "Tool"), Detail: firstNonempty(rawString(request["description"]), rawString(request["blocked_path"])), Options: []PermissionOption{{ID: "allow", Label: "Allow once", Allow: true}, {ID: "deny", Label: "Deny"}}}
	s.emit(Event{Kind: EventPermission, RequestID: json.RawMessage(strconvQuote(id)), Permission: p, Raw: cloneRaw(raw)})
	return nil
}

func strconvQuote(s string) []byte { raw, _ := json.Marshal(s); return raw }

func (s *streamSession) emitClaudeQuestions(id string, request map[string]json.RawMessage, raw json.RawMessage) error {
	var input struct {
		Questions []struct {
			Question, Header string
			Options          []UserInputOption
			MultiSelect      bool `json:"multiSelect"`
		} `json:"questions"`
	}
	if err := decodeUseNumber(request["input"], &input); err != nil || len(input.Questions) == 0 {
		return errors.New("Claude AskUserQuestion has invalid questions")
	}
	questions := make([]UserInputQuestion, 0, len(input.Questions))
	for i, q := range input.Questions {
		q.Question = strings.TrimSpace(q.Question)
		if q.Question == "" {
			return errors.New("Claude question text is empty")
		}
		header := q.Header
		if header == "" {
			header = fmt.Sprintf("Question %d", i+1)
		}
		questions = append(questions, UserInputQuestion{ID: q.Question, Header: header, Question: q.Question, Options: q.Options, MultiSelect: q.MultiSelect})
	}
	s.mu.Lock()
	s.questions[id] = streamQuestion{input: cloneRaw(request["input"])}
	s.mu.Unlock()
	s.emit(Event{Kind: EventUserInputRequested, RequestID: json.RawMessage(strconvQuote(id)), Questions: questions, Raw: cloneRaw(raw)})
	return nil
}

func (s *streamSession) finish(success bool, summary string, raw json.RawMessage) {
	s.mu.Lock()
	if !s.active || s.finished {
		s.mu.Unlock()
		return
	}
	s.active, s.finished = false, true
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnFinished, Success: success, Summary: summary, Raw: cloneRaw(raw)})
}

func (s *streamSession) failActive(reason string) {
	s.mu.Lock()
	active := s.active && !s.finished
	s.active, s.finished = false, true
	s.mu.Unlock()
	if active {
		s.emit(Event{Kind: EventError, Text: reason})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: reason})
	}
}

func (s *streamSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return errors.New("provider prompt is empty")
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("provider turn is already active")
	}
	s.active, s.finished, s.sawText, s.sawReason = true, false, false, false
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnStarted})
	if err := s.writeUser(text, false); err != nil {
		s.failActive(err.Error())
		return err
	}
	return nil
}

func (s *streamSession) writeUser(text string, steer bool) error {
	value := map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": []any{map[string]string{"type": "text", "text": text}}}}
	if s.provider == ProviderClaude {
		value["parent_tool_use_id"] = nil
	}
	if s.provider == ProviderAmp && steer {
		value["steer"] = true
	}
	return s.process.Lines.Write(value)
}

func (s *streamSession) Steer(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	active := s.active && !s.finished
	s.mu.Unlock()
	if !active {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: "provider has no active turn"})
		return errors.New("provider has no active turn")
	}
	if err := s.writeUser(text, true); err != nil {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: err.Error()})
		return err
	}
	s.emit(Event{Kind: EventSteerAccepted, Text: text})
	return nil
}

func (s *streamSession) Cancel(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.provider == ProviderAmp {
		s.failActive("turn cancelled")
		go terminateProcess(s.process)
		return nil
	}
	s.mu.Lock()
	s.next++
	id := fmt.Sprintf("waku-%d", s.next)
	s.mu.Unlock()
	return s.process.Lines.Write(map[string]any{"type": "control_request", "request_id": id, "request": map[string]string{"subtype": "interrupt"}})
}

func decodeRequestID(raw json.RawMessage) (string, error) {
	var id string
	if err := decodeUseNumber(raw, &id); err != nil || id == "" {
		return "", errors.New("request id must be a non-empty string")
	}
	return id, nil
}

func (s *streamSession) writePermission(id, option string) error {
	behavior := map[string]any{"behavior": "allow"}
	if option == "deny" {
		behavior = map[string]any{"behavior": "deny", "message": "The user denied this tool call."}
	}
	return s.process.Lines.Write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": id, "response": behavior}})
}

func (s *streamSession) RespondPermission(ctx context.Context, requestID json.RawMessage, option string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.provider != ProviderClaude {
		return ErrUnsupported
	}
	id, err := decodeRequestID(requestID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	_, ok := s.permissions[id]
	s.mu.Unlock()
	if !ok {
		return errors.New("unknown permission request")
	}
	if option != "allow" && option != "deny" {
		return errors.New("unknown permission option")
	}
	if err := s.writePermission(id, option); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.permissions, id)
	s.mu.Unlock()
	return nil
}

func (s *streamSession) RespondQuestions(ctx context.Context, requestID json.RawMessage, answers []UserInputAnswer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.provider != ProviderClaude {
		return ErrUnsupported
	}
	id, err := decodeRequestID(requestID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	pending, ok := s.questions[id]
	s.mu.Unlock()
	if !ok {
		return errors.New("unknown question request")
	}
	var input map[string]json.RawMessage
	if decodeUseNumber(pending.input, &input) != nil {
		return errors.New("invalid pending question")
	}
	answerMap := make(map[string]any, len(answers))
	for _, answer := range answers {
		if len(answer.Answers) == 1 {
			answerMap[answer.QuestionID] = answer.Answers[0]
		} else {
			answerMap[answer.QuestionID] = answer.Answers
		}
	}
	questionsRaw, ok := input["questions"]
	if !ok {
		return errors.New("pending question is missing questions")
	}
	var originalQuestions any
	if err := decodeUseNumber(questionsRaw, &originalQuestions); err != nil {
		return errors.New("pending questions are invalid")
	}
	frame := map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": id, "response": map[string]any{"behavior": "allow", "updatedInput": map[string]any{"questions": originalQuestions, "answers": answerMap}}}}
	if err := s.process.Lines.Write(frame); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.questions, id)
	s.mu.Unlock()
	return nil
}

func (s *streamSession) Goal(context.Context, GoalOperation) error { return ErrUnsupported }
func (s *streamSession) ApplyOptions(ctx context.Context, options SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.provider == ProviderAmp {
		return false, nil
	}
	if options.Access != "" && normalizedAccess(options.Access) != s.access {
		return false, nil
	}
	if options.ServiceTier != "" {
		return false, nil
	}
	if options.Model == "" {
		return true, nil
	}
	s.mu.Lock()
	s.next++
	id := fmt.Sprintf("waku-%d", s.next)
	s.mu.Unlock()
	err := s.process.Lines.Write(map[string]any{"type": "control_request", "request_id": id, "request": map[string]string{"subtype": "set_model", "model": options.Model}})
	return err == nil, err
}
func (s *streamSession) Rollback(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *streamSession) Fork(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *streamSession) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.failActive("provider session closed")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.process.Terminate(ctx)
	return err
}

func streamToolKind(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "bash") || strings.Contains(lower, "command"):
		return "command"
	case strings.Contains(lower, "edit") || strings.Contains(lower, "write"):
		return "fileChange"
	case strings.Contains(lower, "read"):
		return "fileRead"
	case strings.Contains(lower, "search") || strings.Contains(lower, "grep") || strings.Contains(lower, "web"):
		return "search"
	case strings.Contains(lower, "plan"):
		return "plan"
	default:
		return "tool"
	}
}
func streamToolTitle(name string, input json.RawMessage) string {
	var v map[string]json.RawMessage
	if decodeUseNumber(input, &v) == nil {
		for _, key := range []string{"title", "command", "query", "file_path", "path"} {
			if text := rawString(v[key]); text != "" {
				return stripPrivate(text)
			}
		}
	}
	return stripPrivate(name)
}
func streamActivityBytes(activity Activity) int {
	return len(activity.ID) + len(activity.Kind) + len(activity.Title) + len(activity.Tool) + len(activity.Arguments) + len(activity.Output) + 64
}
func firstNonempty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
