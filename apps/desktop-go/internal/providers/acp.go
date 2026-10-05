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

// AcpAdapter drives Cursor, Fx, Grok Build, and Kimi Code through one bounded
// ACP v1 JSON-RPC transport.
type AcpAdapter struct {
	Provider     Provider
	startProcess func(context.Context, ProcessSpec) (*StdioProcess, error)
}

func (a AcpAdapter) Descriptor() Descriptor { return Descriptors()[a.Provider] }

func (a AcpAdapter) Discover(ctx context.Context, options StartOptions) (Discovery, error) {
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	descriptor, err := a.descriptor()
	if err != nil {
		return Discovery{}, err
	}
	binary := options.Binary
	if binary == "" {
		binary = descriptor.Binary
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return Discovery{Reason: err.Error()}, nil
	}
	return Discovery{Available: true, Executable: path}, nil
}

func (a AcpAdapter) Start(ctx context.Context, options StartOptions, sink Sink) (Session, error) {
	descriptor, err := a.descriptor()
	if err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("provider event sink is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateACPOptions(a.Provider, options); err != nil {
		return nil, err
	}
	cwd := options.WorkingDirectory
	if cwd == "" {
		cwd, err = os.Getwd()
	} else {
		cwd, err = filepath.Abs(cwd)
	}
	if err != nil {
		return nil, fmt.Errorf("ACP working directory: %w", err)
	}
	binary := options.Binary
	if binary == "" {
		binary = descriptor.Binary
	}
	spec := ProcessSpec{Path: binary, Args: acpArgs(a.Provider), Directory: cwd}
	if a.Provider == ProviderGrok {
		spec.Env = append(os.Environ(), "GROK_OAUTH2_REFERRER=waku")
	}
	starter := StartStdio
	if a.startProcess != nil {
		starter = a.startProcess
	}
	process, err := starter(ctx, spec)
	if err != nil {
		return nil, err
	}
	s := &acpSession{
		provider: a.Provider, process: process, sink: sink, cwd: cwd, access: normalizedAccess(options.Access),
		eventQueue: make(chan Event, 256), pendingPrompts: make(map[uint64]acpPrompt), ignoredPrompts: make(map[uint64]struct{}),
		permissions: make(map[string]acpPermissionRequest), questions: make(map[string]acpQuestionRequest), inboundSizes: make(map[string]int),
		stderrDone: make(chan struct{}),
	}
	s.next.Store(10)
	go func() { _, _ = io.Copy(io.Discard, process.Stderr); close(s.stderrDone) }()
	initialized := make(chan error, 1)
	go func() { initialized <- s.initialize(options) }()
	select {
	case err = <-initialized:
	case <-ctx.Done():
		_ = process.Terminate(ctx)
		return nil, ctx.Err()
	}
	if err != nil {
		terminateProcess(process)
		return nil, err
	}
	go s.dispatchEvents()
	go s.readLoop()
	return s, nil
}

func (a AcpAdapter) descriptor() (Descriptor, error) {
	descriptor, ok := Descriptors()[a.Provider]
	if !ok || !descriptor.Implemented || descriptor.Transport != "stdio-acp-jsonrpc" {
		return Descriptor{}, fmt.Errorf("%w: %q does not have an ACP adapter", ErrUnsupported, a.Provider)
	}
	return descriptor, nil
}

func acpArgs(provider Provider) []string {
	switch provider {
	case ProviderCursor, ProviderFx, ProviderKimi:
		return []string{"acp"}
	case ProviderGrok:
		return []string{"agent", "stdio"}
	default:
		return nil
	}
}

func validateACPOptions(provider Provider, options StartOptions) error {
	switch normalizedAccess(options.Access) {
	case AccessAsk, AccessAutoAcceptEdits, AccessAuto, AccessFull:
	default:
		return fmt.Errorf("unknown access mode %q", options.Access)
	}
	if options.Model != "" || options.ReasoningEffort != "" || options.ServiceTier != "" {
		return fmt.Errorf("%w: ACP model and reasoning selection is not implemented", ErrUnsupported)
	}
	if options.ComputerUse {
		return fmt.Errorf("%w: ACP computer use is not implemented", ErrUnsupported)
	}
	if _, err := parseACPCursor(provider, options.ResumeCursor); err != nil {
		return err
	}
	return nil
}

func normalizedAccess(access AccessMode) AccessMode {
	if access == "" {
		return AccessAsk
	}
	return access
}

func parseACPCursor(provider Provider, raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var cursor struct {
		Provider  Provider `json:"provider"`
		SessionID string   `json:"sessionId"`
	}
	if err := decodeUseNumber(raw, &cursor); err != nil {
		return "", fmt.Errorf("ACP resume cursor: %w", err)
	}
	if cursor.Provider != provider {
		return "", fmt.Errorf("%s adapter cannot resume %q cursor", provider, cursor.Provider)
	}
	if cursor.SessionID == "" {
		return "", errors.New("ACP resume cursor has no sessionId")
	}
	return cursor.SessionID, nil
}

type acpPermissionRequest struct {
	id        json.RawMessage
	options   map[string]struct{}
	resolving bool
}
type acpQuestionRequest struct {
	id        json.RawMessage
	method    string
	params    json.RawMessage
	resolving bool
}

type acpPrompt struct {
	size        int
	extensionID string
	kimi        kimiWireOffset
	produced    bool
}

type acpSession struct {
	provider Provider
	process  *StdioProcess
	sink     Sink
	cwd      string
	access   AccessMode
	session  string
	normal   *acpNormalizer

	mu             sync.Mutex
	pendingPrompts map[uint64]acpPrompt
	ignoredPrompts map[uint64]struct{}
	pendingBytes   int
	permissions    map[string]acpPermissionRequest
	questions      map[string]acpQuestionRequest
	inboundSizes   map[string]int
	inboundBytes   int
	next           atomic.Uint64
	closed         atomic.Bool

	eventMu      sync.Mutex
	eventQueue   chan Event
	eventBytes   int
	eventsClosed bool
	slowSink     atomic.Bool
	stderrDone   chan struct{}
}

type acpInitialize struct{ loadSession, resume bool }
type acpSessionHandshake struct {
	sessionID string
	modes     *acpModes
}
type acpModes struct {
	current   string
	available []string
}

func (s *acpSession) initialize(options StartOptions) error {
	capabilities := map[string]any{"fs": map[string]bool{"readTextFile": false, "writeTextFile": false}, "terminal": false}
	if s.provider == ProviderCursor {
		capabilities["_meta"] = map[string]bool{"parameterizedModelPicker": true}
	}
	response, err := s.directCall(1, "initialize", map[string]any{
		"protocolVersion": 1, "clientCapabilities": capabilities,
		"clientInfo": map[string]string{"name": "waku", "version": "go"},
	}, false)
	if err != nil {
		return fmt.Errorf("ACP initialize: %w", err)
	}
	initialized, err := parseACPInitialize(response.Result)
	if err != nil {
		return fmt.Errorf("ACP initialize response: %w", err)
	}
	resumeID, _ := parseACPCursor(s.provider, options.ResumeCursor)
	var sessionResult RPCMessage
	var handshake acpSessionHandshake
	if resumeID != "" && initialized.resume {
		sessionResult, callErr := s.directCall(2, "session/resume", map[string]any{"sessionId": resumeID, "cwd": s.cwd, "mcpServers": []any{}}, false)
		if callErr == nil {
			handshake, err = parseACPSessionResult(sessionResult.Result, false)
			if err != nil {
				return fmt.Errorf("ACP session/resume response: %w", err)
			}
			s.session = resumeID
		}
	}
	if s.session == "" && resumeID != "" && initialized.loadSession {
		sessionResult, callErr := s.directCall(3, "session/load", map[string]any{"sessionId": resumeID, "cwd": s.cwd, "mcpServers": []any{}}, true)
		if callErr == nil {
			handshake, err = parseACPSessionResult(sessionResult.Result, false)
			if err != nil {
				return fmt.Errorf("ACP session/load response: %w", err)
			}
			s.session = resumeID
		}
	}
	if s.session == "" {
		sessionResult, err = s.directCall(4, "session/new", map[string]any{"cwd": s.cwd, "mcpServers": []any{}}, false)
		if err != nil {
			return fmt.Errorf("ACP session/new: %w", err)
		}
		handshake, err = parseACPSessionResult(sessionResult.Result, true)
		if err != nil {
			return fmt.Errorf("ACP session/new response: %w", err)
		}
		s.session = handshake.sessionID
	}
	if s.provider == ProviderFx {
		if mode := fxDesiredMode(handshake.modes, s.access); mode != "" {
			if _, err := s.directCall(5, "session/set_mode", map[string]any{"sessionId": s.session, "modeId": mode}, false); err != nil {
				return fmt.Errorf("Fx session mode: %w", err)
			}
		}
	}
	s.normal = newACPNormalizer(s.provider, s.session)
	cursor, _ := json.Marshal(map[string]any{"provider": s.provider, "sessionId": s.session})
	s.emit(Event{Kind: EventConnected, Cursor: cursor})
	return nil
}

func parseACPInitialize(raw json.RawMessage) (acpInitialize, error) {
	var fields map[string]json.RawMessage
	if err := decodeUseNumber(raw, &fields); err != nil || fields == nil {
		return acpInitialize{}, errors.New("result must be an object")
	}
	var version json.Number
	if err := decodeUseNumber(fields["protocolVersion"], &version); err != nil || version.String() != "1" {
		return acpInitialize{}, errors.New("protocolVersion must be 1")
	}
	var capabilities map[string]json.RawMessage
	if err := decodeUseNumber(fields["agentCapabilities"], &capabilities); err != nil || capabilities == nil {
		return acpInitialize{}, errors.New("agentCapabilities must be an object")
	}
	result := acpInitialize{}
	if rawLoad, exists := capabilities["loadSession"]; exists {
		if string(rawLoad) == "null" || decodeUseNumber(rawLoad, &result.loadSession) != nil {
			return acpInitialize{}, errors.New("loadSession must be a boolean")
		}
	}
	if rawSessions, exists := capabilities["sessionCapabilities"]; exists {
		var sessions map[string]json.RawMessage
		if string(rawSessions) == "null" || decodeUseNumber(rawSessions, &sessions) != nil || sessions == nil {
			return acpInitialize{}, errors.New("sessionCapabilities must be an object")
		}
		if rawResume, exists := sessions["resume"]; exists {
			var resume map[string]json.RawMessage
			if string(rawResume) == "null" || decodeUseNumber(rawResume, &resume) != nil || resume == nil {
				return acpInitialize{}, errors.New("resume capability must be an object")
			}
			result.resume = true
		}
	}
	return result, nil
}

func parseACPSessionResult(raw json.RawMessage, requireID bool) (acpSessionHandshake, error) {
	var fields map[string]json.RawMessage
	if err := decodeUseNumber(raw, &fields); err != nil || fields == nil {
		return acpSessionHandshake{}, errors.New("result must be an object")
	}
	result := acpSessionHandshake{}
	if requireID {
		if err := decodeUseNumber(fields["sessionId"], &result.sessionID); err != nil || strings.TrimSpace(result.sessionID) == "" {
			return acpSessionHandshake{}, errors.New("sessionId must be a non-empty string")
		}
	}
	if rawModes, exists := fields["modes"]; exists {
		if string(rawModes) == "null" {
			return acpSessionHandshake{}, errors.New("modes must not be null")
		}
		var modeFields map[string]json.RawMessage
		if err := decodeUseNumber(rawModes, &modeFields); err != nil || modeFields == nil {
			return acpSessionHandshake{}, errors.New("modes must contain currentModeId and availableModes")
		}
		var current string
		if err := decodeUseNumber(modeFields["currentModeId"], &current); err != nil || strings.TrimSpace(current) == "" {
			return acpSessionHandshake{}, errors.New("modes currentModeId must be a non-empty string")
		}
		var available []json.RawMessage
		if rawAvailable, present := modeFields["availableModes"]; !present || string(rawAvailable) == "null" || decodeUseNumber(rawAvailable, &available) != nil {
			return acpSessionHandshake{}, errors.New("modes availableModes must be an array")
		}
		result.modes = &acpModes{current: current}
		for _, rawMode := range available {
			var mode map[string]json.RawMessage
			if decodeUseNumber(rawMode, &mode) != nil || mode == nil {
				return acpSessionHandshake{}, errors.New("available mode must be an object")
			}
			var id string
			if decodeUseNumber(mode["id"], &id) != nil || strings.TrimSpace(id) == "" {
				return acpSessionHandshake{}, errors.New("available mode has no id")
			}
			result.modes.available = append(result.modes.available, id)
		}
	}
	if rawConfig, exists := fields["configOptions"]; exists {
		if string(rawConfig) == "null" {
			return acpSessionHandshake{}, errors.New("configOptions must not be null")
		}
		var options []json.RawMessage
		if err := decodeUseNumber(rawConfig, &options); err != nil {
			return acpSessionHandshake{}, errors.New("configOptions must be an array")
		}
		for _, option := range options {
			if err := validateACPConfigOption(option); err != nil {
				return acpSessionHandshake{}, fmt.Errorf("invalid config option: %w", err)
			}
		}
	}
	return result, nil
}

func validateACPConfigOption(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if decodeUseNumber(raw, &fields) != nil || fields == nil {
		return errors.New("entry must be an object")
	}
	for _, key := range []string{"id", "name", "type"} {
		var value string
		if decodeUseNumber(fields[key], &value) != nil || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must be a non-empty string", key)
		}
	}
	if err := validateACPOptionalString(fields, "description"); err != nil {
		return err
	}
	if err := validateACPOptionalString(fields, "category"); err != nil {
		return err
	}
	if err := validateACPOptionalObject(fields, "_meta"); err != nil {
		return err
	}
	var kind string
	_ = decodeUseNumber(fields["type"], &kind)
	switch kind {
	case "boolean":
		var current bool
		if decodeUseNumber(fields["currentValue"], &current) != nil {
			return errors.New("boolean currentValue must be a boolean")
		}
	case "select":
		var current string
		if decodeUseNumber(fields["currentValue"], &current) != nil {
			return errors.New("select currentValue must be a string")
		}
		var options []json.RawMessage
		if rawOptions, present := fields["options"]; !present || string(rawOptions) == "null" || decodeUseNumber(rawOptions, &options) != nil {
			return errors.New("select options must be an array")
		}
		shape := ""
		for _, rawOption := range options {
			var option map[string]json.RawMessage
			if decodeUseNumber(rawOption, &option) != nil || option == nil {
				return errors.New("select option must be an object")
			}
			entryShape := "option"
			if _, grouped := option["group"]; grouped {
				entryShape = "group"
			}
			if shape != "" && shape != entryShape {
				return errors.New("select options cannot mix grouped and ungrouped entries")
			}
			shape = entryShape
			if entryShape == "group" {
				if err := validateACPConfigGroup(option); err != nil {
					return err
				}
			} else if err := validateACPConfigSelectValue(option); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported config option type %q", kind)
	}
	return nil
}

func validateACPConfigGroup(group map[string]json.RawMessage) error {
	for _, key := range []string{"group", "name"} {
		var value string
		if decodeUseNumber(group[key], &value) != nil || strings.TrimSpace(value) == "" {
			return fmt.Errorf("config group %s must be a non-empty string", key)
		}
	}
	if err := validateACPOptionalObject(group, "_meta"); err != nil {
		return err
	}
	var options []json.RawMessage
	if rawOptions, present := group["options"]; !present || string(rawOptions) == "null" || decodeUseNumber(rawOptions, &options) != nil {
		return errors.New("config group options must be an array")
	}
	for _, rawOption := range options {
		var option map[string]json.RawMessage
		if decodeUseNumber(rawOption, &option) != nil || option == nil {
			return errors.New("grouped select option must be an object")
		}
		if err := validateACPConfigSelectValue(option); err != nil {
			return err
		}
	}
	return nil
}

func validateACPConfigSelectValue(option map[string]json.RawMessage) error {
	for _, key := range []string{"value", "name"} {
		var value string
		if decodeUseNumber(option[key], &value) != nil || strings.TrimSpace(value) == "" {
			return fmt.Errorf("select option %s must be a non-empty string", key)
		}
	}
	if err := validateACPOptionalString(option, "description"); err != nil {
		return err
	}
	if err := validateACPOptionalObject(option, "_meta"); err != nil {
		return err
	}
	return nil
}

func validateACPOptionalString(fields map[string]json.RawMessage, key string) error {
	raw, present := fields[key]
	if !present || string(raw) == "null" {
		return nil
	}
	var value string
	if decodeUseNumber(raw, &value) != nil {
		return fmt.Errorf("%s must be a string or null", key)
	}
	return nil
}

func validateACPOptionalObject(fields map[string]json.RawMessage, key string) error {
	raw, present := fields[key]
	if !present || string(raw) == "null" {
		return nil
	}
	var value map[string]json.RawMessage
	if decodeUseNumber(raw, &value) != nil || value == nil {
		return fmt.Errorf("%s must be an object or null", key)
	}
	return nil
}

func fxDesiredMode(modes *acpModes, access AccessMode) string {
	if modes == nil {
		return ""
	}
	desired := "code"
	if access == AccessAsk {
		desired = "ask"
	}
	if strings.EqualFold(modes.current, desired) {
		return ""
	}
	for _, mode := range modes.available {
		if strings.EqualFold(mode, desired) {
			return mode
		}
	}
	return ""
}

func (s *acpSession) directCall(id uint64, method string, params any, suppressUpdates bool) (RPCMessage, error) {
	if err := s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return RPCMessage{}, err
	}
	for {
		var raw json.RawMessage
		if err := s.process.Lines.Decode(&raw); err != nil {
			return RPCMessage{}, err
		}
		message, err := decodeRPCMessage(raw)
		if err != nil {
			return RPCMessage{}, err
		}
		if message.JSONRPC != "2.0" {
			return RPCMessage{}, errors.New("ACP message is missing jsonrpc 2.0")
		}
		if message.IsResponse() {
			responseID, ok := rawUint64(message.ID)
			if !ok || responseID != id {
				return RPCMessage{}, fmt.Errorf("ACP handshake received unexpected response id %s", message.ID)
			}
			if len(message.Error) != 0 {
				return message, errors.New(responseErrorText(message.Error))
			}
			return message, nil
		}
		if suppressUpdates && message.Method == "session/update" {
			continue
		}
		if err := s.handleInbound(message, raw); err != nil {
			return RPCMessage{}, err
		}
	}
}

func (s *acpSession) readLoop() {
	var readErr error
	fatalTransport := false
	for {
		var raw json.RawMessage
		if err := s.process.Lines.Decode(&raw); err != nil {
			readErr = err
			break
		}
		message, err := decodeRPCMessage(raw)
		if err != nil {
			if id, ok := malformedResponseID(raw); ok && s.failMalformedPrompt(id, err, raw) {
				continue
			}
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
			continue
		}
		if message.JSONRPC != "2.0" {
			err = errors.New("ACP message is missing jsonrpc 2.0")
			if id, ok := rawUint64(message.ID); ok && s.failMalformedPrompt(id, err, raw) {
				continue
			}
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
			continue
		}
		if message.IsResponse() {
			if !s.handlePromptResponse(message, raw) {
				s.emit(Event{Kind: EventError, Text: "ACP received an uncorrelated response", Raw: cloneRaw(raw)})
			}
			continue
		}
		if err := s.handleInbound(message, raw); err != nil {
			if message.IsRequest() {
				_ = s.writeACPError(message.ID, -32602, err.Error())
			}
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
			if errors.Is(err, errACPToolStateLimit) {
				fatalTransport = true
				break
			}
		}
	}
	if fatalTransport {
		terminateProcess(s.process)
	} else {
		_ = s.process.Close()
	}
	s.failPending("provider process exited")
	waitErr := s.process.Wait()
	<-s.stderrDone
	if readErr != nil && readErr != io.EOF && !s.closed.Load() {
		s.emit(Event{Kind: EventError, Text: readErr.Error()})
	}
	if waitErr != nil && !s.closed.Load() {
		s.emit(Event{Kind: EventError, Text: waitErr.Error()})
	}
	s.eventMu.Lock()
	if !s.eventsClosed {
		s.eventsClosed = true
		close(s.eventQueue)
	}
	s.eventMu.Unlock()
}

func (s *acpSession) handleInbound(message RPCMessage, raw json.RawMessage) error {
	switch message.Method {
	case "session/update":
		if s.normal == nil {
			return errors.New("ACP session/update arrived before session establishment")
		}
		s.mu.Lock()
		events, err := s.normal.consume(message.Params, raw)
		if err == nil && acpEventsProduced(events) {
			s.markLatestPromptProducedLocked()
		}
		s.mu.Unlock()
		if err != nil {
			return err
		}
		for _, event := range events {
			s.emit(event)
		}
		return nil
	case "session/request_permission":
		if !message.IsRequest() {
			return errors.New("ACP permission message is not a request")
		}
		return s.handlePermission(message, raw)
	case "cursor/ask_question":
		if s.provider != ProviderCursor || !message.IsRequest() {
			return s.rejectUnknown(message)
		}
		return s.handleQuestions(message, raw, "cursor")
	case "_x.ai/ask_user_question", "x.ai/ask_user_question":
		if s.provider != ProviderGrok || !message.IsRequest() {
			return s.rejectUnknown(message)
		}
		return s.handleQuestions(message, raw, "xai")
	case "_x.ai/session/prompt_complete":
		if s.provider != ProviderGrok || message.IsRequest() {
			return s.rejectUnknown(message)
		}
		return s.handleGrokPromptComplete(message.Params, raw)
	default:
		if message.IsRequest() {
			return s.rejectUnknown(message)
		}
		return nil
	}
}

func (s *acpSession) rejectUnknown(message RPCMessage) error {
	if !message.IsRequest() {
		return nil
	}
	return s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "id": cloneRaw(message.ID), "error": map[string]any{"code": -32601, "message": "method not supported by Waku"}})
}

func (s *acpSession) handlePermission(message RPCMessage, raw json.RawMessage) error {
	var identity struct {
		SessionID string `json:"sessionId"`
	}
	if err := decodeUseNumber(message.Params, &identity); err != nil || identity.SessionID != s.session {
		return fmt.Errorf("ACP permission has unexpected sessionId %q", identity.SessionID)
	}
	permission, options, err := parseACPPermission(message.Params)
	if err != nil {
		return err
	}
	if s.access != AccessAsk {
		choice := ""
		for _, preferred := range []string{"allow_always", "allow_once"} {
			for _, option := range options {
				if option.kind == preferred {
					choice = option.id
					break
				}
			}
			if choice != "" {
				break
			}
		}
		if choice == "" {
			return s.writeACPResponse(message.ID, map[string]any{"outcome": map[string]string{"outcome": "cancelled"}})
		}
		return s.writeACPResponse(message.ID, map[string]any{"outcome": map[string]string{"outcome": "selected", "optionId": choice}})
	}
	key := string(message.ID)
	s.mu.Lock()
	if _, exists := s.permissions[key]; exists || s.questions[key].id != nil {
		s.mu.Unlock()
		return errors.New("ACP request id is already pending")
	}
	size := len(message.Params) + len(message.ID)
	if len(s.permissions)+len(s.questions) >= maxPendingRPCs || s.inboundBytes+size > maxPendingRPCBytes {
		s.mu.Unlock()
		return errors.New("ACP inbound request limit exceeded")
	}
	validOptions := make(map[string]struct{}, len(options))
	for _, option := range options {
		validOptions[option.id] = struct{}{}
	}
	s.permissions[key] = acpPermissionRequest{id: cloneRaw(message.ID), options: validOptions}
	s.inboundSizes[key] = size
	s.inboundBytes += size
	s.mu.Unlock()
	s.emit(Event{Kind: EventPermission, RequestID: cloneRaw(message.ID), Permission: permission, Raw: cloneRaw(raw)})
	return nil
}

type acpPermissionOption struct{ id, kind string }

func parseACPPermission(params json.RawMessage) (*Permission, []acpPermissionOption, error) {
	var value struct {
		SessionID string `json:"sessionId"`
		ToolCall  struct {
			Title   string            `json:"title"`
			Kind    string            `json:"kind"`
			Content []json.RawMessage `json:"content"`
		} `json:"toolCall"`
		Options []struct {
			ID   string `json:"optionId"`
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"options"`
	}
	if err := decodeUseNumber(params, &value); err != nil {
		return nil, nil, fmt.Errorf("ACP permission params: %w", err)
	}
	if value.SessionID == "" || len(value.Options) == 0 {
		return nil, nil, errors.New("ACP permission request is missing sessionId or options")
	}
	permission := &Permission{Title: strings.TrimSpace(value.ToolCall.Title)}
	if permission.Title == "" {
		permission.Title = "Run a tool"
	}
	var native []acpPermissionOption
	for _, option := range value.Options {
		if option.ID == "" || option.Name == "" || !isPermissionKind(option.Kind) {
			return nil, nil, errors.New("ACP permission request has an invalid option")
		}
		allow := option.Kind == "allow_once" || option.Kind == "allow_always"
		permission.Options = append(permission.Options, PermissionOption{ID: option.ID, Label: option.Name, Allow: allow})
		native = append(native, acpPermissionOption{id: option.ID, kind: option.Kind})
	}
	for _, content := range value.ToolCall.Content {
		var item struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			Content *struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if decodeUseNumber(content, &item) == nil {
			text := strings.TrimSpace(item.Text)
			if item.Content != nil {
				text = strings.TrimSpace(item.Content.Text)
			}
			if text != "" {
				if permission.Detail != "" {
					permission.Detail += "\n"
				}
				permission.Detail += text
			}
		}
	}
	detailRunes := []rune(permission.Detail)
	if len(detailRunes) > 400 {
		permission.Detail = string(detailRunes[:400]) + "…"
	}
	return permission, native, nil
}

func isPermissionKind(kind string) bool {
	return kind == "allow_once" || kind == "allow_always" || kind == "reject_once" || kind == "reject_always"
}

func (s *acpSession) handleQuestions(message RPCMessage, raw json.RawMessage, flavor string) error {
	params := cloneRaw(message.Params)
	if flavor == "xai" {
		var wrapper struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if decodeUseNumber(params, &wrapper) == nil && strings.Contains(wrapper.Method, "ask_user_question") && len(wrapper.Params) != 0 {
			params = wrapper.Params
		}
	}
	questions, err := parseACPQuestions(params, flavor)
	if err != nil {
		return err
	}
	key := string(message.ID)
	s.mu.Lock()
	if _, exists := s.permissions[key]; exists || s.questions[key].id != nil {
		s.mu.Unlock()
		return errors.New("ACP request id is already pending")
	}
	size := len(params) + len(message.ID)
	if len(s.permissions)+len(s.questions) >= maxPendingRPCs || s.inboundBytes+size > maxPendingRPCBytes {
		s.mu.Unlock()
		return errors.New("ACP inbound request limit exceeded")
	}
	s.questions[key] = acpQuestionRequest{id: cloneRaw(message.ID), method: flavor, params: params}
	s.inboundSizes[key] = size
	s.inboundBytes += size
	s.mu.Unlock()
	s.emit(Event{Kind: EventUserInputRequested, RequestID: cloneRaw(message.ID), Questions: questions, Raw: cloneRaw(raw)})
	return nil
}

func parseACPQuestions(params json.RawMessage, flavor string) ([]UserInputQuestion, error) {
	var value struct {
		Questions []map[string]json.RawMessage `json:"questions"`
	}
	if err := decodeUseNumber(params, &value); err != nil {
		return nil, fmt.Errorf("ACP questions: %w", err)
	}
	var result []UserInputQuestion
	for index, question := range value.Questions {
		text := rawString(question["question"])
		if flavor == "cursor" {
			text = rawString(question["prompt"])
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return nil, errors.New("ACP question has no prompt")
		}
		id := rawString(question["id"])
		if id == "" {
			id = text
		}
		header := "Question"
		if flavor == "xai" {
			header = fmt.Sprintf("Question %d", index+1)
		}
		q := UserInputQuestion{ID: id, Header: header, Question: text}
		if raw, present := question["multiSelect"]; present {
			if err := decodeUseNumber(raw, &q.MultiSelect); err != nil {
				return nil, errors.New("ACP question has invalid multiSelect")
			}
		}
		if flavor == "cursor" {
			if raw, present := question["allowMultiple"]; present {
				if err := decodeUseNumber(raw, &q.MultiSelect); err != nil {
					return nil, errors.New("ACP question has invalid allowMultiple")
				}
			}
		}
		var options []map[string]json.RawMessage
		if raw, present := question["options"]; present {
			if err := decodeUseNumber(raw, &options); err != nil {
				return nil, errors.New("ACP question has invalid options")
			}
		}
		for _, option := range options {
			label := strings.TrimSpace(rawString(option["label"]))
			if label == "" {
				return nil, errors.New("ACP question option has no label")
			}
			description := strings.TrimSpace(rawString(option["description"]))
			if flavor == "cursor" && description == "" {
				description = label
			}
			q.Options = append(q.Options, UserInputOption{Label: label, Description: description})
		}
		if len(q.Options) == 0 {
			q.Options = []UserInputOption{{Label: "OK", Description: "Continue"}}
		}
		result = append(result, q)
	}
	if len(result) == 0 {
		return nil, errors.New("ACP question request has no questions")
	}
	return result, nil
}

func (s *acpSession) handlePromptResponse(message RPCMessage, raw json.RawMessage) bool {
	id, ok := rawUint64(message.ID)
	if !ok {
		return false
	}
	s.mu.Lock()
	if _, ignored := s.ignoredPrompts[id]; ignored {
		delete(s.ignoredPrompts, id)
		s.mu.Unlock()
		return true
	}
	prompt, found := s.pendingPrompts[id]
	if !found {
		s.mu.Unlock()
		return false
	}
	delete(s.pendingPrompts, id)
	s.pendingBytes -= prompt.size
	settled := len(s.pendingPrompts) == 0
	s.mu.Unlock()
	if !settled {
		return true
	}
	s.finishPrompt(message.Error, message.Result, raw, prompt.produced, prompt.kimi)
	return true
}

func (s *acpSession) handleGrokPromptComplete(params, raw json.RawMessage) error {
	var value struct {
		SessionID  string `json:"sessionId"`
		PromptID   string `json:"promptId"`
		StopReason string `json:"stopReason"`
	}
	if err := decodeUseNumber(params, &value); err != nil {
		return fmt.Errorf("Grok prompt completion: %w", err)
	}
	if value.SessionID != s.session || value.PromptID == "" || value.StopReason == "" {
		return errors.New("Grok prompt completion has invalid correlation fields")
	}
	s.mu.Lock()
	var id uint64
	var prompt acpPrompt
	for candidate, pending := range s.pendingPrompts {
		if pending.extensionID == value.PromptID {
			id, prompt = candidate, pending
			break
		}
	}
	if id == 0 {
		s.mu.Unlock()
		return nil
	}
	delete(s.pendingPrompts, id)
	s.pendingBytes -= prompt.size
	s.ignorePromptLocked(id)
	settled := len(s.pendingPrompts) == 0
	s.mu.Unlock()
	if !settled {
		return nil
	}
	result, _ := json.Marshal(map[string]string{"stopReason": value.StopReason})
	s.finishPrompt(nil, result, raw, prompt.produced, kimiWireOffset{})
	return nil
}

func (s *acpSession) finishPrompt(errorRaw, resultRaw, raw json.RawMessage, produced bool, kimi kimiWireOffset) {
	if len(errorRaw) != 0 {
		text := responseErrorText(errorRaw)
		s.emit(Event{Kind: EventError, Text: text, Raw: cloneRaw(raw)})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: text, Raw: cloneRaw(raw)})
		return
	}
	var result struct {
		StopReason string `json:"stopReason"`
	}
	if err := decodeUseNumber(resultRaw, &result); err != nil || result.StopReason == "" {
		text := "ACP prompt response has no valid stopReason"
		s.emit(Event{Kind: EventError, Text: text, Raw: cloneRaw(raw)})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: text, Raw: cloneRaw(raw)})
		return
	}
	success, summary, valid := acpStopReason(result.StopReason)
	if !valid {
		s.emit(Event{Kind: EventError, Text: summary, Raw: cloneRaw(raw)})
	}
	if s.provider == ProviderKimi && result.StopReason == "end_turn" && success && !produced {
		success = false
		summary = kimiTurnFailure(kimi)
		if summary == "" {
			summary = "Kimi Code ended without output; the provider may have rejected the turn"
		}
		s.emit(Event{Kind: EventError, Text: summary, Raw: cloneRaw(raw)})
	}
	s.emit(Event{Kind: EventTurnFinished, Success: success, Summary: summary, Raw: cloneRaw(raw)})
}

func (s *acpSession) ignorePromptLocked(id uint64) {
	if len(s.ignoredPrompts) >= maxPendingRPCs {
		for existing := range s.ignoredPrompts {
			delete(s.ignoredPrompts, existing)
			break
		}
	}
	s.ignoredPrompts[id] = struct{}{}
}

func acpEventsProduced(events []Event) bool {
	for _, event := range events {
		if event.Kind == EventTextDelta || event.Kind == EventReasoningDelta || event.Kind == EventActivity {
			return true
		}
	}
	return false
}

func (s *acpSession) markLatestPromptProducedLocked() {
	var latest uint64
	for id := range s.pendingPrompts {
		if id > latest {
			latest = id
		}
	}
	if latest != 0 {
		prompt := s.pendingPrompts[latest]
		prompt.produced = true
		s.pendingPrompts[latest] = prompt
	}
}

func acpStopReason(reason string) (bool, string, bool) {
	switch reason {
	case "end_turn", "cancelled":
		return true, "", true
	case "max_tokens":
		return false, "Agent ran out of context", true
	case "refusal":
		return false, "Agent declined the turn", true
	case "max_turn_requests":
		return false, "Agent reached its turn request limit", true
	default:
		return false, "Agent stopped for an unknown reason: " + reason, false
	}
}

func (s *acpSession) failMalformedPrompt(id uint64, cause error, raw json.RawMessage) bool {
	s.mu.Lock()
	prompt, ok := s.pendingPrompts[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.pendingPrompts, id)
	s.pendingBytes -= prompt.size
	settled := len(s.pendingPrompts) == 0
	s.mu.Unlock()
	if settled {
		s.emit(Event{Kind: EventError, Text: cause.Error(), Raw: cloneRaw(raw)})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: cause.Error(), Raw: cloneRaw(raw)})
	}
	return true
}

func (s *acpSession) emit(event Event) bool {
	size := eventSize(event)
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.eventsClosed {
		return false
	}
	if len(s.eventQueue) == cap(s.eventQueue) || s.eventBytes+size > maxQueuedEventBytes {
		s.slowSink.Store(true)
		go terminateProcess(s.process)
		return false
	}
	s.eventBytes += size
	s.eventQueue <- event
	return true
}

func (s *acpSession) dispatchEvents() {
	for event := range s.eventQueue {
		s.eventMu.Lock()
		s.eventBytes -= eventSize(event)
		if s.eventBytes < 0 {
			s.eventBytes = 0
		}
		s.eventMu.Unlock()
		s.sink.Emit(event)
	}
	if s.slowSink.Load() {
		s.sink.Emit(Event{Kind: EventError, Text: "provider event sink exceeded bounded queue"})
	}
	s.sink.Emit(Event{Kind: EventProcessExited})
}

func (s *acpSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.startPrompt(text, false)
}

func (s *acpSession) Steer(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.Descriptor().Capabilities.Steering {
		return ErrUnsupported
	}
	s.mu.Lock()
	active := len(s.pendingPrompts) != 0
	s.mu.Unlock()
	if !active {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: "provider has no active turn"})
		return errors.New("provider has no active turn")
	}
	return s.startPrompt(text, true)
}

func (s *acpSession) Descriptor() Descriptor { return Descriptors()[s.provider] }

func (s *acpSession) startPrompt(text string, steer bool) error {
	if s.closed.Load() {
		return ErrClosed
	}
	encodedSize := len(text) + len(s.session) + 128
	id := s.next.Add(1)
	prompt := acpPrompt{size: encodedSize}
	if s.provider == ProviderGrok {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return fmt.Errorf("Grok prompt correlation id: %w", err)
		}
		prompt.extensionID = "waku-" + hex.EncodeToString(random[:])
	}
	if s.provider == ProviderKimi {
		prompt.kimi = kimiWireStart(s.session)
	}
	s.mu.Lock()
	if !steer && len(s.pendingPrompts) != 0 {
		s.mu.Unlock()
		return errors.New("ACP session already has an active prompt")
	}
	if len(s.pendingPrompts) >= maxPendingRPCs || s.pendingBytes+encodedSize > maxPendingRPCBytes {
		s.mu.Unlock()
		return errors.New("provider pending RPC limit exceeded")
	}
	if s.normal != nil && !steer {
		s.normal.produced = false
	}
	s.pendingPrompts[id] = prompt
	s.pendingBytes += encodedSize
	if !steer {
		s.emit(Event{Kind: EventTurnStarted})
	}
	params := map[string]any{"sessionId": s.session, "prompt": []any{map[string]string{"type": "text", "text": text}}}
	if prompt.extensionID != "" {
		params["_meta"] = map[string]string{"promptId": prompt.extensionID, "requestId": prompt.extensionID}
	}
	err := s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "id": id, "method": "session/prompt", "params": params})
	if err != nil {
		delete(s.pendingPrompts, id)
		s.pendingBytes -= encodedSize
		s.mu.Unlock()
		if !steer {
			s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: err.Error()})
		}
		return err
	}
	if steer {
		s.emit(Event{Kind: EventSteerAccepted, Text: text})
	}
	s.mu.Unlock()
	return nil
}

func (s *acpSession) Cancel(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": s.session}}); err != nil {
		return err
	}
	s.cancelInbound()
	return nil
}

func (s *acpSession) RespondPermission(ctx context.Context, requestID json.RawMessage, option string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validRPCID(requestID) || option == "" {
		return errors.New("invalid ACP permission response")
	}
	if s.closed.Load() {
		return ErrClosed
	}
	key := string(requestID)
	s.mu.Lock()
	request, ok := s.permissions[key]
	if ok {
		if request.resolving {
			s.mu.Unlock()
			return errors.New("ACP permission request is already resolving")
		}
		if _, valid := request.options[option]; !valid {
			s.mu.Unlock()
			return errors.New("ACP permission option was not advertised")
		}
		request.resolving = true
		s.permissions[key] = request
	}
	s.mu.Unlock()
	if !ok {
		return errors.New("ACP permission request is not pending")
	}
	err := s.writeACPResponse(request.id, map[string]any{"outcome": map[string]string{"outcome": "selected", "optionId": option}})
	s.mu.Lock()
	if current, stillPending := s.permissions[key]; stillPending && current.resolving {
		if err == nil {
			delete(s.permissions, key)
			s.releaseInboundLocked(key)
		} else if !s.closed.Load() {
			current.resolving = false
			s.permissions[key] = current
		}
	}
	s.mu.Unlock()
	return err
}

func (s *acpSession) RespondQuestions(ctx context.Context, requestID json.RawMessage, answers []UserInputAnswer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validRPCID(requestID) {
		return errors.New("invalid ACP question response")
	}
	if s.closed.Load() {
		return ErrClosed
	}
	key := string(requestID)
	s.mu.Lock()
	request, ok := s.questions[key]
	if ok {
		if request.resolving {
			s.mu.Unlock()
			return errors.New("ACP question request is already resolving")
		}
		request.resolving = true
		s.questions[key] = request
	}
	s.mu.Unlock()
	if !ok {
		return errors.New("ACP question request is not pending")
	}
	result, err := acpQuestionResponse(request.method, request.params, answers)
	if err != nil {
		s.mu.Lock()
		if current, stillPending := s.questions[key]; stillPending && current.resolving && !s.closed.Load() {
			current.resolving = false
			s.questions[key] = current
		}
		s.mu.Unlock()
		return err
	}
	err = s.writeACPResponse(request.id, result)
	s.mu.Lock()
	if current, stillPending := s.questions[key]; stillPending && current.resolving {
		if err == nil {
			delete(s.questions, key)
			s.releaseInboundLocked(key)
		} else if !s.closed.Load() {
			current.resolving = false
			s.questions[key] = current
		}
	}
	s.mu.Unlock()
	return err
}

func acpQuestionResponse(flavor string, params json.RawMessage, submitted []UserInputAnswer) (any, error) {
	questions, err := parseACPQuestions(params, flavor)
	if err != nil {
		return nil, err
	}
	answers := make(map[string]any)
	annotations := make(map[string]any)
	var native struct {
		Questions []map[string]json.RawMessage `json:"questions"`
	}
	if err := decodeUseNumber(params, &native); err != nil {
		return nil, err
	}
	for _, question := range questions {
		var values []string
		for _, answer := range submitted {
			if answer.QuestionID == question.ID || answer.QuestionID == question.Question {
				values = append([]string(nil), answer.Answers...)
				break
			}
		}
		if flavor == "cursor" {
			if question.MultiSelect {
				answers[question.ID] = values
			} else if len(values) != 0 {
				answers[question.ID] = values[0]
			} else {
				answers[question.ID] = ""
			}
		} else {
			var source map[string]json.RawMessage
			for _, candidate := range native.Questions {
				if rawString(candidate["id"]) == question.ID || rawString(candidate["question"]) == question.Question {
					source = candidate
					break
				}
			}
			var options []map[string]json.RawMessage
			_ = decodeUseNumber(source["options"], &options)
			labels := make(map[string]string)
			for _, option := range options {
				labels[rawString(option["label"])] = rawString(option["preview"])
			}
			var selected, notes []string
			for _, value := range values {
				if _, ok := labels[value]; ok {
					selected = append(selected, value)
				} else if strings.TrimSpace(value) != "" {
					notes = append(notes, value)
				}
			}
			if len(selected) == 0 && len(notes) != 0 {
				answers[question.Question] = []string{"Other"}
			} else {
				answers[question.Question] = selected
			}
			annotation := make(map[string]string)
			if !question.MultiSelect && len(selected) != 0 && strings.TrimSpace(labels[selected[0]]) != "" {
				annotation["preview"] = strings.TrimSpace(labels[selected[0]])
			}
			if len(notes) != 0 {
				annotation["notes"] = strings.Join(notes, "\n")
			}
			if len(annotation) != 0 {
				annotations[question.Question] = annotation
			}
		}
	}
	if flavor == "cursor" {
		return map[string]any{"answers": answers}, nil
	}
	result := map[string]any{"outcome": "accepted", "answers": answers}
	if len(annotations) != 0 {
		result["annotations"] = annotations
	}
	return result, nil
}

func (s *acpSession) writeACPResponse(id json.RawMessage, result any) error {
	return s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "id": cloneRaw(id), "result": result})
}

func (s *acpSession) writeACPError(id json.RawMessage, code int, message string) error {
	return s.process.Lines.Write(map[string]any{"jsonrpc": "2.0", "id": cloneRaw(id), "error": map[string]any{"code": code, "message": message}})
}

func (s *acpSession) Goal(ctx context.Context, _ GoalOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrUnsupported
}

func (s *acpSession) ApplyOptions(ctx context.Context, options SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if options.Model != "" || options.ReasoningEffort != "" || options.ServiceTier != "" {
		return false, ErrUnsupported
	}
	if options.Access != "" && normalizedAccess(options.Access) != s.access {
		return false, nil
	}
	return true, nil
}

func (s *acpSession) Rollback(ctx context.Context, _ uint64) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrUnsupported
}

func (s *acpSession) Fork(ctx context.Context, _ uint64) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrUnsupported
}

func (s *acpSession) Close() error {
	s.closed.Store(true)
	s.mu.Lock()
	s.permissions = make(map[string]acpPermissionRequest)
	s.questions = make(map[string]acpQuestionRequest)
	s.inboundSizes = make(map[string]int)
	s.inboundBytes = 0
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return s.process.Terminate(ctx)
}

func (s *acpSession) cancelInbound() {
	s.mu.Lock()
	permissions := s.permissions
	questions := s.questions
	s.permissions = make(map[string]acpPermissionRequest)
	s.questions = make(map[string]acpQuestionRequest)
	s.inboundSizes = make(map[string]int)
	s.inboundBytes = 0
	s.mu.Unlock()
	for _, request := range permissions {
		_ = s.writeACPResponse(request.id, map[string]any{"outcome": map[string]string{"outcome": "cancelled"}})
	}
	for _, request := range questions {
		result := map[string]any{"outcome": "cancelled"}
		if request.method == "cursor" {
			result = map[string]any{"answers": map[string]any{}}
		}
		_ = s.writeACPResponse(request.id, result)
	}
}

func (s *acpSession) failPending(reason string) {
	s.mu.Lock()
	hadPrompts := len(s.pendingPrompts) != 0
	s.pendingPrompts = make(map[uint64]acpPrompt)
	s.ignoredPrompts = make(map[uint64]struct{})
	s.pendingBytes = 0
	s.permissions = make(map[string]acpPermissionRequest)
	s.questions = make(map[string]acpQuestionRequest)
	s.inboundSizes = make(map[string]int)
	s.inboundBytes = 0
	s.mu.Unlock()
	if hadPrompts {
		s.emit(Event{Kind: EventError, Text: reason})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: reason})
	}
}

func (s *acpSession) releaseInboundLocked(key string) {
	s.inboundBytes -= s.inboundSizes[key]
	if s.inboundBytes < 0 {
		s.inboundBytes = 0
	}
	delete(s.inboundSizes, key)
}

func terminateProcess(process *StdioProcess) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = process.Terminate(ctx)
}
