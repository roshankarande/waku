package providers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const maxPiReassembledBytes = 64 << 20

type PiAdapter struct {
	Provider     Provider
	startProcess func(context.Context, ProcessSpec) (*StdioProcess, error)
}

func (a PiAdapter) Descriptor() Descriptor { return Descriptors()[a.Provider] }

func (a PiAdapter) descriptor() (Descriptor, error) {
	d, ok := Descriptors()[a.Provider]
	if !ok || !d.Implemented || (a.Provider != ProviderPi && a.Provider != ProviderOhMyPi) {
		return Descriptor{}, fmt.Errorf("%w: %q does not have a Pi RPC adapter", ErrUnsupported, a.Provider)
	}
	return d, nil
}

func (a PiAdapter) Discover(ctx context.Context, options StartOptions) (Discovery, error) {
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
	path := binary
	if a.startProcess == nil {
		path, err = exec.LookPath(binary)
		if err != nil {
			return Discovery{Reason: err.Error()}, nil
		}
	}
	discovery := Discovery{Available: true, Executable: path}
	var failures []string
	if options.ProbeVersion {
		if a.startProcess != nil {
			failures = append(failures, "version probe unavailable for injected process")
		} else if version, probeErr := probeExecutableVersion(ctx, path); probeErr != nil {
			failures = append(failures, probeErr.Error())
		} else {
			discovery.Version = version
		}
	}
	if options.DiscoverModels {
		starter := StartStdio
		if a.startProcess != nil {
			starter = a.startProcess
		}
		spec := ProcessSpec{Path: path, Args: piProbeArgs(a.Provider)}
		if a.Provider == ProviderPi {
			spec.Env = append(os.Environ(), "PI_SKIP_VERSION_CHECK=1")
		}
		models, probeErr := probePiModels(ctx, a.Provider, starter, spec)
		if probeErr != nil {
			failures = append(failures, probeErr.Error())
		} else {
			discovery.Models = models
		}
	}
	discovery.Reason = strings.Join(failures, "; ")
	return discovery, nil
}

func piProbeArgs(provider Provider) []string {
	args := []string{"--mode", "rpc", "--no-session", "--no-skills"}
	if provider == ProviderOhMyPi {
		return append(args, "--no-rules", "--no-extensions")
	}
	return append(args, "--no-prompt-templates", "--no-context-files")
}

const maxVersionOutputBytes = 1 << 20

type boundedOutput struct {
	buffer bytes.Buffer
	limit  int
}

func (b *boundedOutput) Write(data []byte) (int, error) {
	original := len(data)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = b.buffer.Write(data)
	}
	return original, nil
}

func probeExecutableVersion(ctx context.Context, path string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var output boundedOutput
	output.limit = maxVersionOutputBytes
	command := exec.CommandContext(probeCtx, path, "--version")
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		if probeCtx.Err() != nil {
			return "", fmt.Errorf("Pi version probe: %w", probeCtx.Err())
		}
		return "", fmt.Errorf("Pi version probe: %w", err)
	}
	version := strings.TrimSpace(output.buffer.String())
	if index := strings.IndexByte(version, '\n'); index >= 0 {
		version = strings.TrimSpace(version[:index])
	}
	if version == "" {
		return "", errors.New("Pi version probe returned no version")
	}
	return version, nil
}

func probePiModels(ctx context.Context, provider Provider, starter func(context.Context, ProcessSpec) (*StdioProcess, error), spec ProcessSpec) ([]Model, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	process, err := starter(probeCtx, spec)
	if err != nil {
		return nil, err
	}
	stderrDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, process.Stderr); close(stderrDone) }()
	type probeResult struct {
		models []Model
		err    error
	}
	result := make(chan probeResult, 1)
	go func() {
		modelsResponse, callErr := directPiProbeCall(process.Lines, "waku-models", "get_available_models")
		if callErr != nil {
			result <- probeResult{err: callErr}
			return
		}
		stateResponse, _ := directPiProbeCall(process.Lines, "waku-state", "get_state")
		models, parseErr := parsePiModels(provider, modelsResponse, stateResponse)
		result <- probeResult{models: models, err: parseErr}
	}()
	var outcome probeResult
	select {
	case outcome = <-result:
	case <-probeCtx.Done():
		outcome.err = fmt.Errorf("Pi model probe: %w", probeCtx.Err())
	}
	terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if probeCtx.Err() != nil {
		terminateCancel()
		terminateCtx, terminateCancel = context.WithCancel(context.Background())
		terminateCancel()
	}
	_ = process.Terminate(terminateCtx)
	terminateCancel()
	select {
	case <-stderrDone:
	case <-time.After(2 * time.Second):
	}
	return outcome.models, outcome.err
}

func directPiProbeCall(lines *JSONLines, id, method string) (map[string]json.RawMessage, error) {
	if err := lines.Write(map[string]any{"id": id, "type": method}); err != nil {
		return nil, err
	}
	var chunks *piChunk
	for {
		var raw json.RawMessage
		if err := lines.Decode(&raw); err != nil {
			return nil, err
		}
		logical, err := acceptPiChunk(&chunks, raw)
		if err != nil {
			return nil, err
		}
		if logical == nil {
			continue
		}
		var frame map[string]json.RawMessage
		if err := decodeUseNumber(logical, &frame); err != nil || frame == nil {
			return nil, errors.New("Pi model probe received a malformed frame")
		}
		if rawString(frame["type"]) != "response" || rawString(frame["id"]) != id {
			continue
		}
		var success bool
		if err := decodeUseNumber(frame["success"], &success); err != nil || !success {
			return nil, errors.New(firstNonempty(rawString(frame["error"]), "Pi model probe request failed"))
		}
		var data map[string]json.RawMessage
		if err := decodeUseNumber(frame["data"], &data); err != nil || data == nil {
			return nil, errors.New("Pi model probe response has invalid data")
		}
		return data, nil
	}
}

func parsePiModels(providerKind Provider, modelsData, stateData map[string]json.RawMessage) ([]Model, error) {
	var entries []json.RawMessage
	if err := decodeUseNumber(modelsData["models"], &entries); err != nil {
		return nil, errors.New("Pi model catalog is missing models")
	}
	models := make([]Model, 0, len(entries))
	for _, raw := range entries {
		var entry map[string]json.RawMessage
		if err := decodeUseNumber(raw, &entry); err != nil || entry == nil {
			return nil, errors.New("Pi model catalog contains an invalid model")
		}
		provider, id := rawString(entry["provider"]), rawString(entry["id"])
		if strings.TrimSpace(provider) == "" || strings.TrimSpace(id) == "" {
			return nil, errors.New("Pi model catalog contains a model without provider/id")
		}
		label := firstNonempty(rawString(entry["name"]), id)
		slug := provider + "/" + id
		isDefault := false
		var stateModel map[string]json.RawMessage
		if decodeUseNumber(stateData["model"], &stateModel) == nil {
			isDefault = rawString(stateModel["provider"]) == provider && rawString(stateModel["id"]) == id
		}
		efforts, err := piReasoningOptions(providerKind, entry)
		if err != nil {
			return nil, fmt.Errorf("Pi model %s reasoning: %w", slug, err)
		}
		metadata := map[string]any{
			"id":                slug,
			"name":              label,
			"sub_provider":      displaySlug(provider),
			"is_default":        isDefault,
			"reasoning_efforts": efforts,
		}
		if len(efforts) != 0 {
			preferred := ""
			if isDefault {
				preferred = rawString(stateData["thinkingLevel"])
			}
			if !hasModelOption(efforts, preferred) {
				preferred = "medium"
			}
			if !hasModelOption(efforts, preferred) {
				preferred = efforts[0]["id"]
			}
			metadata["default_reasoning_effort"] = preferred
		}
		contextWindow, err := piContextWindow(entry["contextWindow"])
		if err != nil {
			return nil, fmt.Errorf("Pi model %s contextWindow: %w", slug, err)
		}
		if contextWindow == "" && isDefault {
			contextWindow, err = piContextWindow(stateModel["contextWindow"])
			if err != nil {
				return nil, fmt.Errorf("Pi state contextWindow: %w", err)
			}
		}
		if contextWindow != "" {
			metadata["context_windows"] = []map[string]string{{"id": contextWindow, "label": contextWindow}}
			metadata["default_context_window"] = contextWindow
		}
		metadataRaw, err := json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
		models = append(models, Model{ID: slug, Label: label, Raw: metadataRaw})
	}
	return models, nil
}

var piThinkingLevels = []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}

func piReasoningOptions(provider Provider, entry map[string]json.RawMessage) ([]map[string]string, error) {
	var reasoning bool
	if raw, present := entry["reasoning"]; present {
		if err := decodeUseNumber(raw, &reasoning); err != nil {
			return nil, errors.New("reasoning must be boolean")
		}
	}
	if !reasoning {
		return []map[string]string{}, nil
	}
	allowed := make(map[string]bool)
	if provider == ProviderOhMyPi {
		var thinking map[string]json.RawMessage
		if err := decodeUseNumber(entry["thinking"], &thinking); err != nil || thinking == nil {
			return nil, errors.New("Oh My Pi reasoning model is missing thinking")
		}
		var efforts []string
		if err := decodeUseNumber(thinking["efforts"], &efforts); err != nil {
			return nil, errors.New("Oh My Pi thinking efforts must be strings")
		}
		allowed["off"] = true
		for _, effort := range efforts {
			allowed[effort] = true
		}
	} else {
		var levelMap map[string]json.RawMessage
		if raw, present := entry["thinkingLevelMap"]; present && string(raw) != "null" {
			if err := decodeUseNumber(raw, &levelMap); err != nil {
				return nil, errors.New("thinkingLevelMap must be an object")
			}
		}
		for _, level := range piThinkingLevels {
			mapped, present := levelMap[level]
			if level == "xhigh" || level == "max" {
				allowed[level] = present && string(mapped) != "null"
			} else {
				allowed[level] = !present || string(mapped) != "null"
			}
		}
	}
	options := make([]map[string]string, 0, len(piThinkingLevels))
	for _, level := range piThinkingLevels {
		if allowed[level] {
			options = append(options, map[string]string{"id": level, "label": reasoningLabel(level)})
		}
	}
	return options, nil
}

func hasModelOption(options []map[string]string, id string) bool {
	if id == "" {
		return false
	}
	for _, option := range options {
		if option["id"] == id {
			return true
		}
	}
	return false
}

func piContextWindow(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var number json.Number
	if err := decodeUseNumber(raw, &number); err != nil {
		return "", errors.New("must be an unsigned integer")
	}
	if _, err := strconv.ParseUint(number.String(), 10, 64); err != nil {
		return "", errors.New("must be an unsigned integer")
	}
	return number.String(), nil
}

func reasoningLabel(value string) string {
	switch value {
	case "off":
		return "Off"
	case "minimal":
		return "Minimal"
	case "low":
		return "Low"
	case "medium":
		return "Medium"
	case "high":
		return "High"
	case "xhigh":
		return "Extra High"
	case "max":
		return "Max"
	default:
		return displaySlug(value)
	}
}

func displaySlug(value string) string {
	words := strings.Fields(strings.NewReplacer("-", " ", "_", " ").Replace(value))
	for index, word := range words {
		if word != "" {
			words[index] = strings.ToUpper(word[:1]) + word[1:]
		}
	}
	return strings.Join(words, " ")
}

func (a PiAdapter) Start(ctx context.Context, options StartOptions, sink Sink) (Session, error) {
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
	if options.Access != AccessFull {
		return nil, fmt.Errorf("%s currently supports Full access only", piName(a.Provider))
	}
	if options.ServiceTier != "" {
		return nil, fmt.Errorf("%w: Pi service tiers are not supported", ErrUnsupported)
	}
	if options.ComputerUse {
		return nil, fmt.Errorf("%w: Pi computer-use bridge is not implemented", ErrUnsupported)
	}
	modelProvider, modelID, err := parsePiModel(options.Model)
	if err != nil {
		return nil, err
	}
	resume, err := parsePiCursor(a.Provider, options.ResumeCursor)
	if err != nil {
		return nil, err
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
	args := []string{"--mode", "rpc", piFullFlag(a.Provider)}
	spec := ProcessSpec{Path: binary, Args: args, Directory: cwd}
	if a.Provider == ProviderPi {
		spec.Env = append(os.Environ(), "PI_SKIP_VERSION_CHECK=1")
	}
	starter := StartStdio
	if a.startProcess != nil {
		starter = a.startProcess
	}
	process, err := starter(ctx, spec)
	if err != nil {
		return nil, err
	}
	s := &piSession{provider: a.Provider, process: process, sink: sink, events: make(chan Event, 256), pending: make(map[string]chan piResponse), prompts: make(map[string]struct{}), tools: make(map[string]Activity), stderrDone: make(chan struct{})}
	go func() { _, _ = io.Copy(io.Discard, process.Stderr); close(s.stderrDone) }()
	go s.dispatch()
	go s.readLoop()
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if a.Provider == ProviderOhMyPi {
		if _, err = s.call(initCtx, map[string]any{"type": "negotiate_protocol", "protocolVersion": 2}); err != nil {
			return nil, s.startFailure(err)
		}
	}
	if _, err = s.call(initCtx, map[string]any{"type": "get_state"}); err != nil {
		return nil, s.startFailure(err)
	}
	if resume.sessionFile != "" {
		response, callErr := s.call(initCtx, map[string]any{"type": "switch_session", "sessionPath": resume.sessionFile})
		if callErr != nil {
			return nil, s.startFailure(callErr)
		}
		if rawBool(response.data, "cancelled") {
			return nil, s.startFailure(errors.New("Pi session switch was cancelled"))
		}
	}
	if modelProvider != "" {
		if _, err = s.call(initCtx, map[string]any{"type": "set_model", "provider": modelProvider, "modelId": modelID}); err != nil {
			return nil, s.startFailure(err)
		}
	}
	if options.ReasoningEffort != "" {
		if _, err = s.call(initCtx, map[string]any{"type": "set_thinking_level", "level": options.ReasoningEffort}); err != nil {
			return nil, s.startFailure(err)
		}
	}
	state, err := s.call(initCtx, map[string]any{"type": "get_state"})
	if err != nil {
		return nil, s.startFailure(err)
	}
	cursor, err := piCursorFromState(a.Provider, state.data)
	if err != nil {
		return nil, s.startFailure(err)
	}
	s.mu.Lock()
	s.cursor = cursor
	s.model = options.Model
	s.effort = options.ReasoningEffort
	s.mu.Unlock()
	s.emit(Event{Kind: EventConnected, Cursor: cursor.raw()})
	if title := rawString(state.data["sessionName"]); title != "" {
		s.emit(Event{Kind: EventAutoTitleUpdated, Text: stripPrivate(title)})
	}
	if a.Provider == ProviderPi {
		if commands, callErr := s.call(initCtx, map[string]any{"type": "get_commands"}); callErr == nil {
			s.emitPiCommands(commands.data)
		}
	}
	return s, nil
}

func (s *piSession) startFailure(err error) error {
	terminateProcess(s.process)
	s.closed.Store(true)
	s.closeEvents()
	return err
}

func piName(provider Provider) string {
	if provider == ProviderOhMyPi {
		return "Oh My Pi"
	}
	return "Pi"
}
func piFullFlag(provider Provider) string {
	if provider == ProviderOhMyPi {
		return "--yolo"
	}
	return "--approve"
}
func parsePiModel(model string) (string, string, error) {
	if model == "" {
		return "", "", nil
	}
	parts := strings.Split(model, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", errors.New("Pi model must be a provider/model slug")
	}
	return parts[0], parts[1], nil
}

type piCursor struct {
	provider               Provider
	sessionID, sessionFile string
}

func (c piCursor) raw() json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"provider": c.provider, "sessionId": c.sessionID, "sessionFile": c.sessionFile})
	return raw
}
func parsePiCursor(provider Provider, raw json.RawMessage) (piCursor, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return piCursor{provider: provider}, nil
	}
	var value struct {
		Provider    Provider `json:"provider"`
		SessionID   string   `json:"sessionId"`
		SessionFile string   `json:"sessionFile"`
	}
	if err := decodeUseNumber(raw, &value); err != nil {
		return piCursor{}, fmt.Errorf("Pi cursor: %w", err)
	}
	if value.Provider != provider {
		return piCursor{}, fmt.Errorf("%s adapter cannot resume %q cursor", provider, value.Provider)
	}
	if value.SessionID == "" || value.SessionFile == "" {
		return piCursor{}, errors.New("Pi resume cursor requires sessionId and sessionFile")
	}
	return piCursor{provider: provider, sessionID: value.SessionID, sessionFile: value.SessionFile}, nil
}

func piCursorFromState(provider Provider, data map[string]json.RawMessage) (piCursor, error) {
	id := rawString(data["sessionId"])
	if id == "" {
		return piCursor{}, errors.New("Pi state has no sessionId")
	}
	return piCursor{provider: provider, sessionID: id, sessionFile: rawString(data["sessionFile"])}, nil
}

type piResponse struct {
	data map[string]json.RawMessage
	raw  json.RawMessage
	err  error
}
type piChunk struct {
	id                 string
	count, next, total int
	data               []byte
}
type piSession struct {
	provider                           Provider
	process                            *StdioProcess
	sink                               Sink
	mu                                 sync.Mutex
	next                               uint64
	pending                            map[string]chan piResponse
	prompts                            map[string]struct{}
	cursor                             piCursor
	model, effort                      string
	active, failed, sawText, sawReason bool
	tools                              map[string]Activity
	closed                             atomic.Bool
	eventMu                            sync.Mutex
	events                             chan Event
	eventBytes                         int
	eventsClose                        bool
	slowSink                           atomic.Bool
	stderrDone                         chan struct{}
}

func (s *piSession) Descriptor() Descriptor { return Descriptors()[s.provider] }
func (s *piSession) emit(event Event) bool {
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
func (s *piSession) dispatch() {
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
func (s *piSession) closeEvents() {
	s.eventMu.Lock()
	if !s.eventsClose {
		s.eventsClose = true
		close(s.events)
	}
	s.eventMu.Unlock()
}

func (s *piSession) call(ctx context.Context, request map[string]any) (piResponse, error) {
	if err := ctx.Err(); err != nil {
		return piResponse{}, err
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return piResponse{}, ErrClosed
	}
	s.next++
	id := fmt.Sprintf("waku-%d", s.next)
	ch := make(chan piResponse, 1)
	s.pending[id] = ch
	s.mu.Unlock()
	request["id"] = id
	if err := s.process.Lines.Write(request); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return piResponse{}, err
	}
	select {
	case response := <-ch:
		return response, response.err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return piResponse{}, ctx.Err()
	}
}

func (s *piSession) readLoop() {
	var chunk *piChunk
	var readErr error
	for {
		var raw json.RawMessage
		if err := s.process.Lines.Decode(&raw); err != nil {
			readErr = err
			break
		}
		logical, err := acceptPiChunk(&chunk, raw)
		if err != nil {
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
			chunk = nil
			continue
		}
		if logical == nil {
			continue
		}
		if err := s.consume(logical); err != nil {
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(logical)})
		}
	}
	if chunk != nil {
		s.emit(Event{Kind: EventError, Text: "Pi chunk sequence ended before completion"})
	}
	s.mu.Lock()
	for id, ch := range s.pending {
		delete(s.pending, id)
		ch <- piResponse{err: errors.New("Pi RPC process exited")}
	}
	active := s.active
	s.active = false
	s.mu.Unlock()
	if active {
		s.emit(Event{Kind: EventError, Text: "Pi process exited during turn"})
		s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: "Pi process exited during turn"})
	}
	if !s.closed.Load() && readErr != nil && !errors.Is(readErr, io.EOF) {
		s.emit(Event{Kind: EventError, Text: readErr.Error()})
	}
	_ = s.process.Close()
	_ = s.process.Wait()
	<-s.stderrDone
	s.closed.Store(true)
	s.closeEvents()
}

func acceptPiChunk(active **piChunk, raw json.RawMessage) (json.RawMessage, error) {
	var value map[string]json.RawMessage
	if err := decodeUseNumber(raw, &value); err != nil || value == nil {
		return nil, errors.New("Pi frame must be a JSON object")
	}
	if rawString(value["type"]) != "rpc_chunk" {
		if *active != nil {
			return nil, errors.New("Pi chunk sequence was interrupted")
		}
		return raw, nil
	}
	var wire struct {
		ChunkID                  string `json:"chunkId"`
		Index, Count, ByteLength int
		Data                     string `json:"data"`
	}
	if err := decodeUseNumber(raw, &wire); err != nil {
		return nil, fmt.Errorf("Pi chunk: %w", err)
	}
	if wire.ChunkID == "" || wire.Count <= 0 || wire.Index < 0 || wire.Index >= wire.Count || wire.ByteLength < 0 || wire.ByteLength > maxPiReassembledBytes {
		return nil, errors.New("Pi chunk metadata is invalid")
	}
	part, err := base64.StdEncoding.DecodeString(wire.Data)
	if err != nil {
		return nil, errors.New("Pi chunk has invalid base64")
	}
	if *active == nil {
		if wire.Index != 0 {
			return nil, errors.New("Pi chunk sequence must start at zero")
		}
		*active = &piChunk{id: wire.ChunkID, count: wire.Count, total: wire.ByteLength}
	}
	c := *active
	if c.id != wire.ChunkID || c.count != wire.Count || c.total != wire.ByteLength || wire.Index != c.next {
		return nil, errors.New("Pi chunk sequence metadata changed")
	}
	if len(c.data)+len(part) > c.total {
		return nil, errors.New("Pi chunk data exceeds declared length")
	}
	c.data = append(c.data, part...)
	c.next++
	if c.next < c.count {
		return nil, nil
	}
	*active = nil
	if len(c.data) != c.total || !utf8.Valid(c.data) {
		return nil, errors.New("Pi reassembled chunk length or UTF-8 is invalid")
	}
	var check any
	if err := decodeUseNumber(c.data, &check); err != nil {
		return nil, fmt.Errorf("Pi reassembled JSON: %w", err)
	}
	return json.RawMessage(c.data), nil
}

func (s *piSession) consume(raw json.RawMessage) error {
	var value map[string]json.RawMessage
	if err := decodeUseNumber(raw, &value); err != nil || value == nil {
		return errors.New("Pi message must be a JSON object")
	}
	typ := rawString(value["type"])
	if typ == "" {
		return errors.New("Pi message is missing type")
	}
	if typ == "response" {
		return s.consumeResponse(value, raw)
	}
	if typ == "available_commands_update" {
		s.emitPiCommands(value)
		return nil
	}
	if (s.provider == ProviderPi && typ == "session_info_changed") || (s.provider == ProviderOhMyPi && typ == "session_info_update") {
		field := "name"
		if s.provider == ProviderOhMyPi {
			field = "title"
		}
		s.emit(Event{Kind: EventAutoTitleUpdated, Text: stripPrivate(rawString(value[field])), Raw: cloneRaw(raw)})
		return nil
	}
	settled := (s.provider == ProviderPi && typ == "agent_settled") || (s.provider == ProviderOhMyPi && typ == "agent_end")
	if settled {
		if s.provider == ProviderOhMyPi && rawFalse(value["isTerminal"]) {
			return nil
		}
		s.finishPi(raw)
		return nil
	}
	switch typ {
	case "agent_start", "turn_start":
		s.beginPi()
	case "command_output":
		if text := stripPrivate(rawString(value["text"])); text != "" {
			s.beginPi()
			s.emit(Event{Kind: EventTextDelta, Text: text + "\n\n", Raw: cloneRaw(raw)})
		}
	case "message_start":
		var msg map[string]json.RawMessage
		_ = decodeUseNumber(value["message"], &msg)
		if rawString(msg["role"]) == "assistant" {
			s.mu.Lock()
			s.sawText, s.sawReason = false, false
			s.mu.Unlock()
		}
	case "message_update":
		return s.consumePiUpdate(value, raw)
	case "message_end":
		return s.consumePiMessageEnd(value, raw)
	case "tool_execution_start", "tool_execution_update", "tool_execution_end":
		return s.consumePiTool(typ, value, raw)
	case "auto_retry_end":
		if rawBool(value, "success") {
			s.mu.Lock()
			s.failed = false
			s.mu.Unlock()
		} else {
			message := firstNonempty(rawString(value["finalError"]), piName(s.provider)+" exhausted its automatic retries")
			s.mu.Lock()
			s.failed = true
			s.mu.Unlock()
			s.emit(Event{Kind: EventError, Text: message, Raw: cloneRaw(raw)})
		}
	case "extension_error":
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
		s.emit(Event{Kind: EventError, Text: firstNonempty(rawString(value["error"]), rawString(value["errorMessage"]), rawString(value["reason"]), piName(s.provider)+" reported an error"), Raw: cloneRaw(raw)})
	case "extension_ui_request":
		method, id := rawString(value["method"]), rawString(value["id"])
		if id != "" && (method == "select" || method == "confirm" || method == "input" || method == "editor") {
			return s.process.Lines.Write(map[string]any{"type": "extension_ui_response", "id": id, "cancelled": true})
		}
	}
	return nil
}

func (s *piSession) consumeResponse(value map[string]json.RawMessage, raw json.RawMessage) error {
	id := rawString(value["id"])
	if id == "" {
		return errors.New("Pi response is missing id")
	}
	var success bool
	if err := decodeUseNumber(value["success"], &success); err != nil {
		return errors.New("Pi response requires boolean success")
	}
	s.mu.Lock()
	if _, prompt := s.prompts[id]; prompt {
		local := rawFalseField(value["data"], "agentInvoked")
		if success && !local {
			s.mu.Unlock()
			return nil
		}
		delete(s.prompts, id)
		active := s.active
		s.active = false
		s.mu.Unlock()
		if active {
			if !success {
				message := firstNonempty(rawString(value["error"]), piName(s.provider)+" RPC command failed")
				s.emit(Event{Kind: EventError, Text: message, Raw: cloneRaw(raw)})
				s.emit(Event{Kind: EventTurnFinished, Success: false, Summary: message, Raw: cloneRaw(raw)})
			} else {
				s.emit(Event{Kind: EventTurnFinished, Success: true, Raw: cloneRaw(raw)})
			}
		}
		return nil
	}
	ch, ok := s.pending[id]
	if ok {
		delete(s.pending, id)
	}
	s.mu.Unlock()
	if !ok {
		return errors.New("Pi received an uncorrelated response")
	}
	if !success {
		ch <- piResponse{err: errors.New(firstNonempty(rawString(value["error"]), piName(s.provider)+" RPC command failed"))}
		return nil
	}
	var data map[string]json.RawMessage
	if rawData := value["data"]; len(rawData) > 0 && string(rawData) != "null" {
		if err := decodeUseNumber(rawData, &data); err != nil {
			return errors.New("Pi response data must be an object")
		}
	}
	if data == nil {
		data = map[string]json.RawMessage{}
	}
	ch <- piResponse{data: data, raw: cloneRaw(raw)}
	return nil
}

func (s *piSession) beginPi() {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return
	}
	s.active = true
	s.failed = false
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnStarted})
}
func (s *piSession) finishPi(raw json.RawMessage) {
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return
	}
	success := !s.failed
	s.active = false
	s.prompts = make(map[string]struct{})
	s.sawText, s.sawReason = false, false
	s.mu.Unlock()
	summary := ""
	if !success {
		summary = piName(s.provider) + " failed to complete the turn"
	}
	s.emit(Event{Kind: EventTurnFinished, Success: success, Summary: summary, Raw: cloneRaw(raw)})
}
func (s *piSession) consumePiUpdate(value map[string]json.RawMessage, raw json.RawMessage) error {
	var update map[string]json.RawMessage
	if decodeUseNumber(value["assistantMessageEvent"], &update) != nil {
		return errors.New("Pi message_update is malformed")
	}
	typ := rawString(update["type"])
	switch typ {
	case "text_delta":
		if text := stripPrivate(rawString(update["delta"])); text != "" {
			s.mu.Lock()
			s.sawText = true
			s.mu.Unlock()
			s.emit(Event{Kind: EventTextDelta, Text: text, Raw: cloneRaw(raw)})
		}
	case "thinking_delta":
		if text := stripPrivate(rawString(update["delta"])); text != "" {
			s.mu.Lock()
			s.sawReason = true
			s.mu.Unlock()
			s.emit(Event{Kind: EventReasoningDelta, Text: text, Raw: cloneRaw(raw)})
		}
	case "error":
		message := firstNonempty(rawString(update["error"]), piName(s.provider)+" reported an error")
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
		s.emit(Event{Kind: EventError, Text: message, Raw: cloneRaw(raw)})
	}
	return nil
}
func (s *piSession) consumePiMessageEnd(value map[string]json.RawMessage, raw json.RawMessage) error {
	var msg map[string]json.RawMessage
	if decodeUseNumber(value["message"], &msg) != nil || rawString(msg["role"]) != "assistant" {
		return nil
	}
	if usage := piMessageUsage(msg["usage"]); usage != nil {
		s.emit(Event{Kind: EventUsageUpdated, ContextTokens: usage, Raw: cloneRaw(raw)})
	}
	var blocks []map[string]json.RawMessage
	if decodeUseNumber(msg["content"], &blocks) != nil {
		return nil
	}
	s.mu.Lock()
	sawText, sawReason := s.sawText, s.sawReason
	s.mu.Unlock()
	for _, block := range blocks {
		switch rawString(block["type"]) {
		case "text":
			if !sawText {
				if text := stripPrivate(rawString(block["text"])); text != "" {
					s.emit(Event{Kind: EventTextDelta, Text: text, Raw: cloneRaw(raw)})
				}
			}
		case "thinking":
			if !sawReason {
				if text := stripPrivate(rawString(block["thinking"])); text != "" {
					s.emit(Event{Kind: EventReasoningDelta, Text: text, Raw: cloneRaw(raw)})
				}
			}
		}
	}
	return nil
}
func piMessageUsage(raw json.RawMessage) *uint64 {
	var u struct{ Total, Input, Output, CacheRead, CacheWrite uint64 }
	if decodeUseNumber(raw, &u) != nil {
		return nil
	}
	n := u.Total
	if n == 0 {
		n = u.Input + u.Output + u.CacheRead + u.CacheWrite
	}
	if n == 0 {
		return nil
	}
	return &n
}
func (s *piSession) consumePiTool(typ string, value map[string]json.RawMessage, raw json.RawMessage) error {
	id, name := rawString(value["toolCallId"]), rawString(value["toolName"])
	if id == "" {
		return errors.New("Pi tool event is missing toolCallId")
	}
	s.mu.Lock()
	activity, ok := s.tools[id]
	if !ok {
		activity = Activity{ID: id, Kind: streamToolKind(name), Title: firstNonempty(name, "Tool"), Tool: name}
	}
	if typ == "tool_execution_start" {
		activity.Arguments = cloneRaw(value["args"])
		activity.Title = streamToolTitle(activity.Title, value["args"])
		s.tools[id] = activity
	} else if typ == "tool_execution_update" {
		activity.Output = cloneRaw(value["partialResult"])
		s.tools[id] = activity
	} else {
		activity.Output = cloneRaw(value["result"])
		activity.Complete = true
		_ = decodeUseNumber(value["isError"], &activity.Failed)
		delete(s.tools, id)
	}
	s.mu.Unlock()
	a := activity
	s.emit(Event{Kind: EventActivity, Activity: &a, Raw: cloneRaw(raw)})
	return nil
}

func (s *piSession) emitPiCommands(data map[string]json.RawMessage) {
	raw := data["commands"]
	if len(raw) == 0 {
		return
	}
	var commands []map[string]json.RawMessage
	if decodeUseNumber(raw, &commands) != nil {
		return
	}
	items := make([]map[string]string, 0, len(commands))
	for _, command := range commands {
		name := firstNonempty(rawString(command["name"]), rawString(command["command"]))
		if name != "" {
			items = append(items, map[string]string{"name": name, "description": rawString(command["description"])})
		}
	}
	payload, _ := json.Marshal(items)
	s.emit(Event{Kind: EventAvailableCommands, Raw: payload})
}

func (s *piSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return errors.New("Pi prompt is empty")
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("Pi turn is already active")
	}
	s.next++
	id := fmt.Sprintf("waku-%d", s.next)
	s.prompts = map[string]struct{}{id: {}}
	s.active = true
	s.failed = false
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnStarted})
	if err := s.process.Lines.Write(map[string]any{"id": id, "type": "prompt", "message": text}); err != nil {
		s.mu.Lock()
		s.active = false
		delete(s.prompts, id)
		s.mu.Unlock()
		return err
	}
	return nil
}
func (s *piSession) Steer(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: "provider has no active turn"})
		return errors.New("provider has no active turn")
	}
	response, err := s.call(ctx, map[string]any{"type": "steer", "message": text})
	_ = response
	if err != nil {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: err.Error()})
		return err
	}
	s.emit(Event{Kind: EventSteerAccepted, Text: text})
	return nil
}
func (s *piSession) Cancel(ctx context.Context) error {
	_, err := s.call(ctx, map[string]any{"type": "abort"})
	return err
}
func (s *piSession) RespondPermission(context.Context, json.RawMessage, string) error {
	return ErrUnsupported
}
func (s *piSession) RespondQuestions(context.Context, json.RawMessage, []UserInputAnswer) error {
	return ErrUnsupported
}
func (s *piSession) Goal(context.Context, GoalOperation) error { return ErrUnsupported }
func (s *piSession) ApplyOptions(ctx context.Context, options SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if options.Access != "" && options.Access != AccessFull {
		return false, nil
	}
	if options.ServiceTier != "" {
		return false, nil
	}
	if options.Model != "" {
		provider, id, err := parsePiModel(options.Model)
		if err != nil {
			return true, err
		}
		if _, err = s.call(ctx, map[string]any{"type": "set_model", "provider": provider, "modelId": id}); err != nil {
			return true, err
		}
		s.mu.Lock()
		s.model = options.Model
		s.mu.Unlock()
	}
	if options.ReasoningEffort != "" {
		if _, err := s.call(ctx, map[string]any{"type": "set_thinking_level", "level": options.ReasoningEffort}); err != nil {
			return true, err
		}
		s.mu.Lock()
		s.effort = options.ReasoningEffort
		s.mu.Unlock()
	}
	return true, nil
}

func (s *piSession) forkRequest(ctx context.Context, turns uint64) (piCursor, error) {
	messages, err := s.call(ctx, map[string]any{"type": map[bool]string{true: "get_branch_messages", false: "get_fork_messages"}[s.provider == ProviderOhMyPi]})
	if err != nil {
		return piCursor{}, err
	}
	var entries []map[string]json.RawMessage
	if err := decodeUseNumber(messages.data["messages"], &entries); err != nil {
		return piCursor{}, errors.New("Pi branch messages are invalid")
	}
	if turns > uint64(len(entries)) {
		return piCursor{}, errors.New("cannot remove more Pi turns than exist")
	}
	request := map[string]any{"type": "clone"}
	if s.provider == ProviderOhMyPi && turns == 0 {
		return piCursor{}, fmt.Errorf("%w: Oh My Pi whole-session copy requires a launch-time fork", ErrUnsupported)
	}
	if turns > 0 {
		entry := rawString(entries[len(entries)-int(turns)]["entryId"])
		if entry == "" {
			return piCursor{}, errors.New("Pi branch message has no entryId")
		}
		command := "fork"
		if s.provider == ProviderOhMyPi {
			command = "branch"
		}
		request = map[string]any{"type": command, "entryId": entry}
	}
	response, err := s.call(ctx, request)
	if err != nil {
		return piCursor{}, err
	}
	if rawBool(response.data, "cancelled") {
		return piCursor{}, errors.New("Pi branch was cancelled")
	}
	state, err := s.call(ctx, map[string]any{"type": "get_state"})
	if err != nil {
		return piCursor{}, err
	}
	return piCursorFromState(s.provider, state.data)
}
func (s *piSession) Rollback(ctx context.Context, turns uint64) (json.RawMessage, error) {
	cursor, err := s.forkRequest(ctx, turns)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cursor = cursor
	s.mu.Unlock()
	return cursor.raw(), nil
}
func (s *piSession) Fork(ctx context.Context, turns uint64) (json.RawMessage, error) {
	s.mu.Lock()
	source := s.cursor
	s.mu.Unlock()
	cursor, err := s.forkRequest(ctx, turns)
	if err != nil {
		return nil, err
	}
	if source.sessionFile != "" {
		response, restoreErr := s.call(ctx, map[string]any{"type": "switch_session", "sessionPath": source.sessionFile})
		if restoreErr != nil || rawBool(response.data, "cancelled") {
			if restoreErr == nil {
				restoreErr = errors.New("Pi source restore was cancelled")
			}
			go terminateProcess(s.process)
			return nil, restoreErr
		}
		state, stateErr := s.call(ctx, map[string]any{"type": "get_state"})
		if stateErr != nil {
			return nil, stateErr
		}
		restored, stateErr := piCursorFromState(s.provider, state.data)
		if stateErr != nil || restored.sessionID != source.sessionID {
			return nil, errors.New("Pi source restore landed on the wrong session")
		}
	}
	return cursor.raw(), nil
}
func (s *piSession) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.process.Terminate(ctx)
}

func rawBool(fields map[string]json.RawMessage, key string) bool {
	var value bool
	_ = decodeUseNumber(fields[key], &value)
	return value
}
func rawFalse(raw json.RawMessage) bool {
	var value bool
	return decodeUseNumber(raw, &value) == nil && !value
}
func rawFalseField(raw json.RawMessage, key string) bool {
	var fields map[string]json.RawMessage
	if decodeUseNumber(raw, &fields) != nil {
		return false
	}
	return rawFalse(fields[key])
}
