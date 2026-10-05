package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
)

type CodexAdapter struct {
	startProcess func(context.Context, ProcessSpec) (*StdioProcess, error)
}

func (CodexAdapter) Descriptor() Descriptor { return Descriptors()[ProviderCodex] }

func (CodexAdapter) Discover(_ context.Context, options StartOptions) (Discovery, error) {
	binary := options.Binary
	if binary == "" {
		binary = "codex"
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return Discovery{Reason: err.Error()}, nil
	}
	return Discovery{Available: true, Executable: path}, nil
}

func (a CodexAdapter) Start(ctx context.Context, options StartOptions, sink Sink) (Session, error) {
	if sink == nil {
		return nil, fmt.Errorf("provider event sink is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, _, _, err := codexAccess(options.Access); err != nil {
		return nil, err
	}
	if options.ComputerUse {
		return nil, fmt.Errorf("%w: Codex computer use is not implemented", ErrUnsupported)
	}
	if options.Model != "" {
		return nil, fmt.Errorf("%w: Codex model selection awaits model discovery", ErrUnsupported)
	}
	if err := validateCodexCursor(options.ResumeCursor); err != nil {
		return nil, err
	}
	binary := options.Binary
	if binary == "" {
		binary = "codex"
	}
	starter := StartStdio
	if a.startProcess != nil {
		starter = a.startProcess
	}
	process, err := starter(ctx, ProcessSpec{Path: binary, Args: []string{"app-server", "--stdio"}, Directory: options.WorkingDirectory})
	if err != nil {
		return nil, err
	}
	s := &codexSession{process: process, sink: sink, eventQueue: make(chan Event, 256), normalizer: NewCodexNormalizer(), access: options.Access, effort: options.ReasoningEffort, tier: options.ServiceTier, pending: make(map[uint64]chan RPCMessage), pendingSizes: make(map[uint64]int), pendingSteers: make(map[uint64]string), pendingGoals: make(map[uint64]string)}
	s.next.Store(10)
	if err := s.initialize(options); err != nil {
		_ = process.Close()
		return nil, err
	}
	go s.dispatchEvents()
	go s.readLoop()
	return s, nil
}

type codexSession struct {
	process             *StdioProcess
	sink                Sink
	eventQueue          chan Event
	eventMu             sync.Mutex
	eventsClosed        bool
	eventBytes          int
	terminalEvents      []Event
	slowSink            atomic.Bool
	normalizer          *CodexNormalizer
	mu                  sync.RWMutex
	access              AccessMode
	model, effort, tier string
	pending             map[uint64]chan RPCMessage
	pendingSizes        map[uint64]int
	pendingSteers       map[uint64]string
	pendingSteerOrder   []uint64
	pendingGoals        map[uint64]string
	pendingBytes        int
	next                atomic.Uint64
	closed              atomic.Bool
}

func (s *codexSession) initialize(options StartOptions) error {
	if err := s.process.Lines.Write(map[string]any{"method": "initialize", "id": 0, "params": map[string]any{"clientInfo": map[string]string{"name": "waku", "title": "Waku", "version": "go"}, "capabilities": map[string]bool{"experimentalApi": true}}}); err != nil {
		return err
	}
	if err := s.process.Lines.Write(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
		return err
	}
	method := "thread/start"
	params := map[string]any{"cwd": options.WorkingDirectory, "serviceName": "waku"}
	if len(options.ResumeCursor) != 0 {
		var cursor struct {
			ThreadID string `json:"threadId"`
		}
		if err := decodeUseNumber(options.ResumeCursor, &cursor); err != nil {
			return fmt.Errorf("codex resume cursor: %w", err)
		}
		if cursor.ThreadID == "" {
			return fmt.Errorf("codex resume cursor has no threadId")
		}
		method = "thread/resume"
		params["threadId"] = cursor.ThreadID
		delete(params, "serviceName")
	}
	approval, sandbox, reviewer, err := codexAccess(options.Access)
	if err != nil {
		return err
	}
	params["approvalPolicy"] = approval
	params["sandbox"] = sandbox
	params["approvalsReviewer"] = reviewer
	if options.Model != "" {
		params["model"] = options.Model
	}
	if options.ServiceTier != "" {
		params["serviceTier"] = options.ServiceTier
	}
	return s.process.Lines.Write(map[string]any{"method": method, "id": 1, "params": params})
}

func (s *codexSession) readLoop() {
	stderrDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, s.process.Stderr); close(stderrDone) }()
	var readErr error
	for {
		var raw json.RawMessage
		if err := s.process.Lines.Decode(&raw); err != nil {
			readErr = err
			break
		}
		rpc, rpcErr := decodeRPCMessage(raw)
		if rpcErr != nil {
			if id, ok := malformedResponseID(raw); ok && s.rejectCorrelated(id, rpcErr) {
				continue
			}
			s.emit(Event{Kind: EventError, Text: rpcErr.Error(), Raw: cloneRaw(raw)})
			continue
		}
		if rpc.IsResponse() && s.handleCorrelatedResponse(rpc) {
			continue
		}
		s.mu.Lock()
		events, err := s.normalizer.Consume(raw)
		s.mu.Unlock()
		if err != nil {
			s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
			continue
		}
		for _, event := range events {
			s.emit(event)
		}
	}
	_ = s.process.Close()
	s.failPending("provider process exited")
	err := s.process.Wait()
	<-stderrDone
	if readErr != nil && readErr != io.EOF && !s.closed.Load() {
		s.emit(Event{Kind: EventError, Text: readErr.Error()})
	}
	if err != nil && !s.closed.Load() {
		s.emit(Event{Kind: EventError, Text: err.Error()})
	}
	s.eventMu.Lock()
	if !s.eventsClosed {
		s.eventsClosed = true
		close(s.eventQueue)
	}
	s.eventMu.Unlock()
}

const maxQueuedEventBytes = 8 << 20

func (s *codexSession) emit(event Event) bool {
	size := eventSize(event)
	s.eventMu.Lock()
	if s.eventsClosed {
		s.eventMu.Unlock()
		return false
	}
	if len(s.eventQueue) == cap(s.eventQueue) || s.eventBytes+size > maxQueuedEventBytes {
		s.slowSink.Store(true)
		s.eventMu.Unlock()
		_ = s.process.Close()
		return false
	}
	s.eventBytes += size
	s.eventQueue <- event
	s.eventMu.Unlock()
	return true
}
func (s *codexSession) dispatchEvents() {
	for event := range s.eventQueue {
		s.eventMu.Lock()
		s.eventBytes -= eventSize(event)
		if s.eventBytes < 0 {
			s.eventBytes = 0
		}
		s.eventMu.Unlock()
		s.sink.Emit(event)
	}
	s.eventMu.Lock()
	terminal := append([]Event(nil), s.terminalEvents...)
	s.eventMu.Unlock()
	for _, event := range terminal {
		s.sink.Emit(event)
	}
	if s.slowSink.Load() {
		s.sink.Emit(Event{Kind: EventError, Text: "provider event sink exceeded bounded queue"})
	}
	s.sink.Emit(Event{Kind: EventProcessExited})
}

func eventSize(event Event) int {
	size := len(event.Text) + len(event.Summary) + len(event.Raw) + len(event.Goal) + 64
	if event.Activity != nil {
		size += len(event.Activity.Title) + len(event.Activity.Detail) + len(event.Activity.Arguments) + len(event.Activity.Output)
	}
	if event.Background != nil {
		size += len(event.Background.Raw) + len(event.Background.OutputDelta) + len(event.Background.Title)
	}
	return size
}

func (s *codexSession) handleCorrelatedResponse(rpc RPCMessage) bool {
	id, ok := rawUint64(rpc.ID)
	if !ok {
		return false
	}
	s.mu.Lock()
	steer, isSteer := s.pendingSteers[id]
	goalOp := s.pendingGoals[id]
	ch, isCall := s.pending[id]
	if !isSteer && !isCall {
		s.mu.Unlock()
		return false
	}
	s.releasePendingLocked(id)
	s.mu.Unlock()
	if isSteer {
		if len(rpc.Error) != 0 {
			s.emit(Event{Kind: EventSteerRejected, Text: steer, Summary: responseErrorText(rpc.Error)})
		} else {
			s.emit(Event{Kind: EventSteerAccepted, Text: steer})
		}
		return true
	}
	if goalOp != "" {
		if len(rpc.Error) != 0 {
			s.emit(Event{Kind: EventError, Text: responseErrorText(rpc.Error)})
		} else {
			e := Event{Kind: EventGoalUpdated}
			if goalOp != "clear" {
				var result map[string]json.RawMessage
				_ = decodeUseNumber(rpc.Result, &result)
				if goal := result["goal"]; len(goal) != 0 && string(goal) != "null" {
					e.GoalPresent = true
					e.Goal = cloneRaw(goal)
				}
			}
			s.emit(e)
		}
	}
	ch <- rpc
	close(ch)
	return true
}

func (s *codexSession) rejectCorrelated(id uint64, cause error) bool {
	errorRaw, _ := json.Marshal(map[string]any{"code": -32603, "message": cause.Error()})
	return s.handleCorrelatedResponse(RPCMessage{ID: json.RawMessage(fmt.Sprint(id)), Error: errorRaw})
}

func malformedResponseID(raw json.RawMessage) (uint64, bool) {
	var fields map[string]json.RawMessage
	if decodeUseNumber(raw, &fields) != nil || fields == nil {
		return 0, false
	}
	if methodRaw, hasMethod := fields["method"]; hasMethod {
		var method string
		if decodeUseNumber(methodRaw, &method) != nil || method != "" {
			return 0, false
		}
	}
	return rawUint64(fields["id"])
}

func responseErrorText(raw json.RawMessage) string {
	if message, err := rpcErrorMessage(raw); err == nil {
		return message
	}
	var fallback struct {
		Message string `json:"message"`
	}
	_ = decodeUseNumber(raw, &fallback)
	if fallback.Message != "" {
		return fallback.Message
	}
	return "provider RPC failed"
}

func (s *codexSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	thread, _, err := s.ids()
	if err != nil {
		return err
	}
	s.mu.RLock()
	access, model, effort, tier := s.access, s.model, s.effort, s.tier
	s.mu.RUnlock()
	approval, sandbox, reviewer, err := codexAccess(access)
	if err != nil {
		return err
	}
	params := map[string]any{"threadId": thread, "input": []map[string]string{{"type": "text", "text": text}}, "approvalPolicy": approval, "approvalsReviewer": reviewer, "sandboxPolicy": codexSandbox(sandbox), "summary": "auto"}
	if model != "" {
		params["model"] = model
	}
	if effort != "" {
		params["effort"] = effort
	}
	if tier != "" {
		params["serviceTier"] = tier
	}
	return s.notify("turn/start", params)
}

func (s *codexSession) Steer(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	thread, turn, err := s.ids()
	if err != nil {
		return err
	}
	if turn == "" {
		return fmt.Errorf("codex has no active turn")
	}
	id := s.nextID()
	s.mu.Lock()
	if err := s.reservePendingLocked(id, len(text), text, nil); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	if err := s.writeRPC(id, "turn/steer", map[string]any{"threadId": thread, "expectedTurnId": turn, "input": []map[string]string{{"type": "text", "text": text}}}); err != nil {
		s.mu.Lock()
		s.releasePendingLocked(id)
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *codexSession) Cancel(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	thread, turn, err := s.ids()
	if err != nil {
		return err
	}
	if turn == "" {
		return nil
	}
	return s.notify("turn/interrupt", map[string]any{"threadId": thread, "turnId": turn})
}
func (s *codexSession) RespondPermission(ctx context.Context, requestID json.RawMessage, option string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validRPCID(requestID) {
		return errors.New("invalid JSON-RPC request id")
	}
	return s.process.Lines.Write(map[string]any{"id": cloneRaw(requestID), "result": map[string]string{"decision": option}})
}
func (s *codexSession) RespondQuestions(ctx context.Context, requestID json.RawMessage, answers []UserInputAnswer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	values := map[string]any{}
	for _, answer := range answers {
		values[answer.QuestionID] = map[string]any{"answers": answer.Answers}
	}
	if !validRPCID(requestID) {
		return errors.New("invalid JSON-RPC request id")
	}
	return s.process.Lines.Write(map[string]any{"id": cloneRaw(requestID), "result": map[string]any{"answers": values}})
}

func (s *codexSession) Goal(ctx context.Context, op GoalOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	thread, _, err := s.ids()
	if err != nil {
		return err
	}
	method := "thread/goal/get"
	params := map[string]any{"threadId": thread}
	switch op.Kind {
	case "refresh", "":
	case "clear":
		method = "thread/goal/clear"
	case "set":
		if op.Replace {
			if _, err := s.callGoal(ctx, "clear", "thread/goal/clear", map[string]any{"threadId": thread}); err != nil {
				return err
			}
		}
		method = "thread/goal/set"
		if op.Objective != nil {
			params["objective"] = *op.Objective
		}
		if op.Status != nil {
			params["status"] = *op.Status
		}
	default:
		return ErrUnsupported
	}
	_, err = s.callGoal(ctx, map[string]string{"thread/goal/get": "get", "thread/goal/clear": "clear", "thread/goal/set": "set"}[method], method, params)
	return err
}
func (s *codexSession) ApplyOptions(ctx context.Context, options SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if options.Access != "" && options.Access != s.access {
		return false, nil
	}
	if options.Model != "" {
		return false, ErrUnsupported
	}
	s.effort = options.ReasoningEffort
	s.tier = options.ServiceTier
	return true, nil
}

func (s *codexSession) Rollback(ctx context.Context, turns uint64) (json.RawMessage, error) {
	thread, _, err := s.ids()
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	historyLen := len(s.normalizer.turnIDs)
	s.mu.RUnlock()
	if turns > uint64(historyLen) {
		return nil, fmt.Errorf("cannot rollback %d turns from Codex history of %d", turns, historyLen)
	}
	_, err = s.call(ctx, "thread/rollback", map[string]any{"threadId": thread, "numTurns": turns})
	if err == nil {
		s.mu.Lock()
		retained := len(s.normalizer.turnIDs) - int(turns)
		if retained < 0 {
			retained = 0
		}
		s.normalizer.turnIDs = s.normalizer.turnIDs[:retained]
		s.mu.Unlock()
	}
	return nil, err
}
func (s *codexSession) Fork(ctx context.Context, turns uint64) (json.RawMessage, error) {
	thread, _, err := s.ids()
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	historyLen := len(s.normalizer.turnIDs)
	if turns >= uint64(historyLen) {
		s.mu.RUnlock()
		return nil, fmt.Errorf("cannot remove %d turns from Codex history", turns)
	}
	index := historyLen - 1 - int(turns)
	last := s.normalizer.turnIDs[index]
	s.mu.RUnlock()
	response, err := s.call(ctx, "thread/fork", map[string]any{"threadId": thread, "lastTurnId": last})
	if err != nil {
		return nil, err
	}
	var result struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if decodeUseNumber(response.Result, &result) != nil || result.Thread.ID == "" {
		return nil, fmt.Errorf("Codex fork response has no thread id")
	}
	cursor, _ := json.Marshal(map[string]string{"provider": "codex", "threadId": result.Thread.ID})
	return cursor, nil
}
func (s *codexSession) Close() error { s.closed.Store(true); return s.process.Close() }

func (s *codexSession) ids() (string, string, error) {
	if s.closed.Load() {
		return "", "", ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.normalizer.threadID == "" {
		return "", "", fmt.Errorf("codex thread is not connected")
	}
	return s.normalizer.threadID, s.normalizer.turnID, nil
}
func (s *codexSession) nextID() uint64 { return s.next.Add(1) }
func (s *codexSession) notify(method string, params any) error {
	return s.writeRPC(s.nextID(), method, params)
}
func (s *codexSession) writeRPC(id uint64, method string, params any) error {
	return s.process.Lines.Write(map[string]any{"id": id, "method": method, "params": params})
}
func (s *codexSession) call(ctx context.Context, method string, params any) (RPCMessage, error) {
	id := s.nextID()
	ch := make(chan RPCMessage, 1)
	s.mu.Lock()
	size := len(method)
	if encoded, err := json.Marshal(params); err == nil {
		size += len(encoded)
	}
	if err := s.reservePendingLocked(id, size, "", ch); err != nil {
		s.mu.Unlock()
		return RPCMessage{}, err
	}
	s.mu.Unlock()
	if err := s.writeRPC(id, method, params); err != nil {
		s.mu.Lock()
		s.releasePendingLocked(id)
		s.mu.Unlock()
		return RPCMessage{}, err
	}
	select {
	case response := <-ch:
		if len(response.Error) != 0 {
			return response, errors.New(responseErrorText(response.Error))
		}
		return response, nil
	case <-ctx.Done():
		s.mu.Lock()
		s.releasePendingLocked(id)
		s.mu.Unlock()
		return RPCMessage{}, ctx.Err()
	}
}

const maxPendingRPCs = 128
const maxPendingRPCBytes = 1 << 20

func (s *codexSession) reservePendingLocked(id uint64, size int, steer string, ch chan RPCMessage) error {
	if len(s.pending)+len(s.pendingSteers) >= maxPendingRPCs || s.pendingBytes+size > maxPendingRPCBytes {
		return fmt.Errorf("provider pending RPC limit exceeded")
	}
	if steer != "" {
		s.pendingSteers[id] = steer
		s.pendingSteerOrder = append(s.pendingSteerOrder, id)
	} else {
		s.pending[id] = ch
	}
	s.pendingSizes[id] = size
	s.pendingBytes += size
	return nil
}
func (s *codexSession) releasePendingLocked(id uint64) {
	delete(s.pending, id)
	delete(s.pendingSteers, id)
	for i, pendingID := range s.pendingSteerOrder {
		if pendingID == id {
			copy(s.pendingSteerOrder[i:], s.pendingSteerOrder[i+1:])
			s.pendingSteerOrder = s.pendingSteerOrder[:len(s.pendingSteerOrder)-1]
			break
		}
	}
	delete(s.pendingGoals, id)
	s.pendingBytes -= s.pendingSizes[id]
	if s.pendingBytes < 0 {
		s.pendingBytes = 0
	}
	delete(s.pendingSizes, id)
}
func (s *codexSession) failPending(message string) {
	raw, _ := json.Marshal(map[string]any{"code": -32000, "message": message})
	s.mu.Lock()
	calls := s.pending
	steers := s.pendingSteers
	steerOrder := s.pendingSteerOrder
	s.pending = make(map[uint64]chan RPCMessage)
	s.pendingSteers = make(map[uint64]string)
	s.pendingSteerOrder = nil
	s.pendingGoals = make(map[uint64]string)
	s.pendingSizes = make(map[uint64]int)
	s.pendingBytes = 0
	s.mu.Unlock()
	for _, ch := range calls {
		ch <- RPCMessage{Error: raw}
		close(ch)
	}
	for _, id := range steerOrder {
		text, ok := steers[id]
		if !ok {
			continue
		}
		event := Event{Kind: EventSteerRejected, Text: text, Summary: message}
		if !s.emit(event) {
			s.eventMu.Lock()
			s.terminalEvents = append(s.terminalEvents, event)
			s.eventMu.Unlock()
		}
	}
}

func (s *codexSession) callGoal(ctx context.Context, operation, method string, params any) (RPCMessage, error) {
	id := s.nextID()
	ch := make(chan RPCMessage, 1)
	encoded, _ := json.Marshal(params)
	s.mu.Lock()
	if err := s.reservePendingLocked(id, len(method)+len(encoded), "", ch); err != nil {
		s.mu.Unlock()
		return RPCMessage{}, err
	}
	s.pendingGoals[id] = operation
	s.mu.Unlock()
	if err := s.writeRPC(id, method, params); err != nil {
		s.mu.Lock()
		s.releasePendingLocked(id)
		s.mu.Unlock()
		return RPCMessage{}, err
	}
	select {
	case response := <-ch:
		if len(response.Error) != 0 {
			return response, errors.New(responseErrorText(response.Error))
		}
		return response, nil
	case <-ctx.Done():
		s.mu.Lock()
		s.releasePendingLocked(id)
		s.mu.Unlock()
		return RPCMessage{}, ctx.Err()
	}
}

func codexAccess(mode AccessMode) (string, string, string, error) {
	switch mode {
	case "", AccessAsk:
		return "untrusted", "read-only", "user", nil
	case AccessAutoAcceptEdits:
		return "on-request", "workspace-write", "user", nil
	case AccessAuto:
		return "on-request", "workspace-write", "auto_review", nil
	case AccessFull:
		return "never", "danger-full-access", "user", nil
	default:
		return "", "", "", fmt.Errorf("unknown access mode %q", mode)
	}
}

func validateCodexCursor(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var cursor struct {
		Provider Provider `json:"provider"`
		ThreadID string   `json:"threadId"`
	}
	if err := decodeUseNumber(raw, &cursor); err != nil {
		return fmt.Errorf("codex resume cursor: %w", err)
	}
	if cursor.Provider != ProviderCodex {
		return fmt.Errorf("codex adapter cannot resume %q cursor", cursor.Provider)
	}
	if cursor.ThreadID == "" {
		return fmt.Errorf("codex resume cursor has no threadId")
	}
	return nil
}
func codexSandbox(value string) map[string]string {
	switch value {
	case "read-only":
		return map[string]string{"type": "readOnly"}
	case "danger-full-access":
		return map[string]string{"type": "dangerFullAccess"}
	default:
		return map[string]string{"type": "workspaceWrite"}
	}
}
func rawUint64(raw json.RawMessage) (uint64, bool) {
	var value uint64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return 0, false
	}
	return value, true
}
func validRPCID(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var value any
	if decodeUseNumber(raw, &value) != nil {
		return false
	}
	switch value.(type) {
	case string, json.Number:
		return true
	default:
		return false
	}
}
