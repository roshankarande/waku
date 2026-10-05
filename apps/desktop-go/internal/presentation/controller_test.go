package presentation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/components/terminalui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

const (
	testProject = "00000000-0000-4000-8000-000000000101"
	testSession = "00000000-0000-4000-8000-000000000102"
	testRuntime = "00000000-0000-4000-8000-000000000103"
	testEpoch   = "00000000-0000-4000-8000-000000000104"
)

var testPosts struct {
	sync.Mutex
	queue []func()
}

func testPost(fn func()) {
	testPosts.Lock()
	testPosts.queue = append(testPosts.queue, fn)
	testPosts.Unlock()
}

func drainTestPosts() {
	for {
		testPosts.Lock()
		queue := testPosts.queue
		testPosts.queue = nil
		testPosts.Unlock()
		if len(queue) == 0 {
			return
		}
		for _, fn := range queue {
			fn()
		}
	}
}

type fakeTransport struct {
	mu          sync.Mutex
	requests    []protocol.CommandType
	raw         []json.RawMessage
	events      chan protocol.SequencedEvent
	tasks       chan uint64
	done        chan struct{}
	block       chan struct{}
	project     string
	attachNil   bool
	attachBlock chan struct{}
	runtimeID   string
	responses   map[protocol.CommandType]json.RawMessage
	requestErrs map[protocol.CommandType]error
	blocks      map[protocol.CommandType]chan struct{}
}

func newFakeTransport(project string) *fakeTransport {
	return &fakeTransport{events: make(chan protocol.SequencedEvent, 16), tasks: make(chan uint64, 4), done: make(chan struct{}), project: project}
}

func (f *fakeTransport) Request(ctx context.Context, _, _ protocol.UUID, command any) (json.RawMessage, error) {
	raw, _ := json.Marshal(command)
	kind, err := protocol.CommandTypeOf(raw)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.requests = append(f.requests, kind)
	f.raw = append(f.raw, append(json.RawMessage(nil), raw...))
	f.mu.Unlock()
	if wait := f.blocks[kind]; wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := f.requestErrs[kind]; err != nil {
		return nil, err
	}
	if response := f.responses[kind]; response != nil {
		return append(json.RawMessage(nil), response...), nil
	}
	if kind == protocol.CommandLoadTaskState && f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	switch kind {
	case protocol.CommandLoadTaskState:
		return taskStateResponse(f.project), nil
	case protocol.CommandHydrateSession:
		return sessionResponse(), nil
	case protocol.CommandAttachSession:
		if f.attachBlock != nil {
			select {
			case <-f.attachBlock:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if f.attachNil {
			return json.RawMessage(`{"type":"sessionRuntime","runtimeId":null,"supportsSteer":false}`), nil
		}
		runtimeID := f.runtimeID
		if runtimeID == "" {
			runtimeID = testRuntime
		}
		return json.RawMessage(`{"type":"sessionRuntime","runtimeId":"` + runtimeID + `","supportsSteer":true}`), nil
	case protocol.CommandStart:
		return json.RawMessage(`{"type":"started","supportsSteer":true}`), nil
	default:
		return json.RawMessage(`{"type":"ack"}`), nil
	}
}

func TestEventClassifierCoversProtocolV7(t *testing.T) {
	kinds := []protocol.EventKind{protocol.EventConnected, protocol.EventAgentPresetSelected, protocol.EventAutoTitleUpdated, protocol.EventAvailableCommands, protocol.EventTurnStarted, protocol.EventTurnParked, protocol.EventTextDelta, protocol.EventReasoningDelta, protocol.EventActivity, protocol.EventRichActivity, protocol.EventBackgroundWork, protocol.EventPermission, protocol.EventUserInputRequested, protocol.EventComputerUseUpdated, protocol.EventPromptSubmitted, protocol.EventSteerAccepted, protocol.EventSteerRejected, protocol.EventUsageUpdated, protocol.EventPlanUsageUpdated, protocol.EventGoalUpdated, protocol.EventTurnFinished, protocol.EventError, protocol.EventProcessExited, protocol.EventTerminalOutput, protocol.EventTerminalExited, protocol.EventTerminalError}
	for _, kind := range kinds {
		if _, _, ok := classifyEvent(kind); !ok {
			t.Errorf("unclassified %s", kind)
		}
	}
	if _, _, ok := classifyEvent("futureEvent"); ok {
		t.Fatal("unknown event silently classified")
	}
}

func TestDecodeSessionRejectsNullAndDecodesRustBlocks(t *testing.T) {
	if _, err := decodeSession(json.RawMessage("null")); err == nil {
		t.Fatal("null session accepted")
	}
	raw := json.RawMessage(`{"id":"` + testSession + `","title":"History","project_id":"` + testProject + `","status":"idle","updated_at":2,"messages":[{"id":"m1","role":"user","content":"ask"},{"id":"m2","role":"assistant","content":"answer"}],"transcript_blocks":[{"after_message":1,"turn_id":null,"content":{"kind":"reasoning","data":{"content":"thinking","started_at_ms":1,"finished_at_ms":2}}},{"after_message":1,"turn_id":null,"content":{"kind":"activities","data":[{"id":"` + testRuntime + `","kind":"tool","title":"Lookup","detail":"done","complete":true,"failed":false,"image_urls":[],"file_changes":[],"reasoning":null}]}}]}`)
	got, err := decodeSession(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Transcript) != 4 || got.Transcript[0].Body != "ask" || got.Transcript[1].Body != "thinking" || got.Transcript[2].Title != "Lookup" || got.Transcript[3].Body != "answer" {
		t.Fatalf("transcript=%+v", got.Transcript)
	}
	if got.Summary.UpdatedAt.Unix() != 2 {
		t.Fatalf("updated_at=%v", got.Summary.UpdatedAt)
	}
}

func TestDecodeTranscriptReasoningActivityPreservesContentAndRole(t *testing.T) {
	raw := json.RawMessage(`{"after_message":0,"turn_id":null,"content":{"kind":"activities","data":[{"id":"` + testRuntime + `","kind":"reasoning","title":"Reasoning","complete":true,"failed":false,"image_urls":[],"file_changes":[],"reasoning":{"content":"private thought","started_at_ms":1,"finished_at_ms":2}}]}}`)
	items, err := decodeTranscriptBlock(raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Role != RoleReasoning || items[0].Body != "private thought" {
		t.Fatalf("items=%+v", items)
	}
}
func (f *fakeTransport) Notify(context.Context, protocol.UUID, protocol.UUID, any) error { return nil }
func (f *fakeTransport) Subscribe(protocol.UUID, protocol.UUID) (<-chan protocol.SequencedEvent, func()) {
	return f.events, func() {}
}
func (f *fakeTransport) SubscribeTaskState() (<-chan uint64, func()) { return f.tasks, func() {} }
func (f *fakeTransport) Done() <-chan struct{}                       { return f.done }
func (f *fakeTransport) Err() error                                  { return context.Canceled }
func (f *fakeTransport) saw(kind protocol.CommandType) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, got := range f.requests {
		if got == kind {
			return true
		}
	}
	return false
}
func (f *fakeTransport) count(kind protocol.CommandType) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, got := range f.requests {
		if got == kind {
			n++
		}
	}
	return n
}
func (f *fakeTransport) last(kind protocol.CommandType) json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.requests) - 1; i >= 0; i-- {
		if f.requests[i] == kind {
			return append(json.RawMessage(nil), f.raw[i]...)
		}
	}
	return nil
}

func taskStateResponse(project string) json.RawMessage {
	return json.RawMessage(`{"type":"taskState","projects":[{"id":"` + project + `","name":"Workspace","path":"/synthetic"}],"sessions":[` + string(sessionJSON(false)) + `],"defaultCwd":"/synthetic","projectlessRoot":null}`)
}
func sessionResponse() json.RawMessage {
	return json.RawMessage(`{"type":"session","session":` + string(sessionJSON(true)) + `}`)
}
func sessionJSON(hydrated bool) json.RawMessage {
	extra := ""
	if hydrated {
		extra = `,"runtime_event_cursor":{"runtime_id":"` + testRuntime + `","epoch":"` + testEpoch + `","sequence":1},"messages":[]`
	}
	return json.RawMessage(`{"id":"` + testSession + `","title":"Live session","project_id":"` + testProject + `","provider":"codex","runtime_mode":"ask","cwd":"/synthetic","binary":"codex","status":"idle","created_at":1,"updated_at":2` + extra + `}`)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		drainTestPosts()
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestControllerLoadsSummariesThenHydratesSelectedSession(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool {
		return len(model.Sessions) == 1 && model.Sessions[0].Hydrated && model.Sessions[0].RuntimeID != ""
	})
	if model.Projects[0].Name != "Workspace" || model.Sessions[0].RuntimeID != testRuntime {
		t.Fatalf("model=%+v sessions=%+v", model.Projects, model.Sessions)
	}
	if !transport.saw(protocol.CommandLoadTaskState) || !transport.saw(protocol.CommandHydrateSession) {
		t.Fatal("summary/hydration requests missing")
	}
}

func TestControllerRejectsStaleHierarchyCommit(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	stale := newFakeTransport("stale-project")
	stale.block = make(chan struct{})
	controller.SetTransport(stale)
	fresh := newFakeTransport(testProject)
	controller.SetTransport(fresh)
	eventually(t, func() bool { return len(model.Projects) == 1 && model.Projects[0].ID == testProject })
	close(stale.block)
	time.Sleep(20 * time.Millisecond)
	if model.Projects[0].ID != testProject {
		t.Fatalf("stale project committed: %+v", model.Projects)
	}
}

func TestControllerStreamsApprovalAndDispatchesResponse(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool { return len(model.Sessions) == 1 && model.Sessions[0].Hydrated })
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 1, Event: protocol.WireDriverEvent{Kind: string(protocol.EventTextDelta), Payload: json.RawMessage(`"hello"`)}}
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 2, Event: protocol.WireDriverEvent{Kind: string(protocol.EventPermission), Payload: json.RawMessage(`{"requestId":"approval-1","title":"Run command?","detail":"make test","options":[{"id":"yes","label":"Allow","allow":true},{"id":"no","label":"Deny","allow":false}]}`)}}
	eventually(t, func() bool {
		return model.Sessions[0].Status == SessionWaitingApproval && len(model.Sessions[0].Transcript) == 1
	})
	command, ok := model.BeginCommand(testSession, CommandApprove, "")
	command.RequestID = "approval-1"
	if !ok || !controller.Dispatch(command) {
		t.Fatal("approval not dispatched")
	}
	eventually(t, func() bool {
		return transport.saw(protocol.CommandRespond) && model.Sessions[0].Status == SessionRunning
	})
}

func TestControllerSendsAndStopsIndependently(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool { return len(model.Sessions) == 1 && model.Sessions[0].Hydrated })
	send, _ := model.BeginCommand(testSession, CommandSend, "hello")
	if !controller.Dispatch(send) {
		t.Fatal("send queue rejected")
	}
	stop, _ := model.BeginCommand(testSession, CommandStop, "")
	if !controller.Dispatch(stop) {
		t.Fatal("stop queue rejected")
	}
	eventually(t, func() bool { return transport.saw(protocol.CommandPrompt) && transport.saw(protocol.CommandCancel) })
}

func TestControllerAnswersPreparedQuestion(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool { return len(model.Sessions) == 1 && model.Sessions[0].Hydrated })
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 3, Event: protocol.WireDriverEvent{Kind: string(protocol.EventUserInputRequested), Payload: json.RawMessage(`{"requestId":"question-1","questions":[{"id":"target","header":"Target","question":"Where?","options":[],"multiSelect":false},{"id":"checks","header":"Checks","question":"Which?","options":[{"label":"unit","description":"fast checks"},{"label":"race","description":"concurrency checks"}],"multiSelect":true}]}`)}}
	eventually(t, func() bool { return model.Sessions[0].Pending.Kind == InteractionQuestion })
	if len(model.Sessions[0].Pending.Questions) != 2 || !model.Sessions[0].Pending.Questions[1].MultiSelect {
		t.Fatalf("pending=%+v", model.Sessions[0].Pending)
	}
	if model.Sessions[0].Pending.Questions[1].OptionDescriptions["race"] != "concurrency checks" {
		t.Fatalf("descriptions=%+v", model.Sessions[0].Pending.Questions[1].OptionDescriptions)
	}
	command, ok := model.BeginCommand(testSession, CommandApprove, "")
	command.RequestID = "question-1"
	command.Answers = []QuestionAnswer{{QuestionID: "target", Answers: []string{"production"}}, {QuestionID: "checks", Answers: []string{"unit", "race"}}}
	if !ok || !controller.Dispatch(command) {
		t.Fatal("question response not dispatched")
	}
	eventually(t, func() bool { return transport.saw(protocol.CommandRespondUserInput) })
	var response protocol.RespondUserInputCommand
	if err := json.Unmarshal(transport.last(protocol.CommandRespondUserInput), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "question-1" || len(response.Answers) != 2 || response.Answers[0].QuestionID != "target" || response.Answers[1].QuestionID != "checks" || len(response.Answers[1].Answers) != 2 {
		t.Fatalf("response=%+v", response)
	}
}

func TestControllerPreservesAndSendsExactPermissionOption(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool { return len(model.Sessions) == 1 && model.Sessions[0].Hydrated })
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 30, Event: protocol.WireDriverEvent{Kind: string(protocol.EventPermission), Payload: json.RawMessage(`{"requestId":"permission-1","title":"Run?","detail":"Review","options":[{"id":"once","label":"Allow once","allow":true},{"id":"always","label":"Always allow","allow":true},{"id":"deny","label":"Deny","allow":false}]}`)}}
	eventually(t, func() bool { return len(model.Sessions[0].Pending.Options) == 3 })
	command, ok := model.BeginCommand(testSession, CommandApprove, "")
	command.OptionID = "always"
	command.RequestID = "permission-1"
	if !ok || !controller.Dispatch(command) {
		t.Fatal("permission response not dispatched")
	}
	eventually(t, func() bool { return transport.saw(protocol.CommandRespond) })
	var response protocol.RespondCommand
	if err := json.Unmarshal(transport.last(protocol.CommandRespond), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "permission-1" || response.OptionID != "always" {
		t.Fatalf("response=%+v", response)
	}
}

func TestControllerRejectsResponseAfterInteractionReplacement(t *testing.T) {
	model := NewModel(nil, nil)
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.SetTransport(transport)
	eventually(t, func() bool { return len(model.Sessions) == 1 && model.Sessions[0].Hydrated })
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 40, Event: protocol.WireDriverEvent{Kind: string(protocol.EventPermission), Payload: json.RawMessage(`{"requestId":"old","title":"Old","detail":"Old","options":[{"id":"old-allow","label":"Allow old","allow":true}]}`)}}
	eventually(t, func() bool { return model.Sessions[0].Pending.RequestID == "old" })
	command, ok := model.BeginCommand(testSession, CommandApprove, "")
	if !ok {
		t.Fatal("old response command not prepared")
	}
	command.RequestID, command.OptionID = "old", "old-allow"
	transport.events <- protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 41, Event: protocol.WireDriverEvent{Kind: string(protocol.EventPermission), Payload: json.RawMessage(`{"requestId":"new","title":"New","detail":"New","options":[{"id":"new-allow","label":"Allow new","allow":true}]}`)}}
	eventually(t, func() bool { return model.Sessions[0].Pending.RequestID == "new" })
	controller.mu.Lock()
	gen, live := controller.generation, controller.transport
	controller.mu.Unlock()
	controller.execute(gen, live, command)
	drainTestPosts()
	if transport.saw(protocol.CommandRespond) {
		t.Fatal("stale interaction response reached transport")
	}
	session := model.SessionByID(testSession)
	if session.Pending.RequestID != "new" || session.Status != SessionWaitingApproval || CommandPending(session, CommandApprove) || CommandPending(session, CommandReject) {
		t.Fatalf("replacement interaction was not preserved and actionable: %+v", session)
	}
}

func TestSettingsRowsNeverExposePathsOrUnknownValues(t *testing.T) {
	rows, err := settingsRows(json.RawMessage(`{"computer_use_enabled":true,"computer_use_allowed_apps":[{"bundle_id":"secret.bundle"}],"disabled_providers":["codex"],"provider_binary_overrides":{"codex":"/private/token/bin"},"future_secret":"bearer-secret"}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(rows)
	visible := string(encoded)
	for _, secret := range []string{"secret.bundle", "/private/token/bin", "bearer-secret"} {
		if strings.Contains(visible, secret) {
			t.Fatalf("settings UI leaked %q: %s", secret, visible)
		}
	}
	if !strings.Contains(visible, "paths hidden") || !strings.Contains(visible, "preserved setting fields hidden") {
		t.Fatalf("missing honest redaction explanation: %s", visible)
	}
}

func TestRichActivityVisibleSummaryDoesNotExposeArgumentsOrOutput(t *testing.T) {
	model := NewModel([]Project{{ID: "project", Name: "Project"}}, []Session{{ID: testSession, ProjectID: "project", Title: "Session"}})
	controller := NewController(model, testPost)
	defer controller.Close()
	transport := newFakeTransport(testProject)
	controller.mu.Lock()
	controller.transport = transport
	controller.generation = 7
	controller.mu.Unlock()
	controller.applyEvent(7, transport, testSession, protocol.SequencedEvent{SessionID: testSession, RuntimeID: testRuntime, Epoch: testEpoch, Sequence: 31, Event: protocol.WireDriverEvent{Kind: string(protocol.EventRichActivity), Payload: json.RawMessage(`{"id":"dddddddd-dddd-4ddd-8ddd-dddddddddddd","kind":"tool","title":"Safe tool title","detail":"Safe prepared detail","arguments":"bearer-secret","output":"/private/result","image_urls":[],"failed":false,"complete":true,"file_changes":[]}`)}})
	eventually(t, func() bool { return len(model.Sessions[0].Transcript) == 1 })
	item := model.Sessions[0].Transcript[0]
	if item.Title != "Safe tool title" || item.Body != "Safe prepared detail" {
		t.Fatalf("visible activity=%+v", item)
	}
	if strings.Contains(item.Body, "bearer-secret") || strings.Contains(item.Body, "/private/result") {
		t.Fatalf("visible activity leaked provider internals: %+v", item)
	}
	if !bytes.Contains(model.Sessions[0].ProviderEventPayloads[string(protocol.EventRichActivity)], []byte("bearer-secret")) {
		t.Fatal("authoritative provider payload was not retained outside the visible summary")
	}
}

func TestReconnectAlwaysAttachesPersistedCursor(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	first := newFakeTransport(testProject)
	c.SetTransport(first)
	eventually(t, func() bool { return first.count(protocol.CommandAttachSession) > 0 })
	second := newFakeTransport(testProject)
	c.SetTransport(second)
	eventually(t, func() bool { return second.count(protocol.CommandAttachSession) > 0 })
}

func TestInactiveSessionStartsWithFreshRuntime(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	transport := newFakeTransport(testProject)
	transport.attachNil = true
	c.SetTransport(transport)
	eventually(t, func() bool {
		return transport.count(protocol.CommandStart) > 0 && len(model.Sessions) > 0 && model.Sessions[0].RuntimeID != ""
	})
	if model.Sessions[0].RuntimeID == "" || model.Sessions[0].RuntimeID == testRuntime {
		t.Fatalf("runtime=%q", model.Sessions[0].RuntimeID)
	}
}

func TestStaleAttachCannotPoisonNewGeneration(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	stale := newFakeTransport(testProject)
	stale.attachBlock = make(chan struct{})
	c.SetTransport(stale)
	eventually(t, func() bool { return stale.count(protocol.CommandAttachSession) > 0 })
	freshRuntime := "00000000-0000-4000-8000-000000000109"
	fresh := newFakeTransport(testProject)
	fresh.runtimeID = freshRuntime
	c.SetTransport(fresh)
	eventually(t, func() bool {
		return fresh.count(protocol.CommandAttachSession) > 0 && len(model.Sessions) > 0 && model.Sessions[0].RuntimeID == freshRuntime
	})
	close(stale.attachBlock)
	time.Sleep(10 * time.Millisecond)
	drainTestPosts()
	if model.Sessions[0].RuntimeID != freshRuntime {
		t.Fatalf("stale attach committed: %q", model.Sessions[0].RuntimeID)
	}
}

func TestEnsureRuntimeSingleflightsAttach(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	transport := newFakeTransport(testProject)
	transport.attachBlock = make(chan struct{})
	c.mu.Lock()
	c.transport, c.generation = transport, 1
	c.mu.Unlock()
	sid, _ := protocol.ParseUUID(testSession)
	type result struct {
		runtime protocol.UUID
		err     error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			runtime, err := c.ensureRuntime(1, transport, testSession, sid)
			results <- result{runtime, err}
		}()
	}
	eventually(t, func() bool { return transport.count(protocol.CommandAttachSession) == 1 })
	close(transport.attachBlock)
	for range 2 {
		got := <-results
		if got.err != nil || got.runtime != protocol.UUID(testRuntime) {
			t.Fatalf("result=%+v", got)
		}
	}
	if transport.count(protocol.CommandAttachSession) != 1 {
		t.Fatalf("attach count=%d", transport.count(protocol.CommandAttachSession))
	}
}

func TestLateOldGenerationCannotCreateFlightInNewGeneration(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	old := newFakeTransport(testProject)
	c.mu.Lock()
	c.transport, c.generation = old, 1
	c.mu.Unlock()
	fresh := newFakeTransport(testProject)
	c.SetTransport(fresh)
	sid, _ := protocol.ParseUUID(testSession)
	if _, err := c.ensureRuntime(1, old, testSession, sid); err == nil {
		t.Fatal("late old generation established runtime")
	}
	if old.count(protocol.CommandAttachSession) != 0 {
		t.Fatalf("old attach count=%d", old.count(protocol.CommandAttachSession))
	}
	eventually(t, func() bool { return fresh.count(protocol.CommandAttachSession) > 0 })
}

func TestStreamDrainsBeforeUnknownEventAndChannelClose(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	transport := newFakeTransport(testProject)
	c.SetTransport(transport)
	eventually(t, func() bool {
		return len(model.Sessions) == 1 && model.Sessions[0].Hydrated && model.Sessions[0].RuntimeID != ""
	})
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 20, Event: protocol.WireDriverEvent{Kind: string(protocol.EventTextDelta), Payload: json.RawMessage(`"exact"`)}}
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 21, Event: protocol.WireDriverEvent{Kind: "futureEvent", Payload: json.RawMessage(`{"x":1}`)}}
	close(transport.events)
	eventually(t, func() bool { return len(model.Sessions[0].Transcript) >= 2 })
	if model.Sessions[0].Transcript[0].Body != "exact" || model.Sessions[0].Transcript[1].Title != "Provider event error" {
		t.Fatalf("order=%+v", model.Sessions[0].Transcript)
	}
}

func TestEventSemanticsPreserveReasoningAndTerminalState(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	transport := newFakeTransport(testProject)
	c.SetTransport(transport)
	eventually(t, func() bool {
		return len(model.Sessions) == 1 && model.Sessions[0].Hydrated && model.Sessions[0].RuntimeID != ""
	})
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 30, Event: protocol.WireDriverEvent{Kind: string(protocol.EventActivity), Payload: json.RawMessage(`{"id":"r","kind":"reasoning","title":"Thinking","detail":"why","complete":false}`)}}
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 31, Event: protocol.WireDriverEvent{Kind: string(protocol.EventTerminalOutput), Payload: json.RawMessage(`{"data":"aGk="}`)}}
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 32, Event: protocol.WireDriverEvent{Kind: string(protocol.EventTerminalError), Payload: json.RawMessage(`"terminal failed"`)}}
	transport.events <- protocol.SequencedEvent{Epoch: testEpoch, Sequence: 33, Event: protocol.WireDriverEvent{Kind: string(protocol.EventTerminalExited), Payload: json.RawMessage(`null`)}}
	eventually(t, func() bool {
		return model.Sessions[0].Pane.Err == "terminal failed" && strings.Contains(model.Sessions[0].Pane.Summary, "terminal exited")
	})
	s := &model.Sessions[0]
	if len(s.Transcript) == 0 || s.Transcript[0].Role != RoleReasoning || !strings.Contains(s.Pane.Summary, "hi") || s.Status == SessionFailed {
		t.Fatalf("session=%+v", s)
	}
	if string(s.ProviderEventPayloads[string(protocol.EventTerminalOutput)]) != `{"data":"aGk="}` {
		t.Fatalf("payloads=%q", s.ProviderEventPayloads)
	}
}

func TestNilTransportCompletionCannotCrossReconnectOrClose(t *testing.T) {
	model := NewModel([]Project{{ID: testProject}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	command, ok := model.BeginCommand(testSession, CommandSend, "hello")
	if !ok {
		t.Fatal("begin")
	}
	c.complete(command, SessionFailed, "stale", errors.New("offline"), 0, nil)
	c.SetTransport(newFakeTransport(testProject))
	drainTestPosts()
	if model.Sessions[0].Status == SessionFailed {
		t.Fatal("nil-transport completion crossed reconnect")
	}
	c.Close()
	if c.Dispatch(command) {
		t.Fatal("dispatch accepted after Close")
	}
}

func TestHubLoadsPreparedSettingsAndExplainsGatedCapability(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	base := newFakeTransport(testProject)
	base.requestErrs = map[protocol.CommandType]error{protocol.CommandLoadSkills: errors.New("command loadSkills is not implemented by the Go daemon")}
	transport := &fakeSurfaceTransport{fakeTransport: base, settings: protocol.DaemonSettingsValue{DisabledProviders: []string{"amp"}, Extra: map[string]json.RawMessage{"future": json.RawMessage(`{"exact":9007199254740993}`)}}}
	c.mu.Lock()
	c.transport, c.generation = transport, 1
	c.mu.Unlock()
	c.RequestHub(HubSettings)
	eventually(t, func() bool { return !model.Hub.Pending })
	if model.Hub.Kind != HubSettings || len(model.Hub.Rows) != 6 || model.Hub.Settings == nil || model.Hub.Rows[0].Key != "settings-write" {
		t.Fatalf("hub=%+v", model.Hub)
	}
	// A newer server-side document cannot be conditionally updated by protocol
	// v7, so the desktop must never send a stale whole-document replacement.
	transport.mu.Lock()
	transport.settings.Extra["concurrent"] = json.RawMessage(`true`)
	transport.mu.Unlock()
	if len(transport.updates()) != 0 || !strings.Contains(model.Hub.Rows[0].Detail, "no revision") {
		t.Fatalf("settings mutation was not safely gated: hub=%+v updates=%+v", model.Hub, transport.updates())
	}
	c.RequestHub(HubSkills)
	eventually(t, func() bool { return !model.Hub.Pending })
	if !strings.Contains(model.Hub.Unavailable, "not implemented") || len(model.Hub.Rows) != 0 {
		t.Fatalf("hub=%+v", model.Hub)
	}
}

func TestHubRejectsStaleAsyncPanelResult(t *testing.T) {
	model := NewModel(nil, nil)
	c := NewController(model, testPost)
	defer c.Close()
	block := make(chan struct{})
	entered := make(chan struct{})
	base := newFakeTransport(testProject)
	base.requestErrs = map[protocol.CommandType]error{protocol.CommandLoadSkills: errors.New("skills unavailable")}
	transport := &fakeSurfaceTransport{fakeTransport: base, settingsBlock: block, settingsEnter: entered}
	c.mu.Lock()
	c.transport, c.generation = transport, 1
	c.mu.Unlock()
	c.RequestHub(HubSettings)
	<-entered
	c.RequestHub(HubSkills)
	eventually(t, func() bool { return !model.Hub.Pending && model.Hub.Kind == HubSkills })
	close(block)
	time.Sleep(10 * time.Millisecond)
	drainTestPosts()
	if model.Hub.Kind != HubSkills || !strings.Contains(model.Hub.Unavailable, "skills unavailable") {
		t.Fatalf("stale settings replaced panel: %+v", model.Hub)
	}
}

type fakeSurfaceTransport struct {
	*fakeTransport
	mu            sync.Mutex
	settings      protocol.DaemonSettingsValue
	settingsBlock chan struct{}
	settingsEnter chan struct{}
	updated       []protocol.DaemonSettingsValue
	workspace     func(context.Context, any) (any, error)
}

func (f *fakeSurfaceTransport) GetSettings(ctx context.Context) (protocol.DaemonSettingsValue, error) {
	if f.settingsEnter != nil {
		select {
		case <-f.settingsEnter:
		default:
			close(f.settingsEnter)
		}
	}
	if f.settingsBlock != nil {
		select {
		case <-f.settingsBlock:
		case <-ctx.Done():
			return protocol.DaemonSettingsValue{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneSettings(f.settings), nil
}
func (f *fakeSurfaceTransport) UpdateSettings(_ context.Context, settings protocol.DaemonSettingsValue) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updated = append(f.updated, cloneSettings(settings))
	f.settings = cloneSettings(settings)
	return nil
}
func (f *fakeSurfaceTransport) updates() []protocol.DaemonSettingsValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.DaemonSettingsValue(nil), f.updated...)
}
func (f *fakeSurfaceTransport) Workspace(ctx context.Context, operation any) (any, error) {
	if f.workspace == nil {
		return nil, errors.New("workspace unavailable")
	}
	return f.workspace(ctx, operation)
}

type preparedTerminalConn struct {
	closed chan struct{}
	once   sync.Once
}

func newPreparedTerminalConn() *preparedTerminalConn {
	return &preparedTerminalConn{closed: make(chan struct{})}
}
func (c *preparedTerminalConn) Read([]byte) (int, error) { <-c.closed; return 0, io.EOF }
func (c *preparedTerminalConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}
func (c *preparedTerminalConn) Resize(int, int) error { return nil }
func (c *preparedTerminalConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

type fakePreparedTerminal struct {
	pane    *terminalui.Pane
	entered chan struct{}
	release chan struct{}
}

func (f *fakePreparedTerminal) PrepareTerminal(ctx context.Context, _ string) (*terminalui.Pane, error) {
	if f.entered != nil {
		close(f.entered)
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.pane, nil
}

func TestTypedWorkspaceFilesReviewAndCommit(t *testing.T) {
	const root = "ssh://remote.example/repo/../opaque"
	model := NewModel([]Project{{ID: testProject, PathHint: root}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	defer c.Close()
	var operations []any
	var operationsMu sync.Mutex
	transport := &fakeSurfaceTransport{fakeTransport: newFakeTransport(testProject)}
	transport.workspace = func(_ context.Context, operation any) (any, error) {
		operationsMu.Lock()
		operations = append(operations, operation)
		operationsMu.Unlock()
		switch value := operation.(type) {
		case protocol.WorkspaceListTree:
			if value.Root != root {
				t.Fatalf("root was reinterpreted: %q", value.Root)
			}
			return &protocol.WorkspaceWorkingTreeResult{Type: "workingTree", Entries: []protocol.WorkingTreeEntryValue{{RelativePath: "dir", AbsolutePath: "ssh://remote.example/repo/dir", Name: "dir", IsDir: true}, {RelativePath: "README.md", AbsolutePath: "ssh://remote.example/repo/README.md", Name: "README.md"}}}, nil
		case protocol.WorkspaceReadTextFile:
			return &protocol.WorkspaceTextFileResult{Type: "textFile", Content: "remote contents"}, nil
		case protocol.WorkspaceBrowseDirectory:
			return &protocol.WorkspaceDirectoryResult{Type: "directory", Path: *value.Path, Entries: []protocol.WorkingTreeEntryValue{{RelativePath: "nested.go", AbsolutePath: *value.Path + "/nested.go", Name: "nested.go"}}}, nil
		case protocol.WorkspaceCWDOperation:
			return &protocol.WorkspaceCommitSnapshotResult{Type: "commitSnapshot", Snapshot: protocol.CommitSnapshotValue{Branch: "main", HasStaged: true, HasUnstaged: true, StagedAdditions: 2, Additions: 3}}, nil
		case protocol.WorkspaceCollectReviewDiff:
			return &protocol.WorkspaceReviewDiffResult{Type: "reviewDiff", Data: protocol.ReviewDiffDataValue{Source: value.Source, Patch: "@@ -1 +1 @@\n-old\n+new\n", CompleteContext: true}}, nil
		default:
			return nil, fmt.Errorf("unexpected operation %T", operation)
		}
	}
	c.mu.Lock()
	c.transport, c.generation = transport, 1
	c.mu.Unlock()
	c.RequestPane(testSession, PaneFiles)
	eventually(t, func() bool { return len(model.Sessions[0].Pane.WorkspaceEntries) == 2 })
	if !strings.Contains(model.Sessions[0].Pane.CapabilityNote, "compare-and-swap") {
		t.Fatalf("missing save gate: %+v", model.Sessions[0].Pane)
	}
	c.RequestWorkspaceEntry(testSession, "README.md", "ssh://remote.example/repo/README.md", false)
	eventually(t, func() bool { return model.Sessions[0].Pane.Editor != nil })
	if model.Sessions[0].Pane.Editor.Model.Text != "remote contents" {
		t.Fatalf("editor=%+v", model.Sessions[0].Pane.Editor.Model)
	}
	c.RequestWorkspaceEntry(testSession, "dir", "ssh://remote.example/repo/dir", true)
	eventually(t, func() bool { return model.Sessions[0].Pane.WorkspaceRoot == "ssh://remote.example/repo/dir" })
	c.RequestPane(testSession, PaneReview)
	eventually(t, func() bool { return model.Sessions[0].Pane.ReviewSnapshot != nil })
	if !model.Sessions[0].Pane.Git.HasStaged || !strings.Contains(model.Sessions[0].Pane.CapabilityNote, "Commit is unavailable") {
		t.Fatalf("pane=%+v", model.Sessions[0].Pane)
	}
	c.RequestReviewSource(testSession, "staged")
	eventually(t, func() bool { return model.Sessions[0].Pane.ReviewSource == "staged" })
	operationsMu.Lock()
	defer operationsMu.Unlock()
	for _, operation := range operations {
		if _, ok := operation.(protocol.WorkspaceCommit); ok {
			t.Fatal("unrevisioned staged commit was exposed")
		}
	}
}

func TestTypedWorkspaceRejectsStalePaneResult(t *testing.T) {
	model := NewModel([]Project{{ID: testProject, PathHint: "ssh://remote/root"}}, []Session{{ID: testSession, ProjectID: testProject}})
	c := NewController(model, testPost)
	defer c.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	transport := &fakeSurfaceTransport{fakeTransport: newFakeTransport(testProject)}
	transport.workspace = func(ctx context.Context, operation any) (any, error) {
		if _, ok := operation.(protocol.WorkspaceListTree); !ok {
			return nil, fmt.Errorf("unexpected %T", operation)
		}
		close(entered)
		select {
		case <-release:
			return &protocol.WorkspaceWorkingTreeResult{Type: "workingTree", Entries: []protocol.WorkingTreeEntryValue{{Name: "late", RelativePath: "late"}}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	c.transport, c.generation = transport, 1
	c.mu.Unlock()
	c.RequestPane(testSession, PaneFiles)
	<-entered
	c.RequestPane(testSession, PaneBrowser)
	close(release)
	eventually(t, func() bool { return !model.Sessions[0].Pane.Pending })
	if model.Sessions[0].Pane.Kind != PaneBrowser || len(model.Sessions[0].Pane.WorkspaceEntries) != 0 {
		t.Fatalf("stale workspace result replaced pane: %+v", model.Sessions[0].Pane)
	}
}

func TestPreparedRemoteTerminalCommitsAndClosesOnInvalidation(t *testing.T) {
	model := NewModel([]Project{{ID: testProject}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	conn := newPreparedTerminalConn()
	pane, err := terminalui.New(conn, terminalui.Options{NoBlink: true})
	if err != nil {
		t.Fatal(err)
	}
	c.Terminal = &fakePreparedTerminal{pane: pane}
	c.RequestPane(testSession, PaneTerminal)
	eventually(t, func() bool { return model.Sessions[0].Pane.Terminal == pane && !model.Sessions[0].Pane.Pending })
	if !strings.Contains(model.Sessions[0].Pane.CapabilityNote, "navigation are not available") {
		t.Fatalf("capability note=%q", model.Sessions[0].Pane.CapabilityNote)
	}
	c.RequestPane(testSession, PaneBrowser)
	eventually(t, func() bool {
		select {
		case <-conn.closed:
			return true
		default:
			return false
		}
	})
	c.Close()
}

func TestStalePreparedRemoteTerminalIsClosedAndCannotReplacePane(t *testing.T) {
	model := NewModel([]Project{{ID: testProject}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	defer c.Close()
	conn := newPreparedTerminalConn()
	pane, err := terminalui.New(conn, terminalui.Options{NoBlink: true})
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakePreparedTerminal{pane: pane, entered: make(chan struct{}), release: make(chan struct{})}
	c.Terminal = provider
	c.RequestPane(testSession, PaneTerminal)
	<-provider.entered
	c.RequestPane(testSession, PaneBrowser)
	close(provider.release)
	eventually(t, func() bool {
		select {
		case <-conn.closed:
			return true
		default:
			return false
		}
	})
	if model.Sessions[0].Pane.Kind != PaneBrowser || model.Sessions[0].Pane.Terminal != nil {
		t.Fatalf("stale terminal replaced current pane: %+v", model.Sessions[0].Pane)
	}
}

func TestPreparedRemoteTerminalClosesWithController(t *testing.T) {
	model := NewModel([]Project{{ID: testProject}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	conn := newPreparedTerminalConn()
	pane, err := terminalui.New(conn, terminalui.Options{NoBlink: true})
	if err != nil {
		t.Fatal(err)
	}
	c.Terminal = &fakePreparedTerminal{pane: pane}
	c.RequestPane(testSession, PaneTerminal)
	eventually(t, func() bool { return model.Sessions[0].Pane.Terminal == pane })
	c.Close()
	select {
	case <-conn.closed:
	default:
		t.Fatal("controller close did not close its prepared remote terminal")
	}
}

func TestTerminalWithoutPreparedRemoteConnectionIsExplicitlyUnavailable(t *testing.T) {
	model := NewModel([]Project{{ID: testProject}}, []Session{{ID: testSession, ProjectID: testProject, Status: SessionIdle}})
	c := NewController(model, testPost)
	defer c.Close()
	c.RequestPane(testSession, PaneTerminal)
	eventually(t, func() bool { return !model.Sessions[0].Pane.Pending })
	if !strings.Contains(model.Sessions[0].Pane.Summary, "not exposed by this daemon capability") || model.Sessions[0].Pane.Terminal != nil {
		t.Fatalf("terminal capability was overstated: %+v", model.Sessions[0].Pane)
	}
}
