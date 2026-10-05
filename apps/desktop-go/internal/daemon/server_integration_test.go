package daemon_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/client"
	"github.com/roshankarande/waku/apps/desktop-go/internal/daemon"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
	"github.com/roshankarande/waku/apps/desktop-go/internal/providers"
	"github.com/roshankarande/waku/apps/desktop-go/internal/storage"
)

const (
	projectID         protocol.UUID = "00000000-0000-4000-8000-000000000101"
	sessionID         protocol.UUID = "00000000-0000-4000-8000-000000000102"
	runtimeID         protocol.UUID = "00000000-0000-4000-8000-000000000103"
	recoveryRuntimeID protocol.UUID = "00000000-0000-4000-8000-000000000104"
)

type fakeAdapter struct{ session *fakeSession }

func (a *fakeAdapter) Descriptor() providers.Descriptor {
	return providers.Descriptor{Provider: providers.ProviderCodex, Implemented: true, Capabilities: providers.Capabilities{Approvals: true, Steering: true, Cancellation: providers.CancellationProtocol, Rewind: true, Fork: true}}
}
func (a *fakeAdapter) Discover(context.Context, providers.StartOptions) (providers.Discovery, error) {
	return providers.Discovery{Available: true}, nil
}
func (a *fakeAdapter) Start(_ context.Context, _ providers.StartOptions, sink providers.Sink) (providers.Session, error) {
	a.session.mu.Lock()
	a.session.sink = sink
	a.session.mu.Unlock()
	sink.Emit(providers.Event{Kind: providers.EventConnected, Cursor: json.RawMessage(`{"provider":"codex","threadId":"thread-1"}`)})
	return a.session, nil
}

type fakeSession struct {
	mu                 sync.Mutex
	sink               providers.Sink
	permission         json.RawMessage
	question           json.RawMessage
	permissionFailures int
	canceled, closed   bool
}

func (s *fakeSession) Prompt(context.Context, string) error {
	s.mu.Lock()
	sink := s.sink
	s.mu.Unlock()
	sink.Emit(providers.Event{Kind: providers.EventTurnStarted})
	sink.Emit(providers.Event{Kind: providers.EventTextDelta, Text: "hello"})
	sink.Emit(providers.Event{Kind: providers.EventAvailableCommands, Raw: json.RawMessage(`[{"name":"review","description":"Review changes"},{"name":"test","description":"Run tests"}]`)})
	sink.Emit(providers.Event{Kind: providers.EventPermission, RequestID: json.RawMessage(`9007199254740993`), Permission: &providers.Permission{Title: "Command approval", Detail: "go test", Options: []providers.PermissionOption{{ID: "accept", Label: "Allow", Allow: true}}}})
	sink.Emit(providers.Event{Kind: providers.EventUserInputRequested, RequestID: json.RawMessage(`9007199254740993`), Questions: []providers.UserInputQuestion{{ID: "q", Header: "Choice", Question: "Continue?"}}})
	return nil
}
func (s *fakeSession) Steer(context.Context, string) error { return nil }
func (s *fakeSession) Cancel(context.Context) error {
	s.mu.Lock()
	s.canceled = true
	s.mu.Unlock()
	return nil
}
func (s *fakeSession) RespondPermission(_ context.Context, id json.RawMessage, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.permissionFailures > 0 {
		s.permissionFailures--
		return errors.New("synthetic permission failure")
	}
	s.permission = append(json.RawMessage(nil), id...)
	return nil
}
func (s *fakeSession) RespondQuestions(_ context.Context, id json.RawMessage, _ []providers.UserInputAnswer) error {
	s.mu.Lock()
	s.question = append(json.RawMessage(nil), id...)
	s.mu.Unlock()
	return nil
}
func (s *fakeSession) Goal(context.Context, providers.GoalOperation) error {
	return providers.ErrUnsupported
}
func (s *fakeSession) ApplyOptions(context.Context, providers.SessionOptions) (bool, error) {
	return true, nil
}
func (s *fakeSession) Rollback(context.Context, uint64) (json.RawMessage, error) {
	return json.RawMessage(`{"provider":"codex","threadId":"rewound"}`), nil
}
func (s *fakeSession) Fork(context.Context, uint64) (json.RawMessage, error) {
	return json.RawMessage(`{"provider":"codex","threadId":"forked"}`), nil
}
func (s *fakeSession) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }
func (s *fakeSession) emit(event providers.Event) {
	s.mu.Lock()
	sink := s.sink
	s.mu.Unlock()
	sink.Emit(event)
}

func startServer(t *testing.T, dataDir, token string, adapter providers.Adapter) (*daemon.Server, net.Listener) {
	t.Helper()
	server, err := daemon.NewServer(context.Background(), daemon.ServerConfig{DataDir: dataDir, DatabasePath: filepath.Join(dataDir, "waku.sqlite3"), Token: token, Adapter: adapter, DaemonVersion: "test"})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	return server, listener
}

func connect(t *testing.T, address, token string, resume []protocol.ReplayCursor) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, client.ConnectOptions{Address: address, Token: token, ClientID: "00000000-0000-4000-8000-000000000111", ResumeFrom: resume})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func request(t *testing.T, c *client.Client, session, runtime protocol.UUID, command any) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := c.Request(ctx, session, runtime, command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func nextEvent(t *testing.T, events <-chan protocol.SequencedEvent) protocol.SequencedEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
		return protocol.SequencedEvent{}
	}
}

func TestGoDaemonFakeAdapterClientLifecycleReplayPersistenceAndWriterOwnership(t *testing.T) {
	dataDir := t.TempDir()
	token := "test-secret"
	session := &fakeSession{}
	adapter := &fakeAdapter{session: session}
	server, listener := startServer(t, dataDir, token, adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// More failed authentications than the live-session limit must not consume
	// authenticated slots or prevent the following valid client.
	for attempt := 0; attempt < 140; attempt++ {
		if _, err := client.Connect(ctx, client.ConnectOptions{Address: listener.Addr().String(), Token: "wrong"}); err == nil {
			t.Fatal("wrong token connected")
		}
	}
	c := connect(t, listener.Addr().String(), token, nil)
	defer c.CloseTransport()
	// Start before the task-state row exists. The connected cursor must queue
	// and be flushed by the subsequent atomic state save, never be dropped.
	request(t, c, sessionID, runtimeID, protocol.Start(protocol.WireDriverStartOptions{Provider: "codex", Binary: "fake-codex", CWD: "/tmp/waku-go-fixture", Mode: "fullAccess"}))
	project := json.RawMessage(`{"id":"` + string(projectID) + `","name":"Fixture","path":"/tmp/waku-go-fixture","created_at":1}`)
	sessionRaw := json.RawMessage(`{"id":"` + string(sessionID) + `","title":"New task","project_id":"` + string(projectID) + `","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":1}`)
	request(t, c, protocol.NilUUID, protocol.NilUUID, protocol.SaveTaskStateCommand{Type: protocol.CommandSaveTaskState, Projects: []json.RawMessage{project}, Sessions: []json.RawMessage{sessionRaw}, LiveSessionIDs: []protocol.UUID{}})
	state := request(t, c, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandLoadTaskState)})
	if !json.Valid(state) || !containsJSON(state, string(sessionID)) {
		t.Fatalf("state=%s", state)
	}
	hydrated := request(t, c, protocol.NilUUID, protocol.NilUUID, protocol.HydrateSessionCommand{Type: protocol.CommandHydrateSession, SessionID: sessionID})
	if !containsJSON(hydrated, string(sessionID)) {
		t.Fatalf("hydrated=%s", hydrated)
	}
	events, stop := c.Subscribe(sessionID, runtimeID)
	defer stop()
	if event := nextEvent(t, events); event.Event.Kind != "connected" {
		t.Fatalf("first event=%s", event.Event.Kind)
	}
	request(t, c, sessionID, runtimeID, protocol.Prompt("hello", nil, nil))
	permissionID, questionID := "", ""
	for _, want := range []string{"turnStarted", "textDelta", "availableCommands", "permission", "userInputRequested"} {
		if event := nextEvent(t, events); event.Event.Kind != want {
			t.Fatalf("event=%s want=%s", event.Event.Kind, want)
		} else if want == "permission" {
			var payload protocol.PermissionPayload
			if err := protocol.Decode(event.Event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			permissionID = payload.RequestID
		} else if want == "userInputRequested" {
			var payload protocol.UserInputRequestedPayload
			if err := protocol.Decode(event.Event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			questionID = payload.RequestID
		} else if want == "availableCommands" {
			var payload []protocol.ReportedCommandPayload
			if err := protocol.Decode(event.Event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload) != 2 || payload[0].Name != "review" || payload[1].Name != "test" {
				t.Fatalf("commands=%+v", payload)
			}
		}
	}
	if permissionID == questionID || permissionID == "" || questionID == "" {
		t.Fatalf("pending request IDs were not kind-scoped: permission=%q question=%q", permissionID, questionID)
	}
	var cursors []protocol.ReplayCursor
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cursors = c.LastSequences()
		if len(cursors) == 1 && cursors[0].Sequence == 6 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(cursors) != 1 || cursors[0].Sequence != 6 {
		t.Fatalf("cursors=%v", cursors)
	}
	session.mu.Lock()
	session.permissionFailures = 1
	session.mu.Unlock()
	ctxRespond, cancelRespond := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := c.Request(ctxRespond, sessionID, runtimeID, protocol.RespondCommand{Type: protocol.CommandRespond, RequestID: permissionID, OptionID: "accept"})
	cancelRespond()
	if err == nil {
		t.Fatal("synthetic permission failure was not returned")
	}
	request(t, c, sessionID, runtimeID, protocol.RespondCommand{Type: protocol.CommandRespond, RequestID: permissionID, OptionID: "accept"})
	request(t, c, sessionID, runtimeID, protocol.RespondUserInputCommand{Type: protocol.CommandRespondUserInput, RequestID: questionID, Answers: []protocol.UserInputAnswer{{QuestionID: "q", Answers: []string{"yes"}}}})
	session.mu.Lock()
	if string(session.permission) != "9007199254740993" {
		t.Fatalf("permission id=%s", session.permission)
	}
	if string(session.question) != "9007199254740993" {
		t.Fatalf("question id=%s", session.question)
	}
	session.mu.Unlock()
	if raw := request(t, c, sessionID, runtimeID, protocol.TurnsCommand{Type: protocol.CommandRollback, Turns: 1}); !containsJSON(raw, "rewound") {
		t.Fatalf("rollback=%s", raw)
	}
	if raw := request(t, c, sessionID, runtimeID, protocol.ForkCommand{Type: protocol.CommandFork, TurnsToRemove: 1}); !containsJSON(raw, "forked") {
		t.Fatalf("fork=%s", raw)
	}
	c.CloseTransport()
	session.emit(providers.Event{Kind: providers.EventTextDelta, Text: "offline"})
	session.emit(providers.Event{Kind: providers.EventTurnFinished, Success: true})
	reconnected := connect(t, listener.Addr().String(), token, cursors)
	defer reconnected.CloseTransport()
	replayed, stopReplay := reconnected.Subscribe(sessionID, runtimeID)
	defer stopReplay()
	if event := nextEvent(t, replayed); event.Event.Kind != "textDelta" {
		t.Fatalf("replay=%s", event.Event.Kind)
	}
	if event := nextEvent(t, replayed); event.Event.Kind != "turnFinished" {
		t.Fatalf("replay=%s", event.Event.Kind)
	}
	request(t, reconnected, sessionID, runtimeID, protocol.Command{Type: string(protocol.CommandCancel)})
	session.mu.Lock()
	if !session.canceled {
		t.Fatal("cancel did not reach provider")
	}
	session.mu.Unlock()
	session.emit(providers.Event{Kind: providers.EventProcessExited})
	if event := nextEvent(t, replayed); event.Event.Kind != "processExited" {
		t.Fatalf("process exit=%s", event.Event.Kind)
	}
	attached := request(t, reconnected, sessionID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandAttachSession)})
	var attachment protocol.SessionRuntimePayload
	if err := protocol.Decode(attached, &attachment); err != nil {
		t.Fatal(err)
	}
	if attachment.RuntimeID != nil {
		t.Fatalf("exited runtime remained attached: %s", attached)
	}
	request(t, reconnected, sessionID, recoveryRuntimeID, protocol.Start(protocol.WireDriverStartOptions{Provider: "codex", Binary: "fake-codex", CWD: "/tmp/waku-go-fixture", Mode: "fullAccess"}))
	attachment = protocol.SessionRuntimePayload{}
	attachedRaw := request(t, reconnected, sessionID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandAttachSession)})
	if err := protocol.Decode(attachedRaw, &attachment); err != nil {
		t.Fatal(err)
	}
	if runtime := attachment.RuntimeID; runtime == nil || *runtime != recoveryRuntimeID {
		t.Fatalf("replacement runtime was not attached: %s", attachedRaw)
	}
	if _, err := daemon.NewServer(context.Background(), daemon.ServerConfig{DataDir: dataDir, Token: token, Adapter: &fakeAdapter{session: &fakeSession{}}}); !errors.Is(err, storage.ErrDataDirOwned) {
		t.Fatalf("second writer err=%v", err)
	}
	reconnected.CloseTransport()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	restarted, restartListener := startServer(t, dataDir, token, &fakeAdapter{session: &fakeSession{}})
	restored := connect(t, restartListener.Addr().String(), token, nil)
	defer restored.CloseTransport()
	state = request(t, restored, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandLoadTaskState)})
	if !containsJSON(state, "thread-1") {
		t.Fatalf("provider cursor was not persisted: %s", state)
	}
	_ = restarted
}

func TestDaemonHandshakeOriginAndReadiness(t *testing.T) {
	dataDir := t.TempDir()
	server, err := daemon.NewServer(context.Background(), daemon.ServerConfig{DataDir: dataDir, Token: "secret", AllowedOrigins: []string{"https://waku.example"}, Adapter: &fakeAdapter{session: &fakeSession{}}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	var readiness bytes.Buffer
	if err := daemon.WriteReadiness(&readiness, listener); err != nil {
		t.Fatal(err)
	}
	var ready protocol.DaemonReady
	if err := protocol.Decode(bytes.TrimSpace(readiness.Bytes()), &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Address != listener.Addr().String() || ready.PID == 0 || ready.ProtocolVersion != protocol.Version {
		t.Fatalf("ready=%+v", ready)
	}
	status := func(origin string) int {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		request := "GET /v1 HTTP/1.1\r\nHost: " + listener.Addr().String() + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nOrigin: " + origin + "\r\n\r\n"
		if _, err := conn.Write([]byte(request)); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	if got := status("https://evil.example"); got != http.StatusForbidden {
		t.Fatalf("bad origin status=%d", got)
	}
	if got := status("https://waku.example"); got != http.StatusSwitchingProtocols {
		t.Fatalf("allowed origin status=%d", got)
	}
}

func containsJSON(raw []byte, needle string) bool {
	return len(raw) > 0 && string(raw) != "" && contains(string(raw), needle)
}
func contains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
