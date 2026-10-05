package presentation

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func fixtureModel() *Model {
	return NewModel(
		[]Project{{ID: "p1", Name: "Waku"}, {ID: "p2", Name: "MyGo"}},
		[]Session{{ID: "s1", ProjectID: "p1", Title: "Port shell"}, {ID: "s2", ProjectID: "p2", Title: "List proof"}},
	)
}

func TestReplaceSummariesInvalidatesPaneWhenOpaqueProjectRootChanges(t *testing.T) {
	m := NewModel(
		[]Project{{ID: "project", PathHint: "ssh://remote/old/../opaque"}},
		[]Session{{ID: "session", ProjectID: "project", SelectedPane: PaneFiles, Pane: PaneState{Kind: PaneFiles, WorkspaceRoot: "ssh://remote/old/../opaque", WorkspaceEntries: []WorkspaceEntry{{Path: "secret.txt"}}}}},
	)
	m.ReplaceSummaries([]Project{{ID: "project", PathHint: "ssh://remote/new/../opaque"}}, []Session{{ID: "session", ProjectID: "project"}})
	session := m.SessionByID("session")
	if session.SelectedPane != PaneNone || session.Pane.Kind != PaneNone || session.Pane.WorkspaceRoot != "" || len(session.Pane.WorkspaceEntries) != 0 {
		t.Fatalf("obsolete remote root survived task refresh: %+v", session)
	}
}

func TestSessionStateSurvivesNavigation(t *testing.T) {
	m := fixtureModel()
	m.Sessions[0].Draft = "keep this"
	m.Sessions[0].SelectedPane = PaneTerminal
	m.Sessions[0].Pane = PaneState{Kind: PaneTerminal, Summary: "shell output", Generation: 7}
	if !m.SelectSession("s2") || !m.SelectSession("s1") {
		t.Fatal("could not navigate fixtures")
	}
	if got := m.CurrentSession().Draft; got != "keep this" {
		t.Fatalf("draft = %q", got)
	}
	if got := m.CurrentSession().SelectedPane; got != PaneTerminal {
		t.Fatalf("pane = %v", got)
	}
	if got := m.CurrentSession().Pane.Summary; got != "shell output" {
		t.Fatalf("pane content = %q", got)
	}
}

func TestStalePaneWorkCannotCommit(t *testing.T) {
	m := fixtureModel()
	old := m.BeginPaneWork(PaneFiles)
	newer := m.BeginPaneWork(PaneReview)
	if m.CommitPaneWork(old, "stale", nil) {
		t.Fatal("stale generation committed")
	}
	if !m.CommitPaneWork(newer, "3 files", errors.New("synthetic")) {
		t.Fatal("current generation rejected")
	}
	if m.CurrentSession().Pane.Summary != "3 files" || m.CurrentSession().Pane.Err != "synthetic" {
		t.Fatalf("pane = %#v", m.CurrentSession().Pane)
	}
}

func TestPaneWorkMayLandInItsOwningSessionWhileAway(t *testing.T) {
	m := fixtureModel()
	work := m.BeginPaneWork(PaneFiles)
	m.SelectSession("s2")
	if !m.CommitPaneWork(work, "prepared files", nil) {
		t.Fatal("valid background result for inactive session was rejected")
	}
	m.SelectSession("s1")
	if m.CurrentSession().Pane.Summary != "prepared files" || m.CurrentSession().Pane.Pending {
		t.Fatalf("restored pane = %#v", m.CurrentSession().Pane)
	}
}

func TestClosingAndReopeningPanePreservesPreparedContent(t *testing.T) {
	m := fixtureModel()
	m.SetPane(PaneTerminal)
	work := m.BeginPaneWork(PaneTerminal)
	if !m.CommitPaneWork(work, "preserved terminal", nil) {
		t.Fatal("could not prepare terminal")
	}
	m.SetPane(PaneTerminal)
	if m.RightPaneOpen {
		t.Fatal("pane did not close")
	}
	m.SetPane(PaneTerminal)
	if !m.RightPaneOpen || m.CurrentSession().Pane.Summary != "preserved terminal" {
		t.Fatalf("reopened pane = %#v", m.CurrentSession().Pane)
	}
}

func TestStreamingTailRequiresStableTailIdentity(t *testing.T) {
	m := fixtureModel()
	m.AppendTranscript("s1", TranscriptItem{ID: "answer", Streaming: true, Body: "a"})
	if !m.AppendStreamTail("s1", "answer", "b") {
		t.Fatal("tail did not expand")
	}
	if m.AppendStreamTail("s1", "wrong", "c") || m.CurrentSession().Transcript[0].Body != "ab" {
		t.Fatal("wrong identity changed tail")
	}
}

func TestComposerStateMachineLabels(t *testing.T) {
	s := &Session{Draft: "ship it", Status: SessionIdle}
	if !CanSend(s) || PrimaryAction(s) != "Send" {
		t.Fatal("idle composer is not sendable")
	}
	s.Status = SessionRunning
	if CanSend(s) || !CanStop(s) || PrimaryAction(s) != "Stop" {
		t.Fatal("running composer does not offer stop")
	}
	if _, ok := SubmitAction(s); ok {
		t.Fatal("running non-steerable session must not claim queue support")
	}
	s.CanSteer = true
	if kind, ok := SubmitAction(s); !ok || kind != CommandSteer {
		t.Fatalf("steerable submission = %v, %v", kind, ok)
	}
	s.Status = SessionFailed
	if PrimaryAction(s) != "Retry" {
		t.Fatal("failed composer does not offer retry")
	}
	s.Status = SessionStopping
	if PrimaryAction(s) != "Stopping…" {
		t.Fatalf("stopping primary action = %q", PrimaryAction(s))
	}
}

func TestStreamStagerCoalescesTextAndReasoningAtCadence(t *testing.T) {
	start := time.Unix(0, 0)
	s := NewStreamStager(start)
	if _, ready := s.Push(start, StreamDelta{Kind: StreamText, Data: "hello"}); ready {
		t.Fatal("text bypassed cadence")
	}
	if _, ready := s.Push(start.Add(50*time.Millisecond), StreamDelta{Kind: StreamReasoning, Data: "think"}); ready {
		t.Fatal("reasoning bypassed cadence")
	}
	batch, ready := s.DrainDue(start.Add(DefaultStreamInterval))
	if !ready || len(batch.Deltas) != 2 || batch.Bytes != 10 {
		t.Fatalf("batch = %#v, ready %v", batch, ready)
	}
}

func TestStreamStagerUrgentEventsDrainEarlierStreamInProviderOrder(t *testing.T) {
	for _, kind := range []StreamDeltaKind{StreamCancellation, StreamApproval} {
		start := time.Unix(0, 0)
		s := NewStreamStager(start)
		s.Push(start, StreamDelta{Kind: StreamText, Data: "text"})
		s.Push(start, StreamDelta{Kind: StreamReasoning, Data: "reason"})
		batch, ready := s.Push(start.Add(time.Millisecond), StreamDelta{Kind: kind, Data: "urgent"})
		if !ready || len(batch.Deltas) != 3 || batch.Deltas[0].Data != "text" || batch.Deltas[1].Data != "reason" || batch.Deltas[2].Kind != kind {
			t.Fatalf("urgent %v order = %#v, %v", kind, batch, ready)
		}
		if _, ready := s.DrainDue(start.Add(DefaultStreamInterval)); ready {
			t.Fatal("urgent drain retained already published data")
		}
	}
}

func TestStreamStagerBoundsCountAndBytes(t *testing.T) {
	start := time.Unix(0, 0)
	s := NewStreamStager(start)
	s.MaxCount, s.MaxBytes = 2, 8
	s.Push(start, StreamDelta{Kind: StreamText, Data: "1234"})
	batch, ready := s.Push(start, StreamDelta{Kind: StreamReasoning, Data: "5678"})
	if !ready || len(batch.Deltas) != 2 || batch.Bytes > s.MaxBytes {
		t.Fatalf("count batch = %#v, %v", batch, ready)
	}
	batch, ready = s.Push(start, StreamDelta{Kind: StreamText, Data: strings.Repeat("é", 20)})
	if !ready || batch.Bytes != 40 || batch.Deltas[0].Data != strings.Repeat("é", 20) {
		t.Fatalf("exact oversized UTF-8 batch = %#v, %v", batch, ready)
	}
	if _, ready := s.DrainDue(start.Add(DefaultStreamInterval)); ready {
		t.Fatal("oversized immediate delta was retained")
	}
}

func TestStreamStagerPreservesLargeUrgentPayloadExactly(t *testing.T) {
	start := time.Unix(0, 0)
	s := NewStreamStager(start)
	s.MaxBytes = 8
	s.Push(start, StreamDelta{Kind: StreamText, Data: "before"})
	content := strings.Repeat("approval payload ", 10_000)
	batch, ready := s.Push(start, StreamDelta{Kind: StreamApproval, Data: content})
	if !ready || len(batch.Deltas) != 2 || batch.Deltas[0].Data != "before" || batch.Deltas[1].Data != content || batch.Bytes != len("before")+len(content) {
		t.Fatalf("large urgent batch was not authoritative: deltas=%d bytes=%d ready=%v", len(batch.Deltas), batch.Bytes, ready)
	}
	if _, ready := s.DrainDue(start.Add(DefaultStreamInterval)); ready {
		t.Fatal("urgent payload was retained")
	}
}

func TestCommandTokensGateByKindAndRejectStaleCompletions(t *testing.T) {
	m := fixtureModel()
	s := m.CurrentSession()
	s.Status = SessionRunning
	steer, ok := m.BeginCommand(s.ID, CommandSteer, "later")
	if !ok || steer.Token == 0 || !CanStop(s) {
		t.Fatalf("steer=%#v ok=%v canStop=%v", steer, ok, CanStop(s))
	}
	if _, ok := m.BeginCommand(s.ID, CommandSteer, "duplicate"); ok {
		t.Fatal("duplicate steer was accepted")
	}
	stop, ok := m.BeginCommand(s.ID, CommandStop, "")
	if !ok || stop.Token == steer.Token {
		t.Fatalf("stop=%#v ok=%v", stop, ok)
	}
	s.Status, s.Activity = SessionStopping, "stopping"
	if _, ok := m.BeginCommand(s.ID, CommandStop, ""); ok {
		t.Fatal("duplicate stop was accepted")
	}
	if m.CompleteCommand(steer, SessionRunning, "steered") {
		t.Fatal("older steer completion overwrote newer stop state")
	}
	if s.Status != SessionStopping || s.Activity != "stopping" || !CommandPending(s, CommandStop) {
		t.Fatalf("older completion mutated stop state: %#v", s)
	}
	if !m.CompleteCommand(stop, SessionPaused, "stopped") {
		t.Fatal("current stop completion rejected")
	}

	first, ok := m.BeginCommand(s.ID, CommandSteer, "first")
	if !ok || !m.CompleteCommand(first, SessionPaused, "first done") {
		t.Fatal("first steer command failed")
	}
	second, ok := m.BeginCommand(s.ID, CommandSteer, "second")
	if !ok || m.CompleteCommand(first, SessionFailed, "stale") {
		t.Fatal("same-kind stale completion was accepted")
	}
	if !m.CompleteCommand(second, SessionPaused, "second done") {
		t.Fatal("latest same-kind completion rejected")
	}
	approve, ok := m.BeginCommand(s.ID, CommandApprove, "")
	if !ok {
		t.Fatal("approval command rejected")
	}
	if _, ok := m.BeginCommand(s.ID, CommandReject, ""); ok {
		t.Fatal("reject was accepted while approval resolution was pending")
	}
	if !m.CancelCommand(approve) {
		t.Fatal("approval rollback failed")
	}
}
