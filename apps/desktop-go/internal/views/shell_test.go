package views

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/egoist/mygo/ui"
	codecomponent "github.com/roshankarande/waku/apps/desktop-go/internal/components/code"
	"github.com/roshankarande/waku/apps/desktop-go/internal/components/terminalui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/presentation"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

type fakeDispatcher struct{ commands []presentation.Command }

func (f *fakeDispatcher) Dispatch(c presentation.Command) bool {
	f.commands = append(f.commands, c)
	return true
}

type fakeSurfaceDispatcher struct {
	fakeDispatcher
	entries []struct {
		session, path, absolute string
		directory               bool
	}
	reviews []struct{ session, source string }
}

func (f *fakeSurfaceDispatcher) RequestWorkspaceEntry(session, path, absolute string, directory bool) {
	f.entries = append(f.entries, struct {
		session, path, absolute string
		directory               bool
	}{session, path, absolute, directory})
}
func (f *fakeSurfaceDispatcher) RequestReviewSource(session, source string) {
	f.reviews = append(f.reviews, struct{ session, source string }{session, source})
}

type fakeCompositionProbe bool

func (p fakeCompositionProbe) Composing() bool { return bool(p) }

type viewTerminalConn struct {
	closed chan struct{}
	once   sync.Once
}

func newViewTerminalConn() *viewTerminalConn            { return &viewTerminalConn{closed: make(chan struct{})} }
func (c *viewTerminalConn) Read([]byte) (int, error)    { <-c.closed; return 0, io.EOF }
func (c *viewTerminalConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *viewTerminalConn) Resize(int, int) error       { return nil }
func (c *viewTerminalConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestOptionalCompositionCapabilitySeam(t *testing.T) {
	if active, available := compositionState(struct{}{}); active || available {
		t.Fatalf("old framework fallback = %v, %v", active, available)
	}
	if active, available := compositionState(fakeCompositionProbe(true)); !active || !available {
		t.Fatalf("active composition = %v, %v", active, available)
	}
	if active, available := compositionState(fakeCompositionProbe(false)); active || !available {
		t.Fatalf("inactive composition = %v, %v", active, available)
	}
}

func testShell(rows int) (*Shell, *fakeDispatcher) {
	items := make([]presentation.TranscriptItem, rows)
	for i := range items {
		items[i] = presentation.TranscriptItem{ID: fmt.Sprintf("event-%05d", i), Role: presentation.RoleAssistant, Body: fmt.Sprintf("Answer row %d", i)}
	}
	m := presentation.NewModel(
		[]presentation.Project{{ID: "waku", Name: "Waku", PathHint: "/synthetic/waku"}, {ID: "mygo", Name: "MyGo"}},
		[]presentation.Session{
			{ID: "port", ProjectID: "waku", Title: "Port desktop", Activity: "Rendering native shell", Transcript: items},
			{ID: "proof", ProjectID: "mygo", Title: "List proof", Draft: "persistent draft"},
		},
	)
	f := &fakeDispatcher{}
	return NewShell(m, f), f
}

func TestShellShowsProjectSessionActivityHierarchy(t *testing.T) {
	s, _ := testShell(3)
	tt := ui.NewTester(s.View, 1200, 800)
	for _, text := range []string{"Waku", "Port desktop", "Current activity: Rendering native shell", "Projects and sessions", "Transcript", "Message"} {
		if _, ok := tt.Find(text); !ok {
			t.Fatalf("missing %q in texts %q", text, tt.Texts())
		}
	}
}

func TestTranscriptTenThousandRowsIsVirtualizedAndFollowsGrowingTail(t *testing.T) {
	s, _ := testShell(10_000)
	tt := ui.NewTester(s.View, 1000, 720)
	if s.rowsBuilt >= 200 {
		t.Fatalf("built %d of 10,000 rows", s.rowsBuilt)
	}
	state := s.transcripts["port"]
	if state == nil || !state.FollowEnd || !state.AtEnd() {
		t.Fatalf("list did not begin following end: %#v", state)
	}
	last := &s.Model.CurrentSession().Transcript[9_999]
	last.Streaming = true
	if !s.Model.AppendStreamTail("port", last.ID, " — streamed tail expansion") {
		t.Fatal("could not expand stable tail")
	}
	tt.Frame()
	if !state.AtEnd() {
		t.Fatal("growing streamed tail lost FollowEnd")
	}
	if s.rowsBuilt >= 200 {
		t.Fatalf("tail frame built %d rows", s.rowsBuilt)
	}
}

func TestTranscriptDoesNotDragReaderToTailAfterScrollingAway(t *testing.T) {
	s, _ := testShell(400)
	tt := ui.NewTester(s.View, 1000, 720)
	state := s.transcripts["port"]
	r, ok := tt.Find("Answer row 399")
	if !ok {
		t.Fatalf("visible transcript tail not found in %q", tt.Texts())
	}
	tt.Scroll(r.X+r.W/2, r.Y+r.H/2, 0, -300)
	if state.AtEnd() {
		first, last := state.Visible()
		t.Fatalf("scrolling away did not release FollowEnd: bounds=%#v visible=%d..%d", r, first, last)
	}
	s.Model.AppendTranscript("port", presentation.TranscriptItem{ID: "new-tail", Role: presentation.RoleAssistant, Body: "new tail"})
	tt.Frame()
	if state.AtEnd() {
		t.Fatal("new content dragged a reader back to the tail")
	}
}

func TestComposerKeyboardIMEAndStateActions(t *testing.T) {
	s, f := testShell(1)
	s.Model.CurrentSession().Draft = "replace this surrounding text"
	tt := ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	tt.Command("selectAll")
	tt.Compose("こんにちは", 5)
	if caret, ok := tt.TextCaret(); !ok || caret.H == 0 {
		t.Fatalf("missing IME caret: %#v, %v", caret, ok)
	}
	tt.Type("こんにちは build it")
	if s.CompositionGuardAvailable() {
		tt.Key(ui.Cmd, ui.KeyEnter)
	} else if err := tt.Click("Send message"); err != nil {
		t.Fatal(err)
	}
	if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandSend || f.commands[0].Text != "こんにちは build it" {
		t.Fatalf("commands = %#v", f.commands)
	}
	if s.Model.CurrentSession().Status != presentation.SessionPreparing || s.Model.CurrentSession().Draft != "" {
		t.Fatal("accepted send did not enter preparing and clear draft")
	}
	// The client acknowledges command acceptance before stop becomes available.
	s.Model.CurrentSession().CommandBusy = false
	s.Model.CurrentSession().Status = presentation.SessionRunning
	tt.Frame()
	if s.CompositionGuardAvailable() {
		tt.Key(0, ui.KeyEscape)
	} else if err := tt.Click("Stop message"); err != nil {
		t.Fatal(err)
	}
	if len(f.commands) != 2 || f.commands[1].Kind != presentation.CommandStop {
		t.Fatalf("Escape did not stop: %#v", f.commands)
	}
}

func TestActiveIMECompositionSuppressesRootComposerShortcuts(t *testing.T) {
	t.Run("send", func(t *testing.T) {
		s, f := testShell(1)
		s.Model.CurrentSession().Draft = "committed"
		tt := ui.NewTester(s.View, 1000, 720)
		if !s.CompositionGuardAvailable() {
			t.Skip("pinned MyGo does not expose Element.Composing")
		}
		if err := tt.Click("Message"); err != nil {
			t.Fatal(err)
		}
		tt.Compose("仮", 1)
		tt.Key(ui.Cmd, ui.KeyEnter)
		if len(f.commands) != 0 || s.Model.CurrentSession().Draft != "committed" {
			t.Fatalf("composition dispatched send: commands=%#v draft=%q", f.commands, s.Model.CurrentSession().Draft)
		}
		tt.Type("確定")
		tt.Key(ui.Cmd, ui.KeyEnter)
		if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandSend {
			t.Fatalf("committed text did not send: %#v", f.commands)
		}
	})

	t.Run("stop", func(t *testing.T) {
		s, f := testShell(1)
		s.Model.CurrentSession().Status = presentation.SessionRunning
		tt := ui.NewTester(s.View, 1000, 720)
		if !s.CompositionGuardAvailable() {
			t.Skip("pinned MyGo does not expose Element.Composing")
		}
		if err := tt.Click("Message"); err != nil {
			t.Fatal(err)
		}
		tt.Compose("仮", 1)
		tt.Key(0, ui.KeyEscape)
		if len(f.commands) != 0 || s.Model.CurrentSession().Status != presentation.SessionRunning {
			t.Fatalf("composition dispatched stop: commands=%#v status=%v", f.commands, s.Model.CurrentSession().Status)
		}
		tt.Compose("", 0)
		tt.Key(0, ui.KeyEscape)
		if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandStop {
			t.Fatalf("post-composition Escape did not stop: %#v", f.commands)
		}
	})
}

func TestMissingCompositionCapabilitySafelyGatesRootShortcuts(t *testing.T) {
	s, f := testShell(1)
	session := s.Model.CurrentSession()
	session.Draft = "do not dispatch"
	tt := ui.NewTester(s.View, 1000, 720)
	if s.CompositionGuardAvailable() {
		t.Skip("MyGo provides the composition guard")
	}
	if _, ok := tt.Find("Composer shortcut capability: Keyboard send/stop shortcuts unavailable: this MyGo revision cannot report active IME composition. Use the action buttons."); !ok {
		t.Fatalf("missing explicit shortcut capability gate: %q", tt.Texts())
	}
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	tt.Key(ui.Cmd, ui.KeyEnter)
	session.Status = presentation.SessionRunning
	tt.Frame()
	tt.Key(0, ui.KeyEscape)
	if len(f.commands) != 0 || session.Draft != "do not dispatch" || session.Status != presentation.SessionRunning {
		t.Fatalf("gated shortcuts dispatched: commands=%#v session=%#v", f.commands, session)
	}
}

func TestComposerShortcutSteersWithoutTriggeringPrimaryStop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status presentation.SessionStatus
		steer  bool
		want   presentation.CommandKind
	}{{"running steers", presentation.SessionRunning, true, presentation.CommandSteer}} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := testShell(1)
			session := s.Model.CurrentSession()
			session.Status, session.CanSteer, session.Draft = tc.status, tc.steer, "follow up"
			tt := ui.NewTester(s.View, 1000, 720)
			if !s.CompositionGuardAvailable() {
				t.Skip("pinned MyGo safely gates root shortcuts without Element.Composing")
			}
			if err := tt.Click("Message"); err != nil {
				t.Fatal(err)
			}
			tt.Key(ui.Cmd, ui.KeyEnter)
			if len(f.commands) != 1 || f.commands[0].Kind != tc.want || f.commands[0].Text != "follow up" {
				t.Fatalf("shortcut commands = %#v", f.commands)
			}
			for _, command := range f.commands {
				if command.Kind == presentation.CommandStop {
					t.Fatal("submission shortcut triggered primary Stop")
				}
			}
		})
	}
}

func TestStopRemainsAvailableWhileSteerAwaitsAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		steer bool
		kind  presentation.CommandKind
	}{{"steer", true, presentation.CommandSteer}} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := testShell(1)
			session := s.Model.CurrentSession()
			session.Status, session.CanSteer, session.Draft = presentation.SessionRunning, tc.steer, "follow up"
			tt := ui.NewTester(s.View, 1000, 720)
			if err := tt.Click("Message"); err != nil {
				t.Fatal(err)
			}
			s.submitDraft()
			tt.Frame()
			if len(f.commands) != 1 || f.commands[0].Kind != tc.kind || !presentation.CanStop(session) {
				t.Fatalf("submission=%#v canStop=%v", f.commands, presentation.CanStop(session))
			}
			if _, ok := tt.Find("Stop message"); !ok {
				t.Fatalf("Stop disappeared during acknowledgement: %q", tt.Texts())
			}
			if err := tt.Click("Stop message"); err != nil {
				t.Fatal(err)
			}
			if len(f.commands) != 2 || f.commands[1].Kind != presentation.CommandStop {
				t.Fatalf("stop command = %#v", f.commands)
			}
		})
	}
}

func TestStopRemainsAvailableDuringApprovalResolutionAndRetryStartup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status presentation.SessionStatus
		label  string
		first  presentation.CommandKind
	}{{"approval resolution", presentation.SessionWaitingApproval, "Approve", presentation.CommandApprove}, {"retry startup", presentation.SessionFailed, "Retry message", presentation.CommandRetry}} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := testShell(1)
			session := s.Model.CurrentSession()
			session.Status, session.Draft = tc.status, "retry"
			tt := ui.NewTester(s.View, 1000, 720)
			if err := tt.Click(tc.label); err != nil {
				t.Fatal(err)
			}
			if len(f.commands) != 1 || f.commands[0].Kind != tc.first || !presentation.CanStop(session) {
				t.Fatalf("startup=%#v status=%v canStop=%v", f.commands, session.Status, presentation.CanStop(session))
			}
			if _, ok := tt.Find("Stop message"); !ok {
				t.Fatalf("Stop unavailable during %v: %q", session.Status, tt.Texts())
			}
			if err := tt.Click("Stop message"); err != nil {
				t.Fatal(err)
			}
			if len(f.commands) != 2 || f.commands[1].Kind != presentation.CommandStop {
				t.Fatalf("stop command = %#v", f.commands)
			}
		})
	}
}

func TestPrimaryStopRetryAndApprovalRejectDuplicateActivation(t *testing.T) {
	tests := []struct {
		name                string
		status              presentation.SessionStatus
		label               string
		want                presentation.CommandKind
		phase               presentation.SessionStatus
		allowsImmediateStop bool
	}{{"stop", presentation.SessionRunning, "Stop message", presentation.CommandStop, presentation.SessionStopping, false}, {"retry", presentation.SessionFailed, "Retry message", presentation.CommandRetry, presentation.SessionStopping, true}, {"approve", presentation.SessionWaitingApproval, "Approve", presentation.CommandApprove, presentation.SessionResolvingApproval, false}, {"reject", presentation.SessionWaitingApproval, "Reject", presentation.CommandReject, presentation.SessionResolvingApproval, false}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, f := testShell(1)
			session := s.Model.CurrentSession()
			session.Status = tc.status
			session.Draft = "retry body"
			tt := ui.NewTester(s.View, 1000, 720)
			r, ok := tt.Find(tc.label)
			if !ok {
				t.Fatalf("missing %q in %q", tc.label, tt.Texts())
			}
			tt.ClickAt(r.X+r.W/2, r.Y+r.H/2)
			tt.ClickAt(r.X+r.W/2, r.Y+r.H/2)
			wantCount := 1
			if tc.allowsImmediateStop {
				wantCount = 2
			}
			if len(f.commands) != wantCount || f.commands[0].Kind != tc.want || (tc.allowsImmediateStop && f.commands[1].Kind != presentation.CommandStop) {
				t.Fatalf("double activation commands = %#v", f.commands)
			}
			if !session.CommandBusy {
				t.Fatal("accepted command did not enter in-flight state")
			}
			if session.Status != tc.phase {
				t.Fatalf("in-flight phase = %v, want %v", session.Status, tc.phase)
			}
		})
	}
}

func TestStoppingPrimaryActionIsHonestAndDisabled(t *testing.T) {
	s, f := testShell(1)
	s.Model.CurrentSession().Status = presentation.SessionRunning
	tt := ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Stop message"); err != nil {
		t.Fatal(err)
	}
	r, ok := tt.Find("Stopping… message")
	if !ok {
		t.Fatalf("missing stopping transition: %q", tt.Texts())
	}
	tt.ClickAt(r.X+r.W/2, r.Y+r.H/2)
	if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandStop {
		t.Fatalf("disabled stopping state dispatched: %#v", f.commands)
	}
}

func TestKeyboardFocusCanOperateComposerAndNavigation(t *testing.T) {
	s, f := testShell(1)
	s.Model.CurrentSession().Draft = "keyboard request"
	tt := ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	if !tt.Focused("Message") {
		t.Fatal("composer did not take keyboard focus")
	}
	tt.Key(0, ui.KeyTab)
	if !tt.Focused("Send message") {
		t.Fatal("Tab did not reach the primary action")
	}
	tt.Key(0, ui.KeySpace)
	if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandSend {
		t.Fatalf("keyboard primary action = %#v", f.commands)
	}

	s, _ = testShell(1)
	tt = ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Port desktop"); err != nil {
		t.Fatal(err)
	}
	tt.Key(0, ui.KeyDown)
	if s.Model.SelectedSession != "proof" {
		t.Fatalf("keyboard navigation selected %q", s.Model.SelectedSession)
	}
}

func TestNavigationPreservesDraftPaneAndTranscriptState(t *testing.T) {
	s, _ := testShell(30)
	tt := ui.NewTester(s.View, 1100, 760)
	s.Model.CurrentSession().Draft = "keep me"
	if err := tt.Click("Terminal"); err != nil {
		t.Fatal(err)
	}
	work := s.Model.BeginPaneWork(presentation.PaneTerminal)
	if !s.Model.CommitPaneWork(work, "terminal session state", nil) {
		t.Fatal("could not prepare pane")
	}
	firstList := s.transcripts["port"]
	if err := tt.Click("List proof"); err != nil {
		t.Fatal(err)
	}
	if err := tt.Click("Port desktop"); err != nil {
		t.Fatal(err)
	}
	if s.Model.CurrentSession().Draft != "keep me" || s.Model.CurrentSession().SelectedPane != presentation.PaneTerminal {
		t.Fatalf("restored session = %#v", s.Model.CurrentSession())
	}
	if s.Model.CurrentSession().Pane.Summary != "terminal session state" {
		t.Fatalf("pane content was not restored: %#v", s.Model.CurrentSession().Pane)
	}
	if s.transcripts["port"] != firstList {
		t.Fatal("per-session transcript scroll state was replaced")
	}
}

func TestAccessibilityPreferencesAndApprovalControls(t *testing.T) {
	s, f := testShell(1)
	s.Model.CurrentSession().Status = presentation.SessionWaitingApproval
	tt := ui.NewTester(s.View, 1000, 720)
	tt.SetPreferences(ui.Preferences{ReduceMotion: true, HighContrast: true, TextScale: 1.25})
	for _, label := range []string{"Session status: Approval needed", "Approve", "Reject", "Projects and sessions"} {
		if _, ok := tt.Find(label); !ok {
			t.Fatalf("accessible control %q not found", label)
		}
	}
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	tt.Key(ui.Shift, ui.KeyTab)
	if !tt.Focused("Approve") {
		t.Fatalf("Shift+Tab did not focus Approve: %q", tt.Texts())
	}
	tt.Key(0, ui.KeySpace)
	if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandApprove {
		t.Fatalf("approval commands = %#v", f.commands)
	}
	if _, ok := tt.Find("Session status: Resolving approval"); !ok {
		t.Fatalf("accessible in-flight state missing: %q", tt.Texts())
	}
	tt.Frame()
	if !tt.Focused("Message") {
		t.Fatal("focus was not restored to the composer after approval controls disappeared")
	}
	tt.Type("follow up")
	if s.Model.CurrentSession().Draft != "follow up" {
		t.Fatalf("restored composer focus did not accept keyboard input: %q", s.Model.CurrentSession().Draft)
	}
	if err := tt.Click("Files"); err != nil {
		t.Fatal(err)
	}
	tt.Frame()
	if !tt.Focused("Files") || tt.Focused("Message") {
		t.Fatal("one-shot composer restoration stole focus on a later frame")
	}
}

func TestClosingRightPanelRestoresOriginatingTriggerFocusOnce(t *testing.T) {
	s, _ := testShell(1)
	tt := ui.NewTester(s.View, 1100, 760)
	if err := tt.Click("Terminal"); err != nil {
		t.Fatal(err)
	}
	if err := tt.Click("Close panel"); err != nil {
		t.Fatal(err)
	}
	tt.Frame()
	if !tt.Focused("Terminal") {
		t.Fatal("closing the panel did not restore its trigger focus")
	}
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	tt.Frame()
	if !tt.Focused("Message") || tt.Focused("Terminal") {
		t.Fatal("panel focus restoration repeated after its one-shot frame")
	}
}

func TestDeferredFocusDoesNotCrossSessions(t *testing.T) {
	t.Run("approval", func(t *testing.T) {
		s, _ := testShell(1)
		s.Model.CurrentSession().Status = presentation.SessionWaitingApproval
		tt := ui.NewTester(s.View, 1100, 760)
		if err := tt.Click("Approve"); err != nil {
			t.Fatal(err)
		}
		if err := tt.Click("List proof"); err != nil {
			t.Fatal(err)
		}
		tt.Frame()
		if !tt.Focused("List proof") {
			t.Fatal("approval focus restoration crossed into another session")
		}
		if err := tt.Click("Port desktop"); err != nil {
			t.Fatal(err)
		}
		tt.Frame()
		if !tt.Focused("Port desktop") {
			t.Fatal("mismatched approval focus request was retained")
		}
	})

	t.Run("panel", func(t *testing.T) {
		s, _ := testShell(1)
		tt := ui.NewTester(s.View, 1100, 760)
		if err := tt.Click("Terminal"); err != nil {
			t.Fatal(err)
		}
		if err := tt.Click("Close panel"); err != nil {
			t.Fatal(err)
		}
		if err := tt.Click("List proof"); err != nil {
			t.Fatal(err)
		}
		tt.Frame()
		if !tt.Focused("List proof") {
			t.Fatal("panel focus restoration crossed into another session")
		}
		if err := tt.Click("Port desktop"); err != nil {
			t.Fatal(err)
		}
		tt.Frame()
		if !tt.Focused("Port desktop") {
			t.Fatal("mismatched panel focus request was retained")
		}
	})
}

func TestPaneGenerationRejectsBackgroundResultAfterNavigation(t *testing.T) {
	s, _ := testShell(1)
	old := s.Model.BeginPaneWork(presentation.PaneFiles)
	current := s.Model.BeginPaneWork(presentation.PaneReview)
	if s.Model.CommitPaneWork(old, "old session files", nil) {
		t.Fatal("obsolete pane result committed")
	}
	if !s.Model.CommitPaneWork(current, "Synthetic review is ready", nil) {
		t.Fatal("current pane result rejected")
	}
	tt := ui.NewTester(s.View, 1100, 760)
	s.Model.RightPaneOpen = true
	s.Model.CurrentSession().SelectedPane = presentation.PaneReview
	tt.Frame()
	if !tt.HasText("Synthetic review is ready") {
		t.Fatalf("pane summary missing: %q", tt.Texts())
	}
}

func TestPaneLoadingAndErrorsRemainIsolatedBySession(t *testing.T) {
	s, _ := testShell(1)
	s.Model.CurrentSession().SelectedPane = presentation.PaneFiles
	s.Model.RightPaneOpen = true
	first := s.Model.BeginPaneWork(presentation.PaneFiles)
	s.Model.SelectSession("proof")
	s.Model.CurrentSession().SelectedPane = presentation.PaneReview
	s.Model.RightPaneOpen = true
	second := s.Model.BeginPaneWork(presentation.PaneReview)
	if !s.Model.CommitPaneWork(second, "", fmt.Errorf("synthetic review failure")) {
		t.Fatal("could not commit second pane error")
	}
	tt := ui.NewTester(s.View, 1100, 760)
	if !tt.HasText("synthetic review failure") {
		t.Fatalf("second pane error missing: %q", tt.Texts())
	}
	s.Model.SelectSession("port")
	s.Model.RightPaneOpen = true
	tt.Frame()
	if !tt.HasText("Preparing files…") || tt.HasText("synthetic review failure") {
		t.Fatalf("first pane loading state was contaminated: %q", tt.Texts())
	}
	if !s.Model.CommitPaneWork(first, "files ready", nil) {
		t.Fatal("inactive-session work generation was lost")
	}
	tt.Frame()
	if !tt.HasText("files ready") {
		t.Fatalf("first pane completion missing: %q", tt.Texts())
	}
}

func TestLargeSessionHierarchyIsVirtualized(t *testing.T) {
	projects := []presentation.Project{{ID: "p", Name: "Many sessions"}}
	sessions := make([]presentation.Session, 10_000)
	for i := range sessions {
		sessions[i] = presentation.Session{ID: fmt.Sprintf("s-%05d", i), ProjectID: "p", Title: fmt.Sprintf("Session %05d", i)}
	}
	s := NewShell(presentation.NewModel(projects, sessions), &fakeDispatcher{})
	ui.NewTester(s.View, 1000, 720)
	if s.navigationRowsBuilt >= 200 {
		t.Fatalf("built %d of 10,001 navigation rows", s.navigationRowsBuilt)
	}
}

func TestManyProjectNavigationIndexesSelectedLastSession(t *testing.T) {
	const count = 5_000
	projects := make([]presentation.Project, count)
	sessions := make([]presentation.Session, count)
	for i := range projects {
		projects[i] = presentation.Project{ID: fmt.Sprintf("p-%05d", i), Name: fmt.Sprintf("Project %05d", i)}
		sessions[i] = presentation.Session{ID: fmt.Sprintf("s-%05d", i), ProjectID: projects[i].ID, Title: fmt.Sprintf("Session %05d", i)}
	}
	model := presentation.NewModel(projects, sessions)
	if !model.SelectSession(sessions[count-1].ID) {
		t.Fatal("could not select last session")
	}
	s := NewShell(model, &fakeDispatcher{})
	if s.navSelected != len(s.navRows)-1 || len(s.navRows) != count*2 {
		t.Fatalf("navigation selected=%d rows=%d", s.navSelected, len(s.navRows))
	}
	ui.NewTester(s.View, 1000, 720)
	if s.navigationRowsBuilt >= 200 {
		t.Fatalf("built %d navigation rows", s.navigationRowsBuilt)
	}
}

func TestProgrammaticSessionSelectionSynchronizesNavigationInConstantTime(t *testing.T) {
	const count = 10_000
	projects := []presentation.Project{{ID: "p", Name: "Project"}}
	sessions := make([]presentation.Session, count)
	for i := range sessions {
		sessions[i] = presentation.Session{ID: fmt.Sprintf("s-%05d", i), ProjectID: "p", Title: fmt.Sprintf("Session %05d", i)}
	}
	model := presentation.NewModel(projects, sessions)
	s := NewShell(model, &fakeDispatcher{})
	tt := ui.NewTester(s.View, 1000, 720)
	lastID := sessions[count-1].ID
	if !model.SelectSession(lastID) {
		t.Fatal("programmatic selection failed")
	}
	tt.Frame()
	want, ok := s.navRowBySession[lastID]
	if !ok || s.navSelected != want {
		t.Fatalf("navigation selected=%d want=%d indexed=%v", s.navSelected, want, ok)
	}
	if !tt.HasText("Session 09999") {
		t.Fatalf("programmatic selection was not scrolled into view: %q", tt.Texts())
	}
}

type preparedRenderer struct{ calls int }

func (r *preparedRenderer) RenderPrepared(c *ui.Context, item presentation.TranscriptItem) bool {
	r.calls++
	ui.Text(c, "Prepared document "+item.DocumentKey).Selectable()
	return true
}

func TestPreparedDocumentSeamAndBoundedFallback(t *testing.T) {
	s, _ := testShell(0)
	renderer := &preparedRenderer{}
	s.Documents = renderer
	s.Model.CurrentSession().Transcript = []presentation.TranscriptItem{
		{ID: "prepared", DocumentKey: "doc-1", Body: strings.Repeat("unrendered", 10_000)},
		{ID: "fallback", Role: presentation.RoleReasoning, Streaming: true, Body: strings.Repeat("é", maxLiveFallbackBytes)},
	}
	tt := ui.NewTester(s.View, 1000, 720)
	if renderer.calls == 0 || !tt.HasText("Prepared document doc-1") {
		t.Fatalf("prepared renderer was not used: calls=%d texts=%q", renderer.calls, tt.Texts())
	}
	if _, ok := tt.Find("Document fallback is truncated"); !ok {
		t.Fatalf("bounded fallback was not disclosed: %q", tt.Texts())
	}
	body, clipped := boundedDocumentFallback(s.Model.CurrentSession().Transcript[1])
	if !clipped || len(body) > maxLiveFallbackBytes+128 || !strings.HasSuffix(body, "éééé") {
		t.Fatalf("fallback len=%d clipped=%v", len(body), clipped)
	}
}

func BenchmarkTranscriptTenThousandHeadlessFrame(b *testing.B) {
	s, _ := testShell(10_000)
	tt := ui.NewTester(s.View, 1200, 800)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tt.Frame()
	}
}

func TestCommandPaletteKeyboardFocusAndUnavailableCapability(t *testing.T) {
	s, f := testShell(3)
	s.Model.CurrentSession().Status = presentation.SessionRunning
	tt := ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Message"); err != nil {
		t.Fatal(err)
	}
	tt.Key(ui.Cmd, ui.KeyK)
	tt.Frame()
	if _, ok := tt.Find("Search command palette"); !ok {
		t.Fatalf("palette not opened: %q", tt.Texts())
	}
	if err := tt.Click("Open settings"); err != nil {
		t.Fatal(err)
	}
	tt.Frame()
	if !tt.Focused("Message") {
		t.Fatal("mouse palette activation did not restore composer focus")
	}
	if !s.Model.HubOpen || s.Model.Hub.Kind != presentation.HubSettings || !strings.Contains(s.Model.Hub.Unavailable, "does not expose") {
		t.Fatalf("hub=%+v", s.Model.Hub)
	}
	if len(f.commands) != 0 {
		t.Fatalf("palette dispatched session command: %+v", f.commands)
	}
	s.paletteOpen = true
	s.paletteFocus = true
	tt.Frame()
	if !tt.Focused("Search command palette") {
		t.Fatal("palette search did not receive focus")
	}
	tt.Key(0, ui.KeyEscape)
	tt.Frame()
	if s.paletteOpen || len(f.commands) != 0 || s.Model.CurrentSession().Status != presentation.SessionRunning {
		t.Fatal("Escape did not close palette safely")
	}
	if !tt.Focused("Message") {
		t.Fatal("Escape palette close did not restore composer focus")
	}
	tt.Key(ui.Cmd, ui.KeyK)
	if !s.paletteOpen {
		t.Fatal("Cmd-K did not reopen palette")
	}
	tt.Key(ui.Cmd, ui.KeyK)
	tt.Frame()
	if s.paletteOpen || !tt.Focused("Message") {
		t.Fatal("Cmd-K close did not restore composer focus")
	}
}

func TestHubRowsRemainVirtualized(t *testing.T) {
	s, _ := testShell(1)
	rows := make([]presentation.HubRow, 10_000)
	for i := range rows {
		rows[i] = presentation.HubRow{Key: fmt.Sprint(i), Title: "Capability", Detail: "Prepared detail"}
	}
	s.Model.HubOpen = true
	s.Model.Hub = presentation.HubState{Kind: presentation.HubSkills, Rows: rows}
	tt := ui.NewTester(s.View, 1200, 800)
	tt.Frame()
	if len(tt.Texts()) > 500 {
		t.Fatalf("hub eagerly built too many rows: %d", len(tt.Texts()))
	}
}

func TestPreparedRemoteTerminalPaneExposesHonestSearchCapability(t *testing.T) {
	s, _ := testShell(0)
	conn := newViewTerminalConn()
	pane, err := terminalui.New(conn, terminalui.Options{NoBlink: true})
	if err != nil {
		t.Fatal(err)
	}
	defer pane.Close()
	session := s.Model.CurrentSession()
	session.SelectedPane = presentation.PaneTerminal
	session.Pane = presentation.PaneState{
		Kind: presentation.PaneTerminal, Title: "Terminal", Terminal: pane,
		CapabilityNote: "Remote daemon terminal. Search reports match counts; match highlighting and next/previous navigation are not available in this MyGo terminal revision.",
	}
	s.Model.RightPaneOpen = true
	tt := ui.NewTester(s.View, 1100, 760)
	note := "Remote daemon terminal. Search reports match counts; match highlighting and next/previous navigation are not available in this MyGo terminal revision."
	for _, label := range []string{"Terminal capability: " + note, "Search terminal"} {
		if _, ok := tt.Find(label); !ok {
			t.Fatalf("missing %q in %q", label, tt.Texts())
		}
	}
}

func TestTypedSettingsFilesAndReviewControlsDispatchOpaqueIntents(t *testing.T) {
	s, _ := testShell(0)
	dispatcher := &fakeSurfaceDispatcher{}
	s.Dispatcher = dispatcher
	s.Model.HubOpen = true
	s.Model.Hub = presentation.HubState{Kind: presentation.HubSettings, Settings: &protocol.DaemonSettingsValue{ComputerUseEnabled: true}, Rows: []presentation.HubRow{{Key: "settings-write", Title: "Settings editing unavailable", Detail: "No revision precondition", Status: "Read-only"}}}
	tt := ui.NewTester(s.View, 1200, 800)
	if _, ok := tt.Find("Disable computer use in daemon settings"); ok {
		t.Fatal("unrevisioned settings mutation was exposed")
	}
	s.Model.HubOpen = false
	session := s.Model.CurrentSession()
	session.SelectedPane = presentation.PaneFiles
	session.Pane = presentation.PaneState{Kind: presentation.PaneFiles, WorkspaceRoot: "ssh://remote/root/../opaque", WorkspaceEntries: []presentation.WorkspaceEntry{{Path: "src/main.go", AbsolutePath: "ssh://remote/root/src/main.go", Name: "main.go"}}, CapabilityNote: "Remote files; compare-and-swap save unavailable."}
	s.Model.RightPaneOpen = true
	tt.Frame()
	if err := tt.Click("File: main.go"); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.entries) != 1 || dispatcher.entries[0].path != "src/main.go" || dispatcher.entries[0].absolute != "ssh://remote/root/src/main.go" {
		t.Fatalf("workspace intents=%+v", dispatcher.entries)
	}
	session.SelectedPane = presentation.PaneReview
	session.Pane = presentation.PaneState{Kind: presentation.PaneReview, ReviewSnapshot: codecomponent.PrepareDiff("@@ -1 +1 @@\n-old\n+new\n", 1), ReviewView: &codecomponent.ViewState{}, ReviewSource: "unstaged", Git: presentation.GitState{Branch: "main", HasStaged: true}, CapabilityNote: "Commit is unavailable without an index revision."}
	session.Pane.Review = codecomponent.PrepareReview(session.Pane.ReviewSnapshot)
	tt.Frame()
	if err := tt.Click("Show staged"); err != nil {
		t.Fatal(err)
	}
	if _, ok := tt.Find("Commit unavailable: no reviewed index revision precondition"); !ok {
		t.Fatal("commit CAS limitation is not accessible")
	}
	if len(dispatcher.reviews) != 1 || dispatcher.reviews[0].source != "staged" {
		t.Fatalf("review=%+v", dispatcher.reviews)
	}
}

func TestEveryPermissionOptionIsVisibleAndDispatchesExactID(t *testing.T) {
	s, f := testShell(0)
	session := s.Model.CurrentSession()
	session.Status = presentation.SessionWaitingApproval
	session.Pending = presentation.PendingInteraction{
		Kind: presentation.InteractionApproval, RequestID: "permission-1",
		Options: []presentation.PendingPermissionOption{
			{ID: "once", Label: "Allow once", Allow: true},
			{ID: "always", Label: "Always allow", Allow: true},
			{ID: "deny", Label: "Deny", Allow: false},
		},
	}
	tt := ui.NewTester(s.View, 1000, 720)
	for _, label := range []string{"Approval option: Allow once", "Approval option: Always allow", "Approval option: Deny"} {
		if _, ok := tt.Find(label); !ok {
			t.Fatalf("missing %q in %q", label, tt.Texts())
		}
	}
	if err := tt.Click("Approval option: Always allow"); err != nil {
		t.Fatal(err)
	}
	if len(f.commands) != 1 || f.commands[0].Kind != presentation.CommandApprove || f.commands[0].OptionID != "always" {
		t.Fatalf("commands=%+v", f.commands)
	}
	if err := tt.Click("Approval option: Allow once"); err == nil {
		t.Fatal("duplicate approval option remained active")
	}
}

func TestQuestionSelectionHasVisibleAccessibleState(t *testing.T) {
	s, _ := testShell(0)
	session := s.Model.CurrentSession()
	session.Status = presentation.SessionWaitingApproval
	session.Pending = presentation.PendingInteraction{Kind: presentation.InteractionQuestion, Questions: []presentation.PendingQuestion{{ID: "q", Header: "Checks", Question: "Which?", Options: []string{"Race"}, MultiSelect: true}}}
	tt := ui.NewTester(s.View, 1000, 720)
	if err := tt.Click("Checks: Race, not selected"); err != nil {
		t.Fatal(err)
	}
	if _, ok := tt.Find("Checks: Race, selected"); !ok || !tt.HasText("✓ Race") {
		t.Fatalf("selected option was not visibly and accessibly exposed: %q", tt.Texts())
	}
}

func BenchmarkNavigationManyProjectsSelectedLast(b *testing.B) {
	const count = 10_000
	projects := make([]presentation.Project, count)
	sessions := make([]presentation.Session, count)
	for i := range projects {
		projects[i] = presentation.Project{ID: fmt.Sprintf("p-%05d", i), Name: "Project"}
		sessions[i] = presentation.Session{ID: fmt.Sprintf("s-%05d", i), ProjectID: projects[i].ID, Title: "Session"}
	}
	b.ReportAllocs()
	for range b.N {
		model := presentation.NewModel(projects, sessions)
		model.SelectSession(sessions[count-1].ID)
		_ = NewShell(model, &fakeDispatcher{})
	}
}
