// Package views renders Waku's native MyGo desktop. Views only read prepared
// presentation state and enqueue commands; no I/O or parsing is performed in
// a frame or list row builder.
package views

import (
	"fmt"
	"strings"

	"github.com/egoist/mygo/ui"
	codecomponent "github.com/roshankarande/waku/apps/desktop-go/internal/components/code"
	"github.com/roshankarande/waku/apps/desktop-go/internal/components/terminalui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/presentation"
)

type Shell struct {
	Model      *presentation.Model
	Dispatcher presentation.Dispatcher
	Documents  PreparedDocumentRenderer

	sidebarWidth            float32
	paneWidth               float32
	transcripts             map[string]*ui.ListState
	navList                 ui.ListState
	navRows                 []navigationRow
	navSelected             int
	navRowBySession         map[string]int
	focusComposerPending    string
	focusComposerNow        string
	focusPaneSessionPending string
	focusPaneSessionNow     string
	focusPanePending        presentation.PaneKind
	focusPaneNow            presentation.PaneKind
	composerComposing       bool
	compositionGuard        bool
	hierarchyVersion        uint64
	hubList                 ui.ListState
	workspaceLists          map[string]*ui.ListState
	paletteList             ui.ListState
	paletteOpen             bool
	paletteQuery            string
	paletteSelected         int
	paletteFocus            bool

	// rowsBuilt is intentionally package-visible to interaction tests. It is
	// reset each frame and proves the 10k fixture remains virtualized.
	rowsBuilt           int
	navigationRowsBuilt int
}

// PreparedDocumentRenderer is the integration seam for document components.
// Implementations receive an already parsed/highlighted model identified by
// TranscriptItem.DocumentKey and must only build visible UI; they must never
// parse or perform I/O in RenderPrepared. Returning false uses the bounded,
// selectable source fallback.
type PreparedDocumentRenderer interface {
	RenderPrepared(*ui.Context, presentation.TranscriptItem) bool
}

type navigationRow struct {
	key          string
	projectIndex int
	sessionIndex int
	project      bool
}

const (
	maxDocumentFallbackBytes = 64 * 1024
	maxLiveFallbackBytes     = 16 * 1024
)

func NewShell(model *presentation.Model, dispatcher presentation.Dispatcher) *Shell {
	s := &Shell{
		Model: model, Dispatcher: dispatcher,
		sidebarWidth: 260, paneWidth: 360,
		transcripts: map[string]*ui.ListState{}, workspaceLists: map[string]*ui.ListState{}, navSelected: -1, navRowBySession: map[string]int{},
	}
	s.rebuildNavigation()
	s.hierarchyVersion = model.HierarchyVersion
	return s
}

// RebuildNavigation refreshes the prepared flat hierarchy after projects or
// sessions are inserted, removed, or reordered. Ordinary frames only index it.
func (s *Shell) RebuildNavigation() { s.rebuildNavigation() }

func (s *Shell) rebuildNavigation() {
	s.Model.Reindex()
	s.navRows = s.navRows[:0]
	s.navSelected = -1
	clear(s.navRowBySession)
	for projectIndex := range s.Model.Projects {
		project := &s.Model.Projects[projectIndex]
		s.navRows = append(s.navRows, navigationRow{key: "project:" + project.ID, projectIndex: projectIndex, sessionIndex: -1, project: true})
		for _, sessionIndex := range s.Model.SessionIndices(project.ID) {
			session := &s.Model.Sessions[sessionIndex]
			if session.ID == s.Model.SelectedSession {
				s.navSelected = len(s.navRows)
			}
			s.navRowBySession[session.ID] = len(s.navRows)
			s.navRows = append(s.navRows, navigationRow{key: "session:" + session.ID, projectIndex: projectIndex, sessionIndex: sessionIndex})
		}
	}
}

func (s *Shell) View(c *ui.Context) {
	if s.hierarchyVersion != s.Model.HierarchyVersion {
		s.rebuildNavigation()
		s.hierarchyVersion = s.Model.HierarchyVersion
	}
	s.rowsBuilt = 0
	s.navigationRowsBuilt = 0
	s.composerComposing = false
	s.compositionGuard = false
	// Requests raised while building the previous frame are consumed once,
	// after the control which held focus has disappeared.
	s.focusComposerNow, s.focusComposerPending = s.focusComposerPending, ""
	s.focusPaneSessionNow, s.focusPaneSessionPending = s.focusPaneSessionPending, ""
	s.focusPaneNow, s.focusPanePending = s.focusPanePending, presentation.PaneNone
	if selected := s.Model.SelectedSession; s.focusComposerNow != selected {
		s.focusComposerNow = ""
	}
	if selected := s.Model.SelectedSession; s.focusPaneSessionNow != selected {
		s.focusPaneSessionNow, s.focusPaneNow = "", presentation.PaneNone
	}
	s.syncNavigationSelection()
	root := ui.Column(c).Fill().Background(c.Theme().Background)
	root.Children(func() {
		s.topBar(c)
		if s.paletteOpen {
			s.commandPalette(c)
		}
		ui.Row(c).Grow(1).MinHeight(0).AlignItems(ui.Stretch).Children(func() {
			if s.Model.SidebarOpen {
				s.navigation(c)
			}
			s.conversation(c)
			if s.Model.HubOpen {
				s.hubPanel(c)
			} else if s.Model.RightPaneOpen {
				s.rightPane(c)
			}
		})
	})
	if root.Shortcut(ui.Cmd, ui.KeyK) {
		if s.paletteOpen {
			s.closePalette(c)
		} else {
			s.paletteOpen = true
			s.paletteFocus = true
		}
		c.Invalidate()
	}

	if s.compositionGuard && !s.composerComposing && root.Shortcut(ui.Cmd, ui.KeyEnter) {
		s.submitDraft()
	}
	if s.paletteOpen && root.Shortcut(0, ui.KeyEscape) {
		s.closePalette(c)
		return
	}
	if s.compositionGuard && !s.composerComposing && root.Shortcut(0, ui.KeyEscape) {
		if session := s.Model.CurrentSession(); presentation.CanStop(session) {
			s.primaryAction()
		}
	}
}

func (s *Shell) syncNavigationSelection() {
	row, ok := s.navRowBySession[s.Model.SelectedSession]
	if !ok || s.navSelected == row {
		return
	}
	s.navSelected = row
	s.navList.ScrollIntoView(row)
}

func (s *Shell) topBar(c *ui.Context) {
	t := c.Theme()
	ui.Row(c).Height(48).AlignItems(ui.Center).Gap(t.Space(2)).Padding(0, t.Space(3)).BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Children(func() {
		label := "Show navigation"
		if s.Model.SidebarOpen {
			label = "Hide navigation"
		}
		if ui.Button(c, label).Clicked() {
			s.Model.SidebarOpen = !s.Model.SidebarOpen
		}
		ui.Text(c, "Waku").Bold().FontSize(t.FontSize + 1)
		if ui.Button(c, "Command palette").Label("Open command palette, Command K").Clicked() {
			s.paletteOpen = true
			s.paletteFocus = true
			c.Invalidate()
		}
		if ui.Button(c, "Settings").Clicked() {
			s.requestHub(presentation.HubSettings)
		}
		if session := s.Model.CurrentSession(); session != nil {
			ui.Text(c, "  /  "+session.Title).TextColor(t.TextMuted).SingleLine()
			ui.Box(c).Grow(1)
			status(c, session.Status.Label(), session.Status)
		}
	})
}

type paletteCommand struct {
	label, detail string
	run           func()
}

func (s *Shell) paletteCommands() []paletteCommand {
	commands := []paletteCommand{
		{"Open settings", "Daemon settings and provider overrides", func() { s.requestHub(presentation.HubSettings) }},
		{"Check provider and models", "Readiness, version, and discovered models", func() { s.requestHub(presentation.HubProviders) }},
		{"Open skills", "Installed user and project skills", func() { s.requestHub(presentation.HubSkills) }},
		{"Open usage", "Current usage history and quality", func() { s.requestHub(presentation.HubUsage) }},
		{"Toggle navigation", "Show or hide projects and sessions", func() { s.Model.SidebarOpen = !s.Model.SidebarOpen }},
	}
	if session := s.Model.CurrentSession(); session != nil {
		commands = append(commands, paletteCommand{"Open files", "Prepared editor for this session", func() { s.Model.SetPane(presentation.PaneFiles); s.requestPane(session.ID, presentation.PaneFiles) }}, paletteCommand{"Open review", "Prepared virtualized review", func() { s.Model.SetPane(presentation.PaneReview); s.requestPane(session.ID, presentation.PaneReview) }})
	}
	query := strings.ToLower(strings.TrimSpace(s.paletteQuery))
	if query == "" {
		return commands
	}
	out := commands[:0]
	for _, command := range commands {
		if strings.Contains(strings.ToLower(command.label+" "+command.detail), query) {
			out = append(out, command)
		}
	}
	return out
}

func (s *Shell) commandPalette(c *ui.Context) {
	t := c.Theme()
	commands := s.paletteCommands()
	if len(commands) == 0 {
		s.paletteSelected = -1
	} else if s.paletteSelected < 0 || s.paletteSelected >= len(commands) {
		s.paletteSelected = 0
	}
	ui.Column(c).Padding(t.Space(3)).Gap(t.Space(2)).BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Background(t.Surface).Label("Command palette").Children(func() {
		input := ui.TextInput(c, &s.paletteQuery).Placeholder("Search commands").Label("Search command palette").Grow(1)
		if s.paletteFocus {
			input.Focus()
			s.paletteFocus = false
		}
		s.paletteList.Key = func(i int) any { return commands[i].label }
		s.paletteList.Label = func(i int) string { return commands[i].label }
		s.paletteList.Selected = &s.paletteSelected
		list := ui.List(c, &s.paletteList, len(commands), func(i int) {
			command := commands[i]
			ui.Column(c).Padding(t.Space(1), t.Space(2)).Children(func() {
				if ui.Button(c, command.label).Clicked() {
					command.run()
					s.closePalette(c)
				}
				ui.Text(c, command.detail).TextColor(t.TextMuted)
			})
		}).Height(180).Label("Available commands")
		if list.Submitted() && s.paletteSelected >= 0 && s.paletteSelected < len(commands) {
			commands[s.paletteSelected].run()
			s.closePalette(c)
		}
	})
}

func (s *Shell) closePalette(c *ui.Context) {
	s.paletteOpen = false
	s.paletteFocus = false
	s.focusComposerPending = s.Model.SelectedSession
	c.Invalidate()
}

func (s *Shell) requestHub(kind presentation.HubKind) {
	if requester, ok := s.Dispatcher.(interface{ RequestHub(presentation.HubKind) }); ok {
		requester.RequestHub(kind)
	} else {
		generation := s.Model.BeginHubWork(kind)
		s.Model.CommitHubWork(kind, generation, nil, "This desktop controller does not expose the requested daemon capability.", nil)
	}
}
func (s *Shell) requestPane(session string, kind presentation.PaneKind) {
	if requester, ok := s.Dispatcher.(interface {
		RequestPane(string, presentation.PaneKind)
	}); ok {
		requester.RequestPane(session, kind)
	}
}

func (s *Shell) hubPanel(c *ui.Context) {
	t := c.Theme()
	hub := &s.Model.Hub
	s.hubList.Key = func(i int) any { return hub.Rows[i].Key }
	s.hubList.Label = func(i int) string { return hub.Rows[i].Title }
	ui.Column(c).Width(s.paneWidth+80).MinWidth(320).BorderWidth(0, 0, 0, 1).BorderColor(t.Border).Children(func() {
		ui.Row(c).Height(48).AlignItems(ui.Center).Padding(0, t.Space(3)).Children(func() {
			ui.Text(c, hub.Kind.Label()).Bold().Grow(1)
			if ui.Button(c, "Close insights").Clicked() {
				s.Model.HubOpen = false
				c.Invalidate()
			}
		})
		ui.Row(c).Gap(t.Space(1)).Padding(t.Space(2)).Children(func() {
			for _, kind := range []presentation.HubKind{presentation.HubSettings, presentation.HubProviders, presentation.HubSkills, presentation.HubUsage} {
				kind := kind
				if ui.Button(c, kind.Label()).Clicked() {
					s.requestHub(kind)
				}
			}
		})
		switch {
		case hub.Pending:
			ui.Column(c).Padding(t.Space(3)).Children(func() {
				ui.Spinner(c).Label("Loading " + hub.Kind.Label())
				ui.Text(c, "Requesting daemon capability…")
			})
		case hub.Unavailable != "":
			ui.Column(c).Padding(t.Space(3)).Gap(t.Space(2)).Children(func() { ui.Text(c, hub.Kind.Label()+" unavailable").Bold(); ui.Text(c, hub.Unavailable).Selectable() })
		case hub.Err != "":
			ui.Column(c).Padding(t.Space(3)).Gap(t.Space(2)).Children(func() {
				ui.Text(c, "Could not load "+strings.ToLower(hub.Kind.Label())).Bold().TextColor(t.Danger)
				ui.Text(c, hub.Err).Selectable()
			})
		default:
			ui.List(c, &s.hubList, len(hub.Rows), func(i int) {
				row := hub.Rows[i]
				ui.Column(c).Padding(t.Space(2), t.Space(3)).Gap(2).Children(func() {
					ui.Text(c, row.Title).Bold()
					ui.Text(c, row.Detail).Selectable().TextColor(t.TextMuted)
					if row.Status != "" {
						ui.Badge(c, row.Status)
					}
				})
			}).Grow(1).MinHeight(0).Label(hub.Kind.Label() + " results")
		}
	})
}

func (s *Shell) navigation(c *ui.Context) {
	t := c.Theme()
	s.navList.Key = func(i int) any { return s.navRows[i].key }
	s.navList.Label = func(i int) string {
		row := s.navRows[i]
		if row.project {
			return s.Model.Projects[row.projectIndex].Name
		}
		return s.Model.Sessions[row.sessionIndex].Title
	}
	s.navList.Selected = &s.navSelected
	list := ui.List(c, &s.navList, len(s.navRows), func(i int) {
		s.navigationRowsBuilt++
		row := s.navRows[i]
		if row.project {
			ui.Text(c, s.Model.Projects[row.projectIndex].Name).Bold().TextColor(t.TextMuted).Padding(t.Space(2.5), t.Space(3)).Label("Project: " + s.Model.Projects[row.projectIndex].Name)
			return
		}
		session := &s.Model.Sessions[row.sessionIndex]
		ui.Row(c).Key(row.key).AlignItems(ui.Center).Gap(t.Space(2)).Padding(t.Space(1.5), t.Space(3), t.Space(1.5), t.Space(6)).Children(func() {
			ui.Text(c, session.Title).Grow(1).SingleLine()
			if session.Unread {
				ui.Badge(c, "New")
			} else if session.Status != presentation.SessionIdle {
				ui.Badge(c, session.Status.Label())
			}
		})
	}).Width(s.sidebarWidth).Grow(1).Label("Projects and sessions").Background(t.Surface)
	if list.Changed() && s.navSelected >= 0 && s.navSelected < len(s.navRows) {
		row := s.navRows[s.navSelected]
		if row.project {
			s.Model.SelectProject(s.Model.Projects[row.projectIndex].ID)
		} else {
			s.Model.SelectSession(s.Model.Sessions[row.sessionIndex].ID)
		}
		if observer, ok := s.Dispatcher.(interface{ SessionSelected(string) }); ok {
			observer.SessionSelected(s.Model.SelectedSession)
		}
	}
	// A visible separator preserves hierarchy in high-contrast themes too.
	list.BorderWidth(0, 1, 0, 0).BorderColor(t.Border)
}

func (s *Shell) conversation(c *ui.Context) {
	session := s.Model.CurrentSession()
	if session == nil {
		ui.Column(c).Grow(1).Center().Children(func() { ui.Text(c, "Choose a session") })
		return
	}
	t := c.Theme()
	ui.Column(c).Grow(1).MinWidth(320).MinHeight(0).Children(func() {
		ui.Row(c).AlignItems(ui.Center).Gap(t.Space(2)).Padding(t.Space(2.5), t.Space(4)).BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Children(func() {
			ui.Column(c).Grow(1).Gap(2).Children(func() {
				ui.Text(c, session.Title).Bold().SingleLine()
				activity := session.Activity
				if activity == "" {
					activity = session.Status.Label()
				}
				ui.Text(c, activity).TextColor(t.TextMuted).SingleLine().Label("Current activity: " + activity)
			})
			for _, pane := range []presentation.PaneKind{presentation.PaneFiles, presentation.PaneReview, presentation.PaneTerminal, presentation.PaneBrowser} {
				kind := pane
				button := ui.Button(c, kind.Label())
				if s.focusPaneSessionNow == session.ID && s.focusPaneNow == kind {
					button.Focus()
					s.focusPaneNow = presentation.PaneNone
					s.focusPaneSessionNow = ""
				}
				if button.Clicked() {
					s.Model.SetPane(kind)
					if requester, ok := s.Dispatcher.(interface {
						RequestPane(string, presentation.PaneKind)
					}); ok && s.Model.RightPaneOpen {
						requester.RequestPane(session.ID, kind)
					}
				}
			}
		})
		s.transcript(c, session)
		s.composer(c, session)
	})
}

func (s *Shell) transcript(c *ui.Context, session *presentation.Session) {
	state := s.transcripts[session.ID]
	if state == nil {
		state = &ui.ListState{FollowEnd: true}
		s.transcripts[session.ID] = state
	}
	state.Key = func(i int) any { return session.Transcript[i].ID }
	state.Label = func(i int) string {
		item := session.Transcript[i]
		if item.Title != "" {
			return item.Title
		}
		return roleLabel(item.Role)
	}
	t := c.Theme()
	ui.List(c, state, len(session.Transcript), func(i int) {
		s.rowsBuilt++
		item := session.Transcript[i]
		ui.Column(c).Key(item.ID).Gap(t.Space(1)).Padding(t.Space(2), t.Space(4)).Children(func() {
			ui.Row(c).AlignItems(ui.Center).Gap(t.Space(2)).Children(func() {
				ui.Text(c, roleLabel(item.Role)).Bold().FontSize(t.FontSize - 1)
				if item.Streaming {
					ui.Badge(c, "Streaming")
				}
			})
			if item.Title != "" {
				ui.Text(c, item.Title).Bold()
			}
			rendered := false
			if item.DocumentKey != "" && s.Documents != nil {
				rendered = s.Documents.RenderPrepared(c, item)
			}
			if !rendered {
				body, clipped := boundedDocumentFallback(item)
				ui.Text(c, body).Selectable().LineHeight(1.45)
				if clipped {
					ui.Text(c, "Showing a bounded live/source window; open the prepared document for complete content.").TextColor(t.TextMuted).FontSize(t.FontSize - 1).Label("Document fallback is truncated")
				}
			}
		})
	}).Grow(1).MinHeight(0).Justify(ui.End).Label("Transcript")
}

func (s *Shell) composer(c *ui.Context, session *presentation.Session) {
	t := c.Theme()
	ui.Column(c).Gap(t.Space(2)).Padding(t.Space(3), t.Space(4)).BorderWidth(1, 0, 0, 0).BorderColor(t.Border).Children(func() {
		if session.Status == presentation.SessionWaitingApproval {
			ui.Column(c).Gap(t.Space(2)).Children(func() {
				title, detail := "Approval needed", "Review the requested action before continuing."
				if session.Pending.Title != "" {
					title = session.Pending.Title
				}
				if session.Pending.Detail != "" {
					detail = session.Pending.Detail
				}
				ui.Text(c, title).Bold()
				ui.Text(c, detail).TextColor(t.TextMuted)
				if session.Pending.Kind == presentation.InteractionQuestion {
					for i := range session.Pending.Questions {
						q := &session.Pending.Questions[i]
						ui.Text(c, q.Header+": "+q.Question).Label("Question " + q.ID)
						if len(q.Options) == 0 {
							ui.TextArea(c, &q.Custom).Placeholder("Answer").Label("Answer " + q.Header)
						} else {
							ui.Row(c).Gap(t.Space(1)).Children(func() {
								for _, option := range q.Options {
									option := option
									selected := containsAnswer(q.Answers, option)
									label, accessible := option, q.Header+": "+option+", not selected"
									if selected {
										label, accessible = "✓ "+option, q.Header+": "+option+", selected"
									}
									button := ui.Button(c, label).Label(accessible)
									if button.Clicked() {
										if q.MultiSelect {
											q.Answers = toggleAnswer(q.Answers, option)
										} else if selected {
											q.Answers = nil
										} else {
											q.Answers = []string{option}
										}
									}
									if description := q.OptionDescriptions[option]; description != "" {
										ui.Text(c, description).TextColor(t.TextMuted).FontSize(t.FontSize - 1).Label(option + " description")
									}
								}
							})
						}
					}
				}
				ui.Row(c).Gap(t.Space(2)).Children(func() {
					approvalPending := presentation.CommandPending(session, presentation.CommandApprove)
					if session.Pending.Kind == presentation.InteractionApproval {
						for _, option := range session.Pending.Options {
							option := option
							button := ui.Button(c, option.Label).Label("Approval option: " + option.Label).Disabled(approvalPending)
							if button.Clicked() {
								s.resolvePermission(c, option)
							}
						}
						return
					}
					rejectLabel, approveLabel := "Reject", "Approve"
					if session.Pending.Kind == presentation.InteractionQuestion {
						rejectLabel, approveLabel = "Skip", "Answer"
					}
					reject := ui.Button(c, rejectLabel).Disabled(approvalPending)
					if reject.Clicked() {
						s.resolveApproval(c, presentation.CommandReject)
					}
					approve := ui.PrimaryButton(c, approveLabel).Disabled(approvalPending || (session.Pending.Kind == presentation.InteractionQuestion && !questionsAnswered(session.Pending.Questions)))
					if approve.Clicked() {
						s.resolveApproval(c, presentation.CommandApprove)
					}
				})
			})
		}
		ui.Row(c).AlignItems(ui.End).Gap(t.Space(2)).Children(func() {
			composer := ui.TextArea(c, &session.Draft).Placeholder("Ask Waku to work on this project…").Label("Message").Grow(1).MaxHeight(180)
			s.composerComposing, s.compositionGuard = compositionState(composer)
			if s.focusComposerNow == session.ID {
				composer.Focus()
				s.focusComposerNow = ""
			}
			label := presentation.PrimaryAction(session)
			button := ui.PrimaryButton(c, label).Label(label + " message")
			button.Disabled((label == "Stop" && !presentation.CanStop(session)) ||
				(label == "Retry" && presentation.CommandPending(session, presentation.CommandRetry)) ||
				(label == "Send" && !presentation.CanSend(session)) || label == "Stopping…")
			if button.Clicked() {
				s.primaryAction()
			}
		})
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			shortcutHelp := "⌘/Ctrl+Enter to send or steer · Escape to stop"
			if !s.compositionGuard {
				shortcutHelp = "Keyboard send/stop shortcuts unavailable: this MyGo revision cannot report active IME composition. Use the action buttons."
			}
			ui.Text(c, shortcutHelp).TextColor(t.TextMuted).FontSize(t.FontSize - 1).Label("Composer shortcut capability: " + shortcutHelp)
			if session.Failure != "" {
				ui.Text(c, session.Failure).TextColor(t.Danger).Label("Last failure: " + session.Failure)
			}
		})
	})
}

type compositionProbe interface{ Composing() bool }

// compositionState is an optional framework seam. It lets this source compile
// against the exact pinned MyGo revision while activating the native IME guard
// as soon as that revision exposes Element.Composing.
func compositionState(element any) (active, available bool) {
	probe, ok := element.(compositionProbe)
	if !ok {
		return false, false
	}
	return probe.Composing(), true
}

// CompositionGuardAvailable reports whether the active MyGo revision can
// distinguish provisional IME composition from committed composer text.
func (s *Shell) CompositionGuardAvailable() bool { return s.compositionGuard }

func (s *Shell) rightPane(c *ui.Context) {
	t := c.Theme()
	kind := presentation.PaneNone
	session := s.Model.CurrentSession()
	if session != nil {
		kind = session.SelectedPane
	}
	ui.Column(c).Width(s.paneWidth).MinWidth(240).BorderWidth(0, 0, 0, 1).BorderColor(t.Border).Children(func() {
		ui.Row(c).Height(48).AlignItems(ui.Center).Padding(0, t.Space(3)).BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Children(func() {
			ui.Text(c, kind.Label()).Bold().Grow(1)
			if ui.Button(c, "Close panel").Clicked() {
				s.focusPaneSessionPending = session.ID
				s.focusPanePending = kind
				s.Model.RightPaneOpen = false
				c.Invalidate()
			}
		})
		ui.Column(c).Grow(1).Padding(t.Space(4)).Gap(t.Space(2)).Children(func() {
			if session == nil {
				return
			}
			switch {
			case session.Pane.Pending:
				ui.Spinner(c).Label("Loading " + kind.Label())
				if session.Pane.Summary != "" {
					ui.Text(c, session.Pane.Summary).Label(kind.Label() + " summary")
				} else {
					ui.Text(c, "Preparing "+strings.ToLower(kind.Label())+"…")
				}
			case session.Pane.Err != "":
				ui.Text(c, "Could not load "+strings.ToLower(kind.Label())).Bold().TextColor(t.Danger)
				ui.Text(c, session.Pane.Err).Selectable()
			case kind == presentation.PaneFiles:
				if session.Pane.CapabilityNote != "" {
					ui.Text(c, session.Pane.CapabilityNote).TextColor(t.TextMuted).Label("Editor capability")
				}
				state := s.workspaceLists[session.ID]
				if state == nil {
					state = &ui.ListState{}
					s.workspaceLists[session.ID] = state
				}
				state.Key = func(i int) any { return session.Pane.WorkspaceEntries[i].AbsolutePath }
				state.Label = func(i int) string { return session.Pane.WorkspaceEntries[i].Name }
				if session.Pane.WorkspaceParent != nil {
					parent := *session.Pane.WorkspaceParent
					if ui.Button(c, "Parent folder").Label("Browse remote parent folder").Clicked() {
						if requester, ok := s.Dispatcher.(interface {
							RequestWorkspaceEntry(string, string, string, bool)
						}); ok {
							requester.RequestWorkspaceEntry(session.ID, "", parent, true)
						}
					}
				}
				ui.List(c, state, len(session.Pane.WorkspaceEntries), func(i int) {
					entry := session.Pane.WorkspaceEntries[i]
					label := "File: " + entry.Name
					if entry.IsDir {
						label = "Folder: " + entry.Name
					}
					if ui.Button(c, strings.Repeat("  ", int(entry.Depth))+entry.Name).Label(label).Clicked() {
						if requester, ok := s.Dispatcher.(interface {
							RequestWorkspaceEntry(string, string, string, bool)
						}); ok {
							requester.RequestWorkspaceEntry(session.ID, entry.Path, entry.AbsolutePath, entry.IsDir)
						}
					}
				}).Height(180).Label("Remote workspace files")
				if session.Pane.Summary != "" {
					ui.Text(c, session.Pane.Summary).Selectable()
				}
				if session.Pane.Editor != nil {
					session.Pane.Editor.SetInvalidator(c.Invalidate)
					codecomponent.EditorView(c, session.Pane.Editor).Grow(1).MinHeight(0)
					if session.Pane.Editor.LastError != nil {
						ui.Text(c, session.Pane.Editor.LastError.Error()).TextColor(t.Danger).Label("Editor error")
					}
				}
			case kind == presentation.PaneReview && session.Pane.ReviewSnapshot != nil && session.Pane.ReviewView != nil:
				if session.Pane.CapabilityNote != "" {
					ui.Text(c, session.Pane.CapabilityNote).TextColor(t.TextMuted).Label("Review capability")
				}
				codecomponent.DiffView(c, session.Pane.ReviewSnapshot, session.Pane.ReviewView).Grow(1).MinHeight(0)
				if session.Pane.Review != nil {
					ui.Text(c, fmt.Sprintf("%d review hunks", len(session.Pane.Review.Hunks))).TextColor(t.TextMuted).Label("Review summary")
				}
				ui.Text(c, fmt.Sprintf("Branch %s · staged +%d −%d · working tree +%d −%d", session.Pane.Git.Branch, session.Pane.Git.StagedAdditions, session.Pane.Git.StagedDeletions, session.Pane.Git.Additions, session.Pane.Git.Deletions)).Label("Git status")
				ui.Row(c).Gap(t.Space(1)).Children(func() {
					for _, source := range []string{"unstaged", "staged"} {
						source := source
						if ui.Button(c, "Show "+source).Disabled(source == session.Pane.ReviewSource).Clicked() {
							if requester, ok := s.Dispatcher.(interface{ RequestReviewSource(string, string) }); ok {
								requester.RequestReviewSource(session.ID, source)
							}
						}
					}
				})
				ui.Button(c, "Commit unavailable").Disabled(true).Label("Commit unavailable: no reviewed index revision precondition")
			case kind == presentation.PaneTerminal && session.Pane.Terminal != nil:
				if session.Pane.CapabilityNote != "" {
					ui.Text(c, session.Pane.CapabilityNote).TextColor(t.TextMuted).Label("Terminal capability: " + session.Pane.CapabilityNote)
				}
				terminalui.View(c, session.Pane.Terminal).Grow(1).MinHeight(0)
			case session.Pane.Summary != "":
				ui.Text(c, session.Pane.Summary).Selectable()
			default:
				ui.Text(c, kind.Label()+" is ready.").TextColor(t.TextMuted)
			}
		})
	})
}

func (s *Shell) primaryAction() {
	session := s.Model.CurrentSession()
	if session == nil {
		return
	}
	switch {
	case presentation.CanStop(session):
		if s.dispatchCommand(session, presentation.CommandStop, "") {
			session.Status = presentation.SessionStopping
			session.Activity = "Stopping provider"
		}
	case session.Status == presentation.SessionFailed && !presentation.CommandPending(session, presentation.CommandRetry):
		if s.dispatchCommand(session, presentation.CommandRetry, session.LastPrompt) {
			session.Status = presentation.SessionRetrying
			session.Activity = "Retrying last request"
		}
	case presentation.CanSend(session):
		s.submitDraft()
	}
}

func (s *Shell) submitDraft() {
	session := s.Model.CurrentSession()
	kind, ok := presentation.SubmitAction(session)
	if !ok {
		return
	}
	text := session.Draft
	if !s.dispatchCommand(session, kind, text) {
		return
	}
	session.Draft = ""
	switch kind {
	case presentation.CommandSend:
		session.Status = presentation.SessionPreparing
		session.Activity = "Starting provider"
	case presentation.CommandSteer:
		session.Activity = "Steering current turn"
	}
}

func (s *Shell) resolveApproval(c *ui.Context, kind presentation.CommandKind) {
	session := s.Model.CurrentSession()
	if session == nil || presentation.CommandPending(session, kind) || session.Status != presentation.SessionWaitingApproval {
		return
	}
	answers := make([]presentation.QuestionAnswer, 0, len(session.Pending.Questions))
	if session.Pending.Kind == presentation.InteractionQuestion && kind == presentation.CommandApprove {
		for _, q := range session.Pending.Questions {
			values := append([]string(nil), q.Answers...)
			if len(values) == 0 && strings.TrimSpace(q.Custom) != "" {
				values = []string{strings.TrimSpace(q.Custom)}
			}
			answers = append(answers, presentation.QuestionAnswer{QuestionID: q.ID, Answers: values})
		}
	}
	command, ok := s.Model.BeginCommand(session.ID, kind, "")
	if !ok {
		return
	}
	command.Answers = answers
	command.RequestID = session.Pending.RequestID
	if s.dispatch(command) {
		session.Status = presentation.SessionResolvingApproval
		s.focusComposerPending = session.ID
		c.Invalidate()
		if kind == presentation.CommandApprove {
			session.Activity = "Approving requested action"
		} else {
			session.Activity = "Rejecting requested action"
		}
	} else {
		s.Model.CancelCommand(command)
	}
}

func (s *Shell) resolvePermission(c *ui.Context, option presentation.PendingPermissionOption) {
	session := s.Model.CurrentSession()
	if session == nil || option.ID == "" || session.Pending.Kind != presentation.InteractionApproval || session.Status != presentation.SessionWaitingApproval {
		return
	}
	kind := presentation.CommandReject
	if option.Allow {
		kind = presentation.CommandApprove
	}
	command, ok := s.Model.BeginCommand(session.ID, kind, "")
	if !ok {
		return
	}
	command.OptionID = option.ID
	command.RequestID = session.Pending.RequestID
	if s.dispatch(command) {
		session.Status = presentation.SessionResolvingApproval
		session.Activity = "Resolving " + option.Label
		s.focusComposerPending = session.ID
		c.Invalidate()
	} else {
		s.Model.CancelCommand(command)
	}
}

func containsAnswer(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func toggleAnswer(values []string, target string) []string {
	if containsAnswer(values, target) {
		out := values[:0]
		for _, value := range values {
			if value != target {
				out = append(out, value)
			}
		}
		return out
	}
	return append(values, target)
}
func questionsAnswered(questions []presentation.PendingQuestion) bool {
	if len(questions) == 0 {
		return false
	}
	for _, q := range questions {
		if len(q.Answers) == 0 && strings.TrimSpace(q.Custom) == "" {
			return false
		}
	}
	return true
}

func (s *Shell) dispatchCommand(session *presentation.Session, kind presentation.CommandKind, text string) bool {
	command, ok := s.Model.BeginCommand(session.ID, kind, text)
	if !ok {
		return false
	}
	if s.dispatch(command) {
		return true
	}
	s.Model.CancelCommand(command)
	return false
}

func (s *Shell) dispatch(command presentation.Command) bool {
	return s.Dispatcher != nil && s.Dispatcher.Dispatch(command)
}

func roleLabel(role presentation.TranscriptRole) string {
	switch role {
	case presentation.RoleUser:
		return "You"
	case presentation.RoleReasoning:
		return "Reasoning"
	case presentation.RoleTool:
		return "Tool activity"
	case presentation.RoleNotice:
		return "Notice"
	default:
		return "Waku"
	}
}

func status(c *ui.Context, label string, state presentation.SessionStatus) {
	color := c.Theme().TextMuted
	switch state {
	case presentation.SessionRunning, presentation.SessionPreparing:
		color = c.Theme().Accent
	case presentation.SessionWaitingApproval:
		color = c.Theme().Warning
	case presentation.SessionFailed:
		color = c.Theme().Danger
	case presentation.SessionPaused:
		color = c.Theme().TextMuted
	}
	ui.Text(c, "● "+label).TextColor(color).Label("Session status: " + label)
}

func boundedDocumentFallback(item presentation.TranscriptItem) (string, bool) {
	limit := maxDocumentFallbackBytes
	if item.Streaming || item.Role == presentation.RoleReasoning {
		limit = maxLiveFallbackBytes
	}
	if len(item.Body) <= limit {
		return item.Body, false
	}
	const marker = "\n… earlier content omitted from bounded view …\n"
	available := limit - len(marker)
	if item.Streaming || item.Role == presentation.RoleReasoning {
		return marker + utf8Tail(item.Body, available), true
	}
	head := available / 2
	tail := available - head
	return utf8Head(item.Body, head) + marker + utf8Tail(item.Body, tail), true
}

func utf8Head(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && text[end]&0xc0 == 0x80 {
		end--
	}
	return text[:end]
}

func utf8Tail(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	start := len(text) - limit
	for start < len(text) && text[start]&0xc0 == 0x80 {
		start++
	}
	return text[start:]
}
