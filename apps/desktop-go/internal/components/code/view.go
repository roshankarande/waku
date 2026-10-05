package code

import (
	"fmt"
	"unicode/utf8"

	"github.com/egoist/mygo/ui"
)

type ViewState struct {
	List      ui.ListState
	Query     string
	Matches   []Match
	matchByID map[uint64][]Range
	Active    int
	Searching bool
	Selection *Selection
	Caret     Point
}

func (s *ViewState) ApplySearchResult(snap *Snapshot, result SearchResult) bool {
	if result.Err != nil || result.Generation != snap.Generation || result.Query != s.Query {
		return false
	}
	for _, match := range result.Matches {
		line := lineByID(snap, match.LineID)
		if line == nil || match.Range.Start < 0 || match.Range.Start > match.Range.End || match.Range.End > len(line.Text) || !utf8.ValidString(line.Text[match.Range.Start:match.Range.End]) {
			return false
		}
	}
	s.Matches, s.Searching, s.Active = result.Matches, false, 0
	s.matchByID = make(map[uint64][]Range)
	for _, m := range result.Matches {
		if len(s.matchByID[m.LineID]) < MaxPaintMatches {
			s.matchByID[m.LineID] = append(s.matchByID[m.LineID], m.Range)
		}
	}
	s.scrollToActive(snap)
	return true
}

func (s *ViewState) scrollToActive(snap *Snapshot) {
	if len(s.Matches) == 0 {
		return
	}
	id := s.Matches[s.Active%len(s.Matches)].LineID
	for i := range snap.Lines {
		if snap.Lines[i].ID == id {
			s.List.ScrollIntoView(i)
			return
		}
	}
}

// DiffView virtualizes prepared rows. Syntax tokens are immutable paint data;
// this proof keeps one monospace text element per row so highlighting cannot
// affect measurement or trigger work in the row builder.
func DiffView(c *ui.Context, snap *Snapshot, state *ViewState) *ui.Element {
	if snap == nil {
		return ui.Text(c, "Preparing diff…")
	}
	state.List.Key = func(i int) any { return snap.Lines[i].ID }
	state.List.Label = func(i int) string { return snap.Lines[i].Text }
	root := ui.Column(c).MinHeight(0)
	root.Children(func() {
		ui.Row(c).Gap(6).AlignItems(ui.Center).Padding(4, 8).Children(func() {
			if ui.TextInput(c, &state.Query).Label("Search diff").Placeholder("Search diff").Grow(1).Changed() {
				state.Searching = true
				state.Matches = nil
				state.matchByID = nil
			}
			if ui.Button(c, "Previous diff match").Disabled(len(state.Matches) == 0).Clicked() {
				state.Active = (state.Active + len(state.Matches) - 1) % len(state.Matches)
				state.scrollToActive(snap)
			}
			if ui.Button(c, "Next diff match").Disabled(len(state.Matches) == 0).Clicked() {
				state.Active = (state.Active + 1) % len(state.Matches)
				state.scrollToActive(snap)
			}
			label := fmt.Sprintf("%d diff matches", len(state.Matches))
			if state.Searching {
				label = "Diff search pending"
			}
			ui.Text(c, label)
			if ui.Button(c, "Copy diff match").Disabled(len(state.Matches) == 0).Clicked() {
				m := state.Matches[state.Active%len(state.Matches)]
				if l := lineByID(snap, m.LineID); l != nil {
					c.WriteClipboard(l.Text[m.Range.Start:m.Range.End])
				}
			}
			if ui.Button(c, "Copy code selection").Disabled(state.Selection == nil).Clicked() {
				if text, err := copyCodeSelection(snap, state); err == nil {
					c.WriteClipboard(text)
				}
			}
			if ui.Button(c, "Select all diff text").Disabled(len(snap.Lines) == 0).Clicked() {
				selectAll(snap, state)
			}
			ui.Text(c, "Precise pointer text selection unavailable; use keyboard selection").Label("Diff pointer-selection capability")
		})
		list := ui.List(c, &state.List, len(snap.Lines), func(i int) {
			l := snap.Lines[i]
			row := ui.Row(c).Key(l.ID).Height(22).Focusable()
			row.Children(func() {
				ui.Text(c, fmt.Sprintf("%5s %5s", lineNo(l.OldLine), lineNo(l.NewLine))).Font("monospace").TextColor(c.Theme().TextMuted).Width(90)
				ui.RichText(c, styledSpans(c, l, state.matchByID[l.ID], codeSelectionRange(snap, state.Selection, i))...).Font("monospace").NoWrap().Grow(1)
			})
			if row.Shortcut(ui.Cmd, ui.KeyA) {
				selectAll(snap, state)
			}
			if row.Shortcut(ui.Shift, ui.KeyRight) {
				ensureSelection(snap, state, i, 0)
				moveCodeSelection(snap, state, 1)
			}
			if row.Shortcut(ui.Shift, ui.KeyLeft) {
				ensureSelection(snap, state, i, len(l.Text))
				moveCodeSelection(snap, state, -1)
			}
			if row.Shortcut(ui.Shift, ui.KeyDown) {
				ensureSelection(snap, state, i, 0)
				moveCodeSelectionLine(snap, state, 1)
			}
			if row.Shortcut(ui.Shift, ui.KeyUp) {
				ensureSelection(snap, state, i, len(l.Text))
				moveCodeSelectionLine(snap, state, -1)
			}
		}).Grow(1).MinHeight(0).Label("Diff lines")
		if list.Shortcut(ui.Cmd, ui.KeyC) && state.Selection != nil {
			if text, err := copyCodeSelection(snap, state); err == nil {
				c.WriteClipboard(text)
			}
		}
	})
	return root
}

func styledSpans(c *ui.Context, l Line, matches []Range, selected *Range) []ui.Span {
	spans := make([]ui.Span, 0, len(l.Tokens)+len(matches)+1)
	at, ti, mi := 0, 0, 0
	for at < len(l.Text) {
		for ti < len(l.Tokens) && l.Tokens[ti].Range.End <= at {
			ti++
		}
		for mi < len(matches) && matches[mi].End <= at {
			mi++
		}
		next := len(l.Text)
		if ti < len(l.Tokens) {
			if l.Tokens[ti].Range.Start > at {
				next = min(next, l.Tokens[ti].Range.Start)
			} else {
				next = min(next, l.Tokens[ti].Range.End)
			}
		}
		if mi < len(matches) {
			if matches[mi].Start > at {
				next = min(next, matches[mi].Start)
			} else {
				next = min(next, matches[mi].End)
			}
		}
		if selected != nil {
			if selected.Start > at {
				next = min(next, selected.Start)
			} else if selected.End > at {
				next = min(next, selected.End)
			}
		}
		if next <= at {
			next = at + 1
		}
		a, b := at, next
		span := ui.Span{Text: l.Text[a:b]}
		if ti < len(l.Tokens) {
			t := l.Tokens[ti]
			if a >= t.Range.Start && b <= t.Range.End {
				switch t.Kind {
				case Keyword:
					span.Weight = 700
					span.Color = c.Theme().Accent
				case String:
					span.Color = c.Theme().Success
				case Comment:
					span.Color = c.Theme().TextMuted
				case Number:
					span.Color = c.Theme().Warning
				}
			}
		}
		if (mi < len(matches) && a >= matches[mi].Start && b <= matches[mi].End) || (selected != nil && a >= selected.Start && b <= selected.End) {
			span.Background = c.Theme().Selection
		}
		spans = append(spans, span)
		at = next
	}
	if len(spans) == 0 {
		return []ui.Span{{Text: l.Text}}
	}
	return spans
}

const MaxPaintMatches = 512

func lineByID(s *Snapshot, id uint64) *Line {
	if i, ok := s.lineIndex[id]; ok {
		return &s.Lines[i]
	}
	return nil
}
func copyCodeSelection(s *Snapshot, state *ViewState) (string, error) {
	if state.Selection == nil {
		return "", nil
	}
	return s.Copy(*state.Selection)
}

const PrecisePointerSelection = false

func selectAll(s *Snapshot, state *ViewState) {
	if len(s.Lines) == 0 {
		return
	}
	a, b := s.Lines[0], s.Lines[len(s.Lines)-1]
	state.Selection = &Selection{Anchor: Point{a.ID, 0}, Head: Point{b.ID, len(b.Text)}}
	state.Caret = state.Selection.Head
}

func ensureSelection(s *Snapshot, state *ViewState, row, fallback int) {
	if state.Selection != nil || row < 0 || row >= len(s.Lines) {
		return
	}
	p := state.Caret
	if p.LineID != s.Lines[row].ID {
		p = Point{s.Lines[row].ID, fallback}
	} else if _, _, err := s.point(p); err != nil {
		p = Point{s.Lines[row].ID, fallback}
	}
	state.Selection = &Selection{Anchor: p, Head: p}
	state.Caret = p
}
func moveCodeSelection(s *Snapshot, state *ViewState, d int) {
	if state.Selection == nil {
		return
	}
	i, at, e := s.point(state.Selection.Head)
	if e != nil {
		return
	}
	text := s.Lines[i].Text
	if d > 0 {
		if at < len(text) {
			_, n := utf8.DecodeRuneInString(text[at:])
			at += n
		} else if i+1 < len(s.Lines) {
			i++
			at = 0
		}
	} else {
		if at > 0 {
			_, n := utf8.DecodeLastRuneInString(text[:at])
			at -= n
		} else if i > 0 {
			i--
			at = len(s.Lines[i].Text)
		}
	}
	state.Selection.Head = Point{s.Lines[i].ID, at}
	state.Caret = state.Selection.Head
	state.List.ScrollIntoView(i)
}
func moveCodeSelectionLine(s *Snapshot, state *ViewState, d int) {
	if state.Selection == nil {
		return
	}
	i, at, e := s.point(state.Selection.Head)
	if e != nil {
		return
	}
	i = min(max(i+d, 0), len(s.Lines)-1)
	at = validBoundary(s.Lines[i].Text, min(at, len(s.Lines[i].Text)))
	state.Selection.Head = Point{s.Lines[i].ID, at}
	state.Caret = state.Selection.Head
	state.List.ScrollIntoView(i)
}
func codeSelectionRange(s *Snapshot, selection *Selection, index int) *Range {
	if selection == nil {
		return nil
	}
	a, ao, e := s.point(selection.Anchor)
	if e != nil {
		return nil
	}
	b, bo, e := s.point(selection.Head)
	if e != nil {
		return nil
	}
	if a > b || (a == b && ao > bo) {
		a, b, ao, bo = b, a, bo, ao
	}
	if index < a || index > b {
		return nil
	}
	r := Range{0, len(s.Lines[index].Text)}
	if index == a {
		r.Start = ao
	}
	if index == b {
		r.End = bo
	}
	return &r
}

func lineNo(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprint(n)
}

// EditorView binds MyGo's native editing/IME engine to the prepared model.
// Repository work remains in FileWorker; the view only records committed text.

func EditorView(c *ui.Context, session *EditorSession) *ui.Element {
	session.Poll()
	model := &session.Model
	before := model.Text
	e := ui.TextArea(c, &model.Text).Label("Code editor").Font("monospace").Fill()
	if e.Changed() {
		if len(model.Text) > MaxEditableBytes {
			model.Text = before
		} else {
			model.Generation++
			model.Dirty = true
			n := utf8.RuneCountInString(model.Text)
			_ = model.Select(n, n)
		}
	}
	return e
}
