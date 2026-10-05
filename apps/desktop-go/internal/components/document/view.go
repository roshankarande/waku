package document

import (
	"fmt"
	"unicode/utf8"

	"github.com/egoist/mygo/ui"
)

// ViewState is transient UI state and is intentionally separate from Snapshot.
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
		block := blockByID(snap, match.BlockID)
		if block == nil || !match.Text.validFor(block.Text) || !match.Source.validFor(snap.Source) {
			return false
		}
	}
	s.Matches, s.Searching, s.Active = result.Matches, false, 0
	s.matchByID = make(map[uint64][]Range)
	for _, m := range result.Matches {
		if len(s.matchByID[m.BlockID]) < MaxPaintMatches {
			s.matchByID[m.BlockID] = append(s.matchByID[m.BlockID], m.Text)
		}
	}
	s.scrollToActive(snap)
	return true
}

func (s *ViewState) scrollToActive(snap *Snapshot) {
	if len(s.Matches) == 0 {
		return
	}
	id := s.Matches[s.Active%len(s.Matches)].BlockID
	for i := range snap.Blocks {
		if snap.Blocks[i].ID == id {
			s.List.ScrollIntoView(i)
			return
		}
	}
}

// View builds only visible rows from already-prepared data. It performs no
// Markdown parsing, search, highlighting, math rendering, I/O, or locking.
func View(c *ui.Context, snap *Snapshot, state *ViewState) *ui.Element {
	if snap == nil {
		return ui.Text(c, "Preparing document…")
	}
	state.List.Key = func(i int) any { return snap.Blocks[i].ID }
	state.List.Label = func(i int) string { return snap.Blocks[i].Text }
	root := ui.Column(c).MinHeight(0)
	root.Children(func() {
		ui.Row(c).Gap(6).AlignItems(ui.Center).Padding(4, 8).Children(func() {
			if ui.TextInput(c, &state.Query).Label("Search document").Placeholder("Search document").Grow(1).Changed() {
				state.Searching = true
				state.Matches = nil
				state.matchByID = nil
			}
			if ui.Button(c, "Previous match").Disabled(len(state.Matches) == 0).Clicked() {
				state.Active = (state.Active + len(state.Matches) - 1) % len(state.Matches)
				state.scrollToActive(snap)
			}
			if ui.Button(c, "Next match").Disabled(len(state.Matches) == 0).Clicked() {
				state.Active = (state.Active + 1) % len(state.Matches)
				state.scrollToActive(snap)
			}
			label := fmt.Sprintf("%d matches", len(state.Matches))
			if state.Searching {
				label = "Search pending"
			}
			ui.Text(c, label).Label("Document search status: " + label)
			if ui.Button(c, "Copy match").Disabled(len(state.Matches) == 0).Clicked() {
				m := state.Matches[state.Active%len(state.Matches)]
				if b := blockByID(snap, m.BlockID); b != nil {
					c.WriteClipboard(b.Text[m.Text.Start:m.Text.End])
				}
			}
			if ui.Button(c, "Copy document selection").Disabled(state.Selection == nil).Clicked() {
				if text, err := copyViewSelection(snap, state); err == nil {
					c.WriteClipboard(text)
				}
			}
			if ui.Button(c, "Select all document text").Disabled(len(snap.Blocks) == 0).Clicked() {
				selectAll(snap, state)
			}
			ui.Text(c, "Precise pointer text selection unavailable; use keyboard selection").Label("Document pointer-selection capability")
		})
		list := ui.List(c, &state.List, len(snap.Blocks), func(i int) {
			b := snap.Blocks[i]
			row := ui.Box(c).Key(b.ID).Focusable()
			row.Children(func() {
				text := richBlock(c, b, state.matchByID[b.ID], selectionRange(snap, state.Selection, i))
				switch b.Kind {
				case Heading:
					text.Bold().FontSize(float32(24-max(0, b.Level-1)*2)).Padding(5, 8)
				case CodeBlock, Table, Math:
					text.Font("monospace").NoWrap().Padding(8).Background(c.Theme().Surface)
				default:
					text.Padding(5, 8)
				}
			})
			if row.Shortcut(ui.Cmd, ui.KeyA) {
				selectAll(snap, state)
			}
			if row.Shortcut(ui.Shift, ui.KeyRight) {
				ensureSelection(snap, state, i, 0)
				moveSelection(snap, state, 1)
			}
			if row.Shortcut(ui.Shift, ui.KeyLeft) {
				ensureSelection(snap, state, i, len(b.Text))
				moveSelection(snap, state, -1)
			}
			if row.Shortcut(ui.Shift, ui.KeyDown) {
				ensureSelection(snap, state, i, 0)
				moveSelectionBlock(snap, state, 1)
			}
			if row.Shortcut(ui.Shift, ui.KeyUp) {
				ensureSelection(snap, state, i, len(b.Text))
				moveSelectionBlock(snap, state, -1)
			}
		}).Grow(1).MinHeight(0).Label("Document blocks")
		if list.Shortcut(ui.Cmd, ui.KeyC) && state.Selection != nil {
			if text, err := copyViewSelection(snap, state); err == nil {
				c.WriteClipboard(text)
			}
		}
	})
	return root
}

func richBlock(c *ui.Context, b Block, matches []Range, selected *Range) *ui.Element {
	if len(matches) == 0 && selected == nil {
		return ui.Text(c, b.Text)
	}
	spans := make([]ui.Span, 0, len(matches)*2+3)
	at, mi := 0, 0
	for at < len(b.Text) {
		for mi < len(matches) && matches[mi].End <= at {
			mi++
		}
		next := len(b.Text)
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
		span := ui.Span{Text: b.Text[at:next]}
		inMatch := mi < len(matches) && at >= matches[mi].Start && next <= matches[mi].End
		inSelection := selected != nil && at >= selected.Start && next <= selected.End
		if inMatch || inSelection {
			span.Background = c.Theme().Selection
		}
		spans = append(spans, span)
		at = next
	}
	return ui.RichText(c, spans...)
}

const MaxPaintMatches = 512

func blockByID(s *Snapshot, id uint64) *Block {
	if i, ok := s.blockIndex[id]; ok {
		return &s.Blocks[i]
	}
	return nil
}
func copyViewSelection(s *Snapshot, state *ViewState) (string, error) {
	if state.Selection == nil {
		return "", nil
	}
	return s.Copy(*state.Selection)
}

// PrecisePointerSelection is false until MyGo exposes shaped glyph/cluster
// hit-testing. Fixed pixels-per-rune guesses are incorrect for proportional,
// wrapped, ligature, bidi, and fallback-font text.
const PrecisePointerSelection = false

func selectAll(s *Snapshot, state *ViewState) {
	if len(s.Blocks) == 0 {
		return
	}
	a, b := s.Blocks[0], s.Blocks[len(s.Blocks)-1]
	state.Selection = &Selection{Anchor: Point{a.ID, 0}, Head: Point{b.ID, len(b.Text)}}
	state.Caret = state.Selection.Head
}

func ensureSelection(s *Snapshot, state *ViewState, row, fallback int) {
	if state.Selection != nil || row < 0 || row >= len(s.Blocks) {
		return
	}
	p := state.Caret
	if p.BlockID != s.Blocks[row].ID {
		p = Point{s.Blocks[row].ID, fallback}
	} else if _, _, err := s.point(p); err != nil {
		p = Point{s.Blocks[row].ID, fallback}
	}
	state.Selection = &Selection{Anchor: p, Head: p}
	state.Caret = p
}

func moveSelection(s *Snapshot, state *ViewState, direction int) {
	if state.Selection == nil {
		return
	}
	i, at, err := s.point(state.Selection.Head)
	if err != nil {
		return
	}
	text := s.Blocks[i].Text
	if direction > 0 {
		if at < len(text) {
			_, n := utf8.DecodeRuneInString(text[at:])
			at += n
		} else if i+1 < len(s.Blocks) {
			i++
			at = 0
		}
	} else {
		if at > 0 {
			_, n := utf8.DecodeLastRuneInString(text[:at])
			at -= n
		} else if i > 0 {
			i--
			at = len(s.Blocks[i].Text)
		}
	}
	p := Point{s.Blocks[i].ID, at}
	state.Selection.Head = p
	state.Caret = p
	state.List.ScrollIntoView(i)
}
func moveSelectionBlock(s *Snapshot, state *ViewState, direction int) {
	if state.Selection == nil {
		return
	}
	i, at, err := s.point(state.Selection.Head)
	if err != nil {
		return
	}
	i = min(max(i+direction, 0), len(s.Blocks)-1)
	at = min(at, len(s.Blocks[i].Text))
	at = charBoundary(s.Blocks[i].Text, at)
	p := Point{s.Blocks[i].ID, at}
	state.Selection.Head = p
	state.Caret = p
	state.List.ScrollIntoView(i)
}
func selectionRange(s *Snapshot, selection *Selection, index int) *Range {
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
	r := Range{0, len(s.Blocks[index].Text)}
	if index == a {
		r.Start = ao
	}
	if index == b {
		r.End = bo
	}
	return &r
}
