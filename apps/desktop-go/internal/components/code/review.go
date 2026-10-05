package code

import (
	"errors"
	"strings"
)

type Hunk struct {
	ID                  uint64
	Header, First, Last int
	File                string
}
type ReviewSide uint8

const (
	OldSide ReviewSide = iota
	NewSide
)

type ReviewComment struct {
	ID     uint64
	LineID uint64
	Side   ReviewSide
	Body   string
}

type ContextRequest struct {
	Generation, HunkID uint64
	Before, After      int
}
type ContextExpansion struct {
	Generation, HunkID uint64
	Patch              string
}

type StageIntent struct {
	Generation uint64
	Paths      []string
	HunkIDs    []uint64
}
type CommitIntent struct {
	Stage   StageIntent
	Message string
}

type ReviewModel struct {
	Generation uint64
	Hunks      []Hunk
	Comments   []ReviewComment
	commentSeq uint64
	lineToHunk map[uint64]int
}

// FocusHunk/FocusLine bridge prepared navigation to the virtual list without
// requiring a host view to mount intervening rows.
func (r *ReviewModel) FocusHunk(s *Snapshot, state *ViewState, current uint64, delta int) (uint64, bool) {
	if s == nil || s.Generation != r.Generation {
		return 0, false
	}
	h, ok := r.NavigateHunk(current, delta)
	if !ok || h.Header < 0 || h.Header >= len(s.Lines) {
		return 0, false
	}
	state.List.ScrollIntoView(h.Header)
	state.Caret = Point{LineID: s.Lines[h.Header].ID}
	return h.ID, true
}

func (r *ReviewModel) FocusChangedLine(s *Snapshot, state *ViewState, current uint64, delta int) (uint64, bool) {
	if s == nil || s.Generation != r.Generation {
		return 0, false
	}
	l, ok := r.NavigateChangedLine(s, current, delta)
	if !ok {
		return 0, false
	}
	i := s.lineIndex[l.ID]
	state.List.ScrollIntoView(i)
	state.Caret = Point{LineID: l.ID}
	return l.ID, true
}

func PrepareReview(s *Snapshot) *ReviewModel {
	r := &ReviewModel{Generation: s.Generation, lineToHunk: make(map[uint64]int)}
	for i, l := range s.Lines {
		if l.Kind == HunkHeader {
			if n := len(r.Hunks); n > 0 {
				r.Hunks[n-1].Last = i - 1
			}
			r.Hunks = append(r.Hunks, Hunk{ID: l.ID, Header: i, First: i + 1, Last: len(s.Lines) - 1, File: l.File})
		}
		if len(r.Hunks) > 0 {
			r.lineToHunk[l.ID] = len(r.Hunks) - 1
		}
	}
	return r
}

func (r *ReviewModel) NavigateHunk(current uint64, delta int) (Hunk, bool) {
	if len(r.Hunks) == 0 || delta == 0 {
		return Hunk{}, false
	}
	i := -1
	for n, h := range r.Hunks {
		if h.ID == current {
			i = n
			break
		}
	}
	if i < 0 {
		if delta > 0 {
			return r.Hunks[0], true
		}
		return r.Hunks[len(r.Hunks)-1], true
	}
	i = min(max(i+delta, 0), len(r.Hunks)-1)
	return r.Hunks[i], true
}
func (r *ReviewModel) NavigateChangedLine(s *Snapshot, current uint64, delta int) (Line, bool) {
	if s == nil || s.Generation != r.Generation || delta == 0 {
		return Line{}, false
	}
	i := -1
	if at, ok := s.lineIndex[current]; ok {
		i = at
	}
	if i < 0 && delta < 0 {
		i = len(s.Lines)
	}
	for i += delta; i >= 0 && i < len(s.Lines); i += delta {
		if s.Lines[i].Kind == Addition || s.Lines[i].Kind == Deletion {
			return s.Lines[i], true
		}
	}
	return Line{}, false
}
func (r *ReviewModel) ContextIntent(hunk uint64, before, after int) (ContextRequest, error) {
	if before < 0 || after < 0 || before > 10_000 || after > 10_000 || before > 10_000-after {
		return ContextRequest{}, errors.New("code: invalid context expansion")
	}
	for _, h := range r.Hunks {
		if h.ID == hunk {
			return ContextRequest{r.Generation, hunk, before, after}, nil
		}
	}
	return ContextRequest{}, errors.New("code: unknown hunk")
}

// ApplyContext validates the async result and prepares a replacement snapshot.
// Fetching context is the workspace's responsibility.
func (r *ReviewModel) ApplyContext(x ContextExpansion) (*Snapshot, bool) {
	if x.Generation != r.Generation {
		return nil, false
	}
	for _, h := range r.Hunks {
		if h.ID == x.HunkID {
			s := PrepareDiff(x.Patch, r.Generation+1)
			return s, true
		}
	}
	return nil, false
}
func (r *ReviewModel) AddComment(s *Snapshot, line uint64, side ReviewSide, body string) (ReviewComment, error) {
	if s == nil {
		return ReviewComment{}, errors.New("code: invalid review comment")
	}
	l := lineByID(s, line)
	if s.Generation != r.Generation || l == nil || (side != OldSide && side != NewSide) || (side == OldSide && l.OldLine == 0) || (side == NewSide && l.NewLine == 0) || strings.TrimSpace(body) == "" {
		return ReviewComment{}, errors.New("code: invalid review comment")
	}
	r.commentSeq++
	c := ReviewComment{ID: r.commentSeq, LineID: line, Side: side, Body: body}
	r.Comments = append(r.Comments, c)
	return c, nil
}
func (r *ReviewModel) Stage(paths []string, hunks []uint64) (StageIntent, error) {
	known := map[uint64]bool{}
	for _, h := range r.Hunks {
		known[h.ID] = true
	}
	for _, h := range hunks {
		if !known[h] {
			return StageIntent{}, errors.New("code: unknown staged hunk")
		}
	}
	if len(paths) == 0 && len(hunks) == 0 {
		return StageIntent{}, errors.New("code: empty stage intent")
	}
	return StageIntent{Generation: r.Generation, Paths: append([]string(nil), paths...), HunkIDs: append([]uint64(nil), hunks...)}, nil
}
func (r *ReviewModel) Commit(stage StageIntent, message string) (CommitIntent, error) {
	if stage.Generation != r.Generation || strings.TrimSpace(message) == "" {
		return CommitIntent{}, errors.New("code: invalid commit intent")
	}
	stage.Paths = append([]string(nil), stage.Paths...)
	stage.HunkIDs = append([]uint64(nil), stage.HunkIDs...)
	return CommitIntent{Stage: stage, Message: message}, nil
}
