// Package code contains Waku-owned, render-ready code and diff models.
// Parsing and highlighting happen before a Snapshot reaches View.
package code

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type LineKind uint8

const (
	Context LineKind = iota
	Addition
	Deletion
	HunkHeader
	FileHeader
	Meta
)

type TokenKind uint8

const (
	Keyword TokenKind = iota
	String
	Comment
	Number
)

type Range struct{ Start, End int }
type Token struct {
	Range Range
	Kind  TokenKind
}

type Line struct {
	ID               uint64
	Kind             LineKind
	File             string
	OldLine, NewLine int // zero means absent
	Text             string
	Tokens           []Token
}

type Snapshot struct {
	Generation  uint64
	Lines       []Line
	Truncated   bool
	TotalLines  int
	lineIndex   map[uint64]int
	source      string
	stream      *diffStream
	tailStart   int
	tailOrdinal int
	tailState   diffParseState
	settled     int
}

type diffStream struct {
	mu sync.Mutex
	b  strings.Builder
}

type diffParseState struct {
	file             string
	oldLine, newLine int
}

const (
	MaxDiffRows       = 50_000
	MaxDiffInputBytes = 32 << 20
	MaxCodeLineBytes  = 64 << 10
)

// PrepareDiff parses unified diff text and highlights line content. Call it on
// a worker; the UI only indexes the returned snapshot.
func PrepareDiff(patch string, generation uint64) *Snapshot {
	s := &Snapshot{Generation: generation}
	if len(patch) > MaxDiffInputBytes {
		patch = patch[:validBoundary(patch, MaxDiffInputBytes)]
		s.Truncated = true
	}
	s.source = patch
	s.Lines, s.TotalLines, s.tailStart, s.tailOrdinal, s.tailState, s.settled, s.Truncated = parseDiff(patch, 0, diffParseState{}, MaxDiffRows, s.Truncated)
	s.indexLines()
	return s
}

func parseDiff(patch string, ordinalBase int, state diffParseState, maxRows int, truncated bool) ([]Line, int, int, int, diffParseState, int, bool) {
	var lines []Line
	total, offset := 0, 0
	tailStart, tailOrdinal, tailState, settled := 0, ordinalBase, state, 0
	for ordinal, rest := ordinalBase, patch; ; ordinal++ {
		before := state
		raw, next, found := strings.Cut(rest, "\n")
		consumed := len(raw)
		raw = strings.TrimSuffix(raw, "\r")
		if len(raw) > MaxCodeLineBytes {
			raw = raw[:validBoundary(raw, MaxCodeLineBytes)]
			truncated = true
		}
		kind, text := Meta, raw
		switch {
		case strings.HasPrefix(raw, "diff --git "):
			kind = FileHeader
			parts := strings.Fields(raw)
			if len(parts) >= 4 {
				state.file = strings.TrimPrefix(parts[3], "b/")
			}
		case strings.HasPrefix(raw, "@@"):
			kind = HunkHeader
			state.oldLine, state.newLine = parseHunk(raw)
		case strings.HasPrefix(raw, "+") && !strings.HasPrefix(raw, "+++"):
			kind, text = Addition, strings.TrimPrefix(raw, "+")
			state.newLine++
		case strings.HasPrefix(raw, "-") && !strings.HasPrefix(raw, "---"):
			kind, text = Deletion, strings.TrimPrefix(raw, "-")
			state.oldLine++
		case strings.HasPrefix(raw, " "):
			kind, text = Context, strings.TrimPrefix(raw, " ")
			state.oldLine++
			state.newLine++
		}
		line := Line{Kind: kind, File: state.file, Text: text, Tokens: highlight(text)}
		switch kind {
		case Addition:
			line.NewLine = state.newLine - 1
		case Deletion:
			line.OldLine = state.oldLine - 1
		case Context:
			line.OldLine, line.NewLine = state.oldLine-1, state.newLine-1
		}
		line.ID = lineID(ordinal)
		total++
		if len(lines) < maxRows {
			lines = append(lines, line)
		} else {
			truncated = true
			break
		}
		if !found {
			tailStart, tailOrdinal, tailState = offset, ordinal, before
			settled = len(lines) - 1
			break
		}
		offset += consumed + 1
		rest = next
	}
	return lines, total, tailStart, tailOrdinal, tailState, settled, truncated
}

func (s *Snapshot) indexLines() {
	s.lineIndex = make(map[uint64]int, len(s.Lines))
	for i := range s.Lines {
		s.lineIndex[s.Lines[i].ID] = i
	}
}

func parseHunk(s string) (int, int) {
	parts := strings.Fields(s)
	if len(parts) < 3 {
		return 0, 0
	}
	parse := func(v string) int {
		v = strings.TrimLeft(v, "+-")
		v = strings.Split(v, ",")[0]
		n, _ := strconv.Atoi(v)
		return n
	}
	// Counters are the line before the first row, because rows increment first.
	return parse(parts[1]), parse(parts[2])
}

func lineID(ordinal int) uint64 {
	h := fnv.New64a()
	fmt.Fprint(h, ordinal)
	return h.Sum64()
}

// Append incrementally reparses only the unfinished physical row. Completed
// rows retain IDs and prepared tokens; input remains bounded by the same cap.
func (s *Snapshot) Append(delta string) *Snapshot {
	clipped := false
	if remaining := MaxDiffInputBytes - len(s.source); remaining <= 0 || s.Truncated && len(s.Lines) >= MaxDiffRows {
		out := *s
		out.Generation++
		out.Truncated = true
		return &out
	} else if len(delta) > remaining {
		delta = delta[:validBoundary(delta, remaining)]
		clipped = true
	}
	stream := s.stream
	if stream == nil {
		stream = &diffStream{}
		stream.b.Grow(min(MaxDiffInputBytes, len(s.source)+max(len(delta)*8, 4096)))
		stream.b.WriteString(s.source)
	}
	stream.mu.Lock()
	if stream.b.Len() != len(s.source) {
		stream.mu.Unlock()
		stream = &diffStream{}
		stream.b.Grow(min(MaxDiffInputBytes, len(s.source)+max(len(delta)*8, 4096)))
		stream.b.WriteString(s.source)
		stream.mu.Lock()
	}
	stream.b.WriteString(delta)
	combined := stream.b.String()
	stream.mu.Unlock()
	tail := combined[s.tailStart:]
	room := MaxDiffRows - s.settled
	parsed, total, tailStart, tailOrdinal, tailState, settled, truncated := parseDiff(tail, s.tailOrdinal, s.tailState, room, false)
	out := &Snapshot{Generation: s.Generation + 1, source: combined, stream: stream, Truncated: s.Truncated || truncated || clipped}
	out.Lines = make([]Line, 0, s.settled+len(parsed))
	out.Lines = append(out.Lines, s.Lines[:s.settled]...)
	out.Lines = append(out.Lines, parsed...)
	out.TotalLines = s.settled + total
	out.tailStart, out.tailOrdinal, out.tailState, out.settled = s.tailStart+tailStart, tailOrdinal, tailState, s.settled+settled
	out.indexLines()
	return out
}

func validBoundary(s string, at int) int {
	at = min(max(at, 0), len(s))
	for at > 0 && !utf8.ValidString(s[:at]) {
		at--
	}
	return at
}

func highlight(s string) []Token {
	var out []Token
	if i := strings.Index(s, "//"); i >= 0 {
		out = append(out, Token{Range: Range{i, len(s)}, Kind: Comment})
		s = s[:i]
	}
	for i := 0; i < len(s); {
		if s[i] == '"' || s[i] == '\'' {
			q, start := s[i], i
			i++
			for i < len(s) {
				if s[i] == '\\' {
					i += min(2, len(s)-i)
					continue
				}
				i++
				if s[i-1] == q {
					break
				}
			}
			out = append(out, Token{Range: Range{start, i}, Kind: String})
			continue
		}
		if s[i] >= '0' && s[i] <= '9' {
			start := i
			for i < len(s) && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
				i++
			}
			out = append(out, Token{Range: Range{start, i}, Kind: Number})
			continue
		}
		if isIdent(s[i]) {
			start := i
			for i < len(s) && isIdent(s[i]) {
				i++
			}
			if isKeyword(s[start:i]) {
				out = append(out, Token{Range: Range{start, i}, Kind: Keyword})
			}
			continue
		}
		i++
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Range.Start < out[j].Range.Start })
	return out
}

func isIdent(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}
func isKeyword(s string) bool {
	switch s {
	case "break", "case", "const", "continue", "else", "for", "func", "if", "import", "let", "package", "return", "struct", "type", "var":
		return true
	}
	return false
}

type Point struct {
	LineID uint64
	Offset int
}
type Selection struct{ Anchor, Head Point }

func (s *Snapshot) Copy(sel Selection) (string, error) {
	a, ao, e := s.point(sel.Anchor)
	if e != nil {
		return "", e
	}
	b, bo, e := s.point(sel.Head)
	if e != nil {
		return "", e
	}
	if a > b || a == b && ao > bo {
		a, b, ao, bo = b, a, bo, ao
	}
	parts := make([]string, 0, b-a+1)
	for i := a; i <= b; i++ {
		from, to := 0, len(s.Lines[i].Text)
		if i == a {
			from = ao
		}
		if i == b {
			to = bo
		}
		parts = append(parts, s.Lines[i].Text[from:to])
	}
	return strings.Join(parts, "\n"), nil
}

func (s *Snapshot) point(p Point) (int, int, error) {
	if i, ok := s.lineIndex[p.LineID]; ok {
		l := s.Lines[i]
		if p.Offset < 0 || p.Offset > len(l.Text) || !utf8.ValidString(l.Text[:p.Offset]) {
			return 0, 0, fmt.Errorf("code: invalid UTF-8 offset %d for line %d", p.Offset, p.LineID)
		}
		return i, p.Offset, nil
	}
	return 0, 0, fmt.Errorf("code: unknown line %d", p.LineID)
}

type Match struct {
	LineID uint64
	Range  Range
}

func (s *Snapshot) Search(q string) []Match {
	matches, _ := s.SearchContext(context.Background(), q)
	return matches
}

func (s *Snapshot) SearchContext(ctx context.Context, q string) ([]Match, error) {
	if q == "" {
		return nil, nil
	}
	q, _, err := foldWithMapContext(ctx, q)
	if err != nil {
		return nil, err
	}
	var out []Match
	for _, l := range s.Lines {
		if len(out) >= MaxSearchMatches {
			break
		}
		low, offsets, err := foldWithMapContext(ctx, l.Text)
		if err != nil {
			return nil, err
		}
		for from := 0; len(out) < MaxSearchMatches; {
			i := strings.Index(low[from:], q)
			if i < 0 {
				break
			}
			i += from
			out = append(out, Match{l.ID, Range{offsets[i], offsets[i+len(q)]}})
			from = i + max(1, len(q))
		}
	}
	return out, nil
}

func foldWithMap(s string) (string, []int) {
	folded, offsets, _ := foldWithMapContext(context.Background(), s)
	return folded, offsets
}

func foldWithMapContext(ctx context.Context, s string) (string, []int, error) {
	var out strings.Builder
	var offsets []int
	runes := 0
	for at, r := range s {
		if runes%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return "", nil, err
			}
		}
		runes++
		folded := string(simpleFoldRune(r))
		out.WriteString(folded)
		for range len(folded) {
			offsets = append(offsets, at)
		}
	}
	offsets = append(offsets, len(s))
	return out.String(), offsets, nil
}

const MaxSearchMatches = 10_000

func simpleFoldRune(r rune) rune {
	best := r
	for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
		if next < best {
			best = next
		}
	}
	return best
}
