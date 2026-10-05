package document

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Point identifies a byte boundary in one block's rendered/selectable text.
// It stays meaningful when that block is outside the virtualized viewport.
type Point struct {
	BlockID uint64
	Offset  int
}

type Selection struct{ Anchor, Head Point }

type Match struct {
	BlockID uint64
	Text    Range
	Source  Range
}

func (s *Snapshot) Search(query string) []Match {
	matches, _ := s.SearchContext(context.Background(), query)
	return matches
}

func (s *Snapshot) SearchContext(ctx context.Context, query string) ([]Match, error) {
	if query == "" {
		return nil, nil
	}
	needle, _, err := foldWithMapContext(ctx, query)
	if err != nil {
		return nil, err
	}
	var out []Match
	for _, b := range s.Blocks {
		if len(out) >= MaxSearchMatches {
			break
		}
		hay, offsets, err := foldWithMapContext(ctx, b.Text)
		if err != nil {
			return nil, err
		}
		for at := 0; at <= len(hay)-len(needle) && len(out) < MaxSearchMatches; {
			i := strings.Index(hay[at:], needle)
			if i < 0 {
				break
			}
			i += at
			// Exact source coordinates are available where rendered text is a
			// literal slice. For structural Markdown they conservatively cover
			// the owning block, never an invented offset.
			textStart, textEnd := offsets[i], offsets[i+len(needle)]
			source := b.Source
			if strings.Contains(s.Source[b.Source.Start:b.Source.End], b.Text) {
				base := strings.Index(s.Source[b.Source.Start:b.Source.End], b.Text)
				source = Range{b.Source.Start + base + textStart, b.Source.Start + base + textEnd}
			}
			out = append(out, Match{BlockID: b.ID, Text: Range{textStart, textEnd}, Source: source})
			at = i + max(1, len(needle))
		}
	}
	return out, nil
}

// foldWithMap performs Unicode simple case folding while retaining byte maps
// back to the original UTF-8 string. Members of one fold class can have
// different UTF-8 lengths, so folded offsets must not index source text.
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

// Copy joins selected block fragments in document order. It depends on the
// immutable snapshot, not on mounted rows, so scrolling cannot lose selection.
func (s *Snapshot) Copy(sel Selection) (string, error) {
	a, ao, err := s.point(sel.Anchor)
	if err != nil {
		return "", err
	}
	b, bo, err := s.point(sel.Head)
	if err != nil {
		return "", err
	}
	if a > b || (a == b && ao > bo) {
		a, b, ao, bo = b, a, bo, ao
	}
	var out strings.Builder
	for i := a; i <= b; i++ {
		from, to := 0, len(s.Blocks[i].Text)
		if i == a {
			from = ao
		}
		if i == b {
			to = bo
		}
		if i > a && !continuationPart(s.Blocks[i-1], s.Blocks[i]) {
			out.WriteString("\n\n")
		}
		out.WriteString(s.Blocks[i].Text[from:to])
	}
	return out.String(), nil
}

func continuationPart(previous, current Block) bool {
	return current.Part > 0 && current.Part == previous.Part+1 && current.Kind == previous.Kind
}

func (s *Snapshot) point(p Point) (int, int, error) {
	if i, ok := s.blockIndex[p.BlockID]; ok {
		if p.Offset < 0 || p.Offset > len(s.Blocks[i].Text) || !utf8.ValidString(s.Blocks[i].Text[:p.Offset]) {
			return 0, 0, fmt.Errorf("document: invalid UTF-8 offset %d for block %d", p.Offset, p.BlockID)
		}
		return i, p.Offset, nil
	}
	return 0, 0, fmt.Errorf("document: unknown block %d", p.BlockID)
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
