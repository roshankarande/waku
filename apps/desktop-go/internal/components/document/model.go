// Package document prepares source-backed rich documents for Waku's native UI.
// Parsing and searching are deliberately separate from View: callers run them
// on workers and publish immutable Snapshots to the UI thread.
package document

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Range is a half-open byte range in the original UTF-8 Markdown source.
type Range struct{ Start, End int }

func (r Range) validFor(s string) bool {
	return r.Start >= 0 && r.Start <= r.End && r.End <= len(s) &&
		utf8.ValidString(s[r.Start:r.End])
}

type Kind uint8

const (
	Paragraph Kind = iota
	Heading
	CodeBlock
	Table
	Math
)

const (
	// MaxDocumentSourceBytes bounds work per preparation. Source is retained so
	// callers can report truncation without losing the provider payload.
	MaxDocumentSourceBytes = 8 << 20
	// MaxBlockTextBytes keeps one native text layout from becoming pathological.
	// Larger literal blocks are split at UTF-8 boundaries before they reach View.
	MaxBlockTextBytes = 64 << 10
)

// Block is immutable, render-ready data. Text is what selection and copy use;
// Source remains the stable coordinate space used by provider/search actions.
type Block struct {
	ID               uint64
	Part             int
	Kind             Kind
	Source           Range
	Text             string
	Level            int
	Language         string
	Rows             [][]string
	Formula          Formula
	identityStart    int
	hasIdentityStart bool
}

type Formula struct {
	Source  string
	Display bool
}

type Snapshot struct {
	Generation uint64
	Source     string
	Blocks     []Block
	Truncated  bool
	stream     *streamSource
	blockIndex map[uint64]int
}

type streamSource struct {
	mu sync.Mutex
	b  strings.Builder
}

// Prepare parses Markdown synchronously. It performs no I/O and is intended
// for a bounded worker; View never calls it.
func Prepare(source string, generation uint64) *Snapshot {
	s := &Snapshot{Generation: generation, Source: source}
	prepared := source
	if len(prepared) > MaxDocumentSourceBytes {
		end := charBoundary(prepared, MaxDocumentSourceBytes)
		prepared = prepared[:end]
		s.Truncated = true
	}
	lines := lineRanges(prepared)
	for i := 0; i < len(lines); {
		line := prepared[lines[i].Start:lines[i].End]
		trim := strings.TrimSuffix(line, "\n")
		if strings.HasPrefix(trim, "```") || strings.HasPrefix(trim, "~~~") {
			marker := trim[:3]
			start := lines[i].Start
			lang := strings.TrimSpace(trim[3:])
			i++
			var body strings.Builder
			for i < len(lines) {
				v := strings.TrimSuffix(prepared[lines[i].Start:lines[i].End], "\n")
				if strings.HasPrefix(v, marker) {
					i++
					break
				}
				body.WriteString(prepared[lines[i].Start:lines[i].End])
				i++
			}
			text := strings.TrimSuffix(body.String(), "\n")
			end := start
			if i > 0 {
				end = lines[i-1].End
			}
			s.add(Block{Kind: CodeBlock, Source: Range{start, end}, Text: text, Language: lang})
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(trim), "$$") {
			start := lines[i].Start
			first := strings.TrimSpace(trim)
			var formula strings.Builder
			if len(first) > 2 {
				formula.WriteString(strings.TrimSuffix(strings.TrimPrefix(first, "$$"), "$$"))
				i++
			} else {
				i++
				for i < len(lines) {
					v := strings.TrimSpace(strings.TrimSuffix(source[lines[i].Start:lines[i].End], "\n"))
					if v == "$$" {
						i++
						break
					}
					if formula.Len() > 0 {
						formula.WriteByte('\n')
					}
					formula.WriteString(v)
					i++
				}
			}
			end := start
			if i > 0 {
				end = lines[i-1].End
			}
			f := formula.String()
			s.add(Block{Kind: Math, Source: Range{start, end}, Text: f, Formula: Formula{Source: f, Display: true}})
			continue
		}
		if isTableStart(prepared, lines, i) {
			start := lines[i].Start
			rows := [][]string{splitTable(trim)}
			i += 2 // the alignment row is structure, not selectable content
			for i < len(lines) {
				v := strings.TrimSuffix(prepared[lines[i].Start:lines[i].End], "\n")
				if !strings.Contains(v, "|") || strings.TrimSpace(v) == "" {
					break
				}
				rows = append(rows, splitTable(v))
				i++
			}
			end := lines[i-1].End
			parts := make([]string, 0, len(rows))
			for _, row := range rows {
				parts = append(parts, strings.Join(row, "\t"))
			}
			s.add(Block{Kind: Table, Source: Range{start, end}, Text: strings.Join(parts, "\n"), Rows: rows})
			continue
		}
		if strings.TrimSpace(trim) == "" {
			i++
			continue
		}
		start, kind, level := lines[i].Start, Paragraph, 0
		if n := headingLevel(trim); n > 0 {
			kind, level = Heading, n
		}
		var body strings.Builder
		for i < len(lines) {
			v := strings.TrimSuffix(prepared[lines[i].Start:lines[i].End], "\n")
			if strings.TrimSpace(v) == "" || (body.Len() > 0 && (headingLevel(v) > 0 || strings.HasPrefix(v, "```") || strings.HasPrefix(v, "~~~"))) {
				break
			}
			if body.Len() > 0 {
				body.WriteByte('\n')
			}
			if body.Len() == 0 && kind == Heading {
				v = strings.TrimSpace(v[level:])
			}
			body.WriteString(v)
			i++
		}
		end := lines[i-1].End
		s.add(Block{Kind: kind, Source: Range{start, end}, Text: body.String(), Level: level})
	}
	s.indexBlocks()
	return s
}

func (s *Snapshot) indexBlocks() {
	s.blockIndex = make(map[uint64]int, len(s.Blocks))
	for i := range s.Blocks {
		s.blockIndex[s.Blocks[i].ID] = i
	}
}

func (s *Snapshot) add(b Block) {
	identityStart := b.Source.Start
	limit := MaxBlockTextBytes
	if b.Kind == Math {
		limit = MaxMathSourceBytes
	}
	if len(b.Text) > limit {
		raw := s.Source[b.Source.Start:b.Source.End]
		at := strings.Index(raw, b.Text)
		for start, partIndex := 0, 0; start < len(b.Text); partIndex++ {
			end := charBoundary(b.Text, min(start+limit, len(b.Text)))
			part := b
			part.identityStart, part.hasIdentityStart = identityStart, true
			part.Part = partIndex
			part.Text = b.Text[start:end]
			if b.Kind == Math {
				part.Formula.Source = part.Text
			}
			if b.Kind == Table {
				// Do not retain the original (possibly huge) row matrix on every
				// split part. The bounded text is the selectable canonical form.
				part.Rows = tableRowsFromText(part.Text)
			}
			if at >= 0 {
				part.Source = Range{b.Source.Start + at + start, b.Source.Start + at + end}
			}
			s.addOne(part)
			start = end
		}
		return
	}
	s.addOne(b)
}

func tableRowsFromText(text string) [][]string {
	lines := strings.Split(text, "\n")
	rows := make([][]string, len(lines))
	for i := range lines {
		rows[i] = strings.Split(lines[i], "\t")
	}
	return rows
}

func (s *Snapshot) addOne(b Block) {
	h := fnv.New64a()
	// A streamed append may extend or reclassify the active block. Identity is
	// anchored only to its source position and bounded part, never mutable kind.
	identityStart := b.Source.Start
	if b.hasIdentityStart {
		identityStart = b.identityStart
	}
	fmt.Fprintf(h, "%d:%d", identityStart, b.Part)
	b.ID = h.Sum64()
	s.Blocks = append(s.Blocks, b)
}

func charBoundary(s string, at int) int {
	at = min(max(at, 0), len(s))
	for at > 0 && !utf8.ValidString(s[:at]) {
		at--
	}
	return at
}

func lineRanges(s string) []Range {
	if s == "" {
		return nil
	}
	var out []Range
	for start := 0; start < len(s); {
		n := strings.IndexByte(s[start:], '\n')
		if n < 0 {
			out = append(out, Range{start, len(s)})
			break
		}
		out = append(out, Range{start, start + n + 1})
		start += n + 1
	}
	return out
}

func headingLevel(s string) int {
	n := 0
	for n < len(s) && n < 6 && s[n] == '#' {
		n++
	}
	if n > 0 && n < len(s) && s[n] == ' ' {
		return n
	}
	return 0
}

func splitTable(s string) []string {
	s = strings.TrimSpace(strings.Trim(s, "|"))
	parts := strings.Split(s, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func isTableStart(source string, lines []Range, i int) bool {
	if i+1 >= len(lines) || !strings.Contains(source[lines[i].Start:lines[i].End], "|") {
		return false
	}
	next := strings.TrimSpace(strings.Trim(source[lines[i+1].Start:lines[i+1].End], "|\n"))
	if next == "" {
		return false
	}
	for _, cell := range strings.Split(next, "|") {
		cell = strings.Trim(strings.TrimSpace(cell), ":")
		if len(cell) < 3 || strings.Trim(cell, "-") != "" {
			return false
		}
	}
	return true
}

// Append prepares the next streamed snapshot. Existing blocks wholly before
// the appended suffix retain both source ranges and IDs.
func (s *Snapshot) Append(delta string) *Snapshot {
	truncated := s.Truncated
	if remaining := MaxDocumentSourceBytes - len(s.Source); remaining <= 0 {
		out := &Snapshot{Generation: s.Generation + 1, Source: s.Source, Blocks: s.Blocks, Truncated: true, stream: s.stream}
		out.indexBlocks()
		return out
	} else if len(delta) > remaining {
		delta = delta[:charBoundary(delta, remaining)]
		truncated = true
	}
	stream := s.stream
	if stream == nil {
		stream = &streamSource{}
		stream.b.Grow(min(MaxDocumentSourceBytes, len(s.Source)+max(len(delta)*8, 4096)))
		stream.b.WriteString(s.Source)
	}
	stream.mu.Lock()
	// Appending from an older snapshot forks rather than mutating the active
	// stream's newer source.
	if stream.b.Len() != len(s.Source) {
		stream.mu.Unlock()
		stream = &streamSource{}
		stream.b.Grow(min(MaxDocumentSourceBytes, len(s.Source)+max(len(delta)*8, 4096)))
		stream.b.WriteString(s.Source)
		stream.mu.Lock()
	}
	stream.b.WriteString(delta)
	combined := stream.b.String() // zero-copy view; future writes only append.
	stream.mu.Unlock()

	tailStart := 0
	basePart, identityStart, continueParts := 0, 0, false
	for i := len(s.Blocks) - 1; i >= 0; i-- {
		if s.Blocks[i].Part == 0 {
			tailStart = s.Blocks[i].Source.Start
			break
		}
	}
	// A huge plain paragraph cannot make earlier 64 KiB parts structural.
	// Settle them and reparse only its bounded unfinished part.
	if len(s.Blocks) > 0 {
		last := s.Blocks[len(s.Blocks)-1]
		logicalSource := s.Source[tailStart:last.Source.End]
		// A paragraph whose first line contains a pipe can still become a
		// table when its alignment line arrives. Keep its header in the active
		// parse tail rather than freezing earlier split parts prematurely.
		firstLine, _, _ := strings.Cut(logicalSource, "\n")
		mayBecomeTable := strings.Contains(firstLine, "|")
		if last.Kind == Paragraph && last.Part > 0 && !mayBecomeTable {
			basePart, continueParts = last.Part, true
			identityStart = last.identityStart
			if !last.hasIdentityStart {
				identityStart = tailStart
			}
			tailStart = last.Source.Start
		}
	}
	if tailStart > len(combined) {
		tailStart = 0
	}
	tail := Prepare(combined[tailStart:], s.Generation+1)
	out := &Snapshot{Generation: s.Generation + 1, Source: combined, Truncated: truncated || tail.Truncated, stream: stream}
	for _, b := range s.Blocks {
		if b.Source.End <= tailStart {
			out.Blocks = append(out.Blocks, b)
		}
	}
	firstGroup := true
	for i, b := range tail.Blocks {
		if continueParts && firstGroup {
			if i > 0 && b.Part == 0 {
				firstGroup = false
			} else {
				b.Part += basePart
				b.identityStart, b.hasIdentityStart = identityStart, true
			}
		}
		b.Source.Start += tailStart
		b.Source.End += tailStart
		out.addOne(b)
	}
	out.indexBlocks()
	return out
}

func (s *Snapshot) Validate() error {
	last := 0
	var previous Range
	for _, b := range s.Blocks {
		repeatedStructuralPart := b.Part > 0 && b.Source == previous
		if !b.Source.validFor(s.Source) || (b.Source.Start < last && !repeatedStructuralPart) {
			return fmt.Errorf("invalid block source range %+v", b.Source)
		}
		if !repeatedStructuralPart {
			last = b.Source.End
		}
		previous = b.Source
	}
	return nil
}
