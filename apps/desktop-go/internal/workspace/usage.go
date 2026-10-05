package workspace

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type UsageWindow struct {
	Kind  string
	Count uint32
}

type UsageHistoryConfig struct {
	ClaudeRoot, CodexRoot, RatesCachePath        string
	MaxFiles, MaxModels, MaxProjects, MaxRecords int
	MaxFileBytes, MaxTotalBytes                  int64
	MaxLineBytes                                 int
	MaxRates, MaxRatesBytes                      int
	RatesTTL                                     time.Duration
	Now                                          func() time.Time
}

func DefaultUsageHistoryConfig() UsageHistoryConfig {
	home, _ := os.UserHomeDir()
	claude := filepath.Join(home, ".claude", "projects")
	if configured := os.Getenv("CLAUDE_CONFIG_DIR"); filepath.IsAbs(configured) {
		claude = filepath.Join(configured, "projects")
	}
	codex := filepath.Join(home, ".codex", "sessions")
	if configured := os.Getenv("CODEX_HOME"); filepath.IsAbs(configured) {
		codex = filepath.Join(configured, "sessions")
	}
	return UsageHistoryConfig{ClaudeRoot: claude, CodexRoot: codex, MaxFiles: 4096, MaxModels: 1024, MaxProjects: 1024, MaxRecords: 1_000_000, MaxFileBytes: 64 << 20, MaxTotalBytes: 256 << 20, MaxLineBytes: 4 << 20, MaxRates: 20_000, MaxRatesBytes: 8 << 20, RatesTTL: 24 * time.Hour, Now: time.Now}
}

type UsageHistoryService struct{ config UsageHistoryConfig }

func NewUsageHistoryService(config UsageHistoryConfig) (*UsageHistoryService, error) {
	defaults := DefaultUsageHistoryConfig()
	if config.MaxFiles == 0 {
		config.MaxFiles = defaults.MaxFiles
	}
	if config.MaxModels == 0 {
		config.MaxModels = defaults.MaxModels
	}
	if config.MaxProjects == 0 {
		config.MaxProjects = defaults.MaxProjects
	}
	if config.MaxRecords == 0 {
		config.MaxRecords = defaults.MaxRecords
	}
	if config.MaxFileBytes == 0 {
		config.MaxFileBytes = defaults.MaxFileBytes
	}
	if config.MaxTotalBytes == 0 {
		config.MaxTotalBytes = defaults.MaxTotalBytes
	}
	if config.MaxLineBytes == 0 {
		config.MaxLineBytes = defaults.MaxLineBytes
	}
	if config.MaxRates == 0 {
		config.MaxRates = defaults.MaxRates
	}
	if config.MaxRatesBytes == 0 {
		config.MaxRatesBytes = defaults.MaxRatesBytes
	}
	if config.RatesTTL == 0 {
		config.RatesTTL = defaults.RatesTTL
	}
	if config.Now == nil {
		config.Now = defaults.Now
	}
	if config.MaxFiles < 1 || config.MaxModels < 1 || config.MaxProjects < 1 || config.MaxRecords < 1 || config.MaxFileBytes < 1 || config.MaxTotalBytes < 1 || config.MaxLineBytes < 1 || config.MaxRates < 1 || config.MaxRatesBytes < 1 || config.RatesTTL < 1 {
		return nil, errors.New("usage history limits must be positive")
	}
	return &UsageHistoryService{config: config}, nil
}

type usageTotals struct {
	UncachedInput uint64 `json:"uncachedInput"`
	CachedInput   uint64 `json:"cachedInput"`
	CacheCreation uint64 `json:"cacheCreation"`
	Output        uint64 `json:"output"`
	Reasoning     uint64 `json:"reasoning"`
}

func (t *usageTotals) add(v usageTotals) {
	t.UncachedInput = saturatingAdd(t.UncachedInput, v.UncachedInput)
	t.CachedInput = saturatingAdd(t.CachedInput, v.CachedInput)
	t.CacheCreation = saturatingAdd(t.CacheCreation, v.CacheCreation)
	t.Output = saturatingAdd(t.Output, v.Output)
	t.Reasoning = saturatingAdd(t.Reasoning, v.Reasoning)
}
func (t usageTotals) total() uint64 {
	return saturatingAdd(saturatingAdd(t.UncachedInput, t.CachedInput), saturatingAdd(t.CacheCreation, t.Output))
}
func saturatingAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

type usageRecord struct {
	provider, model, session, project, dedupe string
	timestamp                                 time.Time
	totals                                    usageTotals
	reportedCost                              *float64
}
type modelRate struct{ input, output, cacheRead, cacheCreation float64 }
type usageBucket struct {
	totals                      usageTotals
	cost, cacheSavings          float64
	records, reported, unpriced uint64
}
type providerDay struct {
	CostUSD     float64 `json:"costUsd"`
	TotalTokens uint64  `json:"totalTokens"`
}
type aggregate struct {
	buckets       map[string]*usageBucket
	sessions      map[string]bool
	monthSessions map[string]bool
	projects      map[string]*projectAggregate
	seen          map[string]bool
	projectRoots  []string
}
type projectAggregate struct {
	cost     float64
	tokens   uint64
	by       [2]providerDay
	sessions map[string]bool
	models   map[string]float64
	last     string
}

func (s *UsageHistoryService) Load(ctx context.Context, window UsageWindow, projectRoots []string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	since, until, err := usageBounds(window, s.config.Now().In(time.Local))
	if err != nil {
		return nil, err
	}
	rates, pricing, rateWarning := readUsageRates(s.config.RatesCachePath, s.config.Now(), s.config)
	agg := aggregate{buckets: map[string]*usageBucket{}, sessions: map[string]bool{}, monthSessions: map[string]bool{}, projects: map[string]*projectAggregate{}, seen: map[string]bool{}}
	for _, root := range projectRoots {
		if filepath.IsAbs(root) {
			agg.projectRoots = append(agg.projectRoots, filepath.Clean(root))
		}
	}
	sort.Slice(agg.projectRoots, func(i, j int) bool { return len(agg.projectRoots[i]) > len(agg.projectRoots[j]) })
	var scanned, skipped int
	var totalBytes int64
	acceptedRecords := 0
	var scanErrors []string
	scanErrors = make([]string, 0)
	if rateWarning != "" {
		scanErrors = append(scanErrors, rateWarning)
	}
	for _, source := range []struct{ provider, root string }{{"claude", s.config.ClaudeRoot}, {"codex", s.config.CodexRoot}} {
		if source.root == "" {
			continue
		}
		files, omitted, walkErr := s.transcriptFiles(ctx, source.root, since.Add(-36*time.Hour))
		skipped += omitted
		if walkErr != nil && !errors.Is(walkErr, fs.ErrNotExist) {
			scanErrors = append(scanErrors, fmt.Sprintf("%s transcripts at %s could not be read", source.provider, source.root))
		}
		for _, path := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			info, err := os.Stat(path)
			if err != nil {
				skipped++
				continue
			}
			if info.Size() > s.config.MaxFileBytes || totalBytes+info.Size() > s.config.MaxTotalBytes {
				skipped++
				continue
			}
			totalBytes += info.Size()
			scanned++
			records, err := s.readTranscript(ctx, source.provider, path)
			if err != nil {
				skipped++
				scanErrors = appendBounded(scanErrors, fmt.Sprintf("%s: %v", path, err), 32)
				continue
			}
			for _, record := range records {
				if acceptedRecords >= s.config.MaxRecords {
					skipped++
					break
				}
				agg.add(record, rates, since, until)
				acceptedRecords++
			}
		}
	}
	result := deriveUsageHistory(window, since, until, agg, pricing, scanned, skipped, scanErrors, time.Since(started), s.config)
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *UsageHistoryService) transcriptFiles(ctx context.Context, root string, cutoff time.Time) ([]string, int, error) {
	var files []string
	omitted := 0
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err != nil {
			omitted++
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 && entry.IsDir() {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), ".jsonl") {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			omitted++
			return nil
		}
		if info.ModTime().Before(cutoff) {
			omitted++
			return nil
		}
		if len(files) >= s.config.MaxFiles {
			omitted++
			return nil
		}
		files = append(files, path)
		return nil
	})
	sort.Strings(files)
	return files, omitted, err
}

func (s *UsageHistoryService) readTranscript(ctx context.Context, provider, path string) ([]usageRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(io.LimitReader(f, s.config.MaxFileBytes+1))
	scanner.Buffer(make([]byte, 64<<10), s.config.MaxLineBytes)
	var records []usageRecord
	seen := map[string]bool{}
	state := codexUsageState{}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := scanner.Bytes()
		var record usageRecord
		var ok bool
		if provider == "claude" {
			if !bytesContain(line, `"usage"`) {
				continue
			}
			record, ok = parseClaudeUsage(line)
		} else {
			record, ok = parseCodexUsage(line, &state)
		}
		if ok {
			if record.dedupe != "" && seen[record.dedupe] {
				continue
			}
			seen[record.dedupe] = true
			records = append(records, record)
			if len(records) >= s.config.MaxRecords {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func bytesContain(line []byte, text string) bool { return strings.Contains(string(line), text) }
func decodeObject(line []byte) (map[string]any, bool) {
	d := json.NewDecoder(strings.NewReader(string(line)))
	d.UseNumber()
	var v map[string]any
	err := d.Decode(&v)
	return v, err == nil
}
func parseTime(v any) (time.Time, bool) {
	text, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, e := time.Parse(time.RFC3339Nano, text)
	return t, e == nil
}
func object(v any) map[string]any { x, _ := v.(map[string]any); return x }
func text(v any) string           { x, _ := v.(string); return x }
func positiveUint(v any) uint64 {
	n, ok := v.(json.Number)
	if !ok {
		return 0
	}
	if u, e := strconv.ParseUint(string(n), 10, 64); e == nil {
		return u
	}
	f, e := strconv.ParseFloat(string(n), 64)
	if e != nil || f <= 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0
	}
	if f >= math.MaxUint64 {
		return math.MaxUint64
	}
	return uint64(f)
}
func finiteFloat(v any) (float64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	f, e := strconv.ParseFloat(string(n), 64)
	return f, e == nil && !math.IsInf(f, 0) && !math.IsNaN(f)
}

func parseClaudeUsage(line []byte) (usageRecord, bool) {
	v, ok := decodeObject(line)
	if !ok || text(v["type"]) != "assistant" {
		return usageRecord{}, false
	}
	message := object(v["message"])
	usage := object(message["usage"])
	timestamp, ok := parseTime(v["timestamp"])
	model := text(message["model"])
	if !ok || model == "" || usage == nil {
		return usageRecord{}, false
	}
	id, request := text(message["id"]), text(v["requestId"])
	dedupe := ""
	if id != "" || request != "" {
		dedupe = id + ":" + request
	}
	record := usageRecord{provider: "claude", model: model, session: text(v["sessionId"]), project: text(v["cwd"]), dedupe: dedupe, timestamp: timestamp, totals: usageTotals{UncachedInput: positiveUint(usage["input_tokens"]), CachedInput: positiveUint(usage["cache_read_input_tokens"]), CacheCreation: positiveUint(usage["cache_creation_input_tokens"]), Output: positiveUint(usage["output_tokens"])}}
	if cost, ok := finiteFloat(v["costUSD"]); ok {
		record.reportedCost = &cost
	}
	return record, true
}

type codexUsageState struct {
	model, session, cwd, last string
	sawMeta                   bool
	forkMode                  uint8 // 1 awaiting replay, 2 suppressing, 3 complete
	forkAnchor                time.Time
}

func parseCodexUsage(line []byte, state *codexUsageState) (usageRecord, bool) {
	v, ok := decodeObject(line)
	if !ok {
		return usageRecord{}, false
	}
	payload := object(v["payload"])
	kind := text(v["type"])
	if kind == "session_meta" {
		id := text(payload["id"])
		if id == "" {
			id = text(payload["session_id"])
		}
		stamp, _ := parseTime(v["timestamp"])
		if !state.sawMeta {
			state.sawMeta = true
			state.session = id
			state.cwd = text(payload["cwd"])
			spawn := object(object(object(payload["source"])["subagent"])["thread_spawn"])
			if text(payload["forked_from_id"]) != "" || text(spawn["parent_thread_id"]) != "" {
				state.forkMode, state.forkAnchor = 1, stamp
			}
		} else if state.forkMode == 1 {
			if id == state.session {
				state.forkMode = 3
			} else if id != "" {
				state.forkMode, state.forkAnchor = 2, stamp
			}
		} else if state.forkMode == 2 {
			if id == state.session {
				state.forkMode = 3
			} else if id != "" && !stamp.IsZero() {
				state.forkAnchor = stamp
			}
		}
		return usageRecord{}, false
	}
	if kind == "turn_context" {
		if model := text(payload["model"]); model != "" {
			state.model = model
		}
		if cwd := text(payload["cwd"]); cwd != "" {
			state.cwd = cwd
		}
		return usageRecord{}, false
	}
	if text(payload["type"]) != "token_count" || state.model == "" {
		return usageRecord{}, false
	}
	last := object(object(payload["info"])["last_token_usage"])
	if last == nil {
		return usageRecord{}, false
	}
	signatureBytes, _ := json.Marshal(last)
	signature := string(signatureBytes)
	if signature == state.last {
		return usageRecord{}, false
	}
	timestamp, ok := parseTime(v["timestamp"])
	if !ok {
		return usageRecord{}, false
	}
	state.last = signature
	if state.forkMode == 2 {
		if !state.forkAnchor.IsZero() && timestamp.Sub(state.forkAnchor) < time.Second {
			state.forkAnchor = timestamp
			return usageRecord{}, false
		}
		state.forkMode = 3
	} else if state.forkMode == 1 {
		state.forkMode = 3
	}
	input, cached, created, output := positiveUint(last["input_tokens"]), positiveUint(last["cached_input_tokens"]), positiveUint(last["cache_write_input_tokens"]), positiveUint(last["output_tokens"])
	used := saturatingAdd(cached, created)
	uncached := uint64(0)
	if input > used {
		uncached = input - used
	}
	totals := usageTotals{UncachedInput: uncached, CachedInput: cached, CacheCreation: created, Output: output, Reasoning: min(positiveUint(last["reasoning_output_tokens"]), output)}
	if totals.total() == 0 {
		return usageRecord{}, false
	}
	return usageRecord{provider: "codex", model: state.model, session: state.session, project: state.cwd, timestamp: timestamp, totals: totals}, true
}

func (a *aggregate) add(record usageRecord, rates map[string]modelRate, since, until time.Time) {
	if record.dedupe != "" {
		key := record.provider + ":" + record.dedupe
		if a.seen[key] {
			return
		}
		a.seen[key] = true
	}
	dayTime := record.timestamp.In(time.Local)
	day := dayTime.Format("2006-01-02")
	if day < since.Format("2006-01-02") || day > until.Format("2006-01-02") {
		return
	}
	cost, reported := 0.0, false
	unpriced := false
	rate, hasRate := rates[normalizeModel(record.model)]
	if record.reportedCost != nil {
		cost = *record.reportedCost
		reported = true
	} else if hasRate {
		cost = float64(record.totals.UncachedInput)*rate.input + float64(record.totals.CachedInput)*rate.cacheRead + float64(record.totals.CacheCreation)*rate.cacheCreation + float64(record.totals.Output)*rate.output
	} else {
		unpriced = true
	}
	key := day + "\x00" + record.provider + "\x00" + record.model
	b := a.buckets[key]
	if b == nil {
		b = &usageBucket{}
		a.buckets[key] = b
	}
	b.totals.add(record.totals)
	b.cost += cost
	if hasRate {
		b.cacheSavings += float64(record.totals.CachedInput) * (rate.input - rate.cacheRead)
	}
	b.records = saturatingAdd(b.records, 1)
	if reported {
		b.reported = saturatingAdd(b.reported, 1)
	}
	if unpriced {
		b.unpriced = saturatingAdd(b.unpriced, 1)
	}
	identity := record.provider + "\x00" + record.session
	if record.session != "" {
		a.sessions[identity] = true
		a.monthSessions[day[:7]+"\x00"+identity] = true
	}
	project := resolveUsageProject(record.project, a.projectRoots)
	p := a.projects[project]
	if p == nil {
		p = &projectAggregate{sessions: map[string]bool{}, models: map[string]float64{}}
		a.projects[project] = p
	}
	p.cost += cost
	p.tokens = saturatingAdd(p.tokens, record.totals.total())
	idx := 0
	if record.provider == "codex" {
		idx = 1
	}
	p.by[idx].CostUSD += cost
	p.by[idx].TotalTokens = saturatingAdd(p.by[idx].TotalTokens, record.totals.total())
	if record.session != "" {
		p.sessions[identity] = true
	}
	p.models[record.model] += cost
	if day > p.last {
		p.last = day
	}
}
func resolveUsageProject(cwd string, roots []string) string {
	if cwd == "" {
		return ""
	}
	clean := filepath.Clean(cwd)
	for _, root := range roots {
		rel, e := filepath.Rel(root, clean)
		if e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return root
		}
	}
	return cwd
}

func usageBounds(window UsageWindow, now time.Time) (time.Time, time.Time, error) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	switch window.Kind {
	case "trailingDays":
		days := window.Count
		if days < 1 {
			days = 1
		}
		if days > 3660 {
			return time.Time{}, time.Time{}, errors.New("usage trailing-days window exceeds 3660 days")
		}
		return today.AddDate(0, 0, -int(days-1)), today, nil
	case "months":
		months := window.Count
		if months < 1 {
			months = 1
		}
		if months > 120 {
			return time.Time{}, time.Time{}, errors.New("usage month window exceeds 120 months")
		}
		first := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, today.Location())
		return first.AddDate(0, -int(months-1), 0), today, nil
	case "thisMonth":
		return time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, today.Location()), today, nil
	case "lastMonth":
		first := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, today.Location())
		return first.AddDate(0, -1, 0), first.AddDate(0, 0, -1), nil
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("unsupported usage window %q", window.Kind)
	}
}

func readUsageRates(path string, now time.Time, limits UsageHistoryConfig) (map[string]modelRate, string, string) {
	rates := map[string]modelRate{}
	if path == "" {
		return rates, "unavailable", "usage pricing refresh is unavailable and no cache path is configured"
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rates, "unavailable", "usage pricing cache is unavailable; live refresh is not configured"
		}
		return rates, "unavailable", "usage pricing cache could not be read"
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limits.MaxRatesBytes)+1))
	if err != nil || len(data) > limits.MaxRatesBytes {
		return rates, "unavailable", "usage pricing cache exceeds its read bound"
	}
	var doc struct {
		FetchedAtMS int64                `json:"fetched_at_ms"`
		Rates       map[string][]float64 `json:"rates"`
	}
	if json.Unmarshal(data, &doc) != nil {
		return rates, "unavailable", "usage pricing cache is invalid"
	}
	if len(doc.Rates) > limits.MaxRates {
		return rates, "unavailable", "usage pricing cache exceeds its entry bound"
	}
	fetchedAt := time.UnixMilli(doc.FetchedAtMS)
	if doc.FetchedAtMS <= 0 || now.Sub(fetchedAt) >= limits.RatesTTL {
		return rates, "unavailable", "usage pricing cache is stale; live refresh is not configured"
	}
	for name, v := range doc.Rates {
		if len(v) != 4 {
			continue
		}
		good := true
		for _, n := range v {
			if math.IsNaN(n) || math.IsInf(n, 0) {
				good = false
			}
		}
		if good {
			if normalized := normalizeModel(name); normalized != "" {
				rates[normalized] = modelRate{v[0], v[1], v[2], v[3]}
			}
		}
	}
	if len(rates) == 0 {
		return rates, "unavailable", "usage pricing cache contains no usable rates"
	}
	return rates, "cached", ""
}
func normalizeModel(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if i := strings.LastIndexByte(v, '/'); i >= 0 {
		v = v[i+1:]
	}
	switch v {
	case "", "<synthetic>", "synthetic", "opus", "sonnet", "haiku", "fable":
		return ""
	}
	return v
}

func deriveUsageHistory(window UsageWindow, since, until time.Time, a aggregate, pricing string, scanned, skipped int, scanErrors []string, duration time.Duration, limits UsageHistoryConfig) map[string]any {
	var totals usageTotals
	cost := 0.0
	cacheSavings := 0.0
	records, reported, unpriced := uint64(0), uint64(0), uint64(0)
	type sum struct {
		cost   float64
		tokens uint64
	}
	providers := map[string]*sum{}
	models := map[string]*sum{}
	days := map[string]map[string]*sum{}
	monthModels := map[string]map[string]float64{}
	for key, b := range a.buckets {
		parts := strings.Split(key, "\x00")
		day, provider, model := parts[0], parts[1], parts[2]
		totals.add(b.totals)
		cost += b.cost
		cacheSavings += b.cacheSavings
		records = saturatingAdd(records, b.records)
		reported = saturatingAdd(reported, b.reported)
		unpriced = saturatingAdd(unpriced, b.unpriced)
		tokens := b.totals.total()
		if providers[provider] == nil {
			providers[provider] = &sum{}
		}
		providers[provider].cost += b.cost
		providers[provider].tokens = saturatingAdd(providers[provider].tokens, tokens)
		mk := provider + "\x00" + model
		if models[mk] == nil {
			models[mk] = &sum{}
		}
		models[mk].cost += b.cost
		models[mk].tokens = saturatingAdd(models[mk].tokens, tokens)
		if days[day] == nil {
			days[day] = map[string]*sum{}
		}
		if days[day][provider] == nil {
			days[day][provider] = &sum{}
		}
		days[day][provider].cost += b.cost
		days[day][provider].tokens = saturatingAdd(days[day][provider].tokens, tokens)
		month := day[:7]
		if monthModels[month] == nil {
			monthModels[month] = map[string]float64{}
		}
		monthModels[month][model] += b.cost
	}
	share := func(part, total float64) float64 {
		if total == 0 {
			return 0
		}
		return part / total
	}
	providerRows := make([]map[string]any, 0, len(providers))
	for name, v := range providers {
		providerRows = append(providerRows, map[string]any{"provider": name, "costUsd": v.cost, "totalTokens": v.tokens, "costShare": share(v.cost, cost), "tokenShare": share(float64(v.tokens), float64(totals.total()))})
	}
	sort.Slice(providerRows, func(i, j int) bool {
		return providerRows[i]["costUsd"].(float64) > providerRows[j]["costUsd"].(float64)
	})
	modelRows := make([]map[string]any, 0, min(len(models), limits.MaxModels))
	modelKeys := make([]string, 0, len(models))
	for key := range models {
		modelKeys = append(modelKeys, key)
	}
	sort.Slice(modelKeys, func(i, j int) bool { return models[modelKeys[i]].cost > models[modelKeys[j]].cost })
	for _, key := range modelKeys[:min(len(modelKeys), limits.MaxModels)] {
		p := strings.Split(key, "\x00")
		v := models[key]
		modelRows = append(modelRows, map[string]any{"provider": p[0], "model": p[1], "costUsd": v.cost, "totalTokens": v.tokens, "costShare": share(v.cost, cost)})
	}
	dayKeys := make([]string, 0, len(days))
	for d := range days {
		dayKeys = append(dayKeys, d)
	}
	sort.Strings(dayKeys)
	daily := make([]map[string]any, 0, len(dayKeys))
	months := map[string]map[string]any{}
	for _, day := range dayKeys {
		by := [2]providerDay{}
		for provider, v := range days[day] {
			idx := 0
			if provider == "codex" {
				idx = 1
			}
			by[idx] = providerDay{v.cost, v.tokens}
		}
		rowCost := by[0].CostUSD + by[1].CostUSD
		rowTokens := saturatingAdd(by[0].TotalTokens, by[1].TotalTokens)
		daily = append(daily, map[string]any{"day": day, "costUsd": rowCost, "totalTokens": rowTokens, "byProvider": by})
		month := day[:7] + "-01"
		if months[month] == nil {
			months[month] = map[string]any{"firstDay": month, "costUsd": float64(0), "totalTokens": uint64(0), "byProvider": [2]providerDay{}, "sessions": uint64(0), "activeDays": uint32(0), "topModels": [][2]any{}}
		}
		m := months[month]
		m["costUsd"] = m["costUsd"].(float64) + rowCost
		m["totalTokens"] = saturatingAdd(m["totalTokens"].(uint64), rowTokens)
		mb := m["byProvider"].([2]providerDay)
		for i := range mb {
			mb[i].CostUSD += by[i].CostUSD
			mb[i].TotalTokens = saturatingAdd(mb[i].TotalTokens, by[i].TotalTokens)
		}
		m["byProvider"] = mb
		if rowTokens > 0 {
			m["activeDays"] = m["activeDays"].(uint32) + 1
		}
	}
	for identity := range a.monthSessions {
		month := identity[:7] + "-01"
		if months[month] != nil {
			months[month]["sessions"] = saturatingAdd(months[month]["sessions"].(uint64), 1)
		}
	}
	for month, values := range monthModels {
		row := months[month+"-01"]
		if row == nil {
			continue
		}
		pairs := make([][2]any, 0, len(values))
		for model, modelCost := range values {
			pairs = append(pairs, [2]any{model, modelCost})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i][1].(float64) > pairs[j][1].(float64) })
		row["topModels"] = pairs
	}
	monthKeys := make([]string, 0, len(months))
	for k := range months {
		monthKeys = append(monthKeys, k)
	}
	sort.Strings(monthKeys)
	monthRows := make([]map[string]any, 0, len(monthKeys))
	for _, k := range monthKeys {
		monthRows = append(monthRows, months[k])
	}
	projectKeys := make([]string, 0, len(a.projects))
	for k := range a.projects {
		projectKeys = append(projectKeys, k)
	}
	sort.Slice(projectKeys, func(i, j int) bool { return a.projects[projectKeys[i]].cost > a.projects[projectKeys[j]].cost })
	projectRows := make([]map[string]any, 0, min(len(projectKeys), limits.MaxProjects))
	for _, path := range projectKeys[:min(len(projectKeys), limits.MaxProjects)] {
		p := a.projects[path]
		var last any = nil
		if p.last != "" {
			last = p.last
		}
		pairs := make([][2]any, 0, len(p.models))
		for model, c := range p.models {
			pairs = append(pairs, [2]any{model, c})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i][1].(float64) > pairs[j][1].(float64) })
		projectRows = append(projectRows, map[string]any{"path": path, "costUsd": p.cost, "totalTokens": p.tokens, "byProvider": p.by, "sessions": uint64(len(p.sessions)), "costShare": share(p.cost, cost), "lastDay": last, "topModels": pairs})
	}
	priced := uint64(0)
	if records > saturatingAdd(reported, unpriced) {
		priced = records - saturatingAdd(reported, unpriced)
	}
	quality := map[string]any{"providerReportedShare": share(float64(reported), float64(records)), "modelPricedShare": share(float64(priced), float64(records)), "unpricedShare": share(float64(unpriced), float64(records)), "cacheSavingsUsd": cacheSavings}
	return map[string]any{"window": windowJSON(window), "sinceDay": since.Format("2006-01-02"), "untilDay": until.Format("2006-01-02"), "totals": totals, "totalTokens": totals.total(), "costUsd": cost, "records": records, "sessions": uint64(len(a.sessions)), "providers": providerRows, "models": modelRows, "daily": daily, "months": monthRows, "projects": projectRows, "quality": quality, "pricing": pricing, "scannedFiles": uint64(scanned), "skippedFiles": uint64(skipped), "errors": scanErrors, "scanDuration": map[string]any{"secs": uint64(duration / time.Second), "nanos": uint32(duration % time.Second)}}
}
func windowJSON(window UsageWindow) any {
	if window.Kind == "thisMonth" || window.Kind == "lastMonth" {
		return window.Kind
	}
	return map[string]uint32{window.Kind: window.Count}
}
func appendBounded(values []string, value string, max int) []string {
	if len(values) < max {
		return append(values, value)
	}
	return values
}
