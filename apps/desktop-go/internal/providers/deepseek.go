package providers

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	deepSeekStartTimeout       = 20 * time.Second
	deepSeekInteractionTimeout = 30 * time.Second
	maxDeepSeekPendingBytes    = 4 << 20
	maxDeepSeekRecoveryBytes   = 16 << 20
	maxDeepSeekRecoveryEvents  = 10000
)

type DeepSeekAdapter struct {
	BaseURL string
	Client  *http.Client

	// Test seam for deterministic sanitized downlink fixtures. Production
	// always uses both Harness websocket downlinks.
	eventSource func(context.Context, string) (<-chan json.RawMessage, error)
}

func (DeepSeekAdapter) Descriptor() Descriptor { return Descriptors()[ProviderDeepSeek] }

func (a DeepSeekAdapter) Discover(ctx context.Context, options StartOptions) (Discovery, error) {
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	if a.BaseURL != "" {
		if err := validateLoopbackHTTPBase(a.BaseURL); err != nil {
			return Discovery{Reason: err.Error()}, nil
		}
		d := Discovery{Available: true, Executable: a.BaseURL}
		if options.ProbeVersion {
			var value map[string]json.RawMessage
			if err := deepSeekRPC(ctx, boundedHTTP{base: a.BaseURL, client: a.Client}, "host.info", map[string]any{}, &value); err != nil {
				return Discovery{Reason: err.Error()}, nil
			}
			d.Version = rawString(value["version"])
		}
		return d, nil
	}
	binary := options.Binary
	if binary == "" {
		binary = "dsh"
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return Discovery{Reason: err.Error()}, nil
	}
	d := Discovery{Available: true, Executable: path}
	if !providerProcessTreeContained() {
		return Discovery{Executable: path, Reason: "DeepSeek process-tree containment is unavailable on this platform"}, nil
	}
	if options.ProbeVersion {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		out, probeErr := exec.CommandContext(probeCtx, path, "--version").Output()
		if probeErr != nil {
			d.Reason = probeErr.Error()
		} else {
			d.Version = strings.TrimSpace(string(out))
		}
	}
	return d, nil
}

type deepSeekOwnedProcess struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	done     chan error
	guardian io.WriteCloser
}

func startDeepSeekHost(ctx context.Context, binary string) (string, *deepSeekOwnedProcess, error) {
	path, err := exec.LookPath(binary)
	if err != nil {
		return "", nil, err
	}
	if !providerProcessTreeContained() {
		return "", nil, errors.New("DeepSeek process-tree containment is unavailable on this platform")
	}
	helpCtx, helpCancel := context.WithTimeout(ctx, 3*time.Second)
	help, helpErr := exec.CommandContext(helpCtx, path, "web", "--help").Output()
	helpCancel()
	if helpErr != nil {
		return "", nil, fmt.Errorf("probe DeepSeek web flags: %w", helpErr)
	}
	args := []string{"web", "--host", "127.0.0.1", "--port", "0"}
	if !deepSeekSupportsNoOpen(help) {
		return "", nil, errors.New("DeepSeek Harness does not support --no-open")
	}
	args = append(args, "--no-open")
	processCtx, cancel := context.WithCancel(context.Background())
	cmd, guardian, err := newGuardedProviderCommand(processCtx, path, args...)
	if err != nil {
		cancel()
		return "", nil, err
	}
	configureProviderProcessGroup(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return "", nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return "", nil, err
	}
	if err := cmd.Start(); err != nil {
		if guardian != nil {
			_ = guardian.Close()
		}
		cancel()
		return "", nil, err
	}
	p := &deepSeekOwnedProcess{cmd: cmd, cancel: cancel, done: make(chan error, 1), guardian: guardian}
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "dsh web: http://127.0.0.1:") {
				select {
				case ready <- strings.TrimPrefix(line, "dsh web: "):
				default:
				}
			}
		}
	}()
	go func() { p.done <- cmd.Wait(); close(p.done) }()
	timer := time.NewTimer(deepSeekStartTimeout)
	defer timer.Stop()
	select {
	case base := <-ready:
		return base, p, nil
	case err := <-p.done:
		cancel()
		return "", nil, fmt.Errorf("DeepSeek Harness exited before ready: %w", err)
	case <-ctx.Done():
		p.close()
		return "", nil, ctx.Err()
	case <-timer.C:
		p.close()
		return "", nil, errors.New("DeepSeek Harness startup timed out")
	}
}

func deepSeekSupportsNoOpen(help []byte) bool {
	for _, line := range strings.Split(string(help), "\n") {
		if strings.Contains(line, "--no-open") {
			return true
		}
	}
	return false
}

func (p *deepSeekOwnedProcess) close() {
	if p == nil {
		return
	}
	if p.guardian != nil {
		_ = p.guardian.Close()
	}
	_ = terminateProviderProcessGroup(p.cmd, syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		_ = terminateProviderProcessGroup(p.cmd, syscall.SIGKILL)
		p.cancel()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (a DeepSeekAdapter) Start(ctx context.Context, options StartOptions, sink Sink) (Session, error) {
	if sink == nil {
		return nil, errors.New("provider event sink is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.ComputerUse {
		return nil, fmt.Errorf("%w: DeepSeek computer use", ErrUnsupported)
	}
	if options.Model != "" || options.ReasoningEffort != "" || options.ServiceTier != "" {
		return nil, fmt.Errorf("%w: DeepSeek model/reasoning/service-tier options", ErrUnsupported)
	}
	if access := normalizedAccess(options.Access); access != AccessAsk {
		return nil, fmt.Errorf("%w: DeepSeek access mode %q", ErrUnsupported, access)
	}
	cwd := options.WorkingDirectory
	var err error
	if cwd == "" {
		cwd, err = os.Getwd()
	}
	if err == nil {
		cwd, err = filepath.EvalSymlinks(cwd)
	}
	if err != nil {
		return nil, fmt.Errorf("DeepSeek workspace: %w", err)
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return nil, err
	}
	sessionID, err := parseDeepSeekCursor(options.ResumeCursor)
	if err != nil {
		return nil, err
	}
	if sessionID == "" {
		sessionID = deepSeekID()
	}
	base := a.BaseURL
	var owned *deepSeekOwnedProcess
	if base == "" {
		binary := options.Binary
		if binary == "" {
			binary = "dsh"
		}
		base, owned, err = startDeepSeekHost(ctx, binary)
		if err != nil {
			return nil, err
		}
	}
	if err := validateLoopbackHTTPBase(base); err != nil {
		owned.close()
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	eventSource := a.eventSource
	if eventSource == nil {
		eventSource = deepSeekDownlinks
	}
	downlink, err := eventSource(streamCtx, base)
	if err != nil {
		cancel()
		owned.close()
		return nil, err
	}
	s := &deepSeekSession{
		http: boundedHTTP{base: base, client: a.Client}, id: sessionID, cwd: cwd,
		access: normalizedAccess(options.Access), sink: sink, cancelStream: cancel,
		owned: owned, sessionCtx: streamCtx, events: make(chan Event, 256), pending: make(map[string]deepSeekPending),
	}
	go s.dispatch()
	var created map[string]json.RawMessage
	if err := deepSeekRPC(ctx, s.http, "session.create", map[string]any{"cwd": cwd, "sessionId": sessionID}, &created); err != nil {
		s.abortStart()
		return nil, err
	}
	if got := rawString(created["sessionId"]); got != sessionID {
		s.abortStart()
		return nil, fmt.Errorf("DeepSeek Harness returned session %q, want %q", got, sessionID)
	}
	var history map[string]json.RawMessage
	if err := deepSeekRPC(ctx, s.http, "session.history", map[string]any{"sessionId": sessionID, "maxMessages": 200}, &history); err != nil {
		s.abortStart()
		return nil, fmt.Errorf("DeepSeek history: %w", err)
	}
	if err := s.baseline(history["events"]); err != nil {
		s.abortStart()
		return nil, err
	}
	var commands []json.RawMessage
	if err := deepSeekRPC(ctx, s.http, "commands/list", map[string]any{"args": map[string]string{"agentId": sessionID}}, &commands); err != nil {
		s.abortStart()
		return nil, fmt.Errorf("DeepSeek commands: %w", err)
	}
	commandPayload := make([]map[string]string, 0, len(commands))
	for _, raw := range commands {
		var command map[string]json.RawMessage
		if decodeUseNumber(raw, &command) != nil || rawString(command["name"]) == "" {
			s.abortStart()
			return nil, errors.New("DeepSeek command catalog is malformed")
		}
		commandPayload = append(commandPayload, map[string]string{"name": rawString(command["name"]), "description": rawString(command["description"])})
	}
	s.emit(Event{Kind: EventConnected, Cursor: deepSeekCursor(sessionID)})
	commandRaw, _ := json.Marshal(commandPayload)
	s.emit(Event{Kind: EventAvailableCommands, Raw: commandRaw})
	if s.active {
		s.emit(Event{Kind: EventTurnStarted})
	}
	go s.readLoop(streamCtx, downlink)
	if owned != nil {
		go func() {
			exitErr, ok := <-owned.done
			if ok && !s.closed.Load() {
				s.emit(Event{Kind: EventError, Text: fmt.Sprintf("DeepSeek Harness exited: %v", exitErr)})
				s.failActive("DeepSeek Harness exited")
				s.cancelStream()
			}
		}()
	}
	return s, nil
}

func deepSeekDownlinks(ctx context.Context, base string) (<-chan json.RawMessage, error) {
	mux, err := openWebSocket(ctx, base, "/api/events.mux")
	if err != nil {
		return nil, err
	}
	host, err := openWebSocket(ctx, base, "/api/events.host")
	if err != nil {
		_ = mux.Close()
		return nil, err
	}
	out := make(chan json.RawMessage)
	var wg sync.WaitGroup
	for _, source := range []struct {
		path   string
		stream *wsReader
	}{{"/api/events.mux", mux}, {"/api/events.host", host}} {
		wg.Add(1)
		go func(path string, initial *wsReader) {
			defer wg.Done()
			stream := initial
			delay := 200 * time.Millisecond
			for {
				if stream == nil {
					var openErr error
					stream, openErr = openWebSocket(ctx, base, path)
					if openErr != nil {
						select {
						case <-ctx.Done():
							return
						case <-time.After(delay):
						}
						if delay < 2*time.Second {
							delay *= 2
						}
						continue
					}
					delay = 200 * time.Millisecond
				}
				raw, readErr := stream.readText()
				if readErr != nil {
					_ = stream.Close()
					stream = nil
					continue
				}
				select {
				case out <- cloneRaw(raw):
				case <-ctx.Done():
					return
				}
			}
		}(source.path, source.stream)
	}
	go func() { wg.Wait(); close(out) }()
	return out, nil
}

func deepSeekRPC(ctx context.Context, h boundedHTTP, method string, payload any, out any) error {
	id := deepSeekID()
	var envelope map[string]json.RawMessage
	if err := h.request(ctx, http.MethodPost, "/api/"+method, map[string]any{"type": "client-request", "rpcId": id, "method": method, "payload": payload}, &envelope); err != nil {
		return err
	}
	if rawString(envelope["type"]) != "server-response" || rawString(envelope["rpcId"]) != id {
		return errors.New("invalid DeepSeek RPC response envelope")
	}
	var result struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
		Error json.RawMessage `json:"error"`
	}
	if err := decodeUseNumber(envelope["result"], &result); err != nil {
		return errors.New("invalid DeepSeek RPC result")
	}
	if !result.OK {
		return fmt.Errorf("DeepSeek RPC %s failed: %s", method, strings.TrimSpace(string(result.Error)))
	}
	if out != nil {
		if len(result.Value) == 0 || string(result.Value) == "null" {
			return errors.New("DeepSeek RPC returned no value")
		}
		if err := decodeUseNumber(result.Value, out); err != nil {
			return err
		}
	}
	return nil
}

func deepSeekRespond(ctx context.Context, h boundedHTTP, rpcID string, value any) error {
	if rpcID == "" {
		return errors.New("DeepSeek interaction has no rpcId")
	}
	var response struct {
		Accepted bool   `json:"accepted"`
		Reason   string `json:"reason"`
	}
	if err := h.request(ctx, http.MethodPost, "/api/respond", map[string]any{"type": "client-response", "rpcId": rpcID, "result": map[string]any{"ok": true, "value": value}}, &response); err != nil {
		return err
	}
	if !response.Accepted {
		return fmt.Errorf("DeepSeek Harness rejected interaction response: %s", firstNonempty(response.Reason, "unknown reason"))
	}
	return nil
}

func parseDeepSeekCursor(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var cursor struct {
		Provider  Provider `json:"provider"`
		SessionID string   `json:"sessionId"`
	}
	if err := decodeUseNumber(raw, &cursor); err != nil {
		return "", err
	}
	if cursor.Provider != ProviderDeepSeek {
		return "", fmt.Errorf("DeepSeek cannot resume %q cursor", cursor.Provider)
	}
	if cursor.SessionID == "" {
		return "", errors.New("DeepSeek cursor has no sessionId")
	}
	return cursor.SessionID, nil
}

func deepSeekCursor(id string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"provider": ProviderDeepSeek, "sessionId": id})
	return raw
}

func deepSeekID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("waku-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

type deepSeekPending struct {
	kind, rpcID, nativeID string
	questions             []UserInputQuestion
	bytes                 int
}

type deepSeekSession struct {
	http         boundedHTTP
	id, cwd      string
	access       AccessMode
	sink         Sink
	cancelStream context.CancelFunc
	owned        *deepSeekOwnedProcess
	sessionCtx   context.Context
	closed       atomic.Bool
	mu           sync.Mutex
	active       bool
	hasSeq       bool
	lastSeq      uint64
	pending      map[string]deepSeekPending
	pendingBytes int
	eventMu      sync.Mutex
	events       chan Event
	eventBytes   int
	eventsClosed bool
	slow         atomic.Bool
}

func (s *deepSeekSession) abortStart() {
	if s.closed.CompareAndSwap(false, true) {
		s.cancelStream()
		s.owned.close()
	}
	s.closeEvents()
}

func (s *deepSeekSession) baseline(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return errors.New("DeepSeek history has no events")
	}
	var entries []json.RawMessage
	if err := decodeUseNumber(raw, &entries); err != nil {
		return errors.New("DeepSeek history events are malformed")
	}
	for _, entryRaw := range entries {
		var entry map[string]json.RawMessage
		if decodeUseNumber(entryRaw, &entry) != nil {
			return errors.New("DeepSeek history entry is malformed")
		}
		eventRaw := entry["event"]
		if len(eventRaw) == 0 {
			eventRaw = entryRaw
		}
		var event map[string]json.RawMessage
		if decodeUseNumber(eventRaw, &event) != nil {
			return errors.New("DeepSeek history event is malformed")
		}
		seq, ok := rawUint64(event["seq"])
		if !ok {
			continue
		}
		if !s.hasSeq || seq > s.lastSeq {
			s.hasSeq, s.lastSeq = true, seq
		}
		switch rawString(event["type"]) {
		case "turn/start":
			s.active = true
		case "turn/end":
			s.active = false
		}
	}
	return nil
}

func (s *deepSeekSession) Descriptor() Descriptor { return Descriptors()[ProviderDeepSeek] }

func (s *deepSeekSession) emit(event Event) bool {
	size := eventSize(event)
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.eventsClosed {
		return false
	}
	if len(s.events) == cap(s.events) || s.eventBytes+size > maxQueuedEventBytes {
		s.slow.Store(true)
		s.cancelStream()
		return false
	}
	s.eventBytes += size
	s.events <- event
	return true
}

func (s *deepSeekSession) dispatch() {
	for event := range s.events {
		s.eventMu.Lock()
		s.eventBytes -= eventSize(event)
		s.eventMu.Unlock()
		s.sink.Emit(event)
	}
	if s.slow.Load() {
		s.sink.Emit(Event{Kind: EventError, Text: "provider event sink exceeded bounded queue"})
	}
	s.sink.Emit(Event{Kind: EventProcessExited})
}

func (s *deepSeekSession) closeEvents() {
	s.eventMu.Lock()
	if !s.eventsClosed {
		s.eventsClosed = true
		close(s.events)
	}
	s.eventMu.Unlock()
}

func (s *deepSeekSession) readLoop(ctx context.Context, downlink <-chan json.RawMessage) {
	for {
		select {
		case <-ctx.Done():
			s.closeEvents()
			return
		case raw, ok := <-downlink:
			if !ok {
				if !s.closed.Load() {
					s.failActive("DeepSeek Harness event streams closed")
				}
				s.closeEvents()
				return
			}
			if err := s.consume(raw); err != nil {
				s.emit(Event{Kind: EventError, Text: err.Error(), Raw: cloneRaw(raw)})
				s.failActive(err.Error())
				s.closeEvents()
				return
			}
		}
	}
}

func (s *deepSeekSession) consume(raw json.RawMessage) error {
	var envelope map[string]json.RawMessage
	if err := decodeUseNumber(raw, &envelope); err != nil {
		return fmt.Errorf("DeepSeek downlink JSON: %w", err)
	}
	payload := envelope
	if p := envelope["payload"]; len(p) != 0 {
		if err := decodeUseNumber(p, &payload); err != nil {
			return errors.New("DeepSeek downlink payload is malformed")
		}
	}
	kind := rawString(payload["type"])
	if deepSeekSessionScoped(kind) && rawString(payload["sessionId"]) != s.id {
		return nil
	}
	switch kind {
	case "session/event":
		return s.consumeSessionEvent(payload["event"], payload["view"], raw)
	case "session/subscribed":
		remote, ok := rawUint64(payload["lastSeq"])
		s.mu.Lock()
		local, hasLocal := s.lastSeq, s.hasSeq
		s.mu.Unlock()
		if ok && (!hasLocal || remote > local) {
			return s.recoverHistory()
		}
	case "approval/requested":
		return s.consumeApproval(envelope, payload, raw)
	case "question/requested":
		return s.consumeQuestion(envelope, payload, raw)
	case "host/agent-error":
		s.emit(Event{Kind: EventError, Text: firstNonempty(rawString(payload["message"]), "DeepSeek Harness agent failed"), Raw: cloneRaw(raw)})
	case "stream/error":
		s.emit(Event{Kind: EventError, Text: "DeepSeek Harness event stream failed", Raw: cloneRaw(raw)})
	case "session/projection":
		if rawString(payload["key"]) == "title" {
			s.emit(Event{Kind: EventAutoTitleUpdated, Text: rawString(payload["value"]), Raw: cloneRaw(raw)})
		}
	case "approval/resolved", "question/resolved":
		id := firstNonempty(rawString(payload["questionRpcId"]), rawString(payload["approvalId"]))
		s.mu.Lock()
		for key, pending := range s.pending {
			if pending.nativeID == id || pending.rpcID == id {
				s.deletePendingLocked(key)
			}
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *deepSeekSession) recoverHistory() error {
	ctx, cancel := context.WithTimeout(s.sessionCtx, 30*time.Second)
	defer cancel()
	var before *uint64
	seen := make(map[uint64]struct{})
	var pages [][]json.RawMessage
	var projection json.RawMessage
	totalBytes, totalEvents := 0, 0
	for pageCount := 0; pageCount < 256; pageCount++ {
		payload := map[string]any{"sessionId": s.id, "maxMessages": 200}
		if before != nil {
			payload["beforeSeq"] = *before
		}
		var page map[string]json.RawMessage
		if err := deepSeekRPC(ctx, s.http, "session.history", payload, &page); err != nil {
			return fmt.Errorf("DeepSeek stream recovery: %w", err)
		}
		var entries []json.RawMessage
		if err := decodeUseNumber(page["events"], &entries); err != nil {
			return errors.New("DeepSeek recovery history is malformed")
		}
		for _, entry := range entries {
			totalBytes += len(entry)
		}
		totalEvents += len(entries)
		if totalBytes > maxDeepSeekRecoveryBytes || totalEvents > maxDeepSeekRecoveryEvents {
			return errors.New("DeepSeek recovery history exceeds global limit")
		}
		pages = append(pages, entries)
		if len(projection) == 0 {
			projection = cloneRaw(page["projections"])
		}
		var oldest uint64
		haveOldest := false
		for _, entryRaw := range entries {
			var entry map[string]json.RawMessage
			if decodeUseNumber(entryRaw, &entry) != nil {
				continue
			}
			eventRaw := entry["event"]
			if len(eventRaw) == 0 {
				eventRaw = entryRaw
			}
			var event map[string]json.RawMessage
			if decodeUseNumber(eventRaw, &event) != nil {
				continue
			}
			if seq, ok := rawUint64(event["seq"]); ok && (!haveOldest || seq < oldest) {
				oldest, haveOldest = seq, true
			}
		}
		var hasMore bool
		_ = decodeUseNumber(page["hasMore"], &hasMore)
		if !hasMore {
			break
		}
		if !haveOldest || oldest == 0 {
			return errors.New("DeepSeek recovery history did not advance")
		}
		if _, duplicate := seen[oldest]; duplicate {
			return errors.New("DeepSeek recovery history repeated a page")
		}
		seen[oldest] = struct{}{}
		before = &oldest
		if pageCount == 255 {
			return errors.New("DeepSeek recovery history exceeded page limit")
		}
	}
	var ordered []json.RawMessage
	for _, page := range pages {
		ordered = append(ordered, page...)
	}
	sort.SliceStable(ordered, func(i, j int) bool { return deepSeekEntrySeq(ordered[i]) < deepSeekEntrySeq(ordered[j]) })
	for _, entryRaw := range ordered {
		var entry map[string]json.RawMessage
		if decodeUseNumber(entryRaw, &entry) != nil {
			return errors.New("DeepSeek recovery entry is malformed")
		}
		eventRaw := entry["event"]
		if len(eventRaw) == 0 {
			eventRaw = entryRaw
		}
		if err := s.consumeSessionEventMode(eventRaw, entry["view"], entryRaw, false); err != nil {
			return err
		}
	}
	var projections map[string]json.RawMessage
	if decodeUseNumber(projection, &projections) == nil {
		var values map[string]json.RawMessage
		if decodeUseNumber(projections["values"], &values) == nil {
			if title := rawString(values["title"]); title != "" {
				s.emit(Event{Kind: EventAutoTitleUpdated, Text: title, Raw: cloneRaw(projection)})
			}
		}
	}
	return nil
}

func deepSeekEntrySeq(raw json.RawMessage) uint64 {
	var entry map[string]json.RawMessage
	if decodeUseNumber(raw, &entry) != nil {
		return ^uint64(0)
	}
	eventRaw := entry["event"]
	if len(eventRaw) == 0 {
		eventRaw = raw
	}
	var event map[string]json.RawMessage
	if decodeUseNumber(eventRaw, &event) != nil {
		return ^uint64(0)
	}
	seq, ok := rawUint64(event["seq"])
	if !ok {
		return ^uint64(0)
	}
	return seq
}

func deepSeekSessionScoped(kind string) bool {
	switch kind {
	case "session/event", "session/subscribed", "approval/requested", "approval/resolved", "question/requested", "question/resolved", "session/jobs", "session/projection":
		return true
	default:
		return false
	}
}

func (s *deepSeekSession) deletePendingLocked(id string) {
	if pending, ok := s.pending[id]; ok {
		s.pendingBytes -= pending.bytes
		if s.pendingBytes < 0 {
			s.pendingBytes = 0
		}
		delete(s.pending, id)
	}
}

func (s *deepSeekSession) consumeSessionEvent(rawEvent, view, raw json.RawMessage) error {
	return s.consumeSessionEventMode(rawEvent, view, raw, true)
}

func (s *deepSeekSession) consumeSessionEventMode(rawEvent, view, raw json.RawMessage, recoverGap bool) error {
	var event map[string]json.RawMessage
	if err := decodeUseNumber(rawEvent, &event); err != nil {
		return errors.New("DeepSeek session event is malformed")
	}
	seq, ok := rawUint64(event["seq"])
	if !ok {
		return errors.New("DeepSeek session event has invalid seq")
	}
	s.mu.Lock()
	if s.hasSeq && seq <= s.lastSeq {
		s.mu.Unlock()
		return nil
	}
	jump := s.hasSeq && s.lastSeq != ^uint64(0) && seq > s.lastSeq+1
	s.mu.Unlock()
	if jump && recoverGap {
		if err := s.recoverHistory(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.hasSeq && seq <= s.lastSeq {
			s.mu.Unlock()
			return nil
		}
		jump = s.hasSeq && s.lastSeq != ^uint64(0) && seq > s.lastSeq+1
		s.mu.Unlock()
		if jump {
			return fmt.Errorf("DeepSeek event sequence gap before %d", seq)
		}
	} else if jump {
		return fmt.Errorf("DeepSeek recovery history has sequence gap before %d", seq)
	}
	s.mu.Lock()
	if s.hasSeq && seq <= s.lastSeq {
		s.mu.Unlock()
		return nil
	}
	s.hasSeq = true
	s.lastSeq = seq
	s.mu.Unlock()
	var data map[string]json.RawMessage
	_ = decodeUseNumber(event["data"], &data)
	switch rawString(event["type"]) {
	case "turn/start":
		s.beginTurn(raw)
	case "turn/end":
		var reason map[string]json.RawMessage
		_ = decodeUseNumber(data["reason"], &reason)
		kind := rawString(reason["kind"])
		s.finishTurn(kind == "completed" || kind == "max-tokens", kind, raw)
	case "assistant/chunk":
		var chunk map[string]json.RawMessage
		if decodeUseNumber(data["chunk"], &chunk) != nil {
			return errors.New("DeepSeek assistant chunk is malformed")
		}
		text := stripPrivate(rawString(chunk["text"]))
		switch rawString(chunk["type"]) {
		case "text-delta":
			if text != "" {
				s.emit(Event{Kind: EventTextDelta, Text: text, Raw: cloneRaw(raw)})
			}
		case "reasoning-delta":
			if text != "" {
				s.emit(Event{Kind: EventReasoningDelta, Text: text, Raw: cloneRaw(raw)})
			}
		case "usage":
			s.emitUsage(chunk["usage"], raw)
		}
	case "assistant/message":
		s.emitUsage(data["usage"], raw)
	case "tool/call":
		s.emitTool(data, view, false, raw)
	case "tool/result":
		s.emitTool(data, view, true, raw)
	case "request/context":
		if window, ok := rawUint64(data["contextWindow"]); ok && window > 0 {
			s.emit(Event{Kind: EventUsageUpdated, ContextWindow: &window, Raw: cloneRaw(raw)})
		}
	case "session/title":
		s.emit(Event{Kind: EventAutoTitleUpdated, Text: stripPrivate(rawString(data["title"])), Raw: cloneRaw(raw)})
	}
	return nil
}

func (s *deepSeekSession) beginTurn(raw json.RawMessage) {
	s.mu.Lock()
	was := s.active
	s.active = true
	s.mu.Unlock()
	if !was {
		s.emit(Event{Kind: EventTurnStarted, Raw: cloneRaw(raw)})
	}
}

func (s *deepSeekSession) finishTurn(ok bool, summary string, raw json.RawMessage) {
	s.mu.Lock()
	active := s.active
	s.active = false
	s.mu.Unlock()
	if active {
		s.emit(Event{Kind: EventTurnFinished, Success: ok, Summary: summary, Raw: cloneRaw(raw)})
	}
}

func (s *deepSeekSession) failActive(summary string) { s.finishTurn(false, summary, nil) }

func (s *deepSeekSession) emitUsage(rawUsage, raw json.RawMessage) {
	var usage map[string]json.RawMessage
	if decodeUseNumber(rawUsage, &usage) != nil {
		return
	}
	var total uint64
	for _, key := range []string{"inputTokens", "outputTokens", "reasoningTokens", "cacheReadTokens", "cacheWriteTokens"} {
		if n, ok := rawUint64(usage[key]); ok {
			total += n
		}
	}
	if total > 0 {
		s.emit(Event{Kind: EventUsageUpdated, ContextTokens: &total, Raw: cloneRaw(raw)})
	}
}

func (s *deepSeekSession) emitTool(data map[string]json.RawMessage, view json.RawMessage, complete bool, raw json.RawMessage) {
	id := firstNonempty(rawString(data["callId"]), rawString(data["id"]))
	name := firstNonempty(rawString(data["name"]), rawString(data["tool"]), "Tool")
	activity := &Activity{ID: id, Kind: streamToolKind(name), Title: name, Tool: name, Arguments: cloneRaw(data["arguments"]), Output: cloneRaw(data["result"]), Complete: complete}
	if len(view) != 0 && string(view) != "null" {
		activity.Detail = string(view)
	}
	s.emit(Event{Kind: EventActivity, Activity: activity, Raw: cloneRaw(raw)})
}

func (s *deepSeekSession) consumeApproval(envelope, payload map[string]json.RawMessage, raw json.RawMessage) error {
	rpcID := firstNonempty(rawString(envelope["rpcId"]), rawString(payload["rpcId"]))
	id := rawString(payload["approvalId"])
	if rpcID == "" || id == "" {
		return errors.New("DeepSeek approval is malformed")
	}
	if s.access != AccessAsk {
		ctx, cancel := context.WithTimeout(s.sessionCtx, deepSeekInteractionTimeout)
		defer cancel()
		return deepSeekRespond(ctx, s.http, rpcID, map[string]any{"sessionId": s.id, "approvalId": id, "outcome": "allowed-once"})
	}
	requestID := rpcID
	s.mu.Lock()
	if _, duplicate := s.pending[requestID]; duplicate {
		s.mu.Unlock()
		return errors.New("duplicate DeepSeek interaction id")
	}
	if len(s.pending) >= 1024 || s.pendingBytes+len(raw) > maxDeepSeekPendingBytes {
		s.mu.Unlock()
		return errors.New("DeepSeek pending interaction limit exceeded")
	}
	s.pending[requestID] = deepSeekPending{kind: "approval", rpcID: rpcID, nativeID: id, bytes: len(raw)}
	s.pendingBytes += len(raw)
	s.mu.Unlock()
	rid, _ := json.Marshal(requestID)
	tool := firstNonempty(rawString(payload["toolName"]), "tool")
	s.emit(Event{Kind: EventPermission, RequestID: rid, Permission: &Permission{Title: "Allow " + tool + "?", Detail: firstNonempty(rawString(payload["reason"]), "DeepSeek Harness is asking for permission to continue"), Options: []PermissionOption{{ID: "allow", Label: "Allow once", Allow: true}, {ID: "reject", Label: "Deny"}}}, Raw: cloneRaw(raw)})
	return nil
}

func (s *deepSeekSession) consumeQuestion(envelope, payload map[string]json.RawMessage, raw json.RawMessage) error {
	rpcID := firstNonempty(rawString(envelope["rpcId"]), rawString(payload["rpcId"]))
	var native []struct {
		ID, Header, Question string
		Options              []UserInputOption
		MultiSelect          bool `json:"multiSelect"`
	}
	if rpcID == "" || decodeUseNumber(payload["questions"], &native) != nil || len(native) == 0 {
		return errors.New("DeepSeek question is malformed")
	}
	questions := make([]UserInputQuestion, 0, len(native))
	for _, question := range native {
		if question.ID == "" || question.Question == "" {
			return errors.New("DeepSeek question is malformed")
		}
		questions = append(questions, UserInputQuestion{ID: question.ID, Header: question.Header, Question: question.Question, Options: question.Options, MultiSelect: question.MultiSelect})
	}
	s.mu.Lock()
	if _, duplicate := s.pending[rpcID]; duplicate {
		s.mu.Unlock()
		return errors.New("duplicate DeepSeek interaction id")
	}
	if len(s.pending) >= 1024 || s.pendingBytes+len(raw) > maxDeepSeekPendingBytes {
		s.mu.Unlock()
		return errors.New("DeepSeek pending interaction limit exceeded")
	}
	s.pending[rpcID] = deepSeekPending{kind: "question", rpcID: rpcID, questions: questions, bytes: len(raw)}
	s.pendingBytes += len(raw)
	s.mu.Unlock()
	rid, _ := json.Marshal(rpcID)
	s.emit(Event{Kind: EventUserInputRequested, RequestID: rid, Questions: questions, Raw: cloneRaw(raw)})
	return nil
}

func (s *deepSeekSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("DeepSeek turn already active")
	}
	s.active = true
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnStarted})
	var result map[string]json.RawMessage
	err := deepSeekRPC(ctx, s.http, "session.prompt", map[string]any{"sessionId": s.id, "mode": "queue", "content": []map[string]string{{"type": "text", "text": text}}}, &result)
	if err != nil {
		s.finishTurn(false, err.Error(), nil)
	}
	return err
}

func (s *deepSeekSession) Steer(ctx context.Context, text string) error {
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		return errors.New("DeepSeek has no active turn")
	}
	var result map[string]json.RawMessage
	err := deepSeekRPC(ctx, s.http, "session.prompt", map[string]any{"sessionId": s.id, "mode": "steer", "content": []map[string]string{{"type": "text", "text": text}}}, &result)
	if err != nil {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: err.Error()})
		return err
	}
	s.emit(Event{Kind: EventSteerAccepted, Text: text})
	return nil
}

func (s *deepSeekSession) Cancel(ctx context.Context) error {
	var result map[string]json.RawMessage
	return deepSeekRPC(ctx, s.http, "session.cancel", map[string]any{"sessionId": s.id}, &result)
}

func (s *deepSeekSession) RespondPermission(ctx context.Context, raw json.RawMessage, option string) error {
	id, err := decodeRequestID(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	pending, ok := s.pending[id]
	s.mu.Unlock()
	if !ok || pending.kind != "approval" {
		return errors.New("unknown DeepSeek approval")
	}
	if option != "allow" && option != "reject" {
		return errors.New("invalid DeepSeek approval option")
	}
	outcome := "rejected"
	if option == "allow" {
		outcome = "allowed-once"
	}
	if err := deepSeekRespond(ctx, s.http, pending.rpcID, map[string]any{"sessionId": s.id, "approvalId": pending.nativeID, "outcome": outcome}); err != nil {
		return err
	}
	s.mu.Lock()
	s.deletePendingLocked(id)
	s.mu.Unlock()
	return nil
}

func (s *deepSeekSession) RespondQuestions(ctx context.Context, raw json.RawMessage, answers []UserInputAnswer) error {
	id, err := decodeRequestID(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	pending, ok := s.pending[id]
	s.mu.Unlock()
	if !ok || pending.kind != "question" {
		return errors.New("unknown DeepSeek question")
	}
	answerByID := make(map[string][]string)
	for _, answer := range answers {
		answerByID[answer.QuestionID] = append([]string(nil), answer.Answers...)
	}
	native := make([]map[string]any, 0, len(pending.questions))
	for _, question := range pending.questions {
		allowed := make(map[string]struct{}, len(question.Options))
		for _, option := range question.Options {
			allowed[option.Label] = struct{}{}
		}
		var selected, custom []string
		for _, answer := range answerByID[question.ID] {
			if _, ok := allowed[answer]; ok {
				selected = append(selected, answer)
			} else {
				custom = append(custom, answer)
			}
		}
		item := map[string]any{"id": question.ID, "selected": selected}
		if len(custom) != 0 {
			item["custom"] = strings.Join(custom, ", ")
		}
		native = append(native, item)
	}
	if err := deepSeekRespond(ctx, s.http, pending.rpcID, map[string]any{"sessionId": s.id, "answer": map[string]any{"answers": native}}); err != nil {
		return err
	}
	s.mu.Lock()
	s.deletePendingLocked(id)
	s.mu.Unlock()
	return nil
}

func (s *deepSeekSession) Goal(context.Context, GoalOperation) error { return ErrUnsupported }
func (s *deepSeekSession) ApplyOptions(ctx context.Context, options SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if options.Model != "" || options.ReasoningEffort != "" || options.ServiceTier != "" {
		return false, ErrUnsupported
	}
	if options.Access != "" && options.Access != AccessAsk {
		return false, ErrUnsupported
	}
	return true, nil
}
func (s *deepSeekSession) Rollback(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *deepSeekSession) Fork(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *deepSeekSession) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancelStream()
	s.owned.close()
	return nil
}
