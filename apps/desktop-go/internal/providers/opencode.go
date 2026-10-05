package providers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type OpenCodeAdapter struct {
	BaseURL, Password, RegistrationPath string
	Client                              *http.Client
}

type openCodeRegistration struct {
	ID, Version, URL, Password string
	PID                        uint32
}

func probeOpenCode(ctx context.Context, h boundedHTTP, registration openCodeRegistration) (string, error) {
	var info struct {
		Version string `json:"version"`
		PID     uint32 `json:"pid"`
	}
	if err := h.request(ctx, http.MethodGet, "/api/info", nil, &info); err != nil {
		return "", err
	}
	if info.PID == 0 || info.Version == "" {
		return "", errors.New("OpenCode service returned incomplete identity")
	}
	if registration.PID != 0 && info.PID != registration.PID {
		return "", errors.New("OpenCode registration PID does not match service")
	}
	if registration.Version != "" && info.Version != registration.Version {
		return "", errors.New("OpenCode registration version does not match service")
	}
	return info.Version, nil
}

func (OpenCodeAdapter) Descriptor() Descriptor { return Descriptors()[ProviderOpenCode] }
func (a OpenCodeAdapter) endpoint() (boundedHTTP, openCodeRegistration, error) {
	if a.BaseURL != "" {
		if a.Password == "" {
			return boundedHTTP{}, openCodeRegistration{}, errors.New("OpenCode direct endpoint requires a credential")
		}
		if err := validateLoopbackHTTPBase(a.BaseURL); err != nil {
			return boundedHTTP{}, openCodeRegistration{}, err
		}
		return boundedHTTP{base: a.BaseURL, user: "opencode", password: a.Password, client: a.Client}, openCodeRegistration{ID: "direct", URL: a.BaseURL, Password: a.Password}, nil
	}
	path := a.RegistrationPath
	if path == "" {
		state := os.Getenv("XDG_STATE_HOME")
		if state == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return boundedHTTP{}, openCodeRegistration{}, err
			}
			state = filepath.Join(home, ".local", "state")
		}
		path = filepath.Join(state, "opencode", "service.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return boundedHTTP{}, openCodeRegistration{}, err
	}
	var r openCodeRegistration
	if err := decodeUseNumber(raw, &r); err != nil {
		return boundedHTTP{}, r, err
	}
	if r.ID == "" || r.URL == "" || r.Password == "" || r.PID == 0 {
		return boundedHTTP{}, r, errors.New("OpenCode registration is incomplete")
	}
	if err := validateLoopbackHTTPBase(r.URL); err != nil {
		return boundedHTTP{}, r, err
	}
	return boundedHTTP{base: r.URL, user: "opencode", password: r.Password, client: a.Client}, r, nil
}
func (a OpenCodeAdapter) Discover(ctx context.Context, options StartOptions) (Discovery, error) {
	if err := ctx.Err(); err != nil {
		return Discovery{}, err
	}
	h, r, err := a.endpoint()
	if err != nil {
		binary := options.Binary
		if binary == "" {
			binary = "opencode"
		}
		path, _ := exec.LookPath(binary)
		return Discovery{Executable: path, Reason: err.Error()}, nil
	}
	version, err := probeOpenCode(ctx, h, r)
	if err != nil {
		return Discovery{Reason: err.Error()}, nil
	}
	d := Discovery{Available: true, Executable: r.URL}
	if options.ProbeVersion {
		d.Version = version
	}
	if options.DiscoverModels {
		models, modelErr := openCodeModels(ctx, h)
		if modelErr != nil {
			d.Reason = modelErr.Error()
		} else {
			d.Models = models
		}
	}
	return d, nil
}
func openCodeModels(ctx context.Context, h boundedHTTP) ([]Model, error) {
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := h.request(ctx, http.MethodGet, "/api/model", nil, &envelope); err != nil {
		return nil, err
	}
	models := make([]Model, 0, len(envelope.Data))
	for _, raw := range envelope.Data {
		var v map[string]json.RawMessage
		if decodeUseNumber(raw, &v) != nil {
			return nil, errors.New("OpenCode model catalog contains invalid entry")
		}
		provider, id := rawString(v["providerID"]), rawString(v["modelID"])
		if id == "" {
			id = rawString(v["id"])
		}
		if provider == "" || id == "" {
			return nil, errors.New("OpenCode model is missing providerID/modelID")
		}
		label := firstNonempty(rawString(v["name"]), id)
		meta, _ := json.Marshal(map[string]any{"id": provider + "/" + id, "name": label, "sub_provider": displaySlug(provider), "is_default": false})
		models = append(models, Model{ID: provider + "/" + id, Label: label, Raw: meta})
	}
	return models, nil
}

func (a OpenCodeAdapter) Start(ctx context.Context, o StartOptions, sink Sink) (Session, error) {
	if sink == nil {
		return nil, errors.New("provider event sink is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.ComputerUse {
		return nil, fmt.Errorf("%w: OpenCode computer use is not implemented", ErrUnsupported)
	}
	if o.ServiceTier != "" {
		return nil, fmt.Errorf("%w: OpenCode service tier", ErrUnsupported)
	}
	if !validOpenCodeAccess(o.Access) {
		return nil, fmt.Errorf("%w: OpenCode access mode %q", ErrUnsupported, o.Access)
	}
	if err := validateOpenCodeModelOptions(o.Model, o.ReasoningEffort); err != nil {
		return nil, err
	}
	h, registration, err := a.endpoint()
	if err != nil {
		return nil, err
	}
	if _, err := probeOpenCode(ctx, h, registration); err != nil {
		return nil, fmt.Errorf("OpenCode probe: %w", err)
	}
	cwd := o.WorkingDirectory
	if cwd == "" {
		cwd, err = os.Getwd()
	}
	if err == nil {
		cwd, err = filepath.EvalSymlinks(cwd)
	}
	if err != nil {
		return nil, fmt.Errorf("OpenCode workspace: %w", err)
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return nil, err
	}
	sessionID, err := parseOpenCodeCursor(o.ResumeCursor)
	if err != nil {
		return nil, err
	}
	resuming := sessionID != ""
	if !resuming {
		sessionID = newOpenCodeID()
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	body, err := h.openSSE(streamCtx, "/api/event")
	if err != nil {
		cancel()
		return nil, err
	}
	s := &openCodeSession{http: h, reconnect: a.endpoint, sessionCtx: streamCtx, id: sessionID, cwd: cwd, access: normalizedAccess(o.Access), sink: sink, cancelStream: cancel, stream: body, events: make(chan Event, 256), permissions: make(map[string]int), forms: make(map[string][]openCodeFormField), formSizes: make(map[string]int), toolInputs: make(map[string]string), parts: make(map[string]string), overlapOffsets: make(map[string]int), completedTools: make(map[string]bool)}
	go s.dispatch()
	if resuming {
		var envelope struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		err = h.request(ctx, http.MethodGet, "/api/session/"+sessionID, nil, &envelope)
		if err != nil {
			var httpErr *providerHTTPError
			if !errors.As(err, &httpErr) || httpErr.Status != http.StatusNotFound {
				s.abortStart()
				return nil, err
			}
			var migration struct {
				Status string `json:"status"`
			}
			migrationErr := h.request(ctx, http.MethodGet, "/api/experimental/migration/v1", nil, &migration)
			if migrationErr == nil && migration.Status == "running" {
				s.abortStart()
				return nil, errors.New("OpenCode session import is still running; retry resume")
			}
			err = s.create(ctx, o)
		}
	} else {
		err = s.create(ctx, o)
	}
	if err != nil {
		s.abortStart()
		return nil, err
	}
	if resuming {
		head, headErr := s.snapshotMessageHead(ctx, h)
		if headErr != nil {
			s.abortStart()
			return nil, fmt.Errorf("OpenCode resume baseline: %w", headErr)
		}
		s.historyHead = head
	}
	s.emit(Event{Kind: EventConnected, Cursor: openCodeCursor(sessionID, cwd)})
	go s.readLoop(streamCtx)
	return s, nil
}

func validateOpenCodeModelOptions(model, reasoning string) error {
	if model == "" {
		if reasoning != "" {
			return errors.New("OpenCode reasoning effort requires a model")
		}
		return nil
	}
	parts := strings.Split(model, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return errors.New("OpenCode model must be provider/model")
	}
	return nil
}

func validOpenCodeAccess(access AccessMode) bool {
	switch access {
	case "", AccessAsk, AccessAutoAcceptEdits, AccessAuto, AccessFull:
		return true
	default:
		return false
	}
}

func (s *openCodeSession) abortStart() {
	if s.closed.CompareAndSwap(false, true) {
		s.cancelStream()
		s.streamMu.Lock()
		if s.stream != nil {
			_ = s.stream.Close()
		}
		s.streamMu.Unlock()
	}
	s.closeEvents()
}
func (s *openCodeSession) create(ctx context.Context, o StartOptions) error {
	body := map[string]any{"id": s.id, "location": map[string]string{"directory": s.cwd}, "agent": "build"}
	if o.Model != "" {
		parts := strings.SplitN(o.Model, "/", 2)
		if len(parts) != 2 {
			return errors.New("OpenCode model must be provider/model")
		}
		model := map[string]any{"providerID": parts[0], "id": parts[1]}
		if o.ReasoningEffort != "" {
			model["variant"] = o.ReasoningEffort
		}
		body["model"] = model
	}
	var envelope map[string]json.RawMessage
	return s.client().request(ctx, http.MethodPost, "/api/session", body, &envelope)
}
func parseOpenCodeCursor(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var v struct {
		Provider  Provider `json:"provider"`
		SessionID string   `json:"sessionId"`
	}
	if err := decodeUseNumber(raw, &v); err != nil {
		return "", err
	}
	if v.Provider != ProviderOpenCode {
		return "", fmt.Errorf("OpenCode cannot resume %q cursor", v.Provider)
	}
	if v.SessionID == "" {
		return "", errors.New("OpenCode cursor has no sessionId")
	}
	return v.SessionID, nil
}
func openCodeCursor(id, cwd string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"provider": ProviderOpenCode, "sessionId": id, "directory": cwd})
	return raw
}
func newOpenCodeID() string {
	var b [14]byte
	_, _ = rand.Read(b[:])
	return "ses_" + fmt.Sprintf("%012x", ^(uint64(time.Now().UnixMilli())*0x1000)&0xffffffffffff) + hex.EncodeToString(b[:])[:14]
}

type openCodeSession struct {
	httpMu          sync.RWMutex
	http            boundedHTTP
	reconnect       func() (boundedHTTP, openCodeRegistration, error)
	sessionCtx      context.Context
	id, cwd         string
	access          AccessMode
	sink            Sink
	cancelStream    context.CancelFunc
	streamMu        sync.Mutex
	stream          io.ReadCloser
	closed          atomic.Bool
	mu              sync.Mutex
	active          bool
	permissions     map[string]int
	permissionBytes int
	forms           map[string][]openCodeFormField
	formSizes       map[string]int
	formBytes       int
	toolInputs      map[string]string
	toolBytes       int
	parts           map[string]string
	overlapOffsets  map[string]int
	completedTools  map[string]bool
	lastTokens      uint64
	lastTitle       string
	turnBaseline    string
	lastStepID      string
	historyHead     string
	partBytes       int
	dedupeBytes     int
	eventMu         sync.Mutex
	events          chan Event
	eventBytes      int
	eventsClosed    bool
	slow            atomic.Bool
}

func (s *openCodeSession) client() boundedHTTP {
	s.httpMu.RLock()
	defer s.httpMu.RUnlock()
	return s.http
}

type openCodeFormOption struct{ Value, Label, Description string }
type openCodeFormField struct {
	Type, Key, Title, URL string
	Options               []openCodeFormOption
}

func (s *openCodeSession) Descriptor() Descriptor { return Descriptors()[ProviderOpenCode] }
func (s *openCodeSession) emit(e Event) bool {
	size := eventSize(e)
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
	s.events <- e
	return true
}
func (s *openCodeSession) dispatch() {
	for e := range s.events {
		s.eventMu.Lock()
		s.eventBytes -= eventSize(e)
		s.eventMu.Unlock()
		s.sink.Emit(e)
	}
	if s.slow.Load() {
		s.sink.Emit(Event{Kind: EventError, Text: "provider event sink exceeded bounded queue"})
	}
	s.sink.Emit(Event{Kind: EventProcessExited})
}
func (s *openCodeSession) closeEvents() {
	s.eventMu.Lock()
	if !s.eventsClosed {
		s.eventsClosed = true
		close(s.events)
	}
	s.eventMu.Unlock()
}
func (s *openCodeSession) readLoop(ctx context.Context) {
	backoff := 200 * time.Millisecond
	for {
		s.streamMu.Lock()
		stream := s.stream
		s.streamMu.Unlock()
		err := readSSE(ctx, stream, s.consume)
		_ = stream.Close()
		if ctx.Err() != nil || s.closed.Load() {
			s.closeEvents()
			return
		}
		if err != nil && !errors.Is(err, io.EOF) {
			s.emit(Event{Kind: EventError, Text: "OpenCode event stream disconnected: " + err.Error()})
		}
		for {
			select {
			case <-ctx.Done():
				s.closeEvents()
				return
			case <-time.After(backoff):
			}
			probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			nextHTTP, registration, probeErr := s.reconnect()
			if probeErr == nil {
				_, probeErr = probeOpenCode(probeCtx, nextHTTP, registration)
			}
			cancel()
			var next io.ReadCloser
			if probeErr == nil {
				next, probeErr = nextHTTP.openSSE(ctx, "/api/event")
			}
			if probeErr == nil {
				settled, reconcileErr := s.reconcile(ctx, nextHTTP)
				if reconcileErr != nil {
					_ = next.Close()
					if ctx.Err() != nil {
						s.closeEvents()
						return
					}
					s.emit(Event{Kind: EventError, Text: "OpenCode reconnect reconciliation failed: " + reconcileErr.Error()})
					if backoff < 5*time.Second {
						backoff *= 2
					}
					continue
				}
				if settled {
					// The pre-snapshot stream can contain the same terminal turn
					// frames that reconciliation just emitted. finish deliberately
					// clears turn-local dedupe, so establish a fresh post-snapshot
					// boundary instead of feeding those stale frames through it.
					_ = next.Close()
					next, probeErr = nextHTTP.openSSE(ctx, "/api/event")
					if probeErr != nil {
						if backoff < 5*time.Second {
							backoff *= 2
						}
						continue
					}
				}
				s.httpMu.Lock()
				s.http = nextHTTP
				s.httpMu.Unlock()
				s.streamMu.Lock()
				s.stream = next
				s.streamMu.Unlock()
				backoff = 200 * time.Millisecond
				break
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	}
}

func (s *openCodeSession) reconcile(parent context.Context, client boundedHTTP) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	var session struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := client.request(ctx, http.MethodGet, "/api/session/"+s.id, nil, &session); err != nil {
		return false, err
	}
	var permissions struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := client.request(ctx, http.MethodGet, "/api/session/"+s.id+"/permission", nil, &permissions); err != nil {
		return false, err
	}
	seenPermissions := make(map[string]struct{}, len(permissions.Data))
	for _, raw := range permissions.Data {
		var data map[string]json.RawMessage
		if decodeUseNumber(raw, &data) != nil || rawString(data["id"]) == "" {
			return false, errors.New("OpenCode permission snapshot is malformed")
		}
		seenPermissions[rawString(data["id"])] = struct{}{}
	}
	var forms struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := client.request(ctx, http.MethodGet, "/api/session/"+s.id+"/form", nil, &forms); err != nil {
		return false, err
	}
	seenForms := make(map[string]struct{}, len(forms.Data))
	for _, raw := range forms.Data {
		var form map[string]json.RawMessage
		if decodeUseNumber(raw, &form) != nil || rawString(form["id"]) == "" || rawString(form["sessionID"]) != s.id {
			return false, errors.New("OpenCode form snapshot is malformed")
		}
		seenForms[rawString(form["id"])] = struct{}{}
	}
	replay, settled, err := s.fetchReplayMessages(ctx, client)
	if err != nil {
		return false, err
	}
	// Do not mutate reconciliation state until every authoritative snapshot and
	// the bounded message gap have been fetched successfully.
	for _, raw := range permissions.Data {
		var data map[string]json.RawMessage
		_ = decodeUseNumber(raw, &data)
		id := rawString(data["id"])
		s.mu.Lock()
		_, known := s.permissions[id]
		s.mu.Unlock()
		if !known {
			if err := s.consumePermission(data, raw); err != nil {
				return false, err
			}
		}
	}
	s.mu.Lock()
	for id := range s.permissions {
		if _, ok := seenPermissions[id]; !ok {
			s.permissionBytes -= s.permissions[id]
			delete(s.permissions, id)
		}
	}
	s.mu.Unlock()
	s.emitTitle(stripPrivate(rawString(session.Data["title"])), session.Data["title"])
	for _, raw := range forms.Data {
		var form map[string]json.RawMessage
		if decodeUseNumber(raw, &form) != nil {
			return false, errors.New("OpenCode form snapshot is malformed")
		}
		id := rawString(form["id"])
		if id == "" {
			return false, errors.New("OpenCode form snapshot has no id")
		}
		s.mu.Lock()
		_, known := s.forms[id]
		s.mu.Unlock()
		if !known {
			if err := s.consumeForm(map[string]json.RawMessage{"form": raw}, raw); err != nil {
				return false, err
			}
		}
	}
	s.mu.Lock()
	for id := range s.forms {
		if _, ok := seenForms[id]; !ok {
			delete(s.forms, id)
			s.formBytes -= s.formSizes[id]
			delete(s.formSizes, id)
		}
	}
	if s.formBytes < 0 {
		s.formBytes = 0
	}
	s.mu.Unlock()
	// The replacement stream was opened before this snapshot. Its buffered
	// deltas may overlap the snapshot, so remember the pre-snapshot offsets and
	// let normal event consumption suppress exactly that overlap.
	s.mu.Lock()
	s.overlapOffsets = make(map[string]int, len(s.parts))
	for key, text := range s.parts {
		s.overlapOffsets[key] = len(text)
	}
	s.mu.Unlock()
	for _, message := range replay {
		if err := s.replayMessage(message); err != nil {
			return false, err
		}
	}
	if settled {
		s.finish(true, "", nil)
	}
	return settled, nil
}

func (s *openCodeSession) fetchReplayMessages(ctx context.Context, client boundedHTTP) ([]json.RawMessage, bool, error) {
	s.mu.Lock()
	baseline, active := s.turnBaseline, s.active
	s.mu.Unlock()
	if !active {
		return nil, false, nil
	}
	totalBytes, totalMessages := 0, 0
	cursor := ""
	seen := map[string]struct{}{}
	settled := false
	foundBaseline := baseline == ""
	var replay []json.RawMessage
	for pageIndex := 0; pageIndex < 256; pageIndex++ {
		path := "/api/session/" + s.id + "/message?limit=200&order=desc"
		if cursor != "" {
			path = "/api/session/" + s.id + "/message?limit=200&cursor=" + url.QueryEscape(cursor)
		}
		var page struct {
			Data   []json.RawMessage `json:"data"`
			Cursor struct {
				Next string `json:"next"`
			} `json:"cursor"`
		}
		if err := client.request(ctx, http.MethodGet, path, nil, &page); err != nil {
			return nil, false, err
		}
		if pageIndex == 0 {
			for _, raw := range page.Data {
				var message map[string]json.RawMessage
				if decodeUseNumber(raw, &message) != nil {
					continue
				}
				kind := rawString(message["type"])
				if kind == "idle" {
					settled = true
					break
				}
				if kind == "user" || kind == "assistant" {
					break
				}
			}
		}
		stop := false
		for _, raw := range page.Data {
			totalBytes += len(raw)
			totalMessages++
			if totalBytes > maxQueuedEventBytes || totalMessages > 10000 {
				return nil, false, errors.New("OpenCode replay history exceeds limit")
			}
			var message map[string]json.RawMessage
			if decodeUseNumber(raw, &message) != nil {
				return nil, false, errors.New("OpenCode replay message is malformed")
			}
			id := rawString(message["id"])
			if baseline != "" && id == baseline {
				foundBaseline, stop = true, true
				break
			}
			if rawString(message["type"]) == "assistant" {
				replay = append(replay, cloneRaw(raw))
			}
		}
		if stop {
			break
		}
		if len(page.Data) == 0 {
			foundBaseline = baseline == ""
			break
		}
		if page.Cursor.Next == "" {
			return nil, false, errors.New("OpenCode replay page has no continuation cursor")
		}
		if _, duplicate := seen[page.Cursor.Next]; duplicate {
			return nil, false, errors.New("OpenCode replay cursor repeated")
		}
		seen[page.Cursor.Next] = struct{}{}
		cursor = page.Cursor.Next
	}
	if !foundBaseline {
		return nil, false, errors.New("OpenCode turn baseline is absent from bounded history")
	}
	for left, right := 0, len(replay)-1; left < right; left, right = left+1, right-1 {
		replay[left], replay[right] = replay[right], replay[left]
	}
	return replay, settled, nil
}

func (s *openCodeSession) snapshotMessageHead(ctx context.Context, client boundedHTTP) (string, error) {
	var page struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := client.request(ctx, http.MethodGet, "/api/session/"+s.id+"/message?limit=1&order=desc", nil, &page); err != nil {
		return "", err
	}
	if len(page.Data) == 0 {
		return "", nil
	}
	var message map[string]json.RawMessage
	if decodeUseNumber(page.Data[0], &message) != nil || rawString(message["id"]) == "" {
		return "", errors.New("OpenCode message baseline is malformed")
	}
	return rawString(message["id"]), nil
}

func (s *openCodeSession) replayMessage(raw json.RawMessage) error {
	var message map[string]json.RawMessage
	if decodeUseNumber(raw, &message) != nil || rawString(message["type"]) != "assistant" {
		return errors.New("OpenCode replay message is malformed")
	}
	messageID := rawString(message["id"])
	var content []json.RawMessage
	if messageID == "" || decodeUseNumber(message["content"], &content) != nil {
		return errors.New("OpenCode replay assistant message is malformed")
	}
	s.mu.Lock()
	s.lastStepID = messageID
	s.mu.Unlock()
	for ordinal, partRaw := range content {
		var part map[string]json.RawMessage
		if decodeUseNumber(partRaw, &part) != nil {
			continue
		}
		switch rawString(part["type"]) {
		case "text", "reasoning":
			reasoning := rawString(part["type"]) == "reasoning"
			text := stripPrivate(rawString(part["text"]))
			key := fmt.Sprintf("%s:%d:%t", messageID, ordinal, reasoning)
			s.mu.Lock()
			seen := s.parts[key]
			if _, exists := s.overlapOffsets[key]; !exists {
				s.overlapOffsets[key] = 0
			}
			suffix, ok := strings.CutPrefix(text, seen)
			if ok {
				_, exists := s.parts[key]
				nextBytes := s.partBytes - len(seen) + len(text)
				if (!exists && len(s.parts) >= 4096) || nextBytes > maxQueuedEventBytes {
					s.mu.Unlock()
					return errors.New("OpenCode replay state exceeds limit")
				}
				s.parts[key], s.partBytes = text, nextBytes
			}
			s.mu.Unlock()
			if ok && suffix != "" {
				kind := EventTextDelta
				if reasoning {
					kind = EventReasoningDelta
				}
				s.emit(Event{Kind: kind, Text: suffix, Raw: cloneRaw(partRaw)})
			}
		case "tool":
			callID, name := rawString(part["id"]), firstNonempty(rawString(part["name"]), "Tool")
			var state map[string]json.RawMessage
			_ = decodeUseNumber(part["state"], &state)
			status := rawString(state["status"])
			if callID == "" || (status != "completed" && status != "error") {
				continue
			}
			key := messageID + "\x00" + callID
			s.mu.Lock()
			done := s.completedTools[key]
			if !done {
				if len(s.completedTools) >= 4096 || s.dedupeBytes+len(key) > maxQueuedEventBytes {
					s.mu.Unlock()
					return errors.New("OpenCode tool replay state exceeds limit")
				}
				s.completedTools[key] = true
				s.dedupeBytes += len(key)
			}
			if input, exists := s.toolInputs[key]; exists {
				s.toolBytes -= len(key) + len(input)
				delete(s.toolInputs, key)
				if s.toolBytes < 0 {
					s.toolBytes = 0
				}
			}
			s.mu.Unlock()
			if done {
				continue
			}
			output := cloneRaw(state["content"])
			if status == "error" && len(output) == 0 {
				output = cloneRaw(state["error"])
			}
			s.emit(Event{Kind: EventActivity, Activity: &Activity{ID: callID, Kind: streamToolKind(name), Title: streamToolTitle(name, state["input"]), Tool: name, Arguments: cloneRaw(state["input"]), Output: output, Complete: true, Failed: status == "error"}, Raw: cloneRaw(partRaw)})
		}
	}
	tokenData := map[string]json.RawMessage{"tokens": message["tokens"]}
	s.consumeUsage(tokenData, raw)
	return nil
}
func (s *openCodeSession) consume(raw json.RawMessage) error {
	var envelope map[string]json.RawMessage
	if err := decodeUseNumber(raw, &envelope); err != nil {
		return err
	}
	eventRaw := envelope["event"]
	if len(eventRaw) == 0 {
		eventRaw = raw
	}
	var event map[string]json.RawMessage
	if err := decodeUseNumber(eventRaw, &event); err != nil {
		return err
	}
	kind := rawString(event["type"])
	var data map[string]json.RawMessage
	_ = decodeUseNumber(event["data"], &data)
	sid := firstNonempty(rawString(data["sessionID"]), rawString(data["sessionId"]))
	if sid != "" && sid != s.id {
		return nil
	}
	switch kind {
	case "session.execution.started":
		s.mu.Lock()
		if !s.active {
			s.active = true
			s.turnBaseline = s.historyHead
			s.mu.Unlock()
			s.emit(Event{Kind: EventTurnStarted, Raw: cloneRaw(raw)})
		} else {
			s.mu.Unlock()
		}
	case "session.step.started":
		s.mu.Lock()
		s.lastStepID = rawString(data["assistantMessageID"])
		s.mu.Unlock()
	case "session.text.delta":
		if text := stripPrivate(rawString(data["delta"])); text != "" {
			suffix, err := s.trackDelta(data, text, false)
			if err != nil {
				return err
			}
			if suffix != "" {
				s.emit(Event{Kind: EventTextDelta, Text: suffix, Raw: cloneRaw(raw)})
			}
		}
	case "session.reasoning.delta":
		if text := stripPrivate(rawString(data["delta"])); text != "" {
			suffix, err := s.trackDelta(data, text, true)
			if err != nil {
				return err
			}
			if suffix != "" {
				s.emit(Event{Kind: EventReasoningDelta, Text: suffix, Raw: cloneRaw(raw)})
			}
		}
	case "session.text.ended":
		if err := s.trackPart(data, rawString(data["text"]), false, true); err != nil {
			return err
		}
	case "session.reasoning.ended":
		if err := s.trackPart(data, rawString(data["text"]), true, true); err != nil {
			return err
		}
	case "session.tool.input.started", "session.tool.input.delta", "session.tool.called", "session.tool.progress", "session.tool.success", "session.tool.failed":
		return s.consumeTool(kind, data, raw)
	case "session.execution.succeeded":
		s.finish(true, "", raw)
	case "session.execution.failed", "session.execution.interrupted":
		if kind == "session.execution.interrupted" && rawString(data["reason"]) == "shutdown" {
			s.emit(Event{Kind: EventError, Text: "OpenCode service restarted; reconnecting", Raw: cloneRaw(raw)})
			return nil
		}
		summary := firstNonempty(openCodeErrorMessage(data["error"]), rawString(data["message"]), rawString(data["reason"]), kind)
		s.finish(false, summary, raw)
	case "permission.asked":
		return s.consumePermission(data, raw)
	case "form.created":
		return s.consumeForm(data, raw)
	case "session.step.ended":
		s.consumeUsage(data, raw)
	case "session.renamed":
		s.emitTitle(stripPrivate(rawString(data["title"])), raw)
	case "session.deleted":
		s.emit(Event{Kind: EventError, Text: "OpenCode session was deleted", Raw: cloneRaw(raw)})
		return io.EOF
	}
	return nil
}

func (s *openCodeSession) emitTitle(title string, raw json.RawMessage) {
	if title == "" {
		return
	}
	s.mu.Lock()
	if title == s.lastTitle {
		s.mu.Unlock()
		return
	}
	s.lastTitle = title
	s.mu.Unlock()
	s.emit(Event{Kind: EventAutoTitleUpdated, Text: title, Raw: cloneRaw(raw)})
}

func openCodePartKey(data map[string]json.RawMessage, reasoning bool) string {
	message := rawString(data["assistantMessageID"])
	ordinal, ok := rawUint64(data["ordinal"])
	if message == "" || !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d:%t", message, ordinal, reasoning)
}

func (s *openCodeSession) trackPart(data map[string]json.RawMessage, text string, reasoning, replace bool) error {
	key := openCodePartKey(data, reasoning)
	if key == "" {
		return nil
	}
	s.mu.Lock()
	old, exists := s.parts[key]
	next := old + text
	if replace {
		next = text
	}
	bytes := s.partBytes - len(old) + len(next)
	if (!exists && len(s.parts) >= 4096) || bytes > maxQueuedEventBytes {
		s.mu.Unlock()
		return errors.New("OpenCode replay state exceeds limit")
	}
	s.parts[key], s.partBytes = next, bytes
	s.mu.Unlock()
	return nil
}

func (s *openCodeSession) trackDelta(data map[string]json.RawMessage, text string, reasoning bool) (string, error) {
	key := openCodePartKey(data, reasoning)
	if key == "" {
		return text, nil
	}
	s.mu.Lock()
	old, exists := s.parts[key]
	if offset, overlap := s.overlapOffsets[key]; overlap {
		if offset < 0 || offset > len(old) {
			delete(s.overlapOffsets, key)
		} else {
			remaining := old[offset:]
			matched := len(text)
			if matched > len(remaining) {
				matched = len(remaining)
			}
			if strings.HasPrefix(remaining, text[:matched]) {
				s.overlapOffsets[key] = offset + matched
				if matched == len(text) {
					s.mu.Unlock()
					return "", nil
				}
				text = text[matched:]
				delete(s.overlapOffsets, key)
			} else {
				delete(s.overlapOffsets, key)
			}
		}
	}
	nextBytes := s.partBytes + len(text)
	if (!exists && len(s.parts) >= 4096) || nextBytes > maxQueuedEventBytes {
		s.mu.Unlock()
		return "", errors.New("OpenCode replay state exceeds limit")
	}
	s.parts[key] = old + text
	s.partBytes = nextBytes
	s.mu.Unlock()
	return text, nil
}
func (s *openCodeSession) consumeTool(kind string, data map[string]json.RawMessage, raw json.RawMessage) error {
	id := firstNonempty(rawString(data["callID"]), rawString(data["id"]), rawString(data["toolCallID"]))
	name := firstNonempty(rawString(data["tool"]), rawString(data["name"]), "Tool")
	key := rawString(data["assistantMessageID"]) + "\x00" + id
	if id == "" {
		return nil
	}
	s.mu.Lock()
	alreadyComplete := s.completedTools[key]
	s.mu.Unlock()
	if alreadyComplete {
		return nil
	}
	if kind == "session.tool.input.started" {
		s.mu.Lock()
		if _, exists := s.toolInputs[key]; exists {
			s.mu.Unlock()
			return nil
		}
		if len(s.toolInputs) >= 4096 || s.toolBytes+len(key) > maxQueuedEventBytes {
			s.mu.Unlock()
			return errors.New("OpenCode tool input state exceeds limit")
		}
		s.toolInputs[key] = ""
		s.toolBytes += len(key)
		s.mu.Unlock()
		s.emit(Event{Kind: EventActivity, Activity: &Activity{ID: id, Kind: streamToolKind(name), Title: name, Tool: name}, Raw: cloneRaw(raw)})
		return nil
	}
	if kind == "session.tool.input.delta" {
		delta := rawString(data["delta"])
		s.mu.Lock()
		current, exists := s.toolInputs[key]
		if !exists {
			s.mu.Unlock()
			return nil
		}
		if s.toolBytes+len(delta) > maxQueuedEventBytes {
			s.mu.Unlock()
			return errors.New("OpenCode tool input state exceeds limit")
		}
		s.toolInputs[key] = current + delta
		s.toolBytes += len(delta)
		current = s.toolInputs[key]
		s.mu.Unlock()
		if json.Valid([]byte(current)) {
			arguments := json.RawMessage(current)
			s.emit(Event{Kind: EventActivity, Activity: &Activity{ID: id, Kind: streamToolKind(name), Title: streamToolTitle(name, arguments), Tool: name, Arguments: arguments}, Raw: cloneRaw(raw)})
		}
		return nil
	}
	arguments := cloneRaw(data["input"])
	if text := rawString(data["arguments"]); text != "" && json.Valid([]byte(text)) {
		arguments = json.RawMessage(text)
	}
	output := cloneRaw(data["content"])
	if len(output) == 0 {
		output = cloneRaw(data["output"])
	}
	a := &Activity{ID: id, Kind: streamToolKind(name), Title: streamToolTitle(name, arguments), Tool: name, Arguments: arguments, Output: output, Complete: kind == "session.tool.success" || kind == "session.tool.failed", Failed: kind == "session.tool.failed"}
	if a.Complete {
		s.mu.Lock()
		if len(s.completedTools) >= 4096 || s.dedupeBytes+len(key) > maxQueuedEventBytes {
			s.mu.Unlock()
			return errors.New("OpenCode tool replay state exceeds limit")
		}
		s.completedTools[key] = true
		s.dedupeBytes += len(key)
		if input, exists := s.toolInputs[key]; exists {
			s.toolBytes -= len(key) + len(input)
			delete(s.toolInputs, key)
		}
		if s.toolBytes < 0 {
			s.toolBytes = 0
		}
		s.mu.Unlock()
	}
	s.emit(Event{Kind: EventActivity, Activity: a, Raw: cloneRaw(raw)})
	return nil
}

func openCodeErrorMessage(raw json.RawMessage) string {
	if text := rawString(raw); text != "" {
		return text
	}
	var value map[string]json.RawMessage
	if decodeUseNumber(raw, &value) == nil {
		return firstNonempty(rawString(value["message"]), rawString(value["name"]), rawString(value["type"]))
	}
	return ""
}
func (s *openCodeSession) consumeUsage(data map[string]json.RawMessage, raw json.RawMessage) {
	var tokens struct {
		Input, Output, Reasoning uint64
		Cache                    struct{ Read, Write uint64 }
	}
	if decodeUseNumber(data["tokens"], &tokens) != nil {
		return
	}
	n := tokens.Input + tokens.Output + tokens.Reasoning + tokens.Cache.Read + tokens.Cache.Write
	s.mu.Lock()
	changed := n != s.lastTokens
	s.lastTokens = n
	s.mu.Unlock()
	if changed {
		s.emit(Event{Kind: EventUsageUpdated, ContextTokens: &n, Raw: cloneRaw(raw)})
	}
}
func (s *openCodeSession) consumePermission(data map[string]json.RawMessage, raw json.RawMessage) error {
	id := rawString(data["id"])
	if id == "" {
		return errors.New("OpenCode permission missing id")
	}
	if s.access != AccessAsk {
		ctx, cancel := context.WithTimeout(s.sessionCtx, 30*time.Second)
		defer cancel()
		return s.replyPermission(ctx, id, "once")
	}
	s.mu.Lock()
	if _, duplicate := s.permissions[id]; duplicate {
		s.mu.Unlock()
		return errors.New("duplicate OpenCode permission id")
	}
	if len(s.permissions) >= 1024 || s.permissionBytes+len(raw) > maxQueuedEventBytes {
		s.mu.Unlock()
		return errors.New("OpenCode pending permission limit exceeded")
	}
	s.permissions[id] = len(raw)
	s.permissionBytes += len(raw)
	s.mu.Unlock()
	p := &Permission{Title: firstNonempty(rawString(data["permission"]), "Permission"), Detail: rawString(data["message"]), Options: []PermissionOption{{ID: "once", Label: "Allow once", Allow: true}, {ID: "reject", Label: "Deny"}}}
	req, _ := json.Marshal(id)
	s.emit(Event{Kind: EventPermission, RequestID: req, Permission: p, Raw: cloneRaw(raw)})
	return nil
}
func (s *openCodeSession) consumeForm(data map[string]json.RawMessage, raw json.RawMessage) error {
	formData := data
	if nested := data["form"]; len(nested) != 0 {
		if decodeUseNumber(nested, &formData) != nil {
			return errors.New("OpenCode form is malformed")
		}
	}
	id, title := rawString(formData["id"]), rawString(formData["title"])
	if rawString(formData["sessionID"]) != s.id {
		return errors.New("OpenCode form belongs to another session")
	}
	var fields []openCodeFormField
	if id == "" || decodeUseNumber(formData["fields"], &fields) != nil || len(fields) == 0 {
		return errors.New("OpenCode form is malformed")
	}
	qs := make([]UserInputQuestion, 0, len(fields))
	for _, f := range fields {
		if f.Key == "" || f.Type == "" {
			return errors.New("OpenCode form field is malformed")
		}
		header := firstNonempty(f.Title, f.Key)
		question := firstNonempty(title, header)
		if f.Type == "external" && f.URL != "" {
			question = f.URL
		}
		options := make([]UserInputOption, 0, len(f.Options))
		for _, option := range f.Options {
			options = append(options, UserInputOption{Label: option.Label, Description: option.Description})
		}
		qs = append(qs, UserInputQuestion{ID: f.Key, Header: header, Question: question, Options: options, MultiSelect: f.Type == "multiselect"})
	}
	s.mu.Lock()
	if _, duplicate := s.forms[id]; duplicate {
		s.mu.Unlock()
		return errors.New("duplicate OpenCode form id")
	}
	if len(s.forms) >= 256 || s.formBytes+len(raw) > maxQueuedEventBytes {
		s.mu.Unlock()
		return errors.New("OpenCode pending form limit exceeded")
	}
	s.forms[id] = fields
	s.formSizes[id] = len(raw)
	s.formBytes += len(raw)
	s.mu.Unlock()
	req, _ := json.Marshal(id)
	s.emit(Event{Kind: EventUserInputRequested, RequestID: req, Questions: qs, Raw: cloneRaw(raw)})
	return nil
}
func (s *openCodeSession) finish(ok bool, summary string, raw json.RawMessage) {
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return
	}
	s.active = false
	if s.lastStepID != "" {
		s.historyHead = s.lastStepID
	}
	s.parts = make(map[string]string)
	s.overlapOffsets = make(map[string]int)
	s.completedTools = make(map[string]bool)
	s.toolInputs = make(map[string]string)
	s.partBytes, s.dedupeBytes, s.toolBytes = 0, 0, 0
	s.turnBaseline, s.lastStepID = "", ""
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnFinished, Success: ok, Summary: summary, Raw: cloneRaw(raw)})
}
func (s *openCodeSession) Prompt(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	baseline, err := s.snapshotMessageHead(ctx, s.client())
	if err != nil {
		return fmt.Errorf("OpenCode prompt baseline: %w", err)
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("OpenCode turn already active")
	}
	s.active = true
	s.turnBaseline = baseline
	s.mu.Unlock()
	s.emit(Event{Kind: EventTurnStarted})
	var out map[string]json.RawMessage
	if err := s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/prompt", map[string]string{"text": text}, &out); err != nil {
		s.finish(false, err.Error(), nil)
		return err
	}
	return nil
}
func (s *openCodeSession) Steer(ctx context.Context, text string) error {
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		return errors.New("OpenCode has no active turn")
	}
	var out map[string]json.RawMessage
	err := s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/prompt", map[string]string{"text": text, "delivery": "steer"}, &out)
	if err != nil {
		s.emit(Event{Kind: EventSteerRejected, Text: text, Summary: err.Error()})
		return err
	}
	s.emit(Event{Kind: EventSteerAccepted, Text: text})
	return nil
}
func (s *openCodeSession) Cancel(ctx context.Context) error {
	var out map[string]json.RawMessage
	return s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/interrupt", nil, &out)
}
func (s *openCodeSession) replyPermission(ctx context.Context, id, decision string) error {
	return s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/permission/"+id+"/reply", map[string]string{"decision": decision}, nil)
}
func (s *openCodeSession) RespondPermission(ctx context.Context, raw json.RawMessage, option string) error {
	id, err := decodeRequestID(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	_, ok := s.permissions[id]
	s.mu.Unlock()
	if !ok {
		return errors.New("unknown OpenCode permission")
	}
	decision := "once"
	if option == "reject" {
		decision = "reject"
	} else if option != "once" {
		return errors.New("invalid OpenCode permission option")
	}
	if err := s.replyPermission(ctx, id, decision); err != nil {
		return err
	}
	s.mu.Lock()
	s.permissionBytes -= s.permissions[id]
	delete(s.permissions, id)
	if s.permissionBytes < 0 {
		s.permissionBytes = 0
	}
	s.mu.Unlock()
	return nil
}
func (s *openCodeSession) RespondQuestions(ctx context.Context, raw json.RawMessage, answers []UserInputAnswer) error {
	id, err := decodeRequestID(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	fields, ok := s.forms[id]
	s.mu.Unlock()
	if !ok {
		return errors.New("unknown OpenCode form")
	}
	answer := map[string]any{}
	for _, f := range fields {
		for _, a := range answers {
			if a.QuestionID == f.Key {
				resolve := func(value string) string {
					for _, option := range f.Options {
						if option.Label == value {
							return option.Value
						}
					}
					return value
				}
				if f.Type == "multiselect" {
					values := make([]string, 0, len(a.Answers))
					for _, value := range a.Answers {
						values = append(values, resolve(value))
					}
					answer[f.Key] = values
				} else if len(a.Answers) > 0 {
					value := resolve(a.Answers[0])
					switch f.Type {
					case "boolean":
						answer[f.Key] = value == "true" || value == "yes" || value == "1"
					case "number", "integer":
						var number json.Number = json.Number(value)
						if _, err := number.Float64(); err == nil {
							answer[f.Key] = number
						} else {
							answer[f.Key] = value
						}
					default:
						answer[f.Key] = value
					}
				}
			}
		}
	}
	if err := s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/form/"+id+"/reply", map[string]any{"answer": answer}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.forms, id)
	s.formBytes -= s.formSizes[id]
	delete(s.formSizes, id)
	s.mu.Unlock()
	return nil
}
func (s *openCodeSession) Goal(context.Context, GoalOperation) error { return ErrUnsupported }
func (s *openCodeSession) ApplyOptions(ctx context.Context, o SessionOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if o.ServiceTier != "" {
		return false, fmt.Errorf("%w: OpenCode service tier", ErrUnsupported)
	}
	if !validOpenCodeAccess(o.Access) {
		return false, fmt.Errorf("%w: OpenCode access mode %q", ErrUnsupported, o.Access)
	}
	if err := validateOpenCodeModelOptions(o.Model, o.ReasoningEffort); err != nil {
		return false, err
	}
	if o.Model != "" {
		parts := strings.SplitN(o.Model, "/", 2)
		if len(parts) != 2 {
			return true, errors.New("OpenCode model must be provider/model")
		}
		model := map[string]any{"providerID": parts[0], "id": parts[1]}
		if o.ReasoningEffort != "" {
			model["variant"] = o.ReasoningEffort
		}
		if err := s.client().request(ctx, http.MethodPost, "/api/session/"+s.id+"/model", map[string]any{"model": model}, nil); err != nil {
			return true, err
		}
	}
	if o.Access != "" {
		s.access = o.Access
	}
	return true, nil
}
func (s *openCodeSession) Rollback(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *openCodeSession) Fork(context.Context, uint64) (json.RawMessage, error) {
	return nil, ErrUnsupported
}
func (s *openCodeSession) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancelStream()
	s.streamMu.Lock()
	if s.stream != nil {
		_ = s.stream.Close()
	}
	s.streamMu.Unlock()
	return nil
}
