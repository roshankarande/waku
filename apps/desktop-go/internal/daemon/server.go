package daemon

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
	"github.com/roshankarande/waku/apps/desktop-go/internal/providers"
	"github.com/roshankarande/waku/apps/desktop-go/internal/storage"
	"github.com/roshankarande/waku/apps/desktop-go/internal/workspace"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type ServerConfig struct {
	DataDir        string
	DatabasePath   string
	Token          string
	AllowedOrigins []string
	AllowShutdown  bool
	DaemonVersion  string
	Adapter        providers.Adapter
	Adapters       map[providers.Provider]providers.Adapter
}

type Server struct {
	production      *storage.Production
	hub             *Hub
	dispatcher      *Dispatcher
	backend         *runtimeBackend
	tokenHash       [32]byte
	origins         map[string]bool
	version         string
	allowShutdown   bool
	http            *http.Server
	ctx             context.Context
	cancel          context.CancelFunc
	connections     sync.Map
	preauthCount    atomic.Int64
	connectionCount atomic.Int64
	notifyID        atomic.Uint64
	revision        atomic.Uint64
	closeOnce       sync.Once
	closeErr        error
}

func NewServer(ctx context.Context, config ServerConfig) (*Server, error) {
	if strings.TrimSpace(config.Token) == "" {
		return nil, errors.New("daemon authentication token is empty")
	}
	if config.DataDir == "" {
		var err error
		config.DataDir, err = storage.DevelopmentDataDir()
		if err != nil {
			return nil, err
		}
	}
	if config.DatabasePath == "" {
		config.DatabasePath = filepath.Join(config.DataDir, "waku.sqlite3")
	}
	adapters, err := configuredAdapters(config)
	if err != nil {
		return nil, err
	}
	production, err := storage.BootstrapProduction(ctx, config.DataDir, config.DatabasePath, time.Now())
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*Server, error) { _ = production.Close(); return nil, cause }
	hub, err := NewHub(4096, 64*1024*1024, 4096, 64*1024*1024)
	if err != nil {
		return fail(err)
	}
	if config.DaemonVersion == "" {
		config.DaemonVersion = "waku-go-dev"
	}
	serverCtx, cancel := context.WithCancel(ctx)
	s := &Server{production: production, hub: hub, tokenHash: sha256.Sum256([]byte(config.Token)), origins: make(map[string]bool), version: config.DaemonVersion, allowShutdown: config.AllowShutdown, ctx: serverCtx, cancel: cancel}
	for _, origin := range config.AllowedOrigins {
		s.origins[origin] = true
	}
	s.backend = newRuntimeBackend(storage.TaskStateStore{DB: production.DB}, adapters, hub)
	s.backend.settings = &storage.SettingsStore{Path: filepath.Join(config.DataDir, "settings.json")}
	home, err := os.UserHomeDir()
	if err != nil {
		cancel()
		return fail(fmt.Errorf("resolve daemon home for skills: %w", err))
	}
	homePath, err := workspace.NewHostPath(home)
	if err != nil {
		cancel()
		return fail(err)
	}
	trashPath, err := workspace.NewHostPath(filepath.Join(config.DataDir, "skill-trash"))
	if err != nil {
		cancel()
		return fail(err)
	}
	var claudePath *workspace.HostPath
	if configured := os.Getenv("CLAUDE_CONFIG_DIR"); filepath.IsAbs(configured) {
		path, pathErr := workspace.NewHostPath(configured)
		if pathErr != nil {
			cancel()
			return fail(pathErr)
		}
		claudePath = &path
	}
	s.backend.skills, err = workspace.NewSkillsService(homePath, claudePath, trashPath, workspace.SkillsLimits{})
	if err != nil {
		cancel()
		return fail(err)
	}
	usageConfig := workspace.DefaultUsageHistoryConfig()
	usageConfig.RatesCachePath = filepath.Join(config.DataDir, "usage-model-rates.json")
	s.backend.usage, err = workspace.NewUsageHistoryService(usageConfig)
	if err != nil {
		cancel()
		return fail(err)
	}
	s.dispatcher = NewDispatcherWithLimits(64, 1024, 64*1024*1024, 128, 64*1024*1024, s.backend.handle)
	s.http = &http.Server{Handler: http.HandlerFunc(s.handleUpgrade), ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

func configuredAdapters(config ServerConfig) (map[providers.Provider]providers.Adapter, error) {
	adapters := defaultAdapters()
	catalog := providers.Descriptors()
	if config.Adapters != nil {
		adapters = make(map[providers.Provider]providers.Adapter, len(config.Adapters))
		for provider, adapter := range config.Adapters {
			if nilAdapter(adapter) {
				return nil, fmt.Errorf("custom adapter %q is nil", provider)
			}
			if described := adapter.Descriptor().Provider; described != provider {
				return nil, fmt.Errorf("custom adapter map key %q does not match descriptor provider %q", provider, described)
			}
			if _, known := catalog[provider]; !known {
				return nil, fmt.Errorf("custom adapter provider %q is not in the catalog", provider)
			}
			adapters[provider] = adapter
		}
	}
	if config.Adapter != nil {
		if nilAdapter(config.Adapter) {
			return nil, errors.New("custom adapter is nil")
		}
		provider := config.Adapter.Descriptor().Provider
		if _, known := catalog[provider]; !known {
			return nil, fmt.Errorf("custom adapter provider %q is not in the catalog", provider)
		}
		if config.Adapters != nil && adapters[provider] != nil {
			return nil, fmt.Errorf("duplicate custom adapter for provider %q", provider)
		}
		adapters[provider] = config.Adapter
	}
	for provider, descriptor := range catalog {
		if descriptor.Implemented && adapters[provider] == nil {
			return nil, fmt.Errorf("implemented provider %q has no daemon adapter", provider)
		}
		if descriptor.Implemented && !adapters[provider].Descriptor().Implemented {
			return nil, fmt.Errorf("implemented provider %q is wired to a gated adapter", provider)
		}
	}
	return adapters, nil
}

func nilAdapter(adapter providers.Adapter) bool {
	if adapter == nil {
		return true
	}
	value := reflect.ValueOf(adapter)
	return (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil()
}

func defaultAdapters() map[providers.Provider]providers.Adapter {
	return map[providers.Provider]providers.Adapter{
		providers.ProviderCodex:    providers.CodexAdapter{},
		providers.ProviderCursor:   providers.AcpAdapter{Provider: providers.ProviderCursor},
		providers.ProviderFx:       providers.AcpAdapter{Provider: providers.ProviderFx},
		providers.ProviderGrok:     providers.AcpAdapter{Provider: providers.ProviderGrok},
		providers.ProviderKimi:     providers.AcpAdapter{Provider: providers.ProviderKimi},
		providers.ProviderPi:       providers.PiAdapter{Provider: providers.ProviderPi},
		providers.ProviderOhMyPi:   providers.PiAdapter{Provider: providers.ProviderOhMyPi},
		providers.ProviderClaude:   providers.StreamingAdapter{Provider: providers.ProviderClaude},
		providers.ProviderAmp:      providers.StreamingAdapter{Provider: providers.ProviderAmp},
		providers.ProviderOpenCode: providers.OpenCodeAdapter{},
		providers.ProviderDeepSeek: providers.DeepSeekAdapter{},
	}
}

func (s *Server) DB() *sql.DB { return s.production.DB }
func (s *Server) Serve(listener net.Listener) error {
	err := s.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		shuttingDown, _ := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageShuttingDown)})
		s.connections.Range(func(key, _ any) bool {
			connection := key.(*serverConnection)
			_ = connection.ws.SetWriteDeadline(time.Now().Add(time.Second))
			_ = connection.ws.writeJSON(shuttingDown)
			_ = connection.ws.SetReadDeadline(time.Now())
			return true
		})
		s.cancel()
		s.closeErr = errors.Join(s.http.Shutdown(ctx), s.dispatcher.Shutdown(ctx))
		s.backend.close()
		s.hub.Close()
		s.connections.Range(func(key, _ any) bool { _ = key.(*serverConnection).close(); return true })
		s.closeErr = errors.Join(s.closeErr, s.production.Close())
	})
	return s.closeErr
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *Server) broadcast(message protocol.ServerMessage) {
	payload, err := protocol.Marshal(message)
	if err != nil {
		return
	}
	s.connections.Range(func(_, value any) bool { value.(*serverConnection).enqueue(payload); return true })
}

func (s *Server) broadcastExcept(skip *serverConnection, message protocol.ServerMessage) {
	payload, err := protocol.Marshal(message)
	if err != nil {
		return
	}
	s.connections.Range(func(key, value any) bool {
		if key.(*serverConnection) != skip {
			value.(*serverConnection).enqueue(payload)
		}
		return true
	})
}

func (s *Server) handleUpgrade(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != "/v1" {
		http.Error(writer, "unknown daemon endpoint", http.StatusNotFound)
		return
	}
	if origin := request.Header.Get("Origin"); origin != "" && !s.origins[origin] && request.Header.Get("X-Waku-Client") != "native" {
		http.Error(writer, "WebSocket origin is not allowed", http.StatusForbidden)
		return
	}
	if !headerToken(request.Header.Values("Connection"), "upgrade") || !strings.EqualFold(request.Header.Get("Upgrade"), "websocket") || request.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(writer, "invalid WebSocket upgrade", http.StatusBadRequest)
		return
	}
	if _, present := request.Header[http.CanonicalHeaderKey("Sec-WebSocket-Extensions")]; present {
		http.Error(writer, "WebSocket extensions are unsupported", http.StatusBadRequest)
		return
	}
	if _, present := request.Header[http.CanonicalHeaderKey("Sec-WebSocket-Protocol")]; present {
		http.Error(writer, "WebSocket subprotocols are unsupported", http.StatusBadRequest)
		return
	}
	if s.preauthCount.Add(1) > 32 {
		s.preauthCount.Add(-1)
		http.Error(writer, "daemon handshake limit exceeded", http.StatusServiceUnavailable)
		return
	}
	var releaseOnce sync.Once
	releasePreauth := func() { releaseOnce.Do(func() { s.preauthCount.Add(-1) }) }
	defer releasePreauth()
	key, err := base64.StdEncoding.DecodeString(request.Header.Get("Sec-WebSocket-Key"))
	if err != nil || len(key) != 16 {
		http.Error(writer, "invalid WebSocket key", http.StatusBadRequest)
		return
	}
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		http.Error(writer, "WebSocket unavailable", http.StatusInternalServerError)
		return
	}
	conn, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	accept := sha1.Sum([]byte(request.Header.Get("Sec-WebSocket-Key") + websocketGUID))
	if err := writeAllServer(conn, []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(accept[:])+"\r\n\r\n")); err != nil {
		conn.Close()
		return
	}
	ws := &serverWebSocket{Conn: conn, reader: buffered.Reader}
	s.serveConnection(ws, releasePreauth)
}

func headerToken(values []string, want string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

type serverConnection struct {
	server    *Server
	ws        *serverWebSocket
	ctx       context.Context
	cancel    context.CancelFunc
	out       chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (c *serverConnection) enqueue(payload []byte) bool {
	select {
	case c.out <- append([]byte(nil), payload...):
		return true
	default:
		_ = c.close()
		return false
	}
}
func (c *serverConnection) close() error {
	var err error
	c.closeOnce.Do(func() { c.cancel(); err = c.ws.Close() })
	return err
}
func (c *serverConnection) writer() {
	defer close(c.done)
	for {
		select {
		case payload := <-c.out:
			if c.ws.writeJSON(payload) != nil {
				_ = c.close()
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (s *Server) serveConnection(ws *serverWebSocket, releasePreauth func()) {
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	raw, err := ws.readJSON(protocol.MaxHandshakeBytes)
	if err != nil {
		ws.Close()
		return
	}
	var first protocol.ClientMessage
	if protocol.Decode(raw, &first) != nil || first.Hello == nil {
		ws.Close()
		return
	}
	if first.Hello.ProtocolVersion != protocol.Version {
		s.reject(ws, fmt.Sprintf("protocol %d is unsupported; expected %d", first.Hello.ProtocolVersion, protocol.Version))
		return
	}
	candidate := sha256.Sum256([]byte(first.Hello.Token))
	if subtle.ConstantTimeCompare(candidate[:], s.tokenHash[:]) != 1 {
		s.reject(ws, "authentication failed")
		return
	}
	releasePreauth()
	if s.connectionCount.Add(1) > 128 {
		s.connectionCount.Add(-1)
		s.reject(ws, "daemon connection limit exceeded")
		return
	}
	defer s.connectionCount.Add(-1)
	cursors := make([]Cursor, len(first.Hello.ResumeFrom))
	for i, cursor := range first.Hello.ResumeFrom {
		cursors[i] = Cursor{SessionID: string(cursor.SessionID), RuntimeID: string(cursor.RuntimeID), Epoch: string(cursor.Epoch), Sequence: cursor.Sequence}
	}
	sub := s.hub.Subscribe(cursors...)
	if gaps := sub.Gaps(); len(gaps) != 0 {
		sub.Close()
		s.reject(ws, "replay gap; reload task state and reconnect without the stale cursor")
		return
	}
	hello, _ := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageHello), ProtocolVersion: protocol.Version, DaemonVersion: s.version})
	if ws.writeJSON(hello) != nil {
		sub.Close()
		ws.Close()
		return
	}
	_ = ws.SetReadDeadline(time.Time{})
	ctx, cancel := context.WithCancel(s.ctx)
	connection := &serverConnection{server: s, ws: ws, ctx: ctx, cancel: cancel, out: make(chan []byte, 128), done: make(chan struct{})}
	s.connections.Store(connection, connection)
	defer s.connections.Delete(connection)
	defer connection.close()
	defer sub.Close()
	go connection.writer()
	go func() {
		for {
			event, ok := sub.Receive(ctx)
			if !ok {
				connection.close()
				return
			}
			var wire protocol.WireDriverEvent
			if protocol.Decode(event.Payload, &wire) != nil {
				connection.close()
				return
			}
			message, err := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageEvent), Event: &protocol.SequencedEvent{SessionID: protocol.UUID(event.SessionID), RuntimeID: protocol.UUID(event.RuntimeID), Epoch: protocol.UUID(event.Epoch), Sequence: event.Sequence, Event: wire}})
			if err != nil || !connection.enqueue(message) {
				return
			}
		}
	}()
	for {
		raw, err := ws.readJSON(protocol.MaxWireMessageBytes)
		if err != nil {
			return
		}
		var message protocol.ClientMessage
		if err := protocol.Decode(raw, &message); err != nil {
			return
		}
		switch message.Type {
		case protocol.MessageRequest:
			s.dispatch(connection, message.Request)
		case protocol.MessageShutdown:
			if !s.allowShutdown {
				rejected, _ := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageRejected), Message: "daemon shutdown is managed by its service owner"})
				connection.enqueue(rejected)
				continue
			}
			go s.Close()
			return
		case protocol.MessageHello:
			continue
		default:
			return
		}
	}
}

func (s *Server) reject(ws *serverWebSocket, message string) {
	payload, _ := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageRejected), Message: message})
	_ = ws.writeJSON(payload)
	_ = ws.Close()
}

func (s *Server) dispatch(connection *serverConnection, request *protocol.Request) {
	if request == nil {
		return
	}
	payload, err := protocol.Marshal(request)
	if err != nil {
		return
	}
	id := string(request.RequestID)
	notification := request.RequestID == protocol.NilUUID
	if notification {
		id = fmt.Sprintf("notification-%d", s.notifyID.Add(1))
	}
	result := s.dispatcher.Dispatch(connection.ctx, Request{ID: id, SessionID: string(request.SessionID), RuntimeID: string(request.RuntimeID), Payload: payload})
	kind, _ := protocol.CommandTypeOf(request.Command)
	go func() {
		response := <-result
		if notification {
			return
		}
		if response.Err == nil && !response.Cached && (kind == protocol.CommandSaveTaskState || kind == protocol.CommandRemoveSession) {
			s.broadcastExcept(connection, protocol.ServerMessage{Type: string(protocol.MessageTaskStateChanged), Revision: s.revision.Add(1)})
		}
		outcome := &protocol.ResponseOutcome{Status: "ok", Payload: json.RawMessage(response.Payload)}
		if response.Err != nil {
			outcome = &protocol.ResponseOutcome{Status: "error", Error: &protocol.RpcError{Message: response.Err.Error()}}
		}
		message, err := protocol.Marshal(protocol.ServerMessage{Type: string(protocol.MessageResponse), RequestID: request.RequestID, Outcome: outcome})
		if err == nil {
			connection.enqueue(message)
		}
	}()
}

// WriteReadiness emits the sole stdout handshake consumed by the desktop
// supervisor. The address must be the actual bound listener address.
func WriteReadiness(writer io.Writer, listener net.Listener) error {
	ready := protocol.DaemonReady{Address: listener.Addr().String(), ProtocolVersion: protocol.Version, PID: uint32(os.Getpid())}
	payload, err := protocol.Marshal(ready)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return writeAllServer(writer, payload)
}
