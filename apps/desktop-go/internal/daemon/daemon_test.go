package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
	"github.com/roshankarande/waku/apps/desktop-go/internal/providers"
	"github.com/roshankarande/waku/apps/desktop-go/internal/storage"
	terminaltransport "github.com/roshankarande/waku/apps/desktop-go/internal/terminal"
	workspacesvc "github.com/roshankarande/waku/apps/desktop-go/internal/workspace"
)

func mustHub(t *testing.T, replayCount, replayBytes, subscriberCount, subscriberBytes int) *Hub {
	t.Helper()
	h, err := NewHub(replayCount, replayBytes, subscriberCount, subscriberBytes)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestByteCacheBoundsCountAndBytes(t *testing.T) {
	c := NewByteCache(2, 5)
	c.Put("one", []byte("111"))
	c.Put("two", []byte("22"))
	c.Put("three", []byte("333"))
	if _, ok := c.Get("one"); ok {
		t.Fatal("oldest byte-overflow entry was retained")
	}
	if count, bytes := c.Stats(); count != 2 || bytes != 5 {
		t.Fatalf("stats = %d, %d", count, bytes)
	}
	c.Put("oversized", []byte("123456"))
	if _, ok := c.Get("oversized"); ok {
		t.Fatal("oversized value was admitted")
	}
}

func TestDispatcherDeduplicatesInflightAndCachesResponse(t *testing.T) {
	gate := make(chan struct{})
	var calls int
	var mu sync.Mutex
	d := NewDispatcher(2, 4, 1024, func(_ context.Context, request Request) ([]byte, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		<-gate
		return append([]byte("reply:"), request.Payload...), nil
	})
	first := d.Dispatch(context.Background(), Request{ID: "same", SessionID: "s", Payload: []byte("one")})
	second := d.Dispatch(context.Background(), Request{ID: "same", SessionID: "s", Payload: []byte("one")})
	collision := <-d.Dispatch(context.Background(), Request{ID: "same", SessionID: "s", Payload: []byte("ignored")})
	if !errors.Is(collision.Err, ErrRequestCollision) {
		t.Fatalf("collision=%#v", collision)
	}
	close(gate)
	for _, response := range []Response{<-first, <-second} {
		if response.Err != nil || string(response.Payload) != "reply:one" {
			t.Fatalf("response = %#v", response)
		}
	}
	cached := <-d.Dispatch(context.Background(), Request{ID: "same", SessionID: "s", Payload: []byte("one")})
	if !cached.Cached || string(cached.Payload) != "reply:one" {
		t.Fatalf("cached = %#v", cached)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("handler called %d times", calls)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherRejectsCachedUUIDAcrossSessionRuntimeAndPayload(t *testing.T) {
	d := NewDispatcher(1, 4, 4096, func(_ context.Context, request Request) ([]byte, error) { return []byte("ok"), nil })
	base := Request{ID: "uuid", SessionID: "session-a", RuntimeID: "runtime-a", Payload: []byte("command-a")}
	if response := <-d.Dispatch(context.Background(), base); response.Err != nil {
		t.Fatal(response.Err)
	}
	for _, collision := range []Request{
		{ID: "uuid", SessionID: "session-b", RuntimeID: "runtime-a", Payload: []byte("command-a")},
		{ID: "uuid", SessionID: "session-a", RuntimeID: "runtime-b", Payload: []byte("command-a")},
		{ID: "uuid", SessionID: "session-a", RuntimeID: "runtime-a", Payload: []byte("command-b")},
	} {
		if response := <-d.Dispatch(context.Background(), collision); !errors.Is(response.Err, ErrRequestCollision) {
			t.Fatalf("collision %#v = %#v", collision, response)
		}
	}
}

func TestDispatcherCachesBackendErrorAndOperationOutlivesFirstWaiter(t *testing.T) {
	var calls int
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	d := NewDispatcher(1, 4, 1024, func(ctx context.Context, request Request) ([]byte, error) {
		calls++
		if request.ID == "shared" {
			started <- struct{}{}
			select {
			case <-release:
				return []byte("completed"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, errors.New("backend rejected request")
	})
	first := <-d.Dispatch(context.Background(), Request{ID: "error", SessionID: "s"})
	second := <-d.Dispatch(context.Background(), Request{ID: "error", SessionID: "s"})
	if first.Err == nil || second.Err == nil || !second.Cached || calls != 1 {
		t.Fatalf("errors = %#v, %#v calls=%d", first, second, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	canceled := d.Dispatch(ctx, Request{ID: "shared", SessionID: "s"})
	<-started
	secondWaiter := d.Dispatch(context.Background(), Request{ID: "shared", SessionID: "s"})
	cancel()
	if response := <-canceled; !errors.Is(response.Err, context.Canceled) {
		t.Fatalf("first waiter = %#v", response)
	}
	close(release)
	if response := <-secondWaiter; response.Err != nil || string(response.Payload) != "completed" {
		t.Fatalf("second waiter = %#v", response)
	}
	if calls != 2 {
		t.Fatalf("operation restarted; calls=%d", calls)
	}
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := d.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherBoundsInflightCountAndBytes(t *testing.T) {
	release := make(chan struct{})
	d := NewDispatcherWithLimits(4, 4, 1024, 1, 16, func(context.Context, Request) ([]byte, error) { <-release; return nil, nil })
	first := d.Dispatch(context.Background(), Request{ID: "one", SessionID: "s", Payload: []byte("1234")})
	if response := <-d.Dispatch(context.Background(), Request{ID: "two", SessionID: "s"}); !errors.Is(response.Err, ErrOverloaded) {
		t.Fatalf("count admission = %#v", response)
	}
	if response := <-d.Dispatch(context.Background(), Request{ID: "oversized", SessionID: "s", Payload: make([]byte, 32)}); !errors.Is(response.Err, ErrOverloaded) {
		t.Fatalf("byte admission = %#v", response)
	}
	close(release)
	<-first
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestActorCommandsAreOrderedPerSession(t *testing.T) {
	var mu sync.Mutex
	var got []string
	g := NewActorGroup(8, func(_ context.Context, c Command) (any, error) {
		mu.Lock()
		got = append(got, fmt.Sprint(c.Value))
		mu.Unlock()
		return nil, nil
	})
	defer g.Close()
	var results []<-chan Result
	for i := 0; i < 6; i++ {
		results = append(results, g.Submit(context.Background(), Command{SessionID: "a", Value: i}))
	}
	for _, result := range results {
		if r := <-result; r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(got) != "[0 1 2 3 4 5]" {
		t.Fatalf("order = %v", got)
	}
}

func TestSlowSessionDoesNotBlockAnotherSession(t *testing.T) {
	blocked := make(chan struct{})
	g := NewActorGroup(4, func(_ context.Context, c Command) (any, error) {
		if c.SessionID == "slow" {
			<-blocked
		}
		return c.SessionID, nil
	})
	defer g.Close()
	slow := g.Submit(context.Background(), Command{SessionID: "slow"})
	fast := g.Submit(context.Background(), Command{SessionID: "fast"})
	select {
	case result := <-fast:
		if result.Err != nil || result.Value != "fast" {
			t.Fatalf("fast result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated session was blocked")
	}
	close(blocked)
	<-slow
}

func TestFullSlowMailboxDoesNotBlockOtherSessionAdmission(t *testing.T) {
	blocked := make(chan struct{})
	g := NewActorGroup(1, func(_ context.Context, c Command) (any, error) {
		if c.SessionID == "a" {
			<-blocked
		}
		return c.SessionID, nil
	})
	defer g.Close()
	first := g.Submit(context.Background(), Command{SessionID: "a"})
	second := g.Submit(context.Background(), Command{SessionID: "a"})
	thirdStarted := make(chan struct{})
	go func() { close(thirdStarted); g.Submit(context.Background(), Command{SessionID: "a"}) }()
	<-thirdStarted
	time.Sleep(20 * time.Millisecond)
	fast := g.Submit(context.Background(), Command{SessionID: "b"})
	select {
	case result := <-fast:
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("full session A mailbox blocked session B admission")
	}
	close(blocked)
	<-first
	<-second
}

func TestCanceledQueuedWorkIsNotHandled(t *testing.T) {
	blocked := make(chan struct{})
	var mu sync.Mutex
	var handled []string
	g := NewActorGroup(2, func(_ context.Context, c Command) (any, error) {
		if c.Value == "first" {
			<-blocked
		}
		mu.Lock()
		handled = append(handled, fmt.Sprint(c.Value))
		mu.Unlock()
		return nil, nil
	})
	defer g.Close()
	first := g.Submit(context.Background(), Command{SessionID: "a", Value: "first"})
	ctx, cancel := context.WithCancel(context.Background())
	queued := g.Submit(ctx, Command{SessionID: "a", Value: "canceled"})
	cancel()
	close(blocked)
	<-first
	if result := <-queued; !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("queued result = %v", result.Err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(handled) != "[first]" {
		t.Fatalf("handled = %v", handled)
	}
}

func TestShutdownIsCancelableAndCancelsHandler(t *testing.T) {
	started := make(chan struct{})
	g := NewActorGroup(1, func(ctx context.Context, _ Command) (any, error) { close(started); <-ctx.Done(); return nil, ctx.Err() })
	result := g.Submit(context.Background(), Command{SessionID: "a"})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if r := <-result; !errors.Is(r.Err, context.Canceled) {
		t.Fatalf("handler result = %v", r.Err)
	}
}

func TestShutdownReturnsWhenHandlerIgnoresCancellation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	g := NewActorGroup(1, func(context.Context, Command) (any, error) { close(started); <-release; return nil, nil })
	g.Submit(context.Background(), Command{SessionID: "a"})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v", err)
	}
	close(release)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := g.Shutdown(ctx2); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownNeverStartsQueuedHandler(t *testing.T) {
	started := make(chan struct{})
	var calls int
	g := NewActorGroup(2, func(ctx context.Context, _ Command) (any, error) {
		calls++
		if calls == 1 {
			close(started)
			<-ctx.Done()
		}
		return nil, ctx.Err()
	})
	first := g.Submit(context.Background(), Command{SessionID: "s"})
	<-started
	queued := g.Submit(context.Background(), Command{SessionID: "s"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	<-first
	if result := <-queued; !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("queued = %#v", result)
	}
	if calls != 1 {
		t.Fatalf("queued handler performed side effects; calls=%d", calls)
	}
}

func TestShutdownStartBarrierRejectsQueuedSideEffects(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var effects []string
	g := NewActorGroup(2, func(_ context.Context, command Command) (any, error) {
		if command.Value == "first" {
			close(entered)
			<-release // deliberately ignores cancellation
		}
		mu.Lock()
		effects = append(effects, command.Value.(string))
		mu.Unlock()
		return nil, nil
	})
	first := g.Submit(context.Background(), Command{SessionID: "s", Value: "first"})
	<-entered
	queued := g.Submit(context.Background(), Command{SessionID: "s", Value: "queued"})
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- g.Shutdown(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if result := <-first; result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := <-queued; !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("queued = %#v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(effects) != "[first]" {
		t.Fatalf("effects crossed shutdown boundary: %v", effects)
	}
}

func TestShutdownPublishesStartBarrierBeforeBlockedReaderCanStart(t *testing.T) {
	var calls atomic.Int32
	g := NewActorGroup(1, func(context.Context, Command) (any, error) {
		calls.Add(1)
		return nil, nil
	})
	// Hold the writer gate so the actor has received work but cannot acquire
	// its handler-start read lease.
	g.startGate.Lock()
	result := g.Submit(context.Background(), Command{SessionID: "s"})
	shutdown := make(chan error, 1)
	go func() { shutdown <- g.Shutdown(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for !g.startClosing.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !g.startClosing.Load() {
		t.Fatal("shutdown did not publish the start barrier")
	}
	g.startGate.Unlock()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if got := <-result; !errors.Is(got.Err, ErrClosed) {
		t.Fatalf("result=%#v", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler started after shutdown barrier: %d", calls.Load())
	}
}

func TestDispatcherRejectsCacheAndAdmissionAfterShutdown(t *testing.T) {
	d := NewDispatcher(1, 2, 1024, func(context.Context, Request) ([]byte, error) { return []byte("ok"), nil })
	if response := <-d.Dispatch(context.Background(), Request{ID: "cached", SessionID: "s"}); response.Err != nil {
		t.Fatal(response.Err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"cached", "new"} {
		if response := <-d.Dispatch(context.Background(), Request{ID: id, SessionID: "s"}); !errors.Is(response.Err, ErrClosed) || response.Cached {
			t.Fatalf("post-close %s = %#v", id, response)
		}
	}
}

func TestDispatcherShutdownPreventsLateCacheMutation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	d := NewDispatcher(1, 2, 1024, func(context.Context, Request) ([]byte, error) {
		close(started)
		<-release
		return []byte("late"), nil
	})
	response := d.Dispatch(context.Background(), Request{ID: "late", SessionID: "s"})
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- d.Shutdown(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		d.mu.Lock()
		closed := d.closed
		d.mu.Unlock()
		if closed || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if got := <-response; got.Err != nil || string(got.Payload) != "late" {
		t.Fatalf("inflight response=%#v", got)
	}
	if count, bytes := d.cache.Stats(); count != 0 || bytes != 0 {
		t.Fatalf("cache mutated after close: count=%d bytes=%d", count, bytes)
	}
}

func TestReplayIsOrderedAndBoundedByBytes(t *testing.T) {
	h := mustHub(t, 10, 5, 10, 100)
	h.BeginRuntime("s", "r")
	for _, value := range []string{"aa", "bb", "cc"} {
		h.Emit("s", "r", []byte(value), true)
	}
	s := h.Subscribe(Cursor{SessionID: "s", RuntimeID: "r", Epoch: h.Epoch()})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for sequence, want := range []string{"bb", "cc"} {
		e, ok := s.Receive(ctx)
		if !ok || string(e.Payload) != want || e.Sequence != uint64(sequence+2) {
			t.Fatalf("event = %#v, %v", e, ok)
		}
	}
}

func TestSlowSubscriberIsDroppedWithoutBlockingEmitter(t *testing.T) {
	h := mustHub(t, 8, 1024, 1, 3)
	h.BeginRuntime("s", "r")
	slow := h.Subscribe(Cursor{SessionID: "s", RuntimeID: "r", Epoch: h.Epoch()})
	done := make(chan struct{})
	go func() { h.Emit("s", "r", []byte("one"), true); h.Emit("s", "r", []byte("two"), true); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit blocked on slow subscriber")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := slow.Receive(ctx); ok {
		t.Fatal("overwhelmed subscriber remained connected")
	}
	resumed := h.Subscribe(Cursor{SessionID: "s", RuntimeID: "r", Epoch: h.Epoch(), Sequence: 1})
	defer resumed.Close()
	e, ok := resumed.Receive(ctx)
	if !ok || e.Sequence != 2 || string(e.Payload) != "two" {
		t.Fatalf("resume = %#v, %v", e, ok)
	}
}

func TestEphemeralEventIsNotReplayed(t *testing.T) {
	h := mustHub(t, 8, 1024, 8, 1024)
	h.BeginRuntime("terminal", "runtime")
	h.Emit("terminal", "runtime", []byte("pty"), false)
	s := h.Subscribe(Cursor{SessionID: "terminal", RuntimeID: "runtime", Epoch: h.Epoch()})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, ok := s.Receive(ctx); ok {
		t.Fatal("ephemeral PTY output was replayed")
	}
}

func TestReplayReportsGapAndOldEpochReplaysFromStart(t *testing.T) {
	h := mustHub(t, 2, 1024, 8, 1024)
	h.BeginRuntime("s", "r")
	for i := 0; i < 4; i++ {
		h.Emit("s", "r", []byte{byte(i)}, true)
	}
	s := h.Subscribe(Cursor{SessionID: "s", RuntimeID: "r", Epoch: h.Epoch(), Sequence: 1})
	defer s.Close()
	if gaps := s.Gaps(); len(gaps) != 1 || gaps[0].FirstAvailable != 3 {
		t.Fatalf("gaps = %#v", gaps)
	}
	oldEpoch := h.Subscribe(Cursor{SessionID: "s", RuntimeID: "r", Epoch: "old", Sequence: 4})
	defer oldEpoch.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if event, ok := oldEpoch.Receive(ctx); !ok || event.Sequence != 3 || event.Epoch != h.Epoch() {
		t.Fatalf("old epoch replay = %#v, %v", event, ok)
	}
}

func TestHubCloseWakesSubscribersAndStopsEmission(t *testing.T) {
	h := mustHub(t, 2, 1024, 2, 1024)
	h.BeginRuntime("s", "r")
	s := h.Subscribe()
	h.Close()
	h.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := s.Receive(ctx); ok {
		t.Fatal("closed hub subscriber stayed open")
	}
	if event := h.Emit("s", "r", []byte("late"), true); event.Sequence != 0 {
		t.Fatalf("closed hub emitted %#v", event)
	}
}

func TestHubEpochIsCanonicalUUIDAndConcurrentDropCannotBlock(t *testing.T) {
	h := mustHub(t, 4, 1024, 1, 16)
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(h.Epoch()) {
		t.Fatalf("epoch = %q", h.Epoch())
	}
	h.BeginRuntime("s", "r")
	for iteration := 0; iteration < 100; iteration++ {
		s := h.Subscribe()
		h.Emit("s", "r", []byte("queued"), true)
		readerDone := make(chan struct{})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			s.Receive(ctx)
			close(readerDone)
		}()
		dropped := make(chan struct{})
		go func() { h.Emit("s", "r", []byte("overflow"), true); close(dropped) }()
		select {
		case <-dropped:
		case <-time.After(time.Second):
			t.Fatal("subscriber drop blocked")
		}
		<-readerDone
	}
	h.Close()
}

func TestHubCreationFailsClosedWhenEpochEntropyFails(t *testing.T) {
	if hub, err := newHub(1, 1, 1, 1, io.MultiReader()); err == nil || hub != nil {
		t.Fatalf("hub=%#v err=%v", hub, err)
	}
}

func TestStaleRuntimeCannotEmitIntoReplacement(t *testing.T) {
	h := mustHub(t, 8, 1024, 8, 1024)
	h.BeginRuntime("s", "old")
	h.Emit("s", "old", []byte("old"), true)
	h.BeginRuntime("s", "new")
	if event := h.Emit("s", "old", []byte("stale"), true); event.Sequence != 0 {
		t.Fatalf("stale event accepted: %#v", event)
	}
	h.Emit("s", "new", []byte("new"), true)
	s := h.Subscribe()
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, ok := s.Receive(ctx)
	if !ok || event.RuntimeID != "new" || event.Sequence != 1 || string(event.Payload) != "new" {
		t.Fatalf("replacement replay = %#v, %v", event, ok)
	}
}

func TestDefaultAdaptersCoverImplementedCatalog(t *testing.T) {
	adapters := defaultAdapters()
	for provider, descriptor := range providers.Descriptors() {
		if descriptor.Implemented && adapters[provider] == nil {
			t.Errorf("implemented provider %q has no adapter", provider)
		}
	}
}

func TestAvailableCommandsMappingPreservesOrder(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"first","description":"one"},{"name":"second","description":"two"}]}}}`)
	wire, err := providerEventWire(providers.Event{Kind: providers.EventAvailableCommands, Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeEventPayloadAny(wire)
	if err != nil {
		t.Fatal(err)
	}
	commands := decoded.(*[]protocol.ReportedCommandPayload)
	if len(*commands) != 2 || (*commands)[0].Name != "first" || (*commands)[1].Name != "second" {
		t.Fatalf("commands=%+v", *commands)
	}
}

type runtimeStoreFake struct {
	sessionErr  error
	setErr      error
	saveErr     error
	cursors     map[string]json.RawMessage
	setCalls    atomic.Int64
	rowExists   atomic.Bool
	createRow   bool
	saveEntered chan struct{}
	releaseSave chan struct{}
	setEntered  chan struct{}
	releaseSet  chan struct{}
}

func (s *runtimeStoreFake) Session(context.Context, string) (json.RawMessage, bool, error) {
	return nil, false, s.sessionErr
}
func (s *runtimeStoreFake) SaveWithProviderCursors(_ context.Context, _, _ []json.RawMessage, cursors map[string]json.RawMessage) error {
	s.cursors = cursors
	if s.saveEntered != nil {
		close(s.saveEntered)
	}
	if s.releaseSave != nil {
		<-s.releaseSave
	}
	if s.createRow && s.saveErr == nil {
		s.rowExists.Store(true)
	}
	return s.saveErr
}
func (s *runtimeStoreFake) Load(context.Context) ([]json.RawMessage, []json.RawMessage, error) {
	return nil, nil, nil
}
func (s *runtimeStoreFake) RemoveSession(context.Context, string) error { return nil }
func (s *runtimeStoreFake) SetProviderCursor(context.Context, string, json.RawMessage) error {
	s.setCalls.Add(1)
	if s.setEntered != nil {
		close(s.setEntered)
	}
	if s.releaseSet != nil {
		<-s.releaseSet
	}
	if s.createRow && !s.rowExists.Load() {
		return storage.ErrTaskSessionNotFound
	}
	return s.setErr
}

type runtimeAdapterFake struct {
	starts        atomic.Int64
	emitConnected bool
	startEntered  chan struct{}
	releaseStart  chan struct{}
}

func (a *runtimeAdapterFake) Descriptor() providers.Descriptor {
	return providers.Descriptor{Provider: providers.ProviderCodex, Implemented: true, Capabilities: providers.Capabilities{Rewind: true}}
}
func (a *runtimeAdapterFake) Discover(context.Context, providers.StartOptions) (providers.Discovery, error) {
	return providers.Discovery{Available: true}, nil
}
func (a *runtimeAdapterFake) Start(_ context.Context, _ providers.StartOptions, sink providers.Sink) (providers.Session, error) {
	a.starts.Add(1)
	if a.emitConnected {
		sink.Emit(providers.Event{Kind: providers.EventConnected, Cursor: json.RawMessage(`{"provider":"codex","threadId":"x"}`)})
	}
	if a.startEntered != nil {
		close(a.startEntered)
	}
	if a.releaseStart != nil {
		<-a.releaseStart
	}
	return &runtimeSessionFake{}, nil
}

type runtimeSessionFake struct {
	closed         atomic.Bool
	rollbackCursor json.RawMessage
}

func (*runtimeSessionFake) Prompt(context.Context, string) error { return nil }
func (*runtimeSessionFake) Steer(context.Context, string) error  { return nil }
func (*runtimeSessionFake) Cancel(context.Context) error         { return nil }
func (*runtimeSessionFake) RespondPermission(context.Context, json.RawMessage, string) error {
	return nil
}
func (*runtimeSessionFake) RespondQuestions(context.Context, json.RawMessage, []providers.UserInputAnswer) error {
	return nil
}
func (*runtimeSessionFake) Goal(context.Context, providers.GoalOperation) error { return nil }
func (*runtimeSessionFake) ApplyOptions(context.Context, providers.SessionOptions) (bool, error) {
	return true, nil
}
func (s *runtimeSessionFake) Rollback(context.Context, uint64) (json.RawMessage, error) {
	return append(json.RawMessage(nil), s.rollbackCursor...), nil
}
func (*runtimeSessionFake) Fork(context.Context, uint64) (json.RawMessage, error) { return nil, nil }
func (s *runtimeSessionFake) Close() error                                        { s.closed.Store(true); return nil }

func TestStartFailsOnSessionReadErrorBeforeProviderLaunch(t *testing.T) {
	store := &runtimeStoreFake{sessionErr: errors.New("database read failed")}
	adapter := &runtimeAdapterFake{}
	hub := mustHub(t, 8, 4096, 8, 4096)
	backend := newRuntimeBackend(store, map[providers.Provider]providers.Adapter{providers.ProviderCodex: adapter}, hub)
	_, err := backend.start(context.Background(), protocol.Request{SessionID: "00000000-0000-4000-8000-000000000001", RuntimeID: "00000000-0000-4000-8000-000000000002"}, protocol.Start(providersStartOptions()))
	if err == nil || adapter.starts.Load() != 0 {
		t.Fatalf("start err=%v provider starts=%d", err, adapter.starts.Load())
	}
}

func TestStartFailsOnSynchronousCursorPersistenceError(t *testing.T) {
	store := &runtimeStoreFake{setErr: errors.New("database write failed")}
	adapter := &runtimeAdapterFake{emitConnected: true}
	hub := mustHub(t, 8, 4096, 8, 4096)
	backend := newRuntimeBackend(store, map[providers.Provider]providers.Adapter{providers.ProviderCodex: adapter}, hub)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	_, err := backend.start(context.Background(), protocol.Request{SessionID: sessionID, RuntimeID: runtimeID}, protocol.Start(providersStartOptions()))
	if err == nil || adapter.starts.Load() != 1 {
		t.Fatalf("start err=%v provider starts=%d", err, adapter.starts.Load())
	}
	if _, err := backend.runtime(sessionID, runtimeID); err == nil {
		t.Fatal("runtime with failed synchronous cursor persistence was attached")
	}
}

func TestQueuedStartupCursorIsVisibleToConcurrentTaskStateSave(t *testing.T) {
	store := &runtimeStoreFake{setErr: storage.ErrTaskSessionNotFound}
	adapter := &runtimeAdapterFake{emitConnected: true, startEntered: make(chan struct{}), releaseStart: make(chan struct{})}
	backend := newRuntimeBackend(store, map[providers.Provider]providers.Adapter{providers.ProviderCodex: adapter}, mustHub(t, 8, 4096, 8, 4096))
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	started := make(chan error, 1)
	go func() {
		_, err := backend.start(context.Background(), protocol.Request{SessionID: sessionID, RuntimeID: runtimeID}, protocol.Start(providersStartOptions()))
		started <- err
	}()
	select {
	case <-adapter.startEntered:
	case <-time.After(time.Second):
		t.Fatal("adapter did not reach startup barrier")
	}
	snapshot := []json.RawMessage{json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000001"}`)}
	if cursor := backend.currentCursors(snapshot)[string(sessionID)]; !strings.Contains(string(cursor), `"threadId":"x"`) {
		t.Fatalf("queued startup cursor missing from concurrent save: %s", cursor)
	}
	close(adapter.releaseStart)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
}

func providersStartOptions() protocol.WireDriverStartOptions {
	return protocol.WireDriverStartOptions{Provider: string(providers.ProviderCodex), Binary: "fake", CWD: "/tmp", Mode: "fullAccess"}
}

func TestAsyncCursorPersistenceFailureSurfacesAndDetaches(t *testing.T) {
	store := &runtimeStoreFake{setErr: errors.New("disk failed")}
	hub := mustHub(t, 8, 4096, 8, 4096)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	session := &runtimeSessionFake{}
	runtime := &providerRuntime{id: runtimeID, session: session, adapter: &runtimeAdapterFake{}, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend := newRuntimeBackend(store, nil, hub)
	backend.runtimes[sessionID] = runtime
	hub.BeginRuntime(string(sessionID), string(runtimeID))
	sub := hub.Subscribe()
	defer sub.Close()
	backend.emit(sessionID, runtimeID, runtime, providers.Event{Kind: providers.EventConnected, Cursor: json.RawMessage(`{"provider":"codex","threadId":"x"}`)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, ok := sub.Receive(ctx)
	if !ok {
		t.Fatal("persistence failure event was not delivered")
	}
	var wire protocol.WireDriverEvent
	if err := protocol.Decode(event.Payload, &wire); err != nil || wire.Kind != string(protocol.EventError) {
		t.Fatalf("failure event=%s err=%v", event.Payload, err)
	}
	if _, err := backend.runtime(sessionID, runtimeID); err == nil {
		t.Fatal("failed runtime remained attached")
	}
	deadline := time.Now().Add(time.Second)
	for !session.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !session.closed.Load() {
		t.Fatal("failed runtime session was not closed")
	}
}

func TestSaveIncludesOnlyRelevantRuntimeCursor(t *testing.T) {
	store := &runtimeStoreFake{}
	backend := newRuntimeBackend(store, nil, mustHub(t, 8, 4096, 8, 4096))
	for index, id := range []protocol.UUID{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"} {
		backend.runtimes[id] = &providerRuntime{pendingCursor: json.RawMessage(fmt.Sprintf(`{"provider":"codex","threadId":"%d"}`, index))}
	}
	session := json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000001"}`)
	if err := store.SaveWithProviderCursors(context.Background(), nil, []json.RawMessage{session}, backend.currentCursors([]json.RawMessage{session})); err != nil {
		t.Fatal(err)
	}
	if len(store.cursors) != 1 || store.cursors["00000000-0000-4000-8000-000000000001"] == nil {
		t.Fatalf("cursors=%v", store.cursors)
	}
}

func TestProviderPendingRequestsAreCountAndByteBounded(t *testing.T) {
	runtime := &providerRuntime{pending: make(map[pendingKey]*pendingProviderRequest)}
	for i := 0; i < maxPendingProviderRequests; i++ {
		key := pendingKey{kind: "permission", wireID: fmt.Sprintf("id-%d", i)}
		if err := runtime.addPending(key, json.RawMessage(`1`)); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	if err := runtime.addPending(pendingKey{kind: "permission", wireID: "overflow"}, json.RawMessage(`1`)); !errors.Is(err, ErrProviderPendingOverload) {
		t.Fatalf("count overflow=%v", err)
	}
	runtime.clearPending()
	if err := runtime.addPending(pendingKey{kind: "question", wireID: "huge"}, make(json.RawMessage, maxPendingProviderBytes+1)); !errors.Is(err, ErrProviderPendingOverload) {
		t.Fatalf("byte overflow=%v", err)
	}
	if len(runtime.pending) != 0 || runtime.pendingBytes != 0 {
		t.Fatalf("overload retained pending state: count=%d bytes=%d", len(runtime.pending), runtime.pendingBytes)
	}
}

func TestPendingRequestOverloadFailsAndCleansSession(t *testing.T) {
	hub := mustHub(t, 256, 1<<20, 256, 1<<20)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	session := &runtimeSessionFake{}
	runtime := &providerRuntime{id: runtimeID, session: session, adapter: &runtimeAdapterFake{}, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, hub)
	backend.runtimes[sessionID] = runtime
	hub.BeginRuntime(string(sessionID), string(runtimeID))
	for i := 0; i <= maxPendingProviderRequests; i++ {
		backend.emit(sessionID, runtimeID, runtime, providers.Event{
			Kind:       providers.EventPermission,
			RequestID:  json.RawMessage(fmt.Sprintf("%d", i)),
			Permission: &providers.Permission{Title: "Approval", Options: []providers.PermissionOption{{ID: "yes", Label: "Yes", Allow: true}}},
		})
	}
	if _, err := backend.runtime(sessionID, runtimeID); err == nil {
		t.Fatal("overloaded runtime remained attached")
	}
	runtime.mu.Lock()
	count, bytes := len(runtime.pending), runtime.pendingBytes
	runtime.mu.Unlock()
	if count != 0 || bytes != 0 {
		t.Fatalf("overload cleanup count=%d bytes=%d", count, bytes)
	}
	deadline := time.Now().Add(time.Second)
	for !session.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !session.closed.Load() {
		t.Fatal("overloaded provider session was not closed")
	}
}

func TestStaleProviderSinkCannotMutatePersistenceOrPendingState(t *testing.T) {
	store := &runtimeStoreFake{}
	hub := mustHub(t, 8, 4096, 8, 4096)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	oldID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	newID := protocol.UUID("00000000-0000-4000-8000-000000000003")
	oldRuntime := &providerRuntime{id: oldID, pending: make(map[pendingKey]*pendingProviderRequest)}
	newRuntime := &providerRuntime{id: newID, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend := newRuntimeBackend(store, nil, hub)
	backend.runtimes[sessionID] = newRuntime
	hub.BeginRuntime(string(sessionID), string(newID))
	backend.emit(sessionID, oldID, oldRuntime, providers.Event{Kind: providers.EventConnected, Cursor: json.RawMessage(`{"provider":"codex","threadId":"stale"}`)})
	backend.emit(sessionID, oldID, oldRuntime, providers.Event{Kind: providers.EventPermission, RequestID: json.RawMessage(`1`), Permission: &providers.Permission{Title: "stale"}})
	if store.setCalls.Load() != 0 || len(oldRuntime.pending) != 0 || len(newRuntime.pending) != 0 {
		t.Fatalf("stale sink mutated state: calls=%d old=%d new=%d", store.setCalls.Load(), len(oldRuntime.pending), len(newRuntime.pending))
	}
}

func TestSaveSnapshotBeforeConnectedOrdersCursorAfterCommit(t *testing.T) {
	store := &runtimeStoreFake{createRow: true, saveEntered: make(chan struct{}), releaseSave: make(chan struct{})}
	backend := newRuntimeBackend(store, nil, mustHub(t, 8, 4096, 8, 4096))
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	runtime := &providerRuntime{id: runtimeID, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend.runtimes[sessionID] = runtime
	snapshot := []json.RawMessage{json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000001"}`)}
	saved := make(chan error, 1)
	go func() { saved <- backend.saveTaskState(context.Background(), nil, snapshot) }()
	select {
	case <-store.saveEntered:
	case <-time.After(time.Second):
		t.Fatal("save did not reach commit barrier")
	}
	emitted := make(chan struct{})
	go func() {
		backend.emit(sessionID, runtimeID, runtime, providers.Event{Kind: providers.EventConnected, Cursor: json.RawMessage(`{"provider":"codex","threadId":"after"}`)})
		close(emitted)
	}()
	select {
	case <-emitted:
		t.Fatal("Connected bypassed task-state ownership/transaction boundary")
	case <-time.After(20 * time.Millisecond):
	}
	if store.setCalls.Load() != 0 || len(store.cursors) != 0 {
		t.Fatalf("pre-commit cursor mutation calls=%d snapshot=%v", store.setCalls.Load(), store.cursors)
	}
	close(store.releaseSave)
	if err := <-saved; err != nil {
		t.Fatal(err)
	}
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("Connected did not resume after save commit")
	}
	if store.setCalls.Load() != 1 {
		t.Fatalf("post-commit cursor updates=%d", store.setCalls.Load())
	}
	if !store.rowExists.Load() {
		t.Fatal("task-state row was not committed before Connected persistence")
	}
}

func TestCustomAdapterRegistryRejectsMismatchAndDuplicateBeforeDataTouch(t *testing.T) {
	for name, config := range map[string]ServerConfig{
		"mismatch": {
			Adapters: map[providers.Provider]providers.Adapter{providers.ProviderCursor: &runtimeAdapterFake{}},
		},
		"duplicate": {
			Adapters: map[providers.Provider]providers.Adapter{providers.ProviderCodex: &runtimeAdapterFake{}},
			Adapter:  &runtimeAdapterFake{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "must-not-exist")
			config.DataDir = directory
			config.Token = "secret"
			server, err := NewServer(context.Background(), config)
			if err == nil || server != nil {
				t.Fatalf("server=%#v err=%v", server, err)
			}
			if _, statErr := os.Stat(directory); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid registry touched data directory: %v", statErr)
			}
		})
	}
}

func TestRollbackResponseWaitsForDurableCursorAdoption(t *testing.T) {
	store := &runtimeStoreFake{setEntered: make(chan struct{}), releaseSet: make(chan struct{})}
	hub := mustHub(t, 8, 4096, 8, 4096)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	cursor := json.RawMessage(`{"provider":"codex","threadId":"rewound-durable"}`)
	session := &runtimeSessionFake{rollbackCursor: cursor}
	adapter := &runtimeAdapterFake{}
	runtime := &providerRuntime{id: runtimeID, session: session, adapter: adapter, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend := newRuntimeBackend(store, nil, hub)
	backend.runtimes[sessionID] = runtime
	payload := marshalRuntimeRequest(t, sessionID, runtimeID, protocol.TurnsCommand{Type: protocol.CommandRollback, Turns: 1})
	done := make(chan error, 1)
	go func() {
		_, err := backend.handle(context.Background(), Request{Payload: payload})
		done <- err
	}()
	select {
	case <-store.setEntered:
	case <-time.After(time.Second):
		t.Fatal("rollback did not reach durable cursor write")
	}
	select {
	case err := <-done:
		t.Fatalf("rollback responded before cursor durability: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.releaseSet)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	adopted := append(json.RawMessage(nil), runtime.pendingCursor...)
	runtime.mu.Unlock()
	if string(adopted) != string(cursor) {
		t.Fatalf("adopted cursor=%s want=%s", adopted, cursor)
	}
}

func TestRollbackPersistenceFailureCannotAcknowledgeOrLeaveRuntimeActive(t *testing.T) {
	store := &runtimeStoreFake{setErr: errors.New("disk failed")}
	hub := mustHub(t, 8, 4096, 8, 4096)
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000001")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000002")
	session := &runtimeSessionFake{rollbackCursor: json.RawMessage(`{"provider":"codex","threadId":"not-durable"}`)}
	runtime := &providerRuntime{id: runtimeID, session: session, adapter: &runtimeAdapterFake{}, pending: make(map[pendingKey]*pendingProviderRequest)}
	backend := newRuntimeBackend(store, nil, hub)
	backend.runtimes[sessionID] = runtime
	payload := marshalRuntimeRequest(t, sessionID, runtimeID, protocol.TurnsCommand{Type: protocol.CommandRollback, Turns: 1})
	if response, err := backend.handle(context.Background(), Request{Payload: payload}); err == nil || response != nil {
		t.Fatalf("rollback response=%s err=%v", response, err)
	}
	if _, err := backend.runtime(sessionID, runtimeID); err == nil {
		t.Fatal("runtime remained active after rollback durability failure")
	}
}

func marshalRuntimeRequest(t *testing.T, sessionID, runtimeID protocol.UUID, command any) []byte {
	t.Helper()
	commandRaw, err := protocol.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.Marshal(protocol.Request{RequestID: "00000000-0000-4000-8000-000000000010", SessionID: sessionID, RuntimeID: runtimeID, Command: commandRaw})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestActorGroupRetiresSequentialSessionsAndBoundsConcurrentCardinality(t *testing.T) {
	group := NewActorGroupWithLimit(2, 4, func(ctx context.Context, command Command) (any, error) { return command.Value, nil })
	defer group.Close()
	for i := 0; i < 1000; i++ {
		result := <-group.Submit(context.Background(), Command{SessionID: fmt.Sprintf("session-%d", i), Value: i})
		if result.Err != nil || result.Value != i {
			t.Fatalf("session %d result=%+v", i, result)
		}
	}
	deadline := time.Now().Add(time.Second)
	for group.ActorCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count := group.ActorCount(); count != 0 {
		t.Fatalf("sequential actors retained=%d", count)
	}

	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	bounded := NewActorGroupWithLimit(1, 4, func(context.Context, Command) (any, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	})
	defer bounded.Close()
	results := make([]<-chan Result, 4)
	for i := range results {
		results[i] = bounded.Submit(context.Background(), Command{SessionID: fmt.Sprintf("active-%d", i)})
	}
	for range results {
		<-entered
	}
	if result := <-bounded.Submit(context.Background(), Command{SessionID: "overflow"}); !errors.Is(result.Err, ErrActorLimit) {
		t.Fatalf("actor overflow=%v", result.Err)
	}
	close(release)
	for _, result := range results {
		if response := <-result; response.Err != nil {
			t.Fatal(response.Err)
		}
	}
}

func TestHubReplayIsGloballyBoundedAcrossSequentialSessions(t *testing.T) {
	hub := mustHub(t, 16, 64, 64, 4096)
	for i := 0; i < 1000; i++ {
		session, runtime := fmt.Sprintf("s-%d", i), fmt.Sprintf("r-%d", i)
		hub.BeginRuntime(session, runtime)
		hub.Emit(session, runtime, []byte("12345678"), true)
		streams, count, bytes := hub.ReplayStats()
		if streams > 8 || count > 16 || bytes > 64 || len(hub.replayOrder) > 16 {
			t.Fatalf("iteration %d replay streams/count/bytes/order=%d/%d/%d/%d", i, streams, count, bytes, len(hub.replayOrder))
		}
	}
	if gaps := hub.Subscribe().Gaps(); len(gaps) < 900 {
		t.Fatalf("global eviction did not report replay gaps: %d", len(gaps))
	}
	for i := 0; i < 1000; i++ {
		hub.EndRuntime(fmt.Sprintf("s-%d", i), fmt.Sprintf("r-%d", i))
	}
	if streams, count, bytes := hub.ReplayStats(); streams != 0 || count != 0 || bytes != 0 || len(hub.replayOrder) != 0 {
		t.Fatalf("replay retirement streams/count/bytes/order=%d/%d/%d/%d", streams, count, bytes, len(hub.replayOrder))
	}
}

type catalogAdapterFake struct {
	mu    sync.Mutex
	calls []string
}

type discoveryAdapterFake struct {
	provider  providers.Provider
	discovery providers.Discovery
}

func (a discoveryAdapterFake) Descriptor() providers.Descriptor {
	return providers.Descriptor{Provider: a.provider, Implemented: true}
}
func (a discoveryAdapterFake) Discover(context.Context, providers.StartOptions) (providers.Discovery, error) {
	return a.discovery, nil
}
func (discoveryAdapterFake) Start(context.Context, providers.StartOptions, providers.Sink) (providers.Session, error) {
	return nil, providers.ErrUnsupported
}

func (*catalogAdapterFake) Descriptor() providers.Descriptor {
	return providers.Descriptor{Provider: providers.ProviderClaude, Implemented: true, Capabilities: providers.Capabilities{Resume: true, ImportedHistory: true, Fork: true}}
}
func (a *catalogAdapterFake) Discover(context.Context, providers.StartOptions) (providers.Discovery, error) {
	a.record("probe")
	return providers.Discovery{Available: true, Executable: "/usr/bin/claude", Version: "1.2.3", Models: []providers.Model{{ID: "model-a", Label: "Model A"}}}, nil
}
func (*catalogAdapterFake) Start(context.Context, providers.StartOptions, providers.Sink) (providers.Session, error) {
	return &runtimeSessionFake{}, nil
}
func (a *catalogAdapterFake) ListSessions(context.Context, uint64) ([]json.RawMessage, error) {
	a.record("list")
	return []json.RawMessage{json.RawMessage(`{"cursor":{"provider":"claude","sessionId":"native"},"title":"Native","cwd":"/tmp","created_at":1,"updated_at":2}`)}, nil
}
func (a *catalogAdapterFake) LoadSession(context.Context, json.RawMessage, string) (json.RawMessage, error) {
	a.record("load")
	return json.RawMessage(`{"messages":[],"turns":[]}`), nil
}
func (a *catalogAdapterFake) ForkSession(context.Context, json.RawMessage) (json.RawMessage, error) {
	a.record("fork")
	return json.RawMessage(`{"cursor":{"provider":"claude","sessionId":"forked"},"messageIds":{}}`), nil
}
func (a *catalogAdapterFake) record(value string) {
	a.mu.Lock()
	a.calls = append(a.calls, value)
	a.mu.Unlock()
}

func TestProviderDiscoveryAndCatalogCommandsAreReachableAndExact(t *testing.T) {
	adapter := &catalogAdapterFake{}
	backend := newRuntimeBackend(&runtimeStoreFake{}, map[providers.Provider]providers.Adapter{providers.ProviderClaude: adapter}, mustHub(t, 8, 4096, 8, 4096))
	commands := []any{
		protocol.ProbeProviderCommand{Type: protocol.CommandProbeProvider, Provider: "claude", DiscoverModels: true, ProbeVersion: true},
		protocol.ListProviderSessionsCommand{Type: protocol.CommandListProviderSessions, Provider: "claude", Limit: 10},
		protocol.LoadProviderSessionCommand{Type: protocol.CommandLoadProviderSession, Cursor: json.RawMessage(`{"provider":"claude","sessionId":"native"}`), CWD: "/tmp"},
		protocol.ForkProviderSessionCommand{Type: protocol.CommandForkProviderSession, Request: json.RawMessage(`{"provider":"claude","session_id":"native","turn_count":1,"title":"Fork"}`)},
	}
	for _, command := range commands {
		payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, command)
		response, err := backend.handle(context.Background(), Request{Payload: payload})
		if err != nil {
			t.Fatalf("%T: %v", command, err)
		}
		if _, err := protocol.DecodeResponsePayloadAny(response); err != nil {
			t.Fatalf("%T response=%s: %v", command, response, err)
		}
	}
	adapter.mu.Lock()
	got := append([]string(nil), adapter.calls...)
	adapter.mu.Unlock()
	if strings.Join(got, ",") != "probe,list,load,fork" {
		t.Fatalf("provider command order=%v", got)
	}
}

func TestPiDiscoveryRoutingPreservesValidatedModelMetadata(t *testing.T) {
	const native = `{"provider":"anthropic","id":"sonnet","name":"Native Sonnet","opaque":9007199254740993,"is_default":true,"default_reasoning_effort":"high","default_context_window":"200k","reasoning_efforts":[{"id":"high","label":"High"}],"context_windows":[{"id":"200k","label":"200K"}],"service_tiers":[]}`
	for _, provider := range []providers.Provider{providers.ProviderPi, providers.ProviderOhMyPi} {
		t.Run(string(provider), func(t *testing.T) {
			adapter := discoveryAdapterFake{provider: provider, discovery: providers.Discovery{
				Available: true,
				Models:    []providers.Model{{ID: "anthropic/sonnet", Label: "Sonnet", Raw: json.RawMessage(native)}},
			}}
			backend := newRuntimeBackend(&runtimeStoreFake{}, map[providers.Provider]providers.Adapter{provider: adapter}, mustHub(t, 8, 4096, 8, 4096))
			payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.ProbeProviderCommand{
				Type: protocol.CommandProbeProvider, Provider: string(provider), DiscoverModels: true,
			})
			response, err := backend.handle(context.Background(), Request{Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := protocol.DecodeResponsePayloadAny(response); err != nil {
				t.Fatalf("protocol rejected routed metadata: %v\n%s", err, response)
			}
			var wire struct {
				Probe struct {
					Models []json.RawMessage `json:"models"`
				} `json:"probe"`
			}
			if err := protocol.Decode(response, &wire); err != nil || len(wire.Probe.Models) != 1 {
				t.Fatalf("decode models=%d: %v", len(wire.Probe.Models), err)
			}
			model := string(wire.Probe.Models[0])
			for _, exact := range []string{
				`"id":"anthropic/sonnet"`, `"name":"Sonnet"`, `"opaque":9007199254740993`,
				`"is_default":true`, `"default_reasoning_effort":"high"`, `"default_context_window":"200k"`,
				`"reasoning_efforts":[{"id":"high","label":"High"}]`,
				`"context_windows":[{"id":"200k","label":"200K"}]`,
			} {
				if !strings.Contains(model, exact) {
					t.Errorf("model metadata lost %s: %s", exact, model)
				}
			}
		})
	}
}

func TestProviderDiscoveryRejectsMalformedProtocolMetadata(t *testing.T) {
	adapter := discoveryAdapterFake{provider: providers.ProviderPi, discovery: providers.Discovery{Available: true, Models: []providers.Model{{
		ID: "anthropic/sonnet", Label: "Sonnet", Raw: json.RawMessage(`{"reasoning_efforts":[{"id":"high"}]}`),
	}}}}
	backend := newRuntimeBackend(&runtimeStoreFake{}, map[providers.Provider]providers.Adapter{providers.ProviderPi: adapter}, mustHub(t, 8, 4096, 8, 4096))
	payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.ProbeProviderCommand{Type: protocol.CommandProbeProvider, Provider: "pi", DiscoverModels: true})
	if _, err := backend.handle(context.Background(), Request{Payload: payload}); err == nil || !strings.Contains(err.Error(), "reasoning_efforts") {
		t.Fatalf("malformed model metadata error=%v", err)
	}
}

func TestProviderCatalogCommandsRespectCapabilityGate(t *testing.T) {
	adapter := &runtimeAdapterFake{}
	backend := newRuntimeBackend(&runtimeStoreFake{}, map[providers.Provider]providers.Adapter{providers.ProviderCodex: adapter}, mustHub(t, 8, 4096, 8, 4096))
	payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.ListProviderSessionsCommand{Type: protocol.CommandListProviderSessions, Provider: "codex", Limit: 10})
	if _, err := backend.handle(context.Background(), Request{Payload: payload}); err == nil || !strings.Contains(err.Error(), "does not support native session history") {
		t.Fatalf("catalog capability error=%v", err)
	}
}

func TestLiveForkCapabilityDoesNotAdmitOfflineProviderFork(t *testing.T) {
	adapters := defaultAdapters()
	pi := adapters[providers.ProviderPi]
	if pi == nil {
		t.Fatal("Pi adapter is missing")
	}
	if !pi.Descriptor().Capabilities.Fork {
		t.Fatal("Pi must retain its supported live-session fork capability")
	}
	if pi.Descriptor().Capabilities.ImportedHistory {
		t.Fatal("Pi must not advertise native/offline history without a catalog implementation")
	}
	if _, ok := pi.(providers.SessionCatalogAdapter); ok {
		t.Fatal("Pi unexpectedly implements the offline native session catalog")
	}

	backend := newRuntimeBackend(&runtimeStoreFake{}, adapters, mustHub(t, 8, 4096, 8, 4096))
	if _, _, err := backend.sessionCatalog(providers.ProviderPi, true); err == nil || !strings.Contains(err.Error(), "offline native session fork") {
		t.Fatalf("Pi offline fork gate error=%v", err)
	}

	// A live-session Fork flag on any future adapter must remain insufficient
	// to admit the distinct ForkProviderSession route without both the native
	// history capability and its concrete implementation.
	for provider, adapter := range adapters {
		if _, ok := adapter.(providers.SessionCatalogAdapter); ok {
			continue
		}
		if _, _, err := backend.sessionCatalog(provider, true); err == nil {
			t.Errorf("provider %q admitted offline fork without SessionCatalogAdapter (descriptor=%+v)", provider, adapter.Descriptor())
		}
	}
}

func TestWorkspaceCommandsRouteToDaemonHostAndValidateResponses(t *testing.T) {
	directory := t.TempDir()
	git := func(args ...string) {
		command := exec.Command("git", args...)
		command.Dir = directory
		command.Env = append(command.Environ(), "GIT_AUTHOR_NAME=Waku", "GIT_AUTHOR_EMAIL=waku@localhost", "GIT_COMMITTER_NAME=Waku", "GIT_COMMITTER_EMAIL=waku@localhost")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(directory, "note.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "note.txt")
	git("commit", "-q", "-m", "initial")

	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, mustHub(t, 8, 4096, 8, 4096))
	operations := []json.RawMessage{
		json.RawMessage(fmt.Sprintf(`{"type":"readTextFile","root":%q,"relative_path":"note.txt"}`, directory)),
		json.RawMessage(fmt.Sprintf(`{"type":"inspectCommit","cwd":%q}`, directory)),
	}
	for _, operation := range operations {
		payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.WorkspaceCommand{Type: protocol.CommandWorkspace, Operation: operation})
		response, err := backend.handle(context.Background(), Request{Payload: payload})
		if err != nil {
			t.Fatalf("operation=%s: %v", operation, err)
		}
		decoded, err := protocol.DecodeResponsePayloadAny(response)
		if err != nil {
			t.Fatalf("operation=%s response=%s: %v", operation, response, err)
		}
		if _, ok := decoded.(*protocol.WorkspacePayload); !ok {
			t.Fatalf("operation=%s response type=%T", operation, decoded)
		}
	}
	for _, operation := range []json.RawMessage{
		json.RawMessage(fmt.Sprintf(`{"type":"writeTextFile","root":%q,"relative_path":"note.txt","content":"two\n"}`, directory)),
		json.RawMessage(fmt.Sprintf(`{"type":"captureTurn","cwd":%q,"session_id":"00000000-0000-4000-8000-000000000001","turn_count":1}`, directory)),
		json.RawMessage(fmt.Sprintf(`{"type":"restoreRef","cwd":%q,"git_ref":"refs/waku/session-00000000-0000-4000-8000-000000000001-turn-1"}`, directory)),
	} {
		payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.WorkspaceCommand{Type: protocol.CommandWorkspace, Operation: operation})
		if _, err := backend.handle(context.Background(), Request{Payload: payload}); err == nil {
			t.Fatalf("unsafe/incomplete workspace operation was admitted: %s", operation)
		}
	}
	content, err := os.ReadFile(filepath.Join(directory, "note.txt"))
	if err != nil || string(content) != "one\n" {
		t.Fatalf("gated remote write changed content=%q err=%v", content, err)
	}
}

func TestDaemonSettingsPersistUnknownFieldsAndDefaults(t *testing.T) {
	directory := t.TempDir()
	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, mustHub(t, 8, 4096, 8, 4096))
	backend.settings = &storage.SettingsStore{Path: filepath.Join(directory, "settings.json")}
	update := protocol.UpdateSettingsCommand{Type: protocol.CommandUpdateSettings, Settings: json.RawMessage(`{"computer_use_enabled":true,"disabled_providers":["amp"],"future":{"exact":9007199254740993}}`)}
	payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, update)
	if response, err := backend.handle(context.Background(), Request{Payload: payload}); err != nil {
		t.Fatal(err)
	} else if decoded, err := protocol.DecodeResponsePayloadAny(response); err != nil {
		t.Fatalf("update response=%s: %v", response, err)
	} else if _, ok := decoded.(*protocol.AckPayload); !ok {
		t.Fatalf("update response type=%T", decoded)
	}
	get := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.Command{Type: string(protocol.CommandGetSettings)})
	response, err := backend.handle(context.Background(), Request{Payload: get})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.DecodeResponsePayloadAny(response); err != nil {
		t.Fatalf("get response=%s: %v", response, err)
	}
	if !strings.Contains(string(response), `"exact":9007199254740993`) || !strings.Contains(string(response), `"computer_use_allowed_apps":[]`) {
		t.Fatalf("settings did not preserve extras/default collections: %s", response)
	}
	reopened := &storage.SettingsStore{Path: filepath.Join(directory, "settings.json")}
	raw, err := reopened.Load(context.Background())
	if err != nil || !strings.Contains(string(raw), `"exact":9007199254740993`) {
		t.Fatalf("persisted settings=%s err=%v", raw, err)
	}
}

type daemonTerminalPTY struct {
	mu      sync.Mutex
	writes  [][]byte
	resizes [][2]uint16
	closes  int
}

func (p *daemonTerminalPTY) Write(_ context.Context, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, append([]byte(nil), data...))
	return nil
}
func (p *daemonTerminalPTY) Resize(_ context.Context, cols, rows uint16) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizes = append(p.resizes, [2]uint16{cols, rows})
	return nil
}
func (p *daemonTerminalPTY) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes++
	return nil
}

type daemonTerminalBackend struct {
	pty      *daemonTerminalPTY
	emit     func(terminaltransport.BackendEvent)
	cwd      string
	colsRows [2]uint16
}

type planUsageFetcherFake struct {
	provider        string
	binary, version *string
	calls           int
	usage           json.RawMessage
}

func (f *planUsageFetcherFake) Fetch(_ context.Context, provider string, binary, version *string) (json.RawMessage, error) {
	f.calls++
	f.provider, f.binary, f.version = provider, binary, version
	return append(json.RawMessage(nil), f.usage...), nil
}

func TestSkillsUsageAndPlanCommandsRouteDaemonHostServices(t *testing.T) {
	home, trash := t.TempDir(), filepath.Join(t.TempDir(), "trash")
	skillDir := filepath.Join(home, ".agents", "skills", "demo")
	if err := os.MkdirAll(skillDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: demo\ndescription: daemon skill\n---\nBody"), 0o600); err != nil {
		t.Fatal(err)
	}
	homePath, _ := workspacesvc.NewHostPath(home)
	trashPath, _ := workspacesvc.NewHostPath(trash)
	skills, err := workspacesvc.NewSkillsService(homePath, nil, trashPath, workspacesvc.SkillsLimits{})
	if err != nil {
		t.Fatal(err)
	}
	usageRoot := t.TempDir()
	usage, err := workspacesvc.NewUsageHistoryService(workspacesvc.UsageHistoryConfig{ClaudeRoot: usageRoot, CodexRoot: filepath.Join(usageRoot, "missing"), Now: func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	fetcher := &planUsageFetcherFake{usage: json.RawMessage(`{"planLabel":"Pro","windows":[{"label":"Weekly","percent":12.5,"resetsAt":null}]}`)}
	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, mustHub(t, 8, 4096, 8, 4096))
	backend.skills, backend.usage, backend.planUsage = skills, usage, fetcher
	handle := func(command any) []byte {
		t.Helper()
		payload := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, command)
		response, err := backend.handle(context.Background(), Request{Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := protocol.DecodeResponsePayloadAny(response); err != nil {
			t.Fatalf("response=%s: %v", response, err)
		}
		return response
	}
	catalog := handle(protocol.LoadSkillsCommand{Type: protocol.CommandLoadSkills, Projects: []protocol.SkillProject{}})
	if !strings.Contains(string(catalog), `"name":"demo"`) || !strings.Contains(string(catalog), skillDir) {
		t.Fatalf("catalog=%s", catalog)
	}
	handle(protocol.SetSkillsEnabledCommand{Type: protocol.CommandSetSkillsEnabled, Dirs: []string{skillDir}, Enabled: false})
	if _, err := os.Stat(filepath.Join(skillDir, "SKILL.md.disabled")); err != nil {
		t.Fatal(err)
	}
	handle(protocol.TrashSkillsCommand{Type: protocol.CommandTrashSkills, Dirs: []string{skillDir}})
	if _, err := os.Stat(skillDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("skill was not moved to recoverable trash: %v", err)
	}
	history := handle(protocol.LoadUsageHistoryCommand{Type: protocol.CommandLoadUsageHistory, Window: json.RawMessage(`{"trailingDays":7}`), ProjectRoots: []string{home}})
	if !strings.Contains(string(history), `"pricing":"unavailable"`) {
		t.Fatalf("history=%s", history)
	}
	binary, version := "/daemon/bin/claude", "2.1.0"
	handle(protocol.FetchPlanUsageCommand{Type: protocol.CommandFetchPlanUsage, Provider: "claude", BinaryOverride: &binary, CLIVersion: &version})
	if fetcher.calls != 1 || fetcher.provider != "claude" || fetcher.binary == nil || *fetcher.binary != binary || fetcher.version == nil || *fetcher.version != version {
		t.Fatalf("plan route=%+v", fetcher)
	}
	bad := marshalRuntimeRequest(t, protocol.NilUUID, protocol.NilUUID, protocol.FetchPlanUsageCommand{Type: protocol.CommandFetchPlanUsage, Provider: "deepseek"})
	if _, err := backend.handle(context.Background(), Request{Payload: bad}); err == nil || fetcher.calls != 1 {
		t.Fatalf("unsupported plan route err=%v calls=%d", err, fetcher.calls)
	}
}

func (b *daemonTerminalBackend) Open(_ context.Context, cwd string, cols, rows uint16, emit func(terminaltransport.BackendEvent)) (terminaltransport.PTY, error) {
	b.cwd, b.colsRows, b.emit = cwd, [2]uint16{cols, rows}, emit
	return b.pty, nil
}

func TestTerminalCommandsUseDaemonOwnerOrderBoundsAndEphemeralEvents(t *testing.T) {
	hub := mustHub(t, 32, 1<<20, 32, 1<<20)
	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, hub)
	fake := &daemonTerminalBackend{pty: &daemonTerminalPTY{}}
	manager, err := terminaltransport.NewManager(fake, backend.emitTerminal, terminaltransport.DefaultManagerLimits())
	if err != nil {
		t.Fatal(err)
	}
	backend.terminals = manager
	defer manager.Shutdown(context.Background())
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000011")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000012")
	request := func(command any) ([]byte, error) {
		return backend.handle(context.Background(), Request{Payload: marshalRuntimeRequest(t, sessionID, runtimeID, command)})
	}
	if _, err := request(protocol.OpenTerminalCommand{Type: protocol.CommandOpenTerminal, CWD: "/daemon/remote/workspace", Cols: 120, Rows: 40}); err != nil {
		t.Fatal(err)
	}
	if fake.cwd != "/daemon/remote/workspace" || fake.colsRows != [2]uint16{120, 40} {
		t.Fatalf("daemon host open cwd=%q size=%v", fake.cwd, fake.colsRows)
	}
	if _, err := request(protocol.WriteTerminal([]byte("input"))); err != nil {
		t.Fatal(err)
	}
	if _, err := request(protocol.ResizeTerminalCommand{Type: protocol.CommandResizeTerminal, Cols: 100, Rows: 30}); err != nil {
		t.Fatal(err)
	}
	wrong := marshalRuntimeRequest(t, sessionID, protocol.UUID("00000000-0000-4000-8000-000000000013"), protocol.WriteTerminal([]byte("cross-owner")))
	if _, err := backend.handle(context.Background(), Request{Payload: wrong}); !errors.Is(err, terminaltransport.ErrTerminalOwnership) {
		t.Fatalf("cross-runtime input=%v", err)
	}
	sub := hub.Subscribe()
	defer sub.Close()
	fake.emit(terminaltransport.BackendEvent{Data: []byte("first")})
	fake.emit(terminaltransport.BackendEvent{Data: []byte("second")})
	fake.emit(terminaltransport.BackendEvent{Exit: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for index, expected := range []string{"first", "second"} {
		event, ok := sub.Receive(ctx)
		if !ok {
			t.Fatalf("missing output %d", index)
		}
		var wire protocol.WireDriverEvent
		if err := protocol.Decode(event.Payload, &wire); err != nil || wire.Kind != string(protocol.EventTerminalOutput) {
			t.Fatalf("output wire=%s err=%v", event.Payload, err)
		}
		var output protocol.TerminalOutputPayload
		if err := protocol.Decode(wire.Payload, &output); err != nil || string(output.Data) != expected {
			t.Fatalf("output=%q err=%v", output.Data, err)
		}
	}
	exited, ok := sub.Receive(ctx)
	if !ok {
		t.Fatal("missing terminal exit")
	}
	var exitWire protocol.WireDriverEvent
	if err := protocol.Decode(exited.Payload, &exitWire); err != nil || exitWire.Kind != string(protocol.EventTerminalExited) {
		t.Fatalf("exit=%s err=%v", exited.Payload, err)
	}
	if streams, count, bytes := hub.ReplayStats(); streams != 0 || count != 0 || bytes != 0 {
		t.Fatalf("terminal events polluted replay: %d/%d/%d", streams, count, bytes)
	}
	fake.pty.mu.Lock()
	writes, resizes := append([][]byte(nil), fake.pty.writes...), append([][2]uint16(nil), fake.pty.resizes...)
	fake.pty.mu.Unlock()
	if len(writes) != 1 || string(writes[0]) != "input" || !reflect.DeepEqual(resizes, [][2]uint16{{100, 30}}) {
		t.Fatalf("controls writes=%q resizes=%v", writes, resizes)
	}
}

func TestTerminalProductionBackendIsExplicitlyGated(t *testing.T) {
	backend := newRuntimeBackend(&runtimeStoreFake{}, nil, mustHub(t, 8, 4096, 8, 4096))
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000021")
	runtimeID := protocol.UUID("00000000-0000-4000-8000-000000000022")
	payload := marshalRuntimeRequest(t, sessionID, runtimeID, protocol.OpenTerminalCommand{Type: protocol.CommandOpenTerminal, CWD: "/daemon/remote", Cols: 80, Rows: 24})
	if _, err := backend.handle(context.Background(), Request{Payload: payload}); !errors.Is(err, terminaltransport.ErrBackendUnsupported) {
		t.Fatalf("unsupported PTY backend=%v", err)
	}
	if streams, count, bytes := backend.hub.ReplayStats(); streams != 0 || count != 0 || bytes != 0 {
		t.Fatalf("failed terminal open retained hub state: %d/%d/%d", streams, count, bytes)
	}
}

func TestProviderStartRejectsActiveTerminalSession(t *testing.T) {
	adapter := &runtimeAdapterFake{}
	backend := newRuntimeBackend(&runtimeStoreFake{}, map[providers.Provider]providers.Adapter{providers.ProviderCodex: adapter}, mustHub(t, 8, 4096, 8, 4096))
	fake := &daemonTerminalBackend{pty: &daemonTerminalPTY{}}
	manager, _ := terminaltransport.NewManager(fake, backend.emitTerminal, terminaltransport.DefaultManagerLimits())
	backend.terminals = manager
	defer manager.Shutdown(context.Background())
	sessionID := protocol.UUID("00000000-0000-4000-8000-000000000031")
	terminalRuntimeID := protocol.UUID("00000000-0000-4000-8000-000000000032")
	if err := manager.Open(context.Background(), terminaltransport.Owner{SessionID: string(sessionID), RuntimeID: string(terminalRuntimeID)}, "/daemon/remote", 80, 24); err != nil {
		t.Fatal(err)
	}
	_, err := backend.start(context.Background(), protocol.Request{SessionID: sessionID, RuntimeID: "00000000-0000-4000-8000-000000000033"}, protocol.Start(providersStartOptions()))
	if err == nil || adapter.starts.Load() != 0 {
		t.Fatalf("provider/terminal collision err=%v starts=%d", err, adapter.starts.Load())
	}
}
