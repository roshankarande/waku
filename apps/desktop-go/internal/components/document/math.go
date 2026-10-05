package document

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

const MaxMathSourceBytes = 8 * 1024

var ErrMathQueueFull = errors.New("document: math queue full")

type MathStatus uint8

const (
	MathPending MathStatus = iota
	MathReady
	MathInvalid
)

type MathResult struct {
	Status MathStatus
	// Output is an opaque, prepared drawing payload owned by the renderer.
	// It is immutable so cache hits never clone a large buffer on the UI thread.
	Output string
	// Source is always present so pending and invalid formulas stay selectable.
	Source string
	Err    string
}

type MathRenderer interface {
	Render(context.Context, string, bool) ([]byte, error)
}
type MathRendererFunc func(context.Context, string, bool) ([]byte, error)

func (f MathRendererFunc) Render(ctx context.Context, s string, d bool) ([]byte, error) {
	return f(ctx, s, d)
}

type mathKey struct {
	source  string
	display bool
}
type mathJob struct {
	key mathKey
	ctx context.Context
}
type pendingMath struct {
	cancel   context.CancelFunc
	waiters  map[<-chan struct{}]func() bool
	immortal bool
}
type cacheEntry struct {
	key    mathKey
	result MathResult
	bytes  int
}

type MathOptions struct {
	Workers, Queue, CacheEntries, CacheBytes int
}

// MathService bounds worker concurrency, pending work, source size and both
// cache count and bytes. Request never waits for rendering.
type MathService struct {
	renderer                         MathRenderer
	ctx                              context.Context
	cancel                           context.CancelFunc
	jobs                             chan mathJob
	renderSlots                      chan struct{}
	stop                             chan struct{}
	done                             chan struct{}
	wg                               sync.WaitGroup
	mu                               sync.Mutex
	closed                           bool
	pending                          map[mathKey]*pendingMath
	cache                            map[mathKey]*list.Element
	lru                              list.List
	cacheBytes, maxEntries, maxBytes int
}

func NewMathService(renderer MathRenderer, opts MathOptions) *MathService {
	if opts.Workers <= 0 {
		opts.Workers = 2
	}
	if opts.Queue <= 0 {
		opts.Queue = 128
	}
	if opts.CacheEntries <= 0 {
		opts.CacheEntries = 256
	}
	if opts.CacheBytes <= 0 {
		opts.CacheBytes = 32 << 20
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &MathService{renderer: renderer, ctx: ctx, cancel: cancel, jobs: make(chan mathJob, opts.Queue), renderSlots: make(chan struct{}, opts.Workers), stop: make(chan struct{}), done: make(chan struct{}), pending: map[mathKey]*pendingMath{}, cache: map[mathKey]*list.Element{}, maxEntries: opts.CacheEntries, maxBytes: opts.CacheBytes}
	for range opts.Workers {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

func (s *MathService) Request(f Formula) (MathResult, error) {
	return s.RequestContext(context.Background(), f)
}

// RequestContext ties newly scheduled rendering to one caller. Cancellation
// retires queued work and interrupts a renderer that honors its context.
func (s *MathService) RequestContext(ctx context.Context, f Formula) (MathResult, error) {
	if err := ctx.Err(); err != nil {
		return MathResult{Status: MathInvalid, Source: f.Source, Err: err.Error()}, err
	}
	if len(f.Source) > MaxMathSourceBytes || f.Source == "" {
		return MathResult{Status: MathInvalid, Source: f.Source, Err: "formula source limit"}, nil
	}
	k := mathKey{f.Source, f.Display}
	s.mu.Lock()
	if e := s.cache[k]; e != nil {
		s.lru.MoveToFront(e)
		r := e.Value.(*cacheEntry).result
		s.mu.Unlock()
		return r, nil
	}
	if s.closed {
		s.mu.Unlock()
		return MathResult{Status: MathInvalid, Source: f.Source, Err: "renderer closed"}, nil
	}
	if pending := s.pending[k]; pending != nil {
		s.addMathWaiterLocked(k, pending, ctx)
		s.mu.Unlock()
		return MathResult{Status: MathPending, Source: f.Source}, nil
	}
	jobCtx, cancel := context.WithCancel(s.ctx)
	pending := &pendingMath{cancel: cancel, waiters: map[<-chan struct{}]func() bool{}}
	s.pending[k] = pending
	s.addMathWaiterLocked(k, pending, ctx)
	select {
	case s.jobs <- mathJob{key: k, ctx: jobCtx}:
		s.mu.Unlock()
		return MathResult{Status: MathPending, Source: f.Source}, nil
	default:
		delete(s.pending, k)
		s.stopMathWaitersLocked(pending)
		cancel()
		s.mu.Unlock()
		return MathResult{Status: MathPending, Source: f.Source}, ErrMathQueueFull
	}
}

func (s *MathService) addMathWaiterLocked(k mathKey, pending *pendingMath, ctx context.Context) {
	if ctx.Done() == nil {
		pending.immortal = true
		return
	}
	done := ctx.Done()
	if _, exists := pending.waiters[done]; exists {
		return
	}
	pending.waiters[done] = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		current := s.pending[k]
		if current != pending {
			return
		}
		delete(pending.waiters, done)
		if len(pending.waiters) == 0 && !pending.immortal {
			pending.cancel()
		}
	})
}

func (s *MathService) stopMathWaitersLocked(pending *pendingMath) {
	for _, stop := range pending.waiters {
		stop()
	}
	clear(pending.waiters)
}

func (s *MathService) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case j := <-s.jobs:
			if j.ctx.Err() != nil {
				s.complete(j.key, MathResult{}, false)
				continue
			}
			select {
			case s.renderSlots <- struct{}{}:
			case <-j.ctx.Done():
				s.complete(j.key, MathResult{}, false)
				continue
			case <-s.stop:
				s.complete(j.key, MathResult{}, false)
				return
			}
			type rendered struct {
				out []byte
				err error
			}
			finished := make(chan rendered, 1)
			go func() {
				defer func() { <-s.renderSlots }()
				out, err := s.renderer.Render(j.ctx, j.key.source, j.key.display)
				finished <- rendered{out, err}
			}()
			var out []byte
			var err error
			select {
			case result := <-finished:
				out, err = result.out, result.err
			case <-j.ctx.Done():
				s.complete(j.key, MathResult{}, false)
				continue
			case <-s.stop:
				s.complete(j.key, MathResult{}, false)
				return
			}
			r := MathResult{Status: MathReady, Source: j.key.source, Output: string(out)}
			if err != nil {
				r.Status, r.Err, r.Output = MathInvalid, err.Error(), ""
			}
			cache := j.ctx.Err() == nil
			s.complete(j.key, r, cache)
		}
	}
}

func (s *MathService) complete(k mathKey, r MathResult, keep bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.pending[k]
	delete(s.pending, k)
	if pending != nil {
		s.stopMathWaitersLocked(pending)
		pending.cancel()
	}
	if !keep {
		return
	}
	sz := len(k.source) + len(r.Output) + len(r.Err)
	if sz > s.maxBytes {
		return
	}
	e := s.lru.PushFront(&cacheEntry{key: k, result: r, bytes: sz})
	s.cache[k] = e
	s.cacheBytes += sz
	for s.lru.Len() > s.maxEntries || s.cacheBytes > s.maxBytes {
		old := s.lru.Back()
		if old == nil {
			break
		}
		entry := old.Value.(*cacheEntry)
		delete(s.cache, entry.key)
		s.cacheBytes -= entry.bytes
		s.lru.Remove(old)
	}
}

func (s *MathService) Close() {
	s.mu.Lock()
	if s.closed {
		done := s.done
		s.mu.Unlock()
		<-done
		return
	}
	s.closed = true
	for _, pending := range s.pending {
		pending.cancel()
	}
	s.cancel()
	close(s.stop)
	s.mu.Unlock()
	s.wg.Wait()
	// Workers are now stopped, so any queued jobs can be retired without
	// racing a receiver. Stop their caller links as well as their contexts.
	for {
		select {
		case <-s.jobs:
		default:
			s.mu.Lock()
			for _, pending := range s.pending {
				s.stopMathWaitersLocked(pending)
			}
			clear(s.pending)
			close(s.done)
			s.mu.Unlock()
			return
		}
	}
}
