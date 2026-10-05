package code

import (
	"context"
	"errors"
	"sync"
)

var ErrSearchQueueFull = errors.New("code: search queue full")

type SearchResult struct {
	Generation uint64
	Query      string
	Matches    []Match
	Err        error
}
type searchJob struct {
	ctx    context.Context
	snap   *Snapshot
	query  string
	result chan SearchResult
}
type SearchWorker struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan searchJob
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool
}

func NewSearchWorker(workers, queue int) *SearchWorker {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &SearchWorker{ctx: ctx, cancel: cancel, jobs: make(chan searchJob, queue)}
	for range workers {
		w.wg.Add(1)
		go w.run()
	}
	return w
}
func (w *SearchWorker) Submit(ctx context.Context, snap *Snapshot, query string) (<-chan SearchResult, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return nil, context.Canceled
	}
	result := make(chan SearchResult, 1)
	job := searchJob{ctx, snap, query, result}
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case w.jobs <- job:
		return result, nil
	default:
		return nil, ErrSearchQueueFull
	}
}
func (w *SearchWorker) run() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			w.cancelQueued()
			return
		case job := <-w.jobs:
			select {
			case <-job.ctx.Done():
				job.result <- SearchResult{Generation: job.snap.Generation, Query: job.query, Err: job.ctx.Err()}
			case <-w.ctx.Done():
				job.result <- SearchResult{Generation: job.snap.Generation, Query: job.query, Err: w.ctx.Err()}
			default:
				ctx, cancel := context.WithCancel(job.ctx)
				stop := context.AfterFunc(w.ctx, cancel)
				matches, err := job.snap.SearchContext(ctx, job.query)
				stop()
				cancel()
				job.result <- SearchResult{Generation: job.snap.Generation, Query: job.query, Matches: matches, Err: err}
			}
		}
	}
}

func (w *SearchWorker) cancelQueued() {
	for {
		select {
		case job := <-w.jobs:
			job.result <- SearchResult{Generation: job.snap.Generation, Query: job.query, Err: context.Canceled}
		default:
			return
		}
	}
}

func (w *SearchWorker) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.wg.Wait()
		return
	}
	w.closed = true
	w.cancel()
	w.mu.Unlock()
	w.wg.Wait()
}
