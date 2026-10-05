package validation

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// Event is attributable to one build/workload/run. CorrelationID is required
// for paired operations, allowing concurrent input, hydration, and reconnects.
type Event struct {
	Version       int               `json:"version"`
	RunID         string            `json:"runId"`
	Build         string            `json:"build"`
	WorkloadID    string            `json:"workloadId"`
	FixtureSHA256 string            `json:"fixtureSha256"`
	Iteration     int               `json:"iteration"`
	Warmup        bool              `json:"warmup"`
	CorrelationID string            `json:"correlationId,omitempty"`
	Name          string            `json:"name"`
	AtNS          int64             `json:"atNs"`
	Value         float64           `json:"value,omitempty"`
	Fields        map[string]string `json:"fields,omitempty"`
}

const (
	EventProcessStart   = "process_start"
	EventInteractive    = "interactive"
	EventHydrateStart   = "hydrate_start"
	EventHydrateReady   = "hydrate_ready"
	EventInput          = "input"
	EventPaint          = "paint"
	EventPresent        = "present"
	EventFrameWork      = "frame_work"
	EventFramePresented = "frame_presented"
	EventStreamCommit   = "stream_commit"
	EventPaneRender     = "pane_render"
	EventReconnectStart = "reconnect_start"
	EventReconnectReady = "reconnect_ready"
	EventShutdownStart  = "shutdown_start"
	EventProcessExit    = "process_exit"
)

var pairedEvent = map[string]string{EventProcessStart: EventInteractive, EventHydrateStart: EventHydrateReady, EventInput: EventPaint, EventReconnectStart: EventReconnectReady, EventShutdownStart: EventProcessExit}
var pairedEnd = func() map[string]string {
	m := map[string]string{EventPresent: EventInput}
	for start, end := range pairedEvent {
		m[end] = start
	}
	return m
}()
var knownEvent = func() map[string]bool {
	m := map[string]bool{EventPresent: true, EventFrameWork: true, EventFramePresented: true, EventStreamCommit: true, EventPaneRender: true}
	for start, end := range pairedEvent {
		m[start] = true
		m[end] = true
	}
	return m
}()

func DecodeEvents(r io.Reader) ([]Event, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var events []Event
	for line := 1; s.Scan(); line++ {
		var e Event
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("event line %d: %w", line, err)
		}
		if e.Version != SchemaVersion || e.RunID == "" || e.Build == "" || e.WorkloadID == "" || len(e.FixtureSHA256) != 64 || e.Name == "" || e.AtNS < 0 || e.Iteration < 0 {
			return nil, fmt.Errorf("event line %d: missing/invalid attribution, name, or timestamp", line)
		}
		if !knownEvent[e.Name] {
			return nil, fmt.Errorf("event line %d: unknown event %q", line, e.Name)
		}
		if (e.Name == EventFrameWork || e.Name == EventFramePresented) && e.Value < 0 {
			return nil, fmt.Errorf("event line %d: negative frame duration", line)
		}
		if _, ok := pairedEvent[e.Name]; ok && e.CorrelationID == "" {
			return nil, fmt.Errorf("event line %d: %s requires correlationId", line, e.Name)
		}
		if _, ok := pairedEnd[e.Name]; ok && e.CorrelationID == "" {
			return nil, fmt.Errorf("event line %d: %s requires correlationId", line, e.Name)
		}
		events = append(events, e)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].AtNS < events[j].AtNS })
	return events, nil
}

type eventAccumulator struct {
	startup, hydration, inputPaint, inputPresent, frameWork, framePresented, reconnect, shutdown []float64
	frames, commits                                                                              uint64
	panes                                                                                        map[string]uint64
}
type operationKey struct {
	run, correlation string
	iteration        int
	warmup           bool
}

func MetricsFromEvents(events []Event, build, workload, digest string) (Metrics, map[int]bool, error) {
	a := eventAccumulator{panes: map[string]uint64{}}
	operations := map[operationKey]map[string]Event{}
	iterations := map[int]bool{}
	for _, e := range events {
		if e.Build != build || e.WorkloadID != workload || e.FixtureSHA256 != digest {
			return Metrics{}, nil, fmt.Errorf("event attribution %s/%s does not match %s/%s", e.Build, e.WorkloadID, build, workload)
		}
		if !e.Warmup {
			iterations[e.Iteration] = true
		}
		if e.CorrelationID != "" {
			key := operationKey{e.RunID, e.CorrelationID, e.Iteration, e.Warmup}
			if operations[key] == nil {
				operations[key] = map[string]Event{}
			}
			if _, exists := operations[key][e.Name]; exists {
				return Metrics{}, nil, fmt.Errorf("duplicate %s for correlation %q", e.Name, e.CorrelationID)
			}
			operations[key][e.Name] = e
		}
		if e.Warmup {
			continue
		}
		switch e.Name {
		case EventFrameWork:
			a.frameWork = append(a.frameWork, e.Value/float64(time.Millisecond))
			a.frames++
		case EventFramePresented:
			a.framePresented = append(a.framePresented, e.Value/float64(time.Millisecond))
		case EventStreamCommit:
			a.commits++
		case EventPaneRender:
			pane := e.Fields["pane"]
			if pane == "" {
				return Metrics{}, nil, fmt.Errorf("pane_render requires fields.pane")
			}
			a.panes[pane]++
		}
	}
	for key, byName := range operations {
		startKinds := 0
		for start := range pairedEvent {
			if _, ok := byName[start]; ok {
				startKinds++
			}
		}
		if startKinds > 1 {
			return Metrics{}, nil, fmt.Errorf("correlation %q is reused across operation types", key.correlation)
		}
		for startName, endName := range pairedEvent {
			start, hasStart := byName[startName]
			end, hasEnd := byName[endName]
			if hasStart != hasEnd {
				return Metrics{}, nil, fmt.Errorf("correlation %q has unmatched %s/%s", key.correlation, startName, endName)
			}
			if !hasStart {
				continue
			}
			if end.AtNS < start.AtNS {
				return Metrics{}, nil, fmt.Errorf("correlation %q: %s precedes %s", key.correlation, endName, startName)
			}
			if key.warmup {
				continue
			}
			ms := float64(end.AtNS-start.AtNS) / float64(time.Millisecond)
			switch startName {
			case EventProcessStart:
				a.startup = append(a.startup, ms)
			case EventHydrateStart:
				a.hydration = append(a.hydration, ms)
			case EventInput:
				a.inputPaint = append(a.inputPaint, ms)
			case EventReconnectStart:
				a.reconnect = append(a.reconnect, ms)
			case EventShutdownStart:
				a.shutdown = append(a.shutdown, ms)
			}
		}
		if input, ok := byName[EventInput]; ok {
			if present, ok := byName[EventPresent]; ok {
				if present.AtNS < input.AtNS {
					return Metrics{}, nil, fmt.Errorf("correlation %q: present precedes input", key.correlation)
				}
				if paint, ok := byName[EventPaint]; ok && present.AtNS < paint.AtNS {
					return Metrics{}, nil, fmt.Errorf("correlation %q: present precedes paint", key.correlation)
				}
				if !key.warmup {
					a.inputPresent = append(a.inputPresent, float64(present.AtNS-input.AtNS)/float64(time.Millisecond))
				}
			}
		} else if _, ok := byName[EventPresent]; ok {
			return Metrics{}, nil, fmt.Errorf("correlation %q has present without input", key.correlation)
		}
	}
	return Metrics{StartupMS: Summarize(a.startup), HydrationMS: Summarize(a.hydration), InputToPaintMS: Summarize(a.inputPaint), InputToPresentMS: Summarize(a.inputPresent), FrameWorkMS: Summarize(a.frameWork), FramePresentedMS: Summarize(a.framePresented), ReconnectMS: Summarize(a.reconnect), ShutdownMS: Summarize(a.shutdown), Frames: a.frames, StreamCommits: a.commits, PaneRenders: a.panes}, iterations, nil
}
