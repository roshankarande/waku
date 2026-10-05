# Release comparison harness

These fixtures define the same deterministic workload for the current Rust
release and the Go/MyGo release. `BenchmarkFixtureConstruction` is only a
sanity check for fixture generation; its timings are not product benchmarks.

Each application run must emit the versioned NDJSON event protocol from
`internal/validation/events.go`. An external sampler emits
`internal/validation/samples.go`. Run at least one warm-up and five recorded
iterations per scenario on the same machine, display, renderer, refresh rate,
scale, power profile, build mode and fixture digest. Compare both normal GPU
rendering and `MYGO_GPU=0`; do not combine those result sets.

Required scenarios are `long-transcript`, `fast-reasoning`, `large-diff`, and
`terminal-plus-stream`. The application driver must also trigger cold startup,
first-session hydration, text input, reconnect, and orderly shutdown markers.
Record frame CPU work separately from compositor presentation timestamps.

The aggregator refuses to turn missing native instrumentation into zeroes.
Unavailable compositor-present, idle-wakeup, GUI, signing, credential, or
platform evidence stays `blocked` or `unrun` in the result checks.

Every event and sample carries build, workload, run, iteration, and warm-up
attribution. Paired events use unique correlation IDs. Samples contain
cumulative counters and contiguous window boundaries; the aggregator derives
window deltas. Candidate aggregation requires a baseline captured on the same
machine/display/renderer configuration and explicit numeric budgets. Use
`-baseline-mode` only to capture the baseline: it does not make a comparative
performance claim. Comparisons also require `-baseline-events` and
`-baseline-samples`; those raw artifacts are reopened, hashed, decoded, and
attributed instead of trusting digest strings in the result JSON.
