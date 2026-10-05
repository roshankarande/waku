# Waku Go migration contracts

This document freezes the initial package boundaries and ownership for the
`waku-go` migration. It complements `waku-go-plan.md`; protocol v7 and the
existing SQLite schema remain the behavioral authorities.

## Dependency baseline

- Waku behavior baseline: `10bcd728c9b2f07d664cfece3c8c870d3418ddd2`.
- MyGo upstream `main`, verified 2026-10-04: `e112810768ddbc19f8613c99d875a1967ca31af5`.
- MyGo fork pin: `50be73e3d1ba69b8e46641cefd45460e14932ea2`, resolved as
  `github.com/roshankarande/mygo v0.0.0-20261004175020-50be73e3d1ba`.
- Waku imports the fork through the upstream module path
  `github.com/egoist/mygo`; the exact replacement belongs in
  `apps/desktop-go/go.mod`. Local development may use a `go.work` outside the
  committed module, but published builds may not contain a local path replace.
- `CGO_ENABLED=0` is mandatory. Bun remains build/development tooling only.

## Package dependency direction

```text
cmd/waku-go -> views -> presentation -> client -> protocol
                 |            ^
                 v            |
             components ------+

cmd/waku-go-daemon -> daemon -> providers
                         |----> storage
                         |----> workspace
                         |----> terminal
                         `----> protocol
```

Lower layers never import views, presentation, or either command. Provider,
storage, workspace, and terminal packages depend on small daemon-owned
interfaces rather than on each other. Application code may import MyGo;
protocol, daemon, provider, and persistence domain code may not.

## Shared contracts

1. `internal/protocol` is the only package that encodes or decodes protocol v7.
   It preserves tagged camelCase JSON, UUID strings, base64 byte strings,
   absent-versus-null fields, and 64-bit integers. Unknown payloads use
   `json.RawMessage` or a decoder with `UseNumber`; they never round-trip
   through `float64`.
2. `internal/client` owns WebSocket transport, request correlation,
   authentication/origin behavior, reconnect, replay cursors, deduplication,
   epoch resets, and daemon supervision. It exposes events and request methods;
   it does not mutate presentation state or terminate externally managed
   daemons.
3. `internal/presentation` owns immutable/read-mostly snapshots and reducers.
   Background results carry a generation and may commit only when it still
   matches. Views perform no filesystem, process, network, database, parsing,
   highlighting, or blocking-lock work.
4. `internal/views` contains the Waku shell and native interactions.
   `internal/components` contains reusable Waku-owned document, code/diff, and
   transcript components. Lists use stable keys and virtualization. Per-session
   draft, selection, pane, and scroll state survives navigation.
5. `internal/daemon` serializes commands per session actor and owns bounded
   replay/response caches by count and bytes. Slow subscribers cannot block an
   actor or another session. Cancellation, approvals, and lifecycle events
   bypass stream batching; other streaming commits are coalesced near 120 ms.
6. `internal/storage` alone opens writable Waku data. It uses the existing
   migration journal and schema semantics, proves `CGO_ENABLED=0`, locks the
   data directory for the process lifetime, and runs migration tests only on
   fixture copies. Rust and Go daemons must never write one directory together.
7. `internal/providers` normalizes transport-native messages without reordering
   them. Each adapter declares capabilities explicitly and retains unsupported
   gates. Provider processes are session-lived unless the source contract says
   otherwise. OpenCode remains externally managed.
8. `internal/workspace` owns daemon-host paths, filesystem/Git/checkpoints, and
   projectless roots. `internal/terminal` exposes daemon-owned PTY streams;
   remote UI terminal panes must use MyGo terminal's `Conn` seam and never start
   a local shell accidentally.
9. MyGo changes live only in `/workspace/mygo`, require a demonstrated reusable
   gap, public API proposal, fake/unsupported/platform coverage as applicable,
   tests, docs, an example, and a Waku integration test. Waku-specific semantics
   stay GPL code in Waku.
10. The primary agent owns shared contracts, module pins, command wiring,
    integration, and `docs/waku-go-progress.md`. Track owners do not edit those
    files without coordination.

## Initial ownership

| Track | Exclusive initial paths |
| --- | --- |
| Protocol/client | `internal/protocol`, `internal/client`, protocol fixtures |
| Native UI | `internal/presentation`, `internal/views`, `cmd/waku-go`, UI interaction tests |
| Documents | `internal/components/document`, `internal/components/code`, document fixtures |
| Daemon/storage | `internal/daemon`, `internal/storage`, `internal/workspace`, `internal/terminal`, `cmd/waku-go-daemon` |
| Provider groups | `internal/providers`, provider replay fixtures |
| Framework gaps | `/workspace/mygo` only, one proven extension at a time |
| Validation/release | benchmark and validation harnesses, packaging proposals; no shared command or progress files |
| Primary/integration | `go.mod`, `go.sum`, `cmd/waku-go-proof`, this document, progress ledger, integration-only files |

If a track needs a shared API change, it proposes the signature to the primary
agent before editing outside its paths. Substantial slices receive an integration
review for behavior loss, UI-thread work, protocol drift, and missing tests.
