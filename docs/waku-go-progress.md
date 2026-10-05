# Waku Go migration progress

Updated: 2026-10-05 (UTC)

## Status legend

- **passed**: executed and met the stated gate
- **failed**: executed and did not meet the gate
- **blocked**: cannot execute without a named external prerequisite
- **unrun**: not executed yet
- **partial**: useful work exists but the acceptance gate is not met

## Phase status

| Phase | Status | Evidence / next gate |
| --- | --- | --- |
| 0 — inventory and baseline | partial | Source references, dependency revisions, ownership contracts, fixtures, measurement schemas and distribution inspection exist. The Rust headless baseline and real Rust/Go/TypeScript protocol harness pass. A buildable Rust desktop baseline, same-display release measurements and locked comparison budgets remain blocked. |
| 1 — difficult native surfaces | partial | Independently reviewed native shell, virtualized transcript, document/code/search/math/editor/review and injected remote-terminal components exist; MyGo IME/composition and terminal accessibility fixes pass locally. Inline child-browser embedding is absent from every MyGo platform backend, unchanged ordinary panes still rebuild on a requested frame, the pinned MyGo revision lacks terminal viewport accessibility, and live platform checks remain blocked. |
| 2 — protocol and desktop slice | partial | Strict protocol-v7 client/supervision plus typed settings, skills, usage, workspace and terminal helpers pass repeated/race/cross-language tests. The real `waku-go` command connects to the Go daemon, loads summaries lazily, reconnects/replays and routes prompts/approvals/questions. Credentialed native-display end-to-end proof is unavailable, so the gate is not passed. |
| 3 — Go daemon and storage | partial | Real `waku-go-daemon`, authenticated v7 WebSocket transport, globally bounded actors/replay, SQLite migrations/settings, daemon-host read/review/Git/checkpoint primitives, skills and usage storage, and bounded terminal manager exist with independent repair reviews. Unsafe cross-process CAS, turn capture/restore and production PTY opening fail closed; copied-real-database upgrade/rollback and cross-client live proof remain. |
| 4 — provider transports | partial | Fixture-backed Go adapters now cover Codex, Cursor, Fx, Grok, Kimi, Pi, Oh My Pi, Claude, Amp, OpenCode and DeepSeek with honest per-provider gates. Extensive lifecycle, reconnect, session-isolation, bounds and capability reviews pass after repairs. Live authenticated checks are blocked by missing binaries/accounts; some provider-specific offline history, fork/background/computer-use paths remain gated. |
| 5 — full UI/platform parity | partial | Daemon-backed settings display, skills/usage panels, opaque remote file browse/read, status-first staged/unstaged review, command palette and prepared remote terminal/editor/review panes pass headless interaction and cross-build checks. Safe mutations, browser embedding, real PTY, updater, Computer Use, live accessibility/IME and signed installers remain unavailable. |
| 6 — polish and cutover | blocked | Deterministic unsigned Linux/macOS/Windows staging, SBOM/licenses/checksums, role probes and archive reinspection pass. Release benchmark exits 3/Blocked without an external GUI observer; real Rust baseline, Cua/platform payloads, platform signing/installers and clean-machine native runs are unavailable. Cutover remains fail-closed and Go is not declared the release default. |

## Verified dependency decisions

| Item | Result |
| --- | --- |
| MyGo upstream `main` | passed — `e112810768ddbc19f8613c99d875a1967ca31af5` on 2026-10-04 |
| Local/published fork revision | passed — `50be73e3d1ba69b8e46641cefd45460e14932ea2` |
| Exact Go module replacement | passed — `v0.0.0-20261004175020-50be73e3d1ba` |
| New MyGo bounded IME extension | passed locally and independently reviewed; publication/pin blocked — the fork checkout contains uncommitted changes, so Waku intentionally remains on the last remotely resolvable exact revision rather than using a local or nonexistent module version |
| Live docs source equivalence | passed from MyGo repository: the website reads repository `docs/`; live-site rendering is not required for source verification |
| Go toolchain in base runner | blocked initially — `/usr/bin/go` is unrelated; Go 1.27.1 installed under `/workspace/.tools/go` for this task |

## Baseline checks

| Check | Status | Result |
| --- | --- | --- |
| Existing Rust full suite | blocked | Current run reached desktop compilation but the runner lacks `fontconfig.pc`/development headers and has no package-install privilege. |
| Existing Rust headless suite | passed | `cargo test --locked -p waku-protocol -p waku-client -p waku-core -p waku-daemon` completed successfully on this checkout. |
| Existing JS/client suite | passed | Protocol bindings current; `@waku/client` check passed; Bun 1.4.2 ran 23 tests, 0 failed (67 assertions). |
| MyGo Go suite | passed | `CGO_ENABLED=0 go test ./...` passed on Go 1.27.1 for every package in the current fork checkout. |
| MyGo JS/typecheck suite | passed | Bun 1.4.2: 35 passed, 0 failed; `tsc --noEmit` passed. |
| Go validation/benchmark harness | passed (harness only) | Repeated tests, diagnostic race tests, vet, adversarial staging/archive/upgrade checks, raw evidence recomputation, role probes and independent reviews pass. The release benchmark correctly exits 3/Blocked without an external GUI observer. Fixture-construction timings are harness sanity only, not product performance. |
| Integrated Go module | passed (compile/test only) | Full CGO-free tests/vet and Linux amd64, macOS amd64/arm64 and Windows amd64 desktop/daemon cross-builds pass. A stale pre-fix OpenCode reconnect test hang was diagnosed, its exact process tree was terminated, and handler-exit plus explicit-timeout regressions now pass repeatedly. This is not a phase or parity acceptance result. |
| Linux real GUI | blocked | Current container lacks the required real desktop/WebKitGTK helper setup; headless checks do not replace it. |
| macOS native/VoiceOver/signing/notarization | blocked | No macOS runner or credentials attached. |
| Windows native/NVDA/UIA/signing/installer | blocked | No Windows runner or signing credentials attached. |
| Credentialed provider smoke checks | blocked | No provider credentials/binaries are attached; all eleven adapters have sanitized/fake transport fixtures, but those do not replace live validation. |
| Go race detector | diagnostic only | `-race` requires CGO and is not distribution evidence. Repeated diagnostic race suites passed for the concurrency-sensitive Go packages; final distributions remain `CGO_ENABLED=0`. |

## Initial product matrix

Each row must end with a Go destination, behavioral fixture, and platform check
before phase 0 passes.

| Surface | Classification | Destination | Current evidence |
| --- | --- | --- | --- |
| Project/session hierarchy, activity/unread state | required parity + clarity improvement | presentation/views | O(1) indexed selection, virtualized rows, command/focus interaction tests pass; full product state remains unwired |
| Streaming transcript, reasoning, tools, citations | required parity | protocol/presentation/document | exact staged ordering/content, reconnect and bounds have fixture coverage across all adapters; credentialed live fidelity remains blocked |
| Composer, attachments, drafts, slash commands, `/goal` | required parity | views/client/storage | prompt/draft/command foundations exist; attachments and several uncommon flows remain incomplete |
| Approvals, questions, stop/retry/steer/follow-up | required parity | client/daemon/providers/views | exact request IDs/options, retry safety, stale replacement and provider lifecycle tests pass; live checks remain blocked |
| Goals, fork, rewind, checkpoints | required parity | daemon/providers/storage/workspace | provider live fork/rewind paths and internal checkpoints exist; unsafe turn capture/restore and unsupported provider history paths are gated |
| Files/editor/Markdown preview/diff/review/commit | required parity | document/workspace/views | daemon-backed opaque browse/read and status-first diffs are live; cross-process-safe save, reviewed-index commit and several Git mutations are gated |
| Terminal | required parity | terminal + MyGo terminal `Conn` | injected remote pane and bounded daemon manager pass; production opening is blocked by absence of a proven CGO-free PTY/ConPTY backend |
| Embedded browser | macOS current parity; honest gate elsewhere | platform/views or proven MyGo extension | inline child-view proof pending |
| Computer Use | current experimental behavior | providers/platform + retained Cua SDK assets | Go orchestration and packaging pending |
| Settings, skills, usage, models/capabilities | required parity | daemon/providers/storage/views | typed daemon/UI paths and bounded skills/usage/provider probing exist; stale-safe settings mutation and authenticated live plan fetching remain gated |
| Updates and packaging | required parity | commands/build tooling | deterministic unsigned staging/inspection passes; updater, signed installers and clean-machine proof are blocked |
| Accessibility, IME, keyboard, high contrast, scale, motion | required parity | all UI tracks | headless keyboard/focus/IME and MyGo platform-fake tests pass; live VoiceOver/NVDA/Linux AT matrix pending |

## Decisions and retained dependencies

- The final app uses MyGo native `ui` content; a system webview window is not a
  substitute for the desktop shell.
- SQLite is pure Go with `CGO_ENABLED=0`; production bootstrap applies the
  existing migration sequence, enforces WAL/NORMAL, and fully probes schema.
  Upgrade/rollback against copied real user fixtures and clean platform builds
  is still required.
- Ghostty VT remains a third-party native library loaded without cgo and must be
  packaged, checksummed, licensed, and present on first launch.
- Cua Driver may remain a retained third-party native SDK/library for Computer
  Use; Waku-owned orchestration must be Go and SDK/runtime assets must be pinned
  and disclosed.
- JavaScript is permitted only for necessary provider/Computer Use assets and
  Bun development tooling, not for application-owned runtime logic.
- Release inspection requires a manifest naming every Go application executable
  plus every retained native/JavaScript asset, purpose, checksum, and license;
  undeclared executables/assets and application-owned Rust runtime links fail.

## Change log

- 2026-10-04: read Waku/MyGo instructions and required planning references;
  verified upstream and fork revisions; froze package, concurrency, persistence,
  and ownership contracts in `docs/waku-go-contracts.md`.
- 2026-10-04: added deterministic workload fixtures, versioned instrumentation
  and resource-sample schemas, feature/release matrices, archive/distribution
  inspection, and an upgrade/rollback validation plan. The harness is ready for
  app instrumentation but does not yet constitute a Rust-versus-Go benchmark.
- 2026-10-04: the focused Rust protocol/client/core/daemon baseline passed; the
  full desktop build is blocked specifically by absent Fontconfig development
  files. Added bounded protocol/client, native presentation, document/code,
  daemon/storage, provider, and validation foundations. Independent reviews
  found and drove repair cycles for queue bounds and WebSocket validation,
  daemon shutdown/locking/path safety, provider lifecycle/capability honesty,
  document work bounds/selection, stream ordering, command correlation, and
  benchmark false-positive risks. No phase gate is inferred from compilation.
- 2026-10-04: closed independent review cycles for presentation/views,
  document/code, daemon/storage/terminal, Codex provider transport, MyGo IME,
  and the validation harness. Full Go module tests, vet, Linux/Windows/macOS
  CGO-free builds, MyGo tests/vet, and the validation script pass. These are
  foundations and proofs only: there is no release desktop/daemon command,
  non-Codex adapters, browser proof, signed package, real comparable benchmark,
  or live platform/provider evidence, so phases remain partial.
- 2026-10-05: added real `waku-go` and `waku-go-daemon` commands; authenticated
  protocol-v7 transport; globally bounded actor/replay and cursor durability;
  strict Rust/Go/TypeScript interoperability; daemon-host filesystem/Git,
  settings, skills and usage services; native settings/files/review/terminal
  panes; and fixture-backed adapters for all eleven catalog providers.
  Independent adversarial reviews drove repairs for reconnect loss/duplication,
  cross-session events, option/capability honesty, process cleanup, cursor and
  initial-save races, unsafe CAS/checkpoint claims, terminal shutdown races,
  archive trust boundaries and false release evidence.
- 2026-10-05: deterministic unsigned release staging now builds and role-probes
  CGO-free desktop/daemon executables for Linux, macOS and Windows, generates a
  digest-bound SBOM/licenses/checksums, and reinspects private archive snapshots.
  It deliberately remains Blocked on retained Cua/platform assets, real PTY,
  inline browser embedding, signing/installers, clean-machine GUI runs and a
  same-display Rust-versus-Go benchmark. No release or live-data cutover was
  performed.
