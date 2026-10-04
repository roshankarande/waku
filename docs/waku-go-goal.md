# Goal handoff: Waku in Go using MyGo

Paste the following into `/goal` from the Waku repository on `waku-go`:

```text
Implement docs/waku-go-plan.md in roshankarande/waku on branch waku-go.

Build Waku's desktop with MyGo's native Go ui package and translate Waku-owned daemon, provider, persistence and supervision logic to Go. Preserve the product's capabilities, data, protocol interoperability and platform behavior. Make the interface clearer and at least as polished as the current Waku, and demonstrate performance with comparable release benchmarks.

Read AGENTS.md and the plan's source references first. Verify the MyGo dependency revision against https://github.com/egoist/mygo/tree/main and the documentation at https://mygo.egoist.dev/ (or the source docs that power the site if network access is blocked). Start with phase 0 baselines and phase 1 proofs for rich documents, selection/search/math, code/diff, browser embedding, terminal and pane invalidation. Do not assume the framework already provides those behaviors.

Use existing MyGo components where suitable; write Waku-specific custom components in Waku. Extend the existing roshankarande/mygo fork for demonstrated reusable gaps, with platform/fake tests, examples/docs and an exact dependency pin. Preserve CGO_ENABLED=0 and keep Bun as development tooling. Do not create another application repository; implementation remains in roshankarande/waku.

Use sub-agents for independent protocol/client, native UI, document components, daemon/storage, provider groups, framework gaps and validation/release work. Define ownership and shared contracts first, avoid concurrent edits to the same files, and review integrated slices. Use the current checkouts; do not create worktrees unless explicitly requested.

A Go desktop using the current Rust daemon is permitted only as a temporary milestone. Continue through the Go daemon/providers/storage replacement and the release cutover. The final Go distribution must not depend on Waku's Rust desktop/core/client/daemon runtime. Identify any retained third-party native SDK/library or required JavaScript asset explicitly.

Keep docs/waku-go-progress.md current. Proceed autonomously through the plan's acceptance gates; resolve failures rather than weakening requirements. Use safe fixture databases and synthetic workspaces for migrations, preserve user changes and data, and never let Rust and Go daemons write the same data directory concurrently. Do not publish releases, overwrite live data, or alter credentials as a side effect of development.

Require behavioral fixtures, component interaction tests, live native/platform checks, interoperability, upgrade/rollback and comparable performance evidence. Preserve exact provider event order, approvals, goals, rewind/fork, remote path ownership, streaming cadence, virtualized lists, keyboard/IME/screen-reader behavior and honest capability gates. Missing credentials or runners are concrete blockers to the affected validation, not evidence of passing tests.

Finish with the working implementation and a concise report of parity, UI improvements, measured performance, tests, packaging, retained dependencies and any genuine external blockers. A plan, demo shell, successful compile or temporary Rust bridge is not completion.
```
