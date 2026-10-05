# Waku desktop in Go

This module contains the in-progress native MyGo desktop and Go daemon
migration foundations. The only command currently shipped here is
`cmd/waku-go-proof`, a synthetic phase-1 surface proof with no live-data or
daemon access. It is not the release application.
Architecture, dependency, and ownership contracts are in
[`../../docs/waku-go-contracts.md`](../../docs/waku-go-contracts.md). Progress
and validation results are recorded in
[`../../docs/waku-go-progress.md`](../../docs/waku-go-progress.md).

All builds must use `CGO_ENABLED=0`. The exact MyGo fork revision is pinned in
`go.mod`; do not commit an absolute local replacement.

The local `/workspace/mygo` checkout also contains independently reviewed IME
and bounded input-context extensions. They are intentionally not used by the
published pin until that fork revision is committed and remotely resolvable;
the pinned build disables destructive composer shortcuts and exposes the
capability gate instead.
