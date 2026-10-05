#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}

cd "$module_dir"
CGO_ENABLED=0 "$go_bin" test ./internal/validation/... ./benchmarks/...
CGO_ENABLED=0 "$go_bin" test -run '^$' -bench BenchmarkFixtureConstruction -benchtime=1x -benchmem ./benchmarks
