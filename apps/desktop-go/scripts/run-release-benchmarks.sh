#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}
cd "$module_dir"
task_tmp=$(mktemp -d)
trap 'rm -rf -- "$task_tmp"' EXIT HUP INT TERM
CGO_ENABLED=0 "$go_bin" build -o "$task_tmp/release-benchmark" ./benchmarks/releasecmd
"$task_tmp/release-benchmark" "$@"
