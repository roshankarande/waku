#!/bin/sh
set -eu
if [ "$#" -ne 3 ]; then
  echo "usage: $0 STAGED_RELEASE_ROOT SPEC.json OUTPUT.json" >&2
  exit 2
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}
cd "$module_dir"
CGO_ENABLED=0 "$go_bin" run ./internal/validation/manifestcmd -root "$1" -spec "$2" -out "$3"
