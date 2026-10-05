#!/bin/sh
set -eu
if [ "$#" -ne 4 ]; then
  echo "usage: $0 SPEC.json NEW_STAGE_DIR NEW_ARCHIVE REPORT.json" >&2
  exit 2
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}
cd "$module_dir"
CGO_ENABLED=0 "$go_bin" run ./internal/validation/stagecmd -spec "$1" -stage "$2" -archive "$3" -report "$4"
