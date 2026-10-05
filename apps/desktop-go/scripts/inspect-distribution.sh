#!/bin/sh
set -eu

if [ "$#" -eq 2 ] && [ "$1" = "--archive-shape" ]; then
  script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
  module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
  go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}
  cd "$module_dir"
  CGO_ENABLED=0 "$go_bin" run ./internal/validation/archivecmd "$2"
  exit $?
fi
if { [ "$#" -ne 2 ] && [ "$#" -ne 4 ]; } || { [ "$#" -eq 4 ] && [ "$1" != "--archive" ]; }; then
  echo "usage: $0 --archive-shape ARCHIVE | --archive ARCHIVE MANIFEST.json SHA256 | UNPACKED_RELEASE_ROOT MANIFEST.json" >&2
  exit 2
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
go_bin=${WAKU_GO_BIN:-/workspace/.tools/go/bin/go}

cd "$module_dir"
if [ "$#" -eq 4 ] && [ "$1" = "--archive" ]; then
  CGO_ENABLED=0 "$go_bin" run ./internal/validation/inspectcmd -archive "$2" -manifest "$3" -archive-sha256 "$4"
else
  CGO_ENABLED=0 "$go_bin" run ./internal/validation/inspectcmd -root "$1" -manifest "$2"
fi
