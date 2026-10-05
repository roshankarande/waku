//go:build unix

package providers

import (
	"context"
	"io"
	"os/exec"
	"syscall"
)

const providerGuardianScript = `
parent=$$
child=
watcher=
cleanup() {
  trap - EXIT HUP INT TERM
  if [ -n "$watcher" ]; then kill -TERM "$watcher" 2>/dev/null || true; fi
  if [ -n "$child" ]; then kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi
}
trap cleanup EXIT HUP INT TERM
"$@" </dev/null &
child=$!
( while kill -0 "$child" 2>/dev/null; do sleep 1; done; kill -TERM "$parent" 2>/dev/null || true ) &
watcher=$!
while IFS= read -r _; do :; done
`

func newGuardedProviderCommand(ctx context.Context, path string, args ...string) (*exec.Cmd, io.WriteCloser, error) {
	commandArgs := append([]string{"-c", providerGuardianScript, "waku-provider-guardian", path}, args...)
	command := exec.CommandContext(ctx, "/bin/sh", commandArgs...)
	input, err := command.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	return command, input, nil
}

func configureProviderProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func providerProcessTreeContained() bool { return true }

func terminateProviderProcessGroup(command *exec.Cmd, signal syscall.Signal) error {
	if command == nil || command.Process == nil {
		return nil
	}
	return syscall.Kill(-command.Process.Pid, signal)
}
