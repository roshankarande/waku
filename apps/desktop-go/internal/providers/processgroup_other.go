//go:build !unix

package providers

import (
	"context"
	"io"
	"os/exec"
	"syscall"
)

func newGuardedProviderCommand(ctx context.Context, path string, args ...string) (*exec.Cmd, io.WriteCloser, error) {
	return exec.CommandContext(ctx, path, args...), nil, nil
}

func configureProviderProcessGroup(*exec.Cmd) {}
func providerProcessTreeContained() bool      { return false }

func terminateProviderProcessGroup(command *exec.Cmd, _ syscall.Signal) error {
	if command == nil || command.Process == nil {
		return nil
	}
	return command.Process.Kill()
}
