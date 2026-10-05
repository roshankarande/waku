package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type GitEntry struct {
	Path     string
	Index    byte
	Worktree byte
}

type GitStatus struct {
	Branch                           string
	Entries                          []GitEntry
	Additions, Deletions             uint64
	StagedAdditions, StagedDeletions uint64
	HasStaged, HasUnstaged           bool
}

type Diff struct {
	Patch     string
	Numstat   string
	Truncated bool
}

func (s *Service) GitStatus(ctx context.Context) (GitStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return GitStatus{}, err
	}
	branch, err := s.git(ctx, nil, "branch", "--show-current")
	if err != nil {
		return GitStatus{}, err
	}
	raw, err := s.git(ctx, nil, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return GitStatus{}, err
	}
	status := GitStatus{Branch: strings.TrimSpace(string(branch))}
	fields := bytes.Split(raw, []byte{0})
	for index := 0; index < len(fields); index++ {
		field := fields[index]
		if len(field) == 0 {
			continue
		}
		if len(field) < 4 || field[2] != ' ' {
			return GitStatus{}, errors.New("git returned malformed porcelain status")
		}
		entry := GitEntry{Index: field[0], Worktree: field[1], Path: string(field[3:])}
		if entry.Index == 'R' || entry.Index == 'C' {
			index++ // porcelain -z carries the source path as the next field
			if index >= len(fields) || len(fields[index]) == 0 {
				return GitStatus{}, errors.New("git returned malformed rename status")
			}
		}
		entry.Path = filepath.ToSlash(entry.Path)
		status.Entries = append(status.Entries, entry)
		status.HasStaged = status.HasStaged || (entry.Index != ' ' && entry.Index != '?')
		status.HasUnstaged = status.HasUnstaged || entry.Worktree != ' ' || entry.Index == '?'
		if len(status.Entries) > s.limits.MaxEntries {
			return GitStatus{}, ErrLimitExceeded
		}
	}
	all, err := s.git(ctx, nil, "diff", "--no-ext-diff", "--numstat", "HEAD", "--")
	if err != nil {
		all, err = s.git(ctx, nil, "diff", "--no-ext-diff", "--numstat", "--")
	}
	if err != nil {
		return GitStatus{}, fmt.Errorf("inspect worktree numstat: %w", err)
	}
	status.Additions, status.Deletions, err = parseNumstat(all)
	if err != nil {
		return GitStatus{}, fmt.Errorf("parse worktree numstat: %w", err)
	}
	staged, err := s.git(ctx, nil, "diff", "--no-ext-diff", "--cached", "--numstat", "--")
	if err != nil {
		return GitStatus{}, fmt.Errorf("inspect staged numstat: %w", err)
	}
	status.StagedAdditions, status.StagedDeletions, err = parseNumstat(staged)
	if err != nil {
		return GitStatus{}, fmt.Errorf("parse staged numstat: %w", err)
	}
	return status, nil
}

func parseNumstat(raw []byte) (uint64, uint64, error) {
	var additions, deletions uint64
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[0] == "-" && fields[1] == "-" { // Git's binary-file marker
			continue
		}
		added, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid additions %q", fields[0])
		}
		deleted, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid deletions %q", fields[1])
		}
		additions += added
		deletions += deleted
	}
	return additions, deletions, nil
}

func (s *Service) GitDiff(ctx context.Context, staged bool, relative string) (Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return Diff{}, err
	}
	baseArgs := []string{"diff", "--no-ext-diff", "--no-color"}
	if staged {
		baseArgs = append(baseArgs, "--cached")
	} else {
		indexTree, err := s.git(ctx, nil, "write-tree")
		if err != nil {
			return Diff{}, err
		}
		worktreeCommit, err := s.captureWorktreeCommitLocked(ctx, "Waku diff snapshot")
		if err != nil {
			return Diff{}, err
		}
		baseArgs = append(baseArgs, strings.TrimSpace(string(indexTree)), worktreeCommit)
	}
	baseArgs = append(baseArgs, "--")
	if relative != "" {
		if _, err := s.safeGitPath(relative); err != nil {
			return Diff{}, err
		}
		baseArgs = append(baseArgs, filepath.ToSlash(relative))
	}
	fullArgs := append([]string{"diff", "--no-ext-diff", "--no-color", "--unified=2147483647"}, baseArgs[3:]...)
	output, truncated, err := s.gitBounded(ctx, nil, s.limits.MaxGitOutput, fullArgs...)
	if err != nil {
		return Diff{}, err
	}
	if truncated {
		fallbackArgs := append([]string{"diff", "--no-ext-diff", "--no-color", "--unified=3"}, baseArgs[3:]...)
		output, _, err = s.gitBounded(ctx, nil, s.limits.MaxGitOutput, fallbackArgs...)
		if err != nil {
			return Diff{}, err
		}
		truncated = true
	}
	numstatArgs := append([]string{"diff", "--no-ext-diff", "--no-color", "--numstat"}, baseArgs[3:]...)
	numstat, err := s.git(ctx, nil, numstatArgs...)
	return Diff{Patch: string(output), Numstat: string(numstat), Truncated: truncated}, err
}

func (s *Service) Stage(ctx context.Context, paths ...string) error {
	return s.mutateIndex(ctx, true, paths)
}

func (s *Service) Unstage(ctx context.Context, paths ...string) error {
	return s.mutateIndex(ctx, false, paths)
}

func (s *Service) mutateIndex(ctx context.Context, stage bool, paths []string) error {
	if len(paths) == 0 || len(paths) > s.limits.MaxEntries {
		return errors.New("Git path list must be non-empty and bounded")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return err
	}
	args := []string{"add", "-A", "--"}
	if !stage {
		args = []string{"reset", "--quiet", "HEAD", "--"}
	}
	for _, relative := range paths {
		path, err := s.safeGitPath(relative)
		if err != nil {
			return err
		}
		args = append(args, filepath.ToSlash(path))
	}
	_, err := s.git(ctx, nil, args...)
	return err
}

func (s *Service) Commit(ctx context.Context, message string, includeUnstaged bool) (string, error) {
	message = strings.TrimSpace(message)
	if message == "" || strings.ContainsAny(message, "\r\n") || len(message) > s.limits.MaxCommitBytes {
		return "", errors.New("commit message must be one bounded non-empty line")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return "", err
	}
	if includeUnstaged {
		if _, err := s.git(ctx, nil, "add", "-A", "--", "."); err != nil {
			return "", err
		}
	}
	quiet := exec.CommandContext(ctx, "git", "diff", "--cached", "--quiet", "--")
	quiet.Dir = s.root.canonical
	if err := quiet.Run(); err == nil {
		return "", errors.New("there are no staged changes to commit")
	} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		return "", fmt.Errorf("inspect staged changes: %w", err)
	}
	if _, err := s.git(ctx, gitIdentityEnv(), "commit", "-m", message); err != nil {
		return "", err
	}
	commit, err := s.git(ctx, nil, "rev-parse", "HEAD")
	return strings.TrimSpace(string(commit)), err
}

func (s *Service) safeGitPath(relative string) (string, error) {
	if err := validRelative(relative); err != nil {
		return "", err
	}
	if _, err := s.root.ResolveExisting(relative); err == nil {
		return relative, nil
	}
	// Deleted paths no longer resolve, but their existing parent must remain
	// contained. ResolveCreate also rejects a symlink leaf.
	if _, err := s.root.ResolveCreate(relative); err != nil {
		return "", err
	}
	return relative, nil
}

func (s *Service) ensureRepository(ctx context.Context) error {
	output, err := s.git(ctx, nil, "rev-parse", "--is-inside-work-tree")
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || strings.TrimSpace(string(output)) != "true" {
		return errors.New("workspace is not a Git repository")
	}
	return nil
}

type cappedBuffer struct {
	data      bytes.Buffer
	remaining int64
	truncated bool
}

func (b *cappedBuffer) Write(value []byte) (int, error) {
	original := len(value)
	if int64(len(value)) > b.remaining {
		value = value[:max(0, int(b.remaining))]
		b.truncated = true
	}
	_, _ = b.data.Write(value)
	b.remaining -= int64(len(value))
	return original, nil // continue draining the child without retaining bytes
}

func (s *Service) git(ctx context.Context, extraEnv []string, args ...string) ([]byte, error) {
	output, truncated, err := s.gitBounded(ctx, extraEnv, s.limits.MaxGitOutput, args...)
	if truncated && err == nil {
		return nil, ErrLimitExceeded
	}
	return output, err
}

func (s *Service) gitBounded(ctx context.Context, extraEnv []string, limit int64, args ...string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.beforeGit != nil {
		if err := s.beforeGit(append([]string(nil), args...)); err != nil {
			return nil, false, err
		}
	}
	command := exec.CommandContext(ctx, "git", args...)
	command.Dir = s.root.canonical
	command.Env = append(command.Environ(), append([]string{"GIT_TERMINAL_PROMPT=0", "GIT_PAGER=cat", "LC_ALL=C"}, extraEnv...)...)
	command.WaitDelay = time.Second
	stdout := cappedBuffer{remaining: limit}
	stderr := cappedBuffer{remaining: min(limit, 256<<10)}
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if err != nil {
		if ctx.Err() != nil {
			return nil, stdout.truncated, ctx.Err()
		}
		message := strings.TrimSpace(stderr.data.String())
		if message == "" {
			message = err.Error()
		}
		return nil, stdout.truncated, fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
	}
	return stdout.data.Bytes(), stdout.truncated, nil
}

func gitIdentityEnv() []string {
	return []string{
		"GIT_AUTHOR_NAME=Waku", "GIT_AUTHOR_EMAIL=waku@localhost",
		"GIT_COMMITTER_NAME=Waku", "GIT_COMMITTER_EMAIL=waku@localhost",
	}
}

var _ io.Writer = (*cappedBuffer)(nil)
