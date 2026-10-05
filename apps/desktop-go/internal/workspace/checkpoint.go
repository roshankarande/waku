package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var sessionIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var checkpointRefPattern = regexp.MustCompile(`^refs/waku/session-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}-turn-[0-9]+$`)

type Checkpoint struct {
	Ref    string
	Commit string
}

func (s *Service) HasCheckpoint(ctx context.Context, ref string) bool {
	if !checkpointRefPattern.MatchString(ref) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.git(ctx, nil, "rev-parse", "--verify", ref+"^{commit}")
	return err == nil
}

func CheckpointRef(sessionID string, turn uint64) (string, error) {
	if !sessionIDPattern.MatchString(strings.ToLower(sessionID)) {
		return "", errors.New("checkpoint session ID must be a canonical UUID")
	}
	return "refs/waku/session-" + strings.ToLower(sessionID) + "-turn-" + strconv.FormatUint(turn, 10), nil
}

// CaptureCheckpoint snapshots tracked, staged, unstaged, and untracked files
// through an isolated temporary index. It does not mutate the user's index.
func (s *Service) CaptureCheckpoint(ctx context.Context, sessionID string, turn uint64) (Checkpoint, error) {
	ref, err := CheckpointRef(sessionID, turn)
	if err != nil {
		return Checkpoint{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return Checkpoint{}, err
	}
	commit, err := s.captureWorktreeCommitLocked(ctx, "Waku worktree snapshot")
	if err != nil {
		return Checkpoint{}, err
	}
	if _, err := s.git(ctx, nil, "update-ref", ref, commit); err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{Ref: ref, Commit: commit}, nil
}

func (s *Service) captureWorktreeCommitLocked(ctx context.Context, message string) (string, error) {
	commonRaw, err := s.git(ctx, nil, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	common := strings.TrimSpace(string(commonRaw))
	if common == "" {
		return "", errors.New("git returned no common directory")
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(s.root.canonical, common)
	}
	index, err := os.CreateTemp(common, "waku-checkpoint-index-*")
	if err != nil {
		return "", err
	}
	indexPath := index.Name()
	if err := index.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(indexPath); err != nil { // read-tree expects a missing or valid index
		return "", err
	}
	defer os.Remove(indexPath)
	defer os.Remove(indexPath + ".lock")
	environment := append(gitIdentityEnv(), "GIT_INDEX_FILE="+indexPath)
	head, headErr := s.git(ctx, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if headErr == nil {
		if _, err := s.git(ctx, environment, "read-tree", strings.TrimSpace(string(head))); err != nil {
			return "", err
		}
	}
	if _, err := s.git(ctx, environment, "add", "-A", "--", "."); err != nil {
		return "", err
	}
	tree, err := s.git(ctx, environment, "write-tree")
	if err != nil {
		return "", err
	}
	commitArgs := []string{"commit-tree", strings.TrimSpace(string(tree)), "-m", message}
	if headErr == nil {
		commitArgs = append(commitArgs, "-p", strings.TrimSpace(string(head)))
	}
	commitRaw, err := s.git(ctx, environment, commitArgs...)
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(commitRaw))
	if commit == "" {
		return "", errors.New("git commit-tree returned no object ID")
	}
	return commit, nil
}

func (s *Service) RestoreCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if !checkpointRefPattern.MatchString(checkpoint.Ref) {
		return errors.New("invalid Waku checkpoint ref")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureRepository(ctx); err != nil {
		return err
	}
	commitRaw, err := s.git(ctx, nil, "rev-parse", "--verify", checkpoint.Ref+"^{commit}")
	if err != nil {
		return fmt.Errorf("checkpoint is unavailable: %w", err)
	}
	commit := strings.TrimSpace(string(commitRaw))
	if checkpoint.Commit != "" && checkpoint.Commit != commit {
		return errors.New("checkpoint ref no longer identifies the expected commit")
	}
	safetyCommit, err := s.captureWorktreeCommitLocked(ctx, "Waku pre-restore safety snapshot")
	if err != nil {
		return fmt.Errorf("capture restore safety snapshot: %w", err)
	}
	indexTree, err := s.git(ctx, nil, "write-tree")
	if err != nil {
		return fmt.Errorf("capture restore index: %w", err)
	}
	if err := s.applyRestoreLocked(ctx, commit, true); err == nil {
		return nil
	} else {
		cause := err
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rollbackErr := s.applyRestoreLocked(rollbackCtx, safetyCommit, false)
		if rollbackErr == nil {
			_, rollbackErr = s.git(rollbackCtx, nil, "read-tree", strings.TrimSpace(string(indexTree)))
		}
		if rollbackErr != nil {
			return errors.Join(cause, fmt.Errorf("restore rollback failed: %w", rollbackErr))
		}
		return cause
	}
}

func (s *Service) applyRestoreLocked(ctx context.Context, commit string, useHook bool) error {
	steps := [][]string{
		{"restore", "--source", commit, "--worktree", "--staged", "--", "."},
		{"clean", "-fd", "--", "."},
	}
	for index, args := range steps {
		if useHook && s.beforeRestoreStep != nil {
			if err := s.beforeRestoreStep(index); err != nil {
				return err
			}
		}
		if _, err := s.git(ctx, nil, args...); err != nil {
			return err
		}
	}
	if useHook && s.beforeRestoreStep != nil {
		if err := s.beforeRestoreStep(2); err != nil {
			return err
		}
	}
	if _, err := s.git(ctx, nil, "rev-parse", "--verify", "HEAD^{commit}"); err == nil {
		_, err = s.git(ctx, nil, "reset", "--quiet", "--", ".")
		return err
	}
	return nil
}
