package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func testService(t *testing.T, directory string, limits Limits) *Service {
	t.Helper()
	host, err := NewHostPath(directory)
	if err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(host)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func testGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(command.Environ(), gitIdentityEnv()...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func testRepository(t *testing.T) (string, *Service) {
	t.Helper()
	directory := t.TempDir()
	testGit(t, directory, "init", "-q")
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("base\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	testGit(t, directory, "add", "tracked.txt")
	testGit(t, directory, "commit", "-q", "-m", "initial")
	return directory, testService(t, directory, Limits{})
}

func hashText(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func TestBrowseReadAndCASWriteRemainHostOwnedAndBounded(t *testing.T) {
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "dir", "note.txt"), []byte("one\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	service := testService(t, directory, Limits{MaxEntries: 4, MaxFileBytes: 16})
	entries, err := service.Browse(context.Background(), "dir")
	if err != nil || len(entries) != 1 || entries[0].HostPath.String() != filepath.Join(directory, "dir", "note.txt") {
		t.Fatalf("browse=%+v err=%v", entries, err)
	}
	content, hash, err := service.ReadText(context.Background(), filepath.Join("dir", "note.txt"))
	if err != nil || content != "one\n" || hash != hashText(content) {
		t.Fatalf("read=%q hash=%q err=%v", content, hash, err)
	}
	if _, err := service.WriteTextCAS(context.Background(), filepath.Join("dir", "note.txt"), hashText("stale"), "two\n"); !errors.Is(err, ErrContentChanged) {
		t.Fatalf("stale CAS=%v", err)
	}
	newHash, err := service.WriteTextCAS(context.Background(), filepath.Join("dir", "note.txt"), hash, "two\n")
	if err != nil || newHash != hashText("two\n") {
		t.Fatalf("write hash=%q err=%v", newHash, err)
	}
	data, _ := os.ReadFile(filepath.Join(directory, "dir", "note.txt"))
	if string(data) != "two\n" {
		t.Fatalf("atomic content=%q", data)
	}
	if _, err := service.WriteTextCAS(context.Background(), "large", hashText(""), strings.Repeat("x", 17)); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("large write=%v", err)
	}
}

func TestServiceRejectsEscapingSymlinksForReadBrowseAndWrite(t *testing.T) {
	directory, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(directory, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	service := testService(t, directory, Limits{})
	if _, _, err := service.ReadText(context.Background(), filepath.Join("escape", "secret")); err == nil {
		t.Fatal("read followed escaping symlink")
	}
	if _, err := service.Browse(context.Background(), "escape"); err == nil {
		t.Fatal("browse followed escaping symlink")
	}
	if _, err := service.WriteTextCAS(context.Background(), filepath.Join("escape", "new"), hashText(""), "bad"); err == nil {
		t.Fatal("write followed escaping symlink")
	}
}

func TestCASSerializesAcrossServicesForSameHostRoot(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "shared.txt"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := testService(t, directory, Limits{})
	second := testService(t, directory, Limits{})
	start := make(chan struct{})
	results := make(chan error, 2)
	for index, service := range []*Service{first, second} {
		go func(index int, service *Service) {
			<-start
			_, err := service.WriteTextCAS(context.Background(), "shared.txt", hashText("base"), string(rune('a'+index)))
			results <- err
		}(index, service)
	}
	close(start)
	var succeeded, changed int
	for range 2 {
		switch err := <-results; {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrContentChanged):
			changed++
		default:
			t.Fatalf("CAS result=%v", err)
		}
	}
	if succeeded != 1 || changed != 1 {
		t.Fatalf("CAS successes=%d changed=%d", succeeded, changed)
	}
}

func TestGitStatusDiffStageUnstageAndCommit(t *testing.T) {
	directory, service := testRepository(t)
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("base\nchanged\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "new.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	status, err := service.GitStatus(context.Background())
	if err != nil || status.HasStaged || !status.HasUnstaged || len(status.Entries) != 2 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	diff, err := service.GitDiff(context.Background(), false, "tracked.txt")
	if err != nil || !strings.Contains(diff.Patch, "+changed") || diff.Truncated {
		t.Fatalf("diff=%+v err=%v", diff, err)
	}
	allDiff, err := service.GitDiff(context.Background(), false, "")
	if err != nil || !strings.Contains(allDiff.Patch, "new.txt") || !strings.Contains(allDiff.Patch, "+new") {
		t.Fatalf("unstaged diff omitted untracked file: %+v err=%v", allDiff, err)
	}
	if err := service.Stage(context.Background(), "tracked.txt", "new.txt"); err != nil {
		t.Fatal(err)
	}
	status, _ = service.GitStatus(context.Background())
	if !status.HasStaged || status.HasUnstaged {
		t.Fatalf("staged status=%+v", status)
	}
	if err := service.Unstage(context.Background(), "new.txt"); err != nil {
		t.Fatal(err)
	}
	status, _ = service.GitStatus(context.Background())
	if !status.HasStaged || !status.HasUnstaged {
		t.Fatalf("partially unstaged status=%+v", status)
	}
	commit, err := service.Commit(context.Background(), "Update tracked file", false)
	if err != nil || len(commit) < 40 {
		t.Fatalf("commit=%q err=%v", commit, err)
	}
	if got := testGit(t, directory, "show", "-s", "--format=%s", "HEAD"); got != "Update tracked file" {
		t.Fatalf("subject=%q", got)
	}
}

func TestGitOutputAndCancellationAreBounded(t *testing.T) {
	directory, _ := testRepository(t)
	service := testService(t, directory, Limits{MaxGitOutput: 64})
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("base\n"+strings.Repeat("long line\n", 100)), 0o640); err != nil {
		t.Fatal(err)
	}
	diff, err := service.GitDiff(context.Background(), false, "tracked.txt")
	if err != nil || !diff.Truncated || len(diff.Patch) != 64 {
		t.Fatalf("bounded diff bytes=%d truncated=%v err=%v", len(diff.Patch), diff.Truncated, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GitStatus(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled status=%v", err)
	}
}

func TestGitDiffUsesFullContextUntilBoundThenFallsBackHonestly(t *testing.T) {
	directory, service := testRepository(t)
	var original strings.Builder
	for index := 0; index < 400; index++ {
		fmt.Fprintf(&original, "line-%03d\n", index)
	}
	if err := os.WriteFile(filepath.Join(directory, "context.txt"), []byte(original.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	testGit(t, directory, "add", "context.txt")
	testGit(t, directory, "commit", "-q", "-m", "context")
	changed := strings.Replace(original.String(), "line-200\n", "line-200 changed\n", 1)
	if err := os.WriteFile(filepath.Join(directory, "context.txt"), []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	diff, err := service.GitDiff(context.Background(), false, "context.txt")
	if err != nil || diff.Truncated || !strings.Contains(diff.Patch, " line-000") || !strings.Contains(diff.Patch, " line-399") {
		t.Fatalf("full-context diff truncated=%v err=%v first=%v last=%v", diff.Truncated, err, strings.Contains(diff.Patch, " line-000"), strings.Contains(diff.Patch, " line-399"))
	}
	bounded := testService(t, directory, Limits{MaxGitOutput: 512})
	fallback, err := bounded.GitDiff(context.Background(), false, "context.txt")
	if err != nil || !fallback.Truncated || len(fallback.Patch) > 512 || strings.Contains(fallback.Patch, " line-000") {
		t.Fatalf("fallback bytes=%d truncated=%v err=%v", len(fallback.Patch), fallback.Truncated, err)
	}
}

func TestGitStatusFailsWhenNumstatFails(t *testing.T) {
	_, service := testRepository(t)
	service.beforeGit = func(args []string) error {
		for _, arg := range args {
			if arg == "--numstat" {
				return errors.New("synthetic numstat failure")
			}
		}
		return nil
	}
	if _, err := service.GitStatus(context.Background()); err == nil || !strings.Contains(err.Error(), "numstat") {
		t.Fatalf("numstat failure=%v", err)
	}
	if _, _, err := parseNumstat([]byte("not-a-number\t1\tfile\n")); err == nil {
		t.Fatal("malformed numstat was accepted as zero")
	}
}

func TestCheckpointCapturesWithoutMutatingIndexAndRestoresExactTree(t *testing.T) {
	directory, service := testRepository(t)
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("staged\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	testGit(t, directory, "add", "tracked.txt")
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("unstaged\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "untracked.txt"), []byte("untracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	indexBefore := testGit(t, directory, "write-tree")
	checkpoint, err := service.CaptureCheckpoint(context.Background(), "00000000-0000-4000-8000-000000000001", 7)
	if err != nil || checkpoint.Ref != "refs/waku/session-00000000-0000-4000-8000-000000000001-turn-7" || len(checkpoint.Commit) < 40 {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	if indexAfter := testGit(t, directory, "write-tree"); indexAfter != indexBefore {
		t.Fatalf("checkpoint mutated user index: %s != %s", indexAfter, indexBefore)
	}
	if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("later\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, "untracked.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "later.txt"), []byte("remove me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := service.RestoreCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	tracked, _ := os.ReadFile(filepath.Join(directory, "tracked.txt"))
	untracked, _ := os.ReadFile(filepath.Join(directory, "untracked.txt"))
	if string(tracked) != "unstaged\n" || string(untracked) != "untracked\n" {
		t.Fatalf("restored tracked=%q untracked=%q", tracked, untracked)
	}
	if _, err := os.Stat(filepath.Join(directory, "later.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("later file survived restore: %v", err)
	}
}

func TestCheckpointRestoreFailureOrCancellationRollsBackSafetySnapshot(t *testing.T) {
	for _, cancellation := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancellation), func(t *testing.T) {
			directory, service := testRepository(t)
			checkpoint, err := service.CaptureCheckpoint(context.Background(), "00000000-0000-4000-8000-000000000001", 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "tracked.txt"), []byte("safety\n"), 0o640); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "keep.txt"), []byte("keep\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			testGit(t, directory, "add", "tracked.txt")
			indexBefore := testGit(t, directory, "write-tree")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service.beforeRestoreStep = func(step int) error {
				if step != 1 {
					return nil
				}
				if cancellation {
					cancel()
					return ctx.Err()
				}
				return errors.New("synthetic restore failure")
			}
			if err := service.RestoreCheckpoint(ctx, checkpoint); err == nil {
				t.Fatal("restore unexpectedly succeeded")
			}
			tracked, _ := os.ReadFile(filepath.Join(directory, "tracked.txt"))
			keep, _ := os.ReadFile(filepath.Join(directory, "keep.txt"))
			if string(tracked) != "safety\n" || string(keep) != "keep\n" {
				t.Fatalf("rollback tracked=%q keep=%q", tracked, keep)
			}
			if indexAfter := testGit(t, directory, "write-tree"); indexAfter != indexBefore {
				t.Fatalf("rollback index=%s want=%s", indexAfter, indexBefore)
			}
		})
	}
}
