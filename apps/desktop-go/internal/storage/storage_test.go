package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriterLockRejectsSecondOwnerAndReleases(t *testing.T) {
	directory := t.TempDir()
	first, err := AcquireWriter(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWriter(directory); !errors.Is(err, ErrDataDirOwned) {
		t.Fatalf("second acquire = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireWriter(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLockWillNotRemoveChangedOwnership(t *testing.T) {
	directory := t.TempDir()
	lock, err := AcquireWriter(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, lockFilename)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, lockFilename), []byte(`{"token":"someone-else"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(); err == nil {
		t.Fatal("released another owner's lock")
	}
}

func TestWriterReleaseIsConcurrentAndIdempotent(t *testing.T) {
	lock, err := AcquireWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	errors := make(chan error, 8)
	var group sync.WaitGroup
	for i := 0; i < cap(errors); i++ {
		group.Add(1)
		go func() { defer group.Done(); errors <- lock.Release() }()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLegacyDirectoryRequiresVerifiedCutover(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	legacy := filepath.Join(home, ".waku")
	if _, err := AcquireWriter(legacy); !errors.Is(err, ErrSharedDataRequiresCutover) {
		t.Fatalf("legacy acquire = %v", err)
	}
	if guard, err := AcquireCutoverLaunchGuard(legacy); guard != nil || !errors.Is(err, ErrSharedDataRequiresCutover) {
		t.Fatalf("unsafe cutover guard = %#v, %v", guard, err)
	}
	if _, err := AcquireWriterForCutover(legacy, nil); !errors.Is(err, ErrSharedDataRequiresCutover) {
		t.Fatalf("unsafe cutover writer = %v", err)
	}
	if _, err := os.Stat(legacy); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused cutover touched legacy directory: %v", err)
	}
}

func TestLegacySymlinkCannotBypassCutoverGuard(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	legacy := filepath.Join(home, ".waku")
	if err := os.Mkdir(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(home, "alias")
	if err := os.Symlink(legacy, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := AcquireWriter(alias); !errors.Is(err, ErrSharedDataRequiresCutover) {
		t.Fatalf("symlink bypass = %v", err)
	}
	if guard, err := AcquireCutoverLaunchGuard(alias); guard != nil || !errors.Is(err, ErrSharedDataRequiresCutover) {
		t.Fatalf("symlink cutover guard = %#v, %v", guard, err)
	}
}

func TestPublicCutoverAPIsAlwaysRefuse(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	legacy := filepath.Join(home, ".waku")
	for name, call := range map[string]func() error{
		"writer": func() error { _, err := AcquireWriterForCutover(legacy, nil); return err },
		"stale":  func() error { return RemoveStaleLockForCutover(legacy, nil) },
	} {
		if err := call(); !errors.Is(err, ErrSharedDataRequiresCutover) {
			t.Errorf("%s cutover API = %v", name, err)
		}
	}
}

func TestDevelopmentDefaultIsNotLegacyDataDirectory(t *testing.T) {
	t.Setenv("WAKU_GO_DATA_DIR", "")
	directory, err := DevelopmentDataDir()
	if err != nil {
		t.Fatal(err)
	}
	home, _ := os.UserHomeDir()
	legacy, _ := filepath.Abs(filepath.Join(home, ".waku"))
	got, _ := filepath.Abs(directory)
	if got == legacy {
		t.Fatal("Go development default aliases legacy production data")
	}
	if !strings.Contains(got, "go-development") {
		t.Fatalf("unexpected development directory %q", got)
	}
}

type fakeMigrationStore struct {
	journal bool
	applied []string
	fail    string
}

func (f *fakeMigrationStore) EnsureJournal(context.Context) error { f.journal = true; return nil }
func (f *fakeMigrationStore) HasMigration(_ context.Context, tag string) (bool, error) {
	for _, current := range f.applied {
		if current == tag {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeMigrationStore) RunMigration(_ context.Context, m Migration, _ int64) error {
	if m.Tag == f.fail {
		return errors.New("synthetic transaction failure")
	}
	f.applied = append(f.applied, m.Tag)
	return nil
}

func TestMigrationsUseExistingTagsAndAreIdempotent(t *testing.T) {
	f := &fakeMigrationStore{}
	now := time.Unix(1, 0)
	count, err := ApplyMigrations(context.Background(), f, now)
	if err != nil {
		t.Fatal(err)
	}
	if !f.journal || count != 3 {
		t.Fatalf("journal=%v count=%d", f.journal, count)
	}
	if strings.Join(f.applied, ",") != "0000_init,0001_clear_sugar_man,0002_cute_miracleman" {
		t.Fatalf("tags = %v", f.applied)
	}
	count, err = ApplyMigrations(context.Background(), f, now)
	if err != nil || count != 0 {
		t.Fatalf("second apply = %d, %v", count, err)
	}
}

func TestMigrationFailureStopsAndCanResume(t *testing.T) {
	f := &fakeMigrationStore{fail: "0001_clear_sugar_man"}
	count, err := ApplyMigrations(context.Background(), f, time.Unix(1, 0))
	if err == nil || count != 1 || len(f.applied) != 1 {
		t.Fatalf("first run = %d, %v, %v", count, err, f.applied)
	}
	f.fail = ""
	count, err = ApplyMigrations(context.Background(), f, time.Unix(2, 0))
	if err != nil || count != 2 {
		t.Fatalf("resume = %d, %v", count, err)
	}
}
