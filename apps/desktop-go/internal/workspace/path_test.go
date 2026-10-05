package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHostPathRequiresAbsolutePath(t *testing.T) {
	if _, err := NewHostPath("relative"); err == nil {
		t.Fatal("accepted client-relative path")
	}
}

func TestRootResolvesOnlyDaemonOwnedPaths(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "dir", "file"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	host, _ := NewHostPath(rootPath)
	root, err := OpenRoot(host)
	if err != nil {
		t.Fatal(err)
	}
	got, err := root.ResolveExisting(filepath.Join("dir", "file"))
	if err != nil || got.String() != filepath.Join(rootPath, "dir", "file") {
		t.Fatalf("resolve = %q, %v", got.String(), err)
	}
	for _, path := range []string{"", "../outside", filepath.Join(rootPath, "dir", "file")} {
		if _, err := root.ResolveExisting(path); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
}

func TestRootRejectsEscapingSymlink(t *testing.T) {
	rootPath, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("no"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "escape")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	host, _ := NewHostPath(rootPath)
	root, _ := OpenRoot(host)
	if _, err := root.ResolveExisting(filepath.Join("escape", "secret")); err == nil {
		t.Fatal("followed symlink outside daemon root")
	}
	if _, err := root.ResolveCreate(filepath.Join("escape", "new")); err == nil {
		t.Fatal("created through symlink outside daemon root")
	}
}

func TestCreateRejectsLeafSymlinkAndExposesRaceCapability(t *testing.T) {
	rootPath, outside := t.TempDir(), t.TempDir()
	target := filepath.Join(outside, "target")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(rootPath, "leaf")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	host, _ := NewHostPath(rootPath)
	root, _ := OpenRoot(host)
	if _, err := root.ResolveCreate("leaf"); err == nil {
		t.Fatal("accepted leaf symlink")
	}
	if root.RaceSafeMutations() {
		t.Fatal("portable path implementation claimed dirfd-level safety")
	}
	if _, _, err := root.CreateFileExclusive("new", 0o600, true); !errors.Is(err, ErrRaceSafeMutationUnsupported) {
		t.Fatalf("race-safe create = %v", err)
	}
	file, path, err := root.CreateFileExclusive("new", 0o600, false)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if path.String() != filepath.Join(rootPath, "new") {
		t.Fatalf("created %q", path.String())
	}
	if _, _, err := root.CreateFileExclusive("new", 0o600, false); err == nil {
		t.Fatal("exclusive create overwrote leaf")
	}
}
