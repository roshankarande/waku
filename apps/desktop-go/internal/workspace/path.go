// Package workspace owns paths interpreted on the daemon host.
package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrRaceSafeMutationUnsupported = errors.New("race-safe workspace mutation requires platform dirfd/reparse-point support")

// HostPath is explicitly a path on the daemon host. Clients may display its
// String value but must not resolve it on the client machine.
type HostPath struct{ absolute string }

func NewHostPath(path string) (HostPath, error) {
	if path == "" || !filepath.IsAbs(path) {
		return HostPath{}, errors.New("daemon-host path must be absolute")
	}
	return HostPath{absolute: filepath.Clean(path)}, nil
}

func (p HostPath) String() string { return p.absolute }

type Root struct {
	host      HostPath
	canonical string
}

func OpenRoot(host HostPath) (Root, error) {
	info, err := os.Stat(host.absolute)
	if err != nil {
		return Root{}, fmt.Errorf("inspect daemon workspace: %w", err)
	}
	if !info.IsDir() {
		return Root{}, errors.New("daemon workspace root is not a directory")
	}
	canonical, err := filepath.EvalSymlinks(host.absolute)
	if err != nil {
		return Root{}, fmt.Errorf("resolve daemon workspace: %w", err)
	}
	return Root{host: host, canonical: filepath.Clean(canonical)}, nil
}

func (r Root) HostPath() HostPath { return r.host }

// ResolveExisting rejects absolute paths, traversal, and symlinks escaping the
// daemon-owned root.
func (r Root) ResolveExisting(relative string) (HostPath, error) {
	if err := validRelative(relative); err != nil {
		return HostPath{}, err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(r.canonical, relative))
	if err != nil {
		return HostPath{}, fmt.Errorf("resolve workspace path: %w", err)
	}
	resolved = filepath.Clean(resolved)
	if !within(r.canonical, resolved) {
		return HostPath{}, errors.New("workspace path escapes daemon root")
	}
	return HostPath{absolute: resolved}, nil
}

// ResolveCreate permits a missing leaf while verifying the existing parent,
// including symlinks, remains under the root.
func (r Root) ResolveCreate(relative string) (HostPath, error) {
	if err := validRelative(relative); err != nil {
		return HostPath{}, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Join(r.canonical, filepath.Dir(relative)))
	if err != nil {
		return HostPath{}, fmt.Errorf("resolve workspace parent: %w", err)
	}
	if !within(r.canonical, parent) {
		return HostPath{}, errors.New("workspace parent escapes daemon root")
	}
	leaf := filepath.Join(parent, filepath.Base(relative))
	if info, err := os.Lstat(leaf); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return HostPath{}, errors.New("workspace leaf is a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return HostPath{}, err
	}
	return HostPath{absolute: leaf}, nil
}

// RaceSafeMutations reports whether this portable implementation can anchor
// every path component with dirfd/openat or Windows reparse-point handles.
// It is false: callers needing adversarial concurrent-mutation safety must
// retain an honest capability gate until platform implementations land.
func (r Root) RaceSafeMutations() bool { return false }

// CreateFileExclusive is the strongest portable creation seam: it rejects
// known symlink escapes and requires a nonexistent leaf. A hostile process can
// still replace an ancestor between validation and open, so requireRaceSafe
// returns an explicit unsupported error instead of claiming that guarantee.
func (r Root) CreateFileExclusive(relative string, perm os.FileMode, requireRaceSafe bool) (*os.File, HostPath, error) {
	if requireRaceSafe {
		return nil, HostPath{}, ErrRaceSafeMutationUnsupported
	}
	path, err := r.ResolveCreate(relative)
	if err != nil {
		return nil, HostPath{}, err
	}
	file, err := os.OpenFile(path.absolute, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return nil, HostPath{}, fmt.Errorf("create workspace file exclusively: %w", err)
	}
	return file, path, nil
}

func validRelative(path string) error {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("workspace path must be a clean, non-empty relative path")
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("workspace path contains traversal")
		}
	}
	return nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
