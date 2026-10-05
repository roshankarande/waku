// Package storage owns writable Waku data and migration coordination.
package storage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	lockFilename        = ".waku-go-writer.lock"
	launchGuardFilename = ".waku-daemon-launch.lock"
)

var (
	ErrDataDirOwned              = errors.New("Waku data directory already has a writer")
	ErrSharedDataRequiresCutover = errors.New("legacy production data requires an explicit verified cutover")
)

type Lock struct {
	path, token string
	file        *os.File
	mu          sync.Mutex
	released    bool
	releaseErr  error
	launchGuard *LaunchGuard
}

// LaunchGuard is a durable supervisor lease. The supervisor must acquire it
// after stopping the legacy daemon and require every subsequent Rust or Go
// launch to acquire the same guard. A Go writer retains it until Release.
type LaunchGuard struct {
	directory, path string
	file            *os.File
	mu              sync.Mutex
	released        bool
	releaseErr      error
	writers         int
}
type ownerRecord struct {
	PID     int    `json:"pid"`
	Host    string `json:"host"`
	Started string `json:"started"`
	Token   string `json:"token"`
}

// DevelopmentDataDir intentionally differs from the legacy ~/.waku default.
// Sharing production data requires an explicit path and coordinated cutover.
func DevelopmentDataDir() (string, error) {
	if path := os.Getenv("WAKU_GO_DATA_DIR"); path != "" {
		return filepath.Abs(path)
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate user config directory: %w", err)
	}
	return filepath.Join(root, "Waku", "go-development"), nil
}

// AcquireWriter atomically claims directory for this process lifetime. It
// never removes an existing lock automatically: after a crash, recovery must
// verify that neither legacy nor Go daemon is running before RemoveStaleLock.
func AcquireWriter(directory string) (*Lock, error) {
	return acquireWriter(directory, nil)
}

// AcquireCutoverLaunchGuard is reserved for a future externally verifiable
// supervisor protocol. The Go-only guard could not prove that the legacy
// daemon stopped, so the public seam fails closed without filesystem changes.
func AcquireCutoverLaunchGuard(directory string) (*LaunchGuard, error) {
	return nil, ErrSharedDataRequiresCutover
}

func AcquireWriterForCutover(directory string, guard *LaunchGuard) (*Lock, error) {
	return nil, ErrSharedDataRequiresCutover
}

func acquireWriter(directory string, guard *LaunchGuard) (*Lock, error) {
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	legacy, legacyErr := legacyDataDir()
	if legacyErr != nil {
		return nil, legacyErr
	}
	if pathsSame(abs, legacy) {
		if guard == nil || !guard.activeFor(abs) {
			return nil, ErrSharedDataRequiresCutover
		}
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return nil, fmt.Errorf("make lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes[:])
	host, _ := os.Hostname()
	record := ownerRecord{PID: os.Getpid(), Host: host, Started: time.Now().UTC().Format(time.RFC3339Nano), Token: token}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(abs, lockFilename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("%w: %s", ErrDataDirOwned, path)
	}
	if err != nil {
		return nil, fmt.Errorf("claim data directory: %w", err)
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("write ownership record: %w", err)
	}
	if err := syncDirectory(abs); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return &Lock{path: path, token: token, file: file}, nil
}

func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return l.releaseErr
	}
	l.released = true
	// SameFile ties removal to the descriptor retained since O_EXCL creation.
	// Portable os APIs cannot make the final identity-check/unlink indivisible;
	// platform unlinkat/handle implementations remain needed against a hostile
	// process racing this tiny interval.
	pathInfo, err := os.Lstat(l.path)
	fileInfo, fileErr := l.file.Stat()
	if err != nil || fileErr != nil || !os.SameFile(pathInfo, fileInfo) {
		l.releaseErr = errors.New("data directory ownership file changed; refusing to remove lock")
		_ = l.file.Close()
		return l.releaseErr
	}
	if err := os.Remove(l.path); err != nil {
		l.releaseErr = fmt.Errorf("release data directory: %w", err)
	}
	if closeErr := l.file.Close(); l.releaseErr == nil && closeErr != nil {
		l.releaseErr = closeErr
	}
	if l.releaseErr == nil {
		l.releaseErr = syncDirectory(filepath.Dir(l.path))
	}
	if l.launchGuard != nil {
		if err := l.launchGuard.releaseWriter(); l.releaseErr == nil {
			l.releaseErr = err
		}
	}
	return l.releaseErr
}

func (l *Lock) activeFor(directory string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || !pathsSame(filepath.Dir(l.path), directory) {
		return false
	}
	pathInfo, err := os.Lstat(l.path)
	fileInfo, fileErr := l.file.Stat()
	return err == nil && fileErr == nil && os.SameFile(pathInfo, fileInfo)
}

// RemoveStaleLock is deliberately separate from acquisition. Callers must
// obtain operator/supervisor proof that no Rust or Go writer is alive first.
func RemoveStaleLock(directory string) error {
	abs, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	legacy, err := legacyDataDir()
	if err != nil {
		return err
	}
	if pathsSame(abs, legacy) {
		return ErrSharedDataRequiresCutover
	}
	return removeStaleLock(abs)
}

func RemoveStaleLockForCutover(directory string, guard *LaunchGuard) error {
	return ErrSharedDataRequiresCutover
}

func removeStaleLock(directory string) error {
	if err := os.Remove(filepath.Join(directory, lockFilename)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func legacyDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(home, ".waku"))
}
func pathsSame(a, b string) bool {
	left, right := canonicalPath(a), canonicalPath(b)
	if leftInfo, err := os.Stat(left); err == nil {
		if rightInfo, err := os.Stat(right); err == nil && os.SameFile(leftInfo, rightInfo) {
			return true
		}
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func canonicalPath(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	absolute, _ := filepath.Abs(path)
	return absolute
}

func (g *LaunchGuard) activeFor(directory string) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released || !pathsSame(g.directory, directory) {
		return false
	}
	pathInfo, err := os.Lstat(g.path)
	fileInfo, fileErr := g.file.Stat()
	return err == nil && fileErr == nil && os.SameFile(pathInfo, fileInfo)
}

// acquireWriter pins the launch guard before writer acquisition begins. A
// concurrent Release cannot remove the guard in the activeFor→O_EXCL window.
func (g *LaunchGuard) acquireWriter(directory string) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released || !pathsSame(g.directory, directory) || !g.identityLocked() {
		return false
	}
	g.writers++
	return true
}

func (g *LaunchGuard) cancelWriter() {
	g.mu.Lock()
	if g.writers > 0 {
		g.writers--
	}
	g.mu.Unlock()
}

func (g *LaunchGuard) releaseWriter() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.writers < 1 {
		return errors.New("supervisor launch guard has no writer lease")
	}
	g.writers--
	if g.writers != 0 {
		return nil
	}
	return g.releaseLocked()
}

func (g *LaunchGuard) identityLocked() bool {
	pathInfo, err := os.Lstat(g.path)
	fileInfo, fileErr := g.file.Stat()
	return err == nil && fileErr == nil && os.SameFile(pathInfo, fileInfo)
}

func (g *LaunchGuard) Release() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return g.releaseErr
	}
	if g.writers != 0 {
		return ErrDataDirOwned
	}
	return g.releaseLocked()
}

func (g *LaunchGuard) releaseLocked() error {
	if g.released {
		return g.releaseErr
	}
	g.released = true
	if !g.identityLocked() {
		g.releaseErr = errors.New("supervisor launch guard changed")
		_ = g.file.Close()
		return g.releaseErr
	}
	if err := os.Remove(g.path); err != nil {
		g.releaseErr = err
	}
	if err := g.file.Close(); g.releaseErr == nil {
		g.releaseErr = err
	}
	if g.releaseErr == nil {
		g.releaseErr = syncDirectory(g.directory)
	}
	return g.releaseErr
}
