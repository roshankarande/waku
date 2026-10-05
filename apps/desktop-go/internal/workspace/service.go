package workspace

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"unicode/utf8"
)

var (
	ErrContentChanged = errors.New("workspace file changed since it was read")
	ErrLimitExceeded  = errors.New("workspace operation exceeded its configured limit")
)

const MaxReviewPatchBytes int64 = 32 << 20

type Limits struct {
	MaxEntries     int
	MaxFileBytes   int64
	MaxGitOutput   int64
	MaxCommitBytes int
}

func DefaultLimits() Limits {
	return Limits{MaxEntries: 10_000, MaxFileBytes: 8 << 20, MaxGitOutput: MaxReviewPatchBytes, MaxCommitBytes: 4096}
}

type Service struct {
	root   Root
	anchor *os.Root
	limits Limits
	mu     *sync.Mutex // shared per canonical root across Service instances
	close  sync.Once
	// Test seams run while mu is held and are never exposed to daemon callers.
	beforeGit         func([]string) error
	beforeRestoreStep func(int) error
}

var serviceRootLocks = struct {
	sync.Mutex
	values map[string]*rootLock
}{values: make(map[string]*rootLock)}

type rootLock struct {
	mu   sync.Mutex
	refs int
}

func NewService(root Root, limits Limits) (*Service, error) {
	defaults := DefaultLimits()
	if limits.MaxEntries == 0 {
		limits.MaxEntries = defaults.MaxEntries
	}
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaults.MaxFileBytes
	}
	if limits.MaxGitOutput == 0 {
		limits.MaxGitOutput = defaults.MaxGitOutput
	} else if limits.MaxGitOutput > MaxReviewPatchBytes {
		limits.MaxGitOutput = MaxReviewPatchBytes
	}
	if limits.MaxCommitBytes == 0 {
		limits.MaxCommitBytes = defaults.MaxCommitBytes
	}
	if limits.MaxEntries < 1 || limits.MaxFileBytes < 1 || limits.MaxGitOutput < 1 || limits.MaxCommitBytes < 1 {
		return nil, errors.New("workspace limits must be positive")
	}
	anchor, err := os.OpenRoot(root.canonical)
	if err != nil {
		return nil, fmt.Errorf("anchor workspace root: %w", err)
	}
	serviceRootLocks.Lock()
	lock := serviceRootLocks.values[root.canonical]
	if lock == nil {
		lock = &rootLock{}
		serviceRootLocks.values[root.canonical] = lock
	}
	lock.refs++
	serviceRootLocks.Unlock()
	return &Service{root: root, anchor: anchor, limits: limits, mu: &lock.mu}, nil
}

func (s *Service) Close() (err error) {
	s.close.Do(func() {
		err = s.anchor.Close()
		serviceRootLocks.Lock()
		lock := serviceRootLocks.values[s.root.canonical]
		if lock != nil {
			lock.refs--
			if lock.refs == 0 {
				delete(serviceRootLocks.values, s.root.canonical)
			}
		}
		serviceRootLocks.Unlock()
	})
	return err
}

func (s *Service) HostRoot() HostPath { return HostPath{absolute: s.root.canonical} }

// RaceSafeMutations is false because portable filesystems provide atomic
// replacement but not an atomic compare-hash-and-replace primitive against
// writers outside this process. Protocol clients must not be offered CAS
// writes until a platform/filesystem-specific proof is available.
func (s *Service) RaceSafeMutations() bool { return false }

type Entry struct {
	Name     string
	Relative string
	HostPath HostPath
	IsDir    bool
	Size     int64
}

func (s *Service) Browse(ctx context.Context, relative string) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	base := ""
	if relative != "" {
		if err := validRelative(relative); err != nil {
			return nil, err
		}
		base = relative
	}
	directory := base
	if directory == "" {
		directory = "."
	}
	info, err := s.anchor.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("browse workspace directory: %w", firstError(err, errors.New("not a directory")))
	}
	opened, err := s.anchor.Open(directory)
	if err != nil {
		return nil, err
	}
	defer opened.Close()
	children, err := opened.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	if len(children) > s.limits.MaxEntries {
		return nil, fmt.Errorf("directory has %d entries (maximum %d): %w", len(children), s.limits.MaxEntries, ErrLimitExceeded)
	}
	entries := make([]Entry, 0, len(children))
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if child.Name() == ".git" {
			continue
		}
		rel := filepath.Join(base, child.Name())
		metadata, err := s.anchor.Stat(rel)
		if err != nil { // omit broken or escaping symlinks
			continue
		}
		host := HostPath{absolute: filepath.Join(s.root.canonical, rel)}
		entries = append(entries, Entry{Name: child.Name(), Relative: rel, HostPath: host, IsDir: metadata.IsDir(), Size: metadata.Size()})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

func (s *Service) ReadText(ctx context.Context, relative string) (content string, sha256Hex string, err error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validRelative(relative); err != nil {
		return "", "", err
	}
	file, err := s.anchor.Open(relative)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, s.limits.MaxFileBytes+1))
	if err != nil {
		return "", "", err
	}
	if int64(len(data)) > s.limits.MaxFileBytes {
		return "", "", ErrLimitExceeded
	}
	if !utf8.Valid(data) {
		return "", "", errors.New("workspace file is not UTF-8 text")
	}
	hash := sha256.Sum256(data)
	return string(data), hex.EncodeToString(hash[:]), nil
}

// WriteTextCAS performs an atomic same-directory replacement only when the
// current content hash equals expectedSHA256. A missing file has the SHA-256
// hash of an empty byte slice. The service mutex closes races between callers
// of this Service; Root.RaceSafeMutations remains the honest cross-process
// dirfd/reparse-point capability gate.
func (s *Service) WriteTextCAS(ctx context.Context, relative, expectedSHA256 string, content string) (string, error) {
	if int64(len(content)) > s.limits.MaxFileBytes {
		return "", ErrLimitExceeded
	}
	if !utf8.ValidString(content) {
		return "", errors.New("workspace content is not UTF-8")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validRelative(relative); err != nil {
		return "", err
	}
	if info, err := s.anchor.Lstat(relative); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("workspace leaf is a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	current, mode, err := boundedCurrentRoot(s.anchor, relative, s.limits.MaxFileBytes)
	if err != nil {
		return "", err
	}
	currentHash := sha256.Sum256(current)
	if expectedSHA256 == "" || !bytes.EqualFold([]byte(expectedSHA256), []byte(hex.EncodeToString(currentHash[:]))) {
		return "", ErrContentChanged
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	temporary, temporaryRelative, err := createAnchoredTemp(s.anchor, filepath.Dir(relative), mode)
	if err != nil {
		return "", err
	}
	defer s.anchor.Remove(temporaryRelative)
	_, err = io.WriteString(temporary, content)
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	latest, _, err := boundedCurrentRoot(s.anchor, relative, s.limits.MaxFileBytes)
	if err != nil {
		return "", err
	}
	latestHash := sha256.Sum256(latest)
	if latestHash != currentHash {
		return "", ErrContentChanged
	}
	if err := s.anchor.Rename(temporaryRelative, relative); err != nil {
		return "", err
	}
	if directory, err := s.anchor.Open(filepath.Dir(relative)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	newHash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(newHash[:]), nil
}

func boundedCurrentRoot(root *os.Root, path string, limit int64) ([]byte, os.FileMode, error) {
	file, err := root.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0o600, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, 0, firstError(err, errors.New("workspace target is not a regular file"))
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, 0, err
	}
	if int64(len(data)) > limit {
		return nil, 0, ErrLimitExceeded
	}
	return data, info.Mode().Perm(), nil
}

func createAnchoredTemp(root *os.Root, directory string, mode os.FileMode) (*os.File, string, error) {
	for range 16 {
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := filepath.Join(directory, ".waku-write-"+hex.EncodeToString(random[:]))
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("could not allocate workspace temporary file")
}

func firstError(first, fallback error) error {
	if first != nil {
		return first
	}
	return fallback
}
