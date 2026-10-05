package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SettingsStore owns the daemon-host settings.json used by the Rust daemon.
// Unknown fields are retained because the complete validated JSON document is
// replaced atomically rather than projected into a version-specific schema.
type SettingsStore struct {
	Path string
	mu   sync.Mutex
	// Deterministic durability/cancellation seams for package tests.
	beforeRename  func()
	syncDirectory func(string) error
}

func (s *SettingsStore) Load(ctx context.Context) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return json.RawMessage(`{}`), nil
	}
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > 1<<20 || !json.Valid(data) {
		if err := s.quarantineLocked(ctx); err != nil {
			return nil, fmt.Errorf("quarantine corrupt daemon settings: %w", err)
		}
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), data...), nil
}

func (s *SettingsStore) Save(ctx context.Context, settings json.RawMessage) error {
	if len(settings) > 1<<20 || !json.Valid(settings) {
		return errors.New("daemon settings must be valid JSON within 1 MiB")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := filepath.Dir(s.Path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".settings-*.tmp")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	err = temporary.Chmod(0o600)
	if err == nil {
		var written int
		written, err = temporary.Write(settings)
		if err == nil && written != len(settings) {
			err = errors.New("short daemon settings write")
		}
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if s.beforeRename != nil {
		s.beforeRename()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Rename(filepath.Base(name), filepath.Base(s.Path)); err != nil {
		return fmt.Errorf("replace daemon settings: %w", err)
	}
	return s.syncDirectoryLocked(directory)
}

func (s *SettingsStore) quarantineLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := filepath.Dir(s.Path)
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	quarantine := filepath.Base(s.Path) + fmt.Sprintf(".corrupt-%d", time.Now().UnixNano())
	if err := root.Rename(filepath.Base(s.Path), quarantine); err != nil {
		return err
	}
	return s.syncDirectoryLocked(directory)
}

func (s *SettingsStore) syncDirectoryLocked(directory string) error {
	if s.syncDirectory != nil {
		return s.syncDirectory(directory)
	}
	parent, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := parent.Sync(); err != nil {
		_ = parent.Close()
		return err
	}
	return parent.Close()
}
