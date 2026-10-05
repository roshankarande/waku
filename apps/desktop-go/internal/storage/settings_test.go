package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSettingsStorePersistsAtomicallyAndHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	store := &SettingsStore{Path: path}
	if raw, err := store.Load(context.Background()); err != nil || string(raw) != "{}" {
		t.Fatalf("missing settings=%s err=%v", raw, err)
	}
	first := []byte(`{"computer_use_enabled":true,"future":9007199254740993}`)
	if err := store.Save(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Save(ctx, []byte(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled save=%v", err)
	}
	raw, err := store.Load(context.Background())
	if err != nil || string(raw) != string(first) {
		t.Fatalf("settings after canceled save=%s err=%v", raw, err)
	}
	if err := store.Save(context.Background(), []byte(`{`)); err == nil {
		t.Fatal("invalid JSON replaced settings")
	}
	if leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".settings-*.tmp")); len(leftovers) != 0 {
		t.Fatalf("temporary settings files remain: %v", leftovers)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("settings mode=%v", info.Mode().Perm())
	}
}

func TestSettingsStoreQuarantinesCorruptionAndRecoversDefaults(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "settings.json")
	corrupt := []byte(`{"computer_use_enabled":`)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	store := &SettingsStore{Path: path}
	raw, err := store.Load(context.Background())
	if err != nil || string(raw) != "{}" {
		t.Fatalf("recovery=%s err=%v", raw, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt source still active: %v", err)
	}
	quarantined, err := filepath.Glob(filepath.Join(directory, "settings.json.corrupt-*"))
	if err != nil || len(quarantined) != 1 {
		t.Fatalf("quarantine=%v err=%v", quarantined, err)
	}
	got, _ := os.ReadFile(quarantined[0])
	if string(got) != string(corrupt) {
		t.Fatalf("quarantine content=%q", got)
	}
}

func TestSettingsStorePropagatesDurabilityAndPreRenameCancellation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "settings.json")
	if err := os.WriteFile(path, []byte(`{"old":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &SettingsStore{Path: path, beforeRename: cancel}
	if err := store.Save(ctx, []byte(`{"new":true}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-rename cancellation=%v", err)
	}
	if raw, _ := os.ReadFile(path); string(raw) != `{"old":true}` {
		t.Fatalf("canceled write replaced settings: %s", raw)
	}
	durable := &SettingsStore{Path: path, syncDirectory: func(string) error { return errors.New("synthetic directory fsync failure") }}
	if err := durable.Save(context.Background(), []byte(`{"durable":false}`)); err == nil || !strings.Contains(err.Error(), "fsync") {
		t.Fatalf("directory durability error=%v", err)
	}
	corruptPath := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(corruptPath, []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}
	quarantine := &SettingsStore{Path: corruptPath, syncDirectory: func(string) error { return errors.New("synthetic quarantine fsync failure") }}
	if _, err := quarantine.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "fsync") {
		t.Fatalf("quarantine durability error=%v", err)
	}
}
