package storage

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Migration struct{ Tag, SQL string }

// MigrationStore must execute SQL and journal insertion in one transaction.
// This keeps the migration policy independent of the selected pure-Go SQLite
// driver while retaining the Rust journal semantics.
type MigrationStore interface {
	EnsureJournal(context.Context) error
	HasMigration(context.Context, string) (bool, error)
	RunMigration(context.Context, Migration, int64) error
}

func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	var result []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		data, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, Migration{Tag: strings.TrimSuffix(entry.Name(), ".sql"), SQL: string(data)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Tag < result[j].Tag })
	for index, migration := range result {
		prefix, _, ok := strings.Cut(migration.Tag, "_")
		number, err := strconv.Atoi(prefix)
		if !ok || len(prefix) != 4 || err != nil || number != index {
			return nil, fmt.Errorf("migration tags must be contiguous from 0000: %q at position %d", migration.Tag, index)
		}
	}
	return result, nil
}

func ApplyMigrations(ctx context.Context, store MigrationStore, now time.Time) (int, error) {
	if err := store.EnsureJournal(ctx); err != nil {
		return 0, fmt.Errorf("ensure migration journal: %w", err)
	}
	migrations, err := Migrations()
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, migration := range migrations {
		has, err := store.HasMigration(ctx, migration.Tag)
		if err != nil {
			return applied, fmt.Errorf("inspect migration %s: %w", migration.Tag, err)
		}
		if has {
			continue
		}
		if err := store.RunMigration(ctx, migration, now.Unix()); err != nil {
			return applied, fmt.Errorf("migration %s failed: %w", migration.Tag, err)
		}
		applied++
	}
	return applied, nil
}
