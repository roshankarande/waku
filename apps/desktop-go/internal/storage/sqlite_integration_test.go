package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const independentInitialSchema = `
CREATE TABLE messages (id text PRIMARY KEY NOT NULL, session_id text NOT NULL, turn_id text, position integer NOT NULL, role text NOT NULL, content text NOT NULL, created_at integer NOT NULL, streaming integer NOT NULL);
CREATE INDEX messages_by_session ON messages(session_id,position);
CREATE TABLE projects (id text PRIMARY KEY NOT NULL, name text NOT NULL, path text NOT NULL, position integer NOT NULL, created_at integer NOT NULL);
CREATE TABLE session_details (session_id text PRIMARY KEY NOT NULL, data text NOT NULL);
CREATE TABLE sessions (id text PRIMARY KEY NOT NULL, project_id text NOT NULL, title text NOT NULL, provider text NOT NULL, model text, status text NOT NULL, created_at integer NOT NULL, updated_at integer NOT NULL, last_reply_at integer);
CREATE INDEX sessions_by_project ON sessions(project_id,updated_at);
CREATE INDEX sessions_by_updated_at ON sessions(updated_at);
CREATE INDEX sessions_by_last_reply_at ON sessions(last_reply_at);`

func openTestSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriver, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPureGoSQLiteMigratesCopiedExistingSchema(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "existing.db")
	source := openTestSQLite(t, sourcePath)
	store := SQLMigrationStore{DB: source}
	if _, err := source.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureJournal(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, independentInitialSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `INSERT INTO migrations(tag,applied_at) VALUES('0000_init',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `INSERT INTO projects(id,name,path,position,created_at) VALUES('p','Project','/daemon/workspace',0,1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `INSERT INTO sessions(id,project_id,title,provider,status,created_at,updated_at) VALUES('s','p','Existing title','codex','idle',1,1)`); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(directory, "fixture.db")
	if err := SnapshotDatabase(ctx, source, fixturePath); err != nil {
		t.Fatal(err)
	}
	fixture := openTestSQLite(t, fixturePath)
	applied, err := ApplyMigrations(ctx, SQLMigrationStore{DB: fixture}, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("applied %d migrations, want 2", applied)
	}
	if err := ProbeExistingSchema(ctx, fixture); err != nil {
		t.Fatal(err)
	}
	var title string
	var autoTitle sql.NullString
	if err := fixture.QueryRowContext(ctx, `SELECT title,auto_title FROM sessions WHERE id='s'`).Scan(&title, &autoTitle); err != nil {
		t.Fatal(err)
	}
	if title != "New task" || !autoTitle.Valid || autoTitle.String != "Existing title" {
		t.Fatalf("migrated titles = %q, %#v", title, autoTitle)
	}
	applied, err = ApplyMigrations(ctx, SQLMigrationStore{DB: fixture}, time.Unix(3, 0))
	if err != nil || applied != 0 {
		t.Fatalf("idempotent apply = %d, %v", applied, err)
	}
	if info, err := os.Stat(sourcePath); err != nil || info.Size() == 0 {
		t.Fatalf("source database was lost: %v, %v", info, err)
	}
	if err := SnapshotDatabase(ctx, source, fixturePath); err == nil {
		t.Fatal("snapshot overwrote existing fixture")
	}
}

func TestProviderCursorRefusesMissingSessionAndPersistsExactJSON(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	production, err := BootstrapProduction(ctx, directory, filepath.Join(directory, "waku.sqlite3"), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer production.Close()
	store := TaskStateStore{DB: production.DB}
	if err := store.SetProviderCursor(ctx, "s", []byte(`{"provider":"codex","threadId":"before"}`)); !errors.Is(err, ErrTaskSessionNotFound) {
		t.Fatalf("missing session cursor error=%v", err)
	}
	session := []byte(`{"id":"s","title":"Task","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":1}`)
	if err := store.Save(ctx, nil, []json.RawMessage{session}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetProviderCursor(ctx, "s", []byte(`{"provider":"codex","threadId":"after"}`)); err != nil {
		t.Fatal(err)
	}
	raw, found, err := store.Session(ctx, "s")
	if err != nil || !found || !strings.Contains(string(raw), `"threadId":"after"`) {
		t.Fatalf("session=%s found=%v err=%v", raw, found, err)
	}
}

func TestTaskStateAndRelevantCursorCommitAtomically(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	production, err := BootstrapProduction(ctx, directory, filepath.Join(directory, "waku.sqlite3"), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer production.Close()
	store := TaskStateStore{DB: production.DB}
	project := json.RawMessage(`{"id":"p","name":"Original","path":"/tmp/p","created_at":1}`)
	session := json.RawMessage(`{"id":"s","title":"Task","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":1}`)
	if err := store.SaveWithProviderCursors(ctx, []json.RawMessage{project}, []json.RawMessage{session}, map[string]json.RawMessage{
		"s":       json.RawMessage(`{"provider":"codex","threadId":"kept"}`),
		"missing": json.RawMessage(`{"provider":"codex","threadId":"ignored"}`),
	}); err != nil {
		t.Fatal(err)
	}
	raw, found, err := store.Session(ctx, "s")
	if err != nil || !found || !strings.Contains(string(raw), `"threadId":"kept"`) {
		t.Fatalf("session=%s found=%v err=%v", raw, found, err)
	}
	stale := json.RawMessage(`{"id":"s","title":"Stale UI","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2,"provider_cursor":{"provider":"codex","threadId":"stale"}}`)
	if err := store.Save(ctx, nil, []json.RawMessage{stale}); err != nil {
		t.Fatal(err)
	}
	raw, _, _ = store.Session(ctx, "s")
	if !strings.Contains(string(raw), `"threadId":"kept"`) || strings.Contains(string(raw), `"threadId":"stale"`) {
		t.Fatalf("stale non-null UI cursor overwrote daemon cursor: %s", raw)
	}
	changed := json.RawMessage(`{"id":"p","name":"Changed","path":"/tmp/p","created_at":1}`)
	if err := store.SaveWithProviderCursors(ctx, []json.RawMessage{changed}, []json.RawMessage{session}, map[string]json.RawMessage{"s": json.RawMessage(`{`)}); err == nil {
		t.Fatal("invalid relevant cursor committed")
	}
	var name string
	if err := production.DB.QueryRowContext(ctx, `SELECT name FROM projects WHERE id='p'`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Original" {
		t.Fatalf("failed cursor transaction changed project to %q", name)
	}
}

func TestSaveSnapshotBeforeCursorUpdateSerializesWithoutOverwrite(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	production, err := BootstrapProduction(ctx, directory, filepath.Join(directory, "waku.sqlite3"), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer production.Close()
	base := TaskStateStore{DB: production.DB}
	initial := json.RawMessage(`{"id":"s","title":"Initial","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":1}`)
	if err := base.Save(ctx, nil, []json.RawMessage{initial}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	barrier := TaskStateStore{DB: production.DB, beforeSaveCommit: func() { close(entered); <-release }}
	changed := json.RawMessage(`{"id":"s","title":"Changed","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":2}`)
	saved := make(chan error, 1)
	go func() { saved <- barrier.SaveWithProviderCursors(ctx, nil, []json.RawMessage{changed}, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("save did not reach transaction barrier")
	}
	cursorDone := make(chan error, 1)
	go func() {
		cursorDone <- base.SetProviderCursor(ctx, "s", json.RawMessage(`{"provider":"codex","threadId":"late"}`))
	}()
	select {
	case err := <-cursorDone:
		t.Fatalf("cursor update bypassed active save transaction: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-saved; err != nil {
		t.Fatal(err)
	}
	if err := <-cursorDone; err != nil {
		t.Fatal(err)
	}
	raw, found, err := base.Session(ctx, "s")
	if err != nil || !found || !strings.Contains(string(raw), `"title":"Changed"`) || !strings.Contains(string(raw), `"threadId":"late"`) {
		t.Fatalf("serialized session=%s found=%v err=%v", raw, found, err)
	}
	// The opposite ordering also preserves the daemon-owned cursor when a stale
	// UI snapshot is saved after the cursor statement completes.
	if err := base.SetProviderCursor(ctx, "s", json.RawMessage(`{"provider":"codex","threadId":"first"}`)); err != nil {
		t.Fatal(err)
	}
	newer := json.RawMessage(`{"id":"s","title":"Newest","project_id":"p","provider":"codex","runtime_mode":"fullAccess","status":"idle","created_at":1,"updated_at":3}`)
	if err := base.Save(ctx, nil, []json.RawMessage{newer}); err != nil {
		t.Fatal(err)
	}
	raw, _, _ = base.Session(ctx, "s")
	if !strings.Contains(string(raw), `"title":"Newest"`) || !strings.Contains(string(raw), `"threadId":"first"`) {
		t.Fatalf("cursor-first ordering lost data: %s", raw)
	}
}

func TestSQLiteMigrationAndJournalRollbackTogether(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t, filepath.Join(t.TempDir(), "failure.db"))
	store := SQLMigrationStore{DB: db}
	if err := store.EnsureJournal(ctx); err != nil {
		t.Fatal(err)
	}
	bad := Migration{Tag: "synthetic_failure", SQL: `CREATE TABLE transient(value TEXT); THIS IS NOT SQL;`}
	if err := store.RunMigration(ctx, bad, 1); err == nil {
		t.Fatal("invalid migration succeeded")
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transient'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("failed migration left schema changes behind")
	}
	has, err := store.HasMigration(ctx, bad.Tag)
	if err != nil || has {
		t.Fatalf("failed migration was journaled: %v, %v", has, err)
	}
	good := Migration{Tag: bad.Tag, SQL: `CREATE TABLE transient(value TEXT);`}
	if err := store.RunMigration(ctx, good, 2); err != nil {
		t.Fatalf("recovery migration: %v", err)
	}
	has, err = store.HasMigration(ctx, good.Tag)
	if err != nil || !has {
		t.Fatalf("recovery was not journaled: %v, %v", has, err)
	}
}

func TestSchemaProbeRejectsMissingIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t, filepath.Join(t.TempDir(), "schema.db"))
	if _, err := ApplyMigrations(ctx, SQLMigrationStore{DB: db}, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := ProbeExistingSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX sessions_by_updated_at`); err != nil {
		t.Fatal(err)
	}
	if err := ProbeExistingSchema(ctx, db); err == nil {
		t.Fatal("schema probe accepted a missing required index")
	}
}

func TestSchemaProbeRejectsBaseColumnParityDrift(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t, filepath.Join(t.TempDir(), "schema-drift.db"))
	if _, err := ApplyMigrations(ctx, SQLMigrationStore{DB: db}, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE projects RENAME TO projects_old;
CREATE TABLE projects (id text PRIMARY KEY NOT NULL, name text NOT NULL, path text, position integer NOT NULL, created_at integer NOT NULL);
DROP TABLE projects_old;`); err != nil {
		t.Fatal(err)
	}
	if err := ProbeExistingSchema(ctx, db); err == nil {
		t.Fatal("schema probe accepted nullable base path column")
	}
}

func TestProductionOpenRequiresWriterAndEnforcesJournalPolicy(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "app.db")
	if _, err := OpenProduction(ctx, path, nil); !errors.Is(err, ErrDataDirOwned) {
		t.Fatalf("unowned open = %v", err)
	}
	lock, err := AcquireWriter(directory)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenProduction(ctx, path, lock)
	if err != nil {
		t.Fatal(err)
	}
	var journal string
	var synchronous int
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if journal != "wal" || synchronous != 1 {
		t.Fatalf("policy = %q, %d", journal, synchronous)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenProduction(ctx, path, lock); !errors.Is(err, ErrDataDirOwned) {
		t.Fatalf("released lock open = %v", err)
	}
}

func TestBootstrapProductionOwnsMigratesAndProbesNewDatabase(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "app.db")
	production, err := BootstrapProduction(ctx, directory, path, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := ProbeExistingSchema(ctx, production.DB); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWriter(directory); !errors.Is(err, ErrDataDirOwned) {
		t.Fatalf("bootstrap did not retain ownership: %v", err)
	}
	if err := production.Close(); err != nil {
		t.Fatal(err)
	}
	if err := production.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireWriter(directory)
	if err != nil {
		t.Fatalf("bootstrap did not release ownership: %v", err)
	}
	_ = lock.Release()
}

func TestBootstrapProductionValidatesExistingPrefixBeforeMigration(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "existing.db")
	db := openTestSQLite(t, path)
	if err := (SQLMigrationStore{DB: db}).EnsureJournal(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, independentInitialSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO migrations(tag,applied_at) VALUES('0000_init',1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	production, err := BootstrapProduction(ctx, directory, path, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer production.Close()
	if err := ProbeExistingSchema(ctx, production.DB); err != nil {
		t.Fatal(err)
	}
	var migrations int
	if err := production.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM migrations`).Scan(&migrations); err != nil || migrations != 3 {
		t.Fatalf("migration count=%d err=%v", migrations, err)
	}
}

func TestBootstrapProductionRefusesUnversionedExistingDatabase(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "unversioned.db")
	db := openTestSQLite(t, path)
	if _, err := db.ExecContext(ctx, `CREATE TABLE foreign_data(value TEXT); INSERT INTO foreign_data VALUES('preserve')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := BootstrapProduction(ctx, directory, path, time.Unix(2, 0)); !errors.Is(err, ErrExistingSchemaUnversioned) {
		t.Fatalf("bootstrap=%v", err)
	}
	check := openTestSQLite(t, path)
	var value string
	if err := check.QueryRowContext(ctx, `SELECT value FROM foreign_data`).Scan(&value); err != nil || value != "preserve" {
		t.Fatalf("foreign data changed: %q %v", value, err)
	}
	lock, err := AcquireWriter(directory)
	if err != nil {
		t.Fatalf("failed bootstrap leaked ownership: %v", err)
	}
	_ = lock.Release()
}

func TestBootstrapProductionDoesNotMigrateDriftedExistingPrefix(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "drifted.db")
	db := openTestSQLite(t, path)
	if err := (SQLMigrationStore{DB: db}).EnsureJournal(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, independentInitialSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO migrations(tag,applied_at) VALUES('0000_init',1);
ALTER TABLE projects RENAME TO projects_old;
CREATE TABLE projects (id text PRIMARY KEY NOT NULL, name text NOT NULL, path text, position integer NOT NULL, created_at integer NOT NULL);
DROP TABLE projects_old;`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := BootstrapProduction(ctx, directory, path, time.Unix(2, 0)); err == nil || !strings.Contains(err.Error(), "before migration") {
		t.Fatalf("bootstrap=%v", err)
	}
	check := openTestSQLite(t, path)
	var count int
	if err := check.QueryRowContext(ctx, `SELECT COUNT(*) FROM migrations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("drifted database was mutated: count=%d err=%v", count, err)
	}
}
