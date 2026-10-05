package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

var RequiredTables = []string{"messages", "projects", "session_details", "sessions", "migrations"}

var ErrExistingSchemaUnversioned = errors.New("existing Waku database has no compatible migration journal")

type indexRule struct {
	table           string
	columns         []string
	unique, partial bool
}

var requiredIndexes = map[string]indexRule{
	"messages_by_session":       {table: "messages", columns: []string{"session_id", "position"}},
	"sessions_by_project":       {table: "sessions", columns: []string{"project_id", "updated_at"}},
	"sessions_by_updated_at":    {table: "sessions", columns: []string{"updated_at"}},
	"sessions_by_last_reply_at": {table: "sessions", columns: []string{"last_reply_at"}},
}

var expectedSchema = map[string]map[string]columnRule{
	"messages": {
		"id": {kind: "TEXT", notNull: true, primaryKey: 1}, "session_id": {kind: "TEXT", notNull: true}, "turn_id": {kind: "TEXT"},
		"position": {kind: "INTEGER", notNull: true}, "role": {kind: "TEXT", notNull: true}, "content": {kind: "TEXT", notNull: true},
		"created_at": {kind: "INTEGER", notNull: true}, "streaming": {kind: "INTEGER", notNull: true}, "display_content": {kind: "TEXT"},
		"attachments": {kind: "TEXT", notNull: true, defaultValue: "'[]'"},
	},
	"projects": {
		"id": {kind: "TEXT", notNull: true, primaryKey: 1}, "name": {kind: "TEXT", notNull: true}, "path": {kind: "TEXT", notNull: true},
		"position": {kind: "INTEGER", notNull: true}, "created_at": {kind: "INTEGER", notNull: true},
	},
	"session_details": {"session_id": {kind: "TEXT", notNull: true, primaryKey: 1}, "data": {kind: "TEXT", notNull: true}},
	"sessions": {
		"id": {kind: "TEXT", notNull: true, primaryKey: 1}, "project_id": {kind: "TEXT", notNull: true}, "title": {kind: "TEXT", notNull: true},
		"provider": {kind: "TEXT", notNull: true}, "model": {kind: "TEXT"}, "status": {kind: "TEXT", notNull: true},
		"created_at": {kind: "INTEGER", notNull: true}, "updated_at": {kind: "INTEGER", notNull: true}, "last_reply_at": {kind: "INTEGER"},
		"auto_title": {kind: "TEXT"},
	},
	"migrations": {"tag": {kind: "TEXT", primaryKey: 1}, "applied_at": {kind: "INTEGER", notNull: true}},
}

const migrationJournalSQL = `CREATE TABLE IF NOT EXISTS migrations (
tag TEXT PRIMARY KEY,
applied_at INTEGER NOT NULL
)`

// SQLMigrationStore adapts database/sql. driver.go registers the pinned,
// CGO-free SQLite implementation used by the application.
type SQLMigrationStore struct{ DB *sql.DB }

// OpenProduction requires the process-lifetime writer lock and configures the
// single pooled connection with Waku's durable production journal policy.
func OpenProduction(ctx context.Context, path string, lock *Lock) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if lock == nil || !lock.activeFor(filepath.Dir(abs)) {
		return nil, ErrDataDirOwned
	}
	db, err := sql.Open(SQLiteDriver, abs)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	failed := true
	defer func() {
		if failed {
			db.Close()
		}
	}()
	var journal string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return nil, err
	}
	if !strings.EqualFold(journal, "wal") {
		return nil, fmt.Errorf("SQLite journal_mode = %s, want wal", journal)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA synchronous=NORMAL"); err != nil {
		return nil, err
	}
	var synchronous int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return nil, err
	}
	if synchronous != 1 {
		return nil, fmt.Errorf("SQLite synchronous = %d, want NORMAL(1)", synchronous)
	}
	failed = false
	return db, nil
}

// Production owns both the database handle and the process-lifetime writer
// lease. Close releases them in that order so another daemon cannot open the
// directory while SQLite still has live WAL state.
type Production struct {
	DB   *sql.DB
	Lock *Lock
	once sync.Once
	err  error
}

func (p *Production) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		if p.DB != nil {
			p.err = p.DB.Close()
		}
		if err := p.Lock.Release(); p.err == nil {
			p.err = err
		}
	})
	return p.err
}

// BootstrapProduction is the only production database bootstrap seam. It
// composes ownership, WAL policy, existing-schema compatibility, migrations,
// and the final full-schema probe. Existing non-empty databases must carry a
// contiguous prefix of Waku's journal and match the schema at that prefix;
// new/empty databases receive the complete migration set.
func BootstrapProduction(ctx context.Context, directory, path string, now time.Time) (*Production, error) {
	absDir, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !pathsSame(filepath.Dir(absPath), absDir) {
		return nil, errors.New("production database must be directly inside the owned data directory")
	}
	lock, err := AcquireWriter(absDir)
	if err != nil {
		return nil, err
	}
	fail := func(db *sql.DB, cause error) (*Production, error) {
		if db != nil {
			_ = db.Close()
		}
		if releaseErr := lock.Release(); releaseErr != nil {
			return nil, errors.Join(cause, releaseErr)
		}
		return nil, cause
	}
	existing := false
	if info, statErr := os.Lstat(absPath); statErr == nil {
		if !info.Mode().IsRegular() {
			return fail(nil, errors.New("production database path is not a regular file"))
		}
		existing = info.Size() > 0
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fail(nil, statErr)
	}
	if existing {
		readOnlyDSN := (&url.URL{Scheme: "file", Path: filepath.ToSlash(absPath), RawQuery: "mode=ro"}).String()
		probeDB, openErr := sql.Open(SQLiteDriver, readOnlyDSN)
		if openErr != nil {
			return fail(nil, openErr)
		}
		probeDB.SetMaxOpenConns(1)
		probeDB.SetMaxIdleConns(1)
		count, inspectErr := inspectMigrationPrefix(ctx, probeDB)
		if inspectErr != nil {
			_ = probeDB.Close()
			return fail(nil, inspectErr)
		}
		if inspectErr = probeSchema(ctx, probeDB, expectedSchemaAt(count)); inspectErr != nil {
			_ = probeDB.Close()
			return fail(nil, fmt.Errorf("existing schema before migration: %w", inspectErr))
		}
		if closeErr := probeDB.Close(); closeErr != nil {
			return fail(nil, closeErr)
		}
	}
	db, err := OpenProduction(ctx, absPath, lock)
	if err != nil {
		return fail(nil, err)
	}
	if _, err := ApplyMigrations(ctx, SQLMigrationStore{DB: db}, now); err != nil {
		return fail(db, err)
	}
	if err := ProbeExistingSchema(ctx, db); err != nil {
		return fail(db, err)
	}
	return &Production{DB: db, Lock: lock}, nil
}

func inspectMigrationPrefix(ctx context.Context, db *sql.DB) (int, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='migrations')").Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, ErrExistingSchemaUnversioned
	}
	migrations, err := Migrations()
	if err != nil {
		return 0, err
	}
	rows, err := db.QueryContext(ctx, "SELECT tag FROM migrations ORDER BY tag")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return 0, err
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(tags) == 0 || len(tags) > len(migrations) {
		return 0, ErrExistingSchemaUnversioned
	}
	for i, tag := range tags {
		if tag != migrations[i].Tag {
			return 0, fmt.Errorf("%w: journal tag %q at position %d", ErrExistingSchemaUnversioned, tag, i)
		}
	}
	return len(tags), nil
}

func (s SQLMigrationStore) EnsureJournal(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, migrationJournalSQL)
	return err
}

func (s SQLMigrationStore) HasMigration(ctx context.Context, tag string) (bool, error) {
	var present bool
	err := s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM migrations WHERE tag = ?)", tag).Scan(&present)
	return present, err
}

func (s SQLMigrationStore) RunMigration(ctx context.Context, migration Migration, appliedAt int64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, migration.SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO migrations(tag, applied_at) VALUES(?, ?)", migration.Tag, appliedAt); err != nil {
		return err
	}
	return tx.Commit()
}

// SnapshotDatabase asks SQLite itself for a transactionally consistent copy,
// including committed WAL contents. SQLite refuses an existing destination.
func SnapshotDatabase(ctx context.Context, source *sql.DB, destination string) error {
	abs, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(abs); err == nil {
		return errors.New("database snapshot destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := source.ExecContext(ctx, "VACUUM INTO ?", abs); err != nil {
		return fmt.Errorf("create consistent SQLite snapshot: %w", err)
	}
	return syncDirectory(filepath.Dir(abs))
}

// ProbeExistingSchema performs a read-only compatibility check against a copy
// of an existing database. A pure-Go database/sql driver must be registered by
// the integration layer; storage intentionally does not silently select one.
func ProbeExistingSchema(ctx context.Context, db *sql.DB) error {
	return probeSchema(ctx, db, expectedSchema)
}

func probeSchema(ctx context.Context, db *sql.DB, schema map[string]map[string]columnRule) error {
	for _, table := range RequiredTables {
		var name string
		err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			return fmt.Errorf("existing Waku schema is missing %s: %w", table, err)
		}
	}
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&integrity); err != nil {
		return fmt.Errorf("SQLite quick_check: %w", err)
	}
	if integrity != "ok" {
		return fmt.Errorf("SQLite quick_check: %s", integrity)
	}
	for table, columns := range schema {
		if err := validateColumns(ctx, db, table, columns); err != nil {
			return err
		}
	}
	for index, want := range requiredIndexes {
		var name string
		if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='index' AND name=?", index).Scan(&name); err != nil {
			return fmt.Errorf("existing Waku schema is missing index %s: %w", index, err)
		}
		var unique, partial bool
		if err := db.QueryRowContext(ctx, `SELECT "unique", partial FROM pragma_index_list(?) WHERE name=?`, want.table, index).Scan(&unique, &partial); err != nil {
			return fmt.Errorf("inspect schema index %s: %w", index, err)
		}
		if unique != want.unique || partial != want.partial {
			return fmt.Errorf("schema index %s unique/partial = %v/%v, want %v/%v", index, unique, partial, want.unique, want.partial)
		}
		rows, err := db.QueryContext(ctx, "PRAGMA index_info("+index+")")
		if err != nil {
			return err
		}
		var columns []string
		for rows.Next() {
			var sequence, cid int
			var column string
			if err := rows.Scan(&sequence, &cid, &column); err != nil {
				rows.Close()
				return err
			}
			columns = append(columns, column)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if !slices.Equal(columns, want.columns) {
			return fmt.Errorf("schema index %s columns = %v, want %v", index, columns, want.columns)
		}
	}
	return nil
}

func expectedSchemaAt(applied int) map[string]map[string]columnRule {
	result := make(map[string]map[string]columnRule, len(expectedSchema))
	for table, columns := range expectedSchema {
		copyColumns := make(map[string]columnRule, len(columns))
		for name, rule := range columns {
			copyColumns[name] = rule
		}
		result[table] = copyColumns
	}
	if applied < 3 {
		delete(result["messages"], "display_content")
		delete(result["messages"], "attachments")
	}
	if applied < 2 {
		delete(result["sessions"], "auto_title")
	}
	return result
}

type columnRule struct {
	kind         string
	notNull      bool
	defaultValue string
	primaryKey   int
}

func validateColumns(ctx context.Context, db *sql.DB, table string, required map[string]columnRule) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	defer rows.Close()
	found := make(map[string]columnRule)
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primary int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return err
		}
		found[name] = columnRule{kind: strings.ToUpper(kind), notNull: notNull != 0, defaultValue: defaultValue.String, primaryKey: primary}
	}
	if len(found) != len(required) {
		return fmt.Errorf("schema table %s has %d columns, want %d", table, len(found), len(required))
	}
	for name, want := range required {
		got, ok := found[name]
		if !ok || got != want {
			return fmt.Errorf("schema column %s.%s = %#v, want %#v", table, name, got, want)
		}
	}
	return rows.Err()
}
