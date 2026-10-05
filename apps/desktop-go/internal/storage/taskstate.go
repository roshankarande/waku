package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// TaskStateStore persists Rust-shaped protocol objects without re-defining the
// complete model in the storage package. Indexed columns remain available to
// both runtimes while session_details retains fields introduced by newer
// protocol revisions.
type TaskStateStore struct {
	DB *sql.DB
	// beforeSaveCommit is a deterministic transaction barrier for storage
	// package tests. Production stores leave it nil.
	beforeSaveCommit func()
}

var ErrTaskSessionNotFound = errors.New("task session is not persisted")

type storedProject struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Path      string `json:"path"`
	CreatedAt uint64 `json:"created_at"`
}

type storedSession struct {
	ID          string  `json:"id"`
	ProjectID   string  `json:"project_id"`
	Title       string  `json:"title"`
	Provider    string  `json:"provider"`
	Model       *string `json:"model"`
	Status      string  `json:"status"`
	CreatedAt   uint64  `json:"created_at"`
	UpdatedAt   uint64  `json:"updated_at"`
	LastReplyAt *uint64 `json:"last_reply_at"`
	AutoTitle   *string `json:"auto_title"`
}

func decodeStored[T any](raw json.RawMessage, value *T) error {
	if !json.Valid(raw) {
		return errors.New("task object is not valid JSON")
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return err
	}
	return nil
}

// Save merges the supplied catalog. It intentionally does not delete omitted
// rows: multiple clients may hold snapshots of different ages.
func (s TaskStateStore) Save(ctx context.Context, projects, sessions []json.RawMessage) error {
	return s.SaveWithProviderCursors(ctx, projects, sessions, nil)
}

// SaveWithProviderCursors commits the UI snapshot and daemon-owned resume
// cursors together. Cursors for sessions absent from this snapshot are ignored:
// one client's save must not depend on unrelated live sessions having rows.
func (s TaskStateStore) SaveWithProviderCursors(ctx context.Context, projects, sessions []json.RawMessage, cursors map[string]json.RawMessage) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for position, raw := range projects {
		var p storedProject
		if err := decodeStored(raw, &p); err != nil {
			return fmt.Errorf("project[%d]: %w", position, err)
		}
		if p.ID == "" || p.Name == "" || p.Path == "" {
			return fmt.Errorf("project[%d] is missing id, name, or path", position)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO projects(id,name,path,position,created_at) VALUES(?,?,?,?,?)
			ON CONFLICT(id) DO UPDATE SET name=excluded.name,path=excluded.path,position=excluded.position,created_at=excluded.created_at`,
			p.ID, p.Name, p.Path, position, p.CreatedAt); err != nil {
			return err
		}
	}
	for position, raw := range sessions {
		var v storedSession
		if err := decodeStored(raw, &v); err != nil {
			return fmt.Errorf("session[%d]: %w", position, err)
		}
		if v.ID == "" || v.ProjectID == "" || v.Title == "" || v.Provider == "" || v.Status == "" {
			return fmt.Errorf("session[%d] is missing indexed fields", position)
		}
		// A UI snapshot can predate the provider handshake. Preserve the
		// daemon-owned resume cursor unless the snapshot carries a newer value.
		var incoming map[string]json.RawMessage
		if err := json.Unmarshal(raw, &incoming); err != nil {
			return err
		}
		if cursor := cursors[v.ID]; len(cursor) != 0 {
			if !json.Valid(cursor) || string(cursor) == "null" {
				return fmt.Errorf("session[%d] provider cursor is invalid", position)
			}
			incoming["provider_cursor"] = append(json.RawMessage(nil), cursor...)
			raw, err = json.Marshal(incoming)
			if err != nil {
				return err
			}
		}
		// Once a row exists, provider_cursor is daemon-owned. Always preserve
		// the stored value over UI snapshots, including stale non-null cursors.
		// Only the current runtime cursor supplied above may replace it.
		if len(cursors[v.ID]) == 0 {
			var existingData string
			existingErr := tx.QueryRowContext(ctx, `SELECT data FROM session_details WHERE session_id=?`, v.ID).Scan(&existingData)
			if existingErr == nil {
				var existing map[string]json.RawMessage
				if json.Unmarshal([]byte(existingData), &existing) == nil && len(existing["provider_cursor"]) != 0 && string(existing["provider_cursor"]) != "null" {
					incoming["provider_cursor"] = existing["provider_cursor"]
					raw, err = json.Marshal(incoming)
					if err != nil {
						return err
					}
				}
			} else if !errors.Is(existingErr, sql.ErrNoRows) {
				return existingErr
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sessions(id,project_id,title,provider,model,status,created_at,updated_at,last_reply_at,auto_title)
			VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET project_id=excluded.project_id,title=excluded.title,provider=excluded.provider,
			model=excluded.model,status=excluded.status,created_at=excluded.created_at,updated_at=excluded.updated_at,last_reply_at=excluded.last_reply_at,auto_title=excluded.auto_title`,
			v.ID, v.ProjectID, v.Title, v.Provider, v.Model, v.Status, v.CreatedAt, v.UpdatedAt, v.LastReplyAt, v.AutoTitle); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_details(session_id,data) VALUES(?,?)
			ON CONFLICT(session_id) DO UPDATE SET data=excluded.data`, v.ID, string(raw)); err != nil {
			return err
		}
	}
	if s.beforeSaveCommit != nil {
		s.beforeSaveCommit()
	}
	return tx.Commit()
}

func (s TaskStateStore) Load(ctx context.Context) (projects, sessions []json.RawMessage, err error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,name,path,created_at FROM projects ORDER BY position,id`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var p storedProject
		if err = rows.Scan(&p.ID, &p.Name, &p.Path, &p.CreatedAt); err != nil {
			rows.Close()
			return nil, nil, err
		}
		raw, marshalErr := json.Marshal(p)
		if marshalErr != nil {
			rows.Close()
			return nil, nil, marshalErr
		}
		projects = append(projects, raw)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	rows, err = s.DB.QueryContext(ctx, `SELECT data FROM session_details JOIN sessions ON sessions.id=session_details.session_id ORDER BY sessions.updated_at DESC,sessions.id`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var data string
		if err = rows.Scan(&data); err != nil {
			return nil, nil, err
		}
		if !json.Valid([]byte(data)) {
			return nil, nil, errors.New("stored session detail is invalid JSON")
		}
		sessions = append(sessions, json.RawMessage(data))
	}
	return projects, sessions, rows.Err()
}

func (s TaskStateStore) Session(ctx context.Context, id string) (json.RawMessage, bool, error) {
	var data string
	err := s.DB.QueryRowContext(ctx, `SELECT data FROM session_details WHERE session_id=?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !json.Valid([]byte(data)) {
		return nil, false, errors.New("stored session detail is invalid JSON")
	}
	return json.RawMessage(data), true, nil
}

// SetProviderCursor persists resume ownership immediately after the provider
// handshake instead of waiting for a later UI snapshot.
func (s TaskStateStore) SetProviderCursor(ctx context.Context, id string, cursor json.RawMessage) error {
	if len(cursor) == 0 || !json.Valid(cursor) || string(cursor) == "null" {
		return errors.New("provider cursor is invalid")
	}
	// Update only the daemon-owned field inside SQLite. This is one serialized
	// statement, so a concurrent task-state save cannot be overwritten by a
	// read-modify-write based on stale session JSON.
	result, err := s.DB.ExecContext(ctx, `UPDATE session_details
		SET data=json_set(data,'$.provider_cursor',json(?)) WHERE session_id=?`, string(cursor), id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrTaskSessionNotFound
	}
	return nil
}

func (s TaskStateStore) RemoveSession(ctx context.Context, id string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE session_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM session_details WHERE session_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}
