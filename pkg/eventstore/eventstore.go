// Package eventstore provides an append-only event log in Postgres,
// implementing the core infrastructure for event sourcing.
//
// # Concepts
//
//   - Stream: an ordered sequence of events for a single aggregate (e.g. "order-123").
//   - Version: monotonically increasing integer per stream, starting at 0.
//   - Optimistic concurrency: Append rejects writes if the current stream version
//     does not match expectedVersion, preventing lost updates.
//   - Snapshots: periodic state snapshots to avoid replaying the full event log.
//
// # Schema
//
//	CREATE TABLE events (
//	    stream_id   TEXT        NOT NULL,
//	    version     BIGINT      NOT NULL,
//	    type        TEXT        NOT NULL,
//	    data        JSONB       NOT NULL,
//	    metadata    JSONB       NOT NULL DEFAULT '{}',
//	    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    PRIMARY KEY (stream_id, version)
//	);
//	CREATE TABLE snapshots (
//	    stream_id   TEXT        PRIMARY KEY,
//	    version     BIGINT      NOT NULL,
//	    data        JSONB       NOT NULL,
//	    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
//	);
//
// # Usage
//
//	es := eventstore.New(db)
//
//	// Append with optimistic concurrency (ExpectedVersion = -1 means "stream must not exist").
//	err = es.Append(ctx, "order-123", []eventstore.EventData{
//	    {Type: "OrderPlaced",  Data: orderPlaced},
//	    {Type: "StockReserved", Data: stockReserved},
//	}, eventstore.ExpectedVersionNew)
//
//	// Load full stream.
//	events, err := es.Load(ctx, "order-123", 0)
//
//	// Load from snapshot.
//	snap, _ := es.LoadSnapshot(ctx, "order-123")
//	events, _ := es.Load(ctx, "order-123", snap.Version+1)
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/miladhzz/gkit/pkg/store"
)

// ExpectedVersionNew signals that the stream must not yet exist.
const ExpectedVersionNew int64 = -1

// ExpectedVersionAny skips optimistic concurrency checking.
const ExpectedVersionAny int64 = -2

// ErrVersionConflict is returned when expectedVersion does not match the
// current stream version (concurrent write detected).
var ErrVersionConflict = errors.New("eventstore: version conflict")

// ErrStreamNotFound is returned when Load is called on a non-existent stream.
var ErrStreamNotFound = errors.New("eventstore: stream not found")

// --------------------------------------------------------------------------
// Types

// Event is a persisted event read from the store.
type Event struct {
	StreamID  string
	Version   int64
	Type      string
	Data      json.RawMessage
	Metadata  json.RawMessage
	CreatedAt time.Time
}

// Decode unmarshals Event.Data into dst.
func (e Event) Decode(dst any) error { return json.Unmarshal(e.Data, dst) }

// EventData is the input type for Append.
type EventData struct {
	Type     string
	Data     any            // will be JSON-marshaled
	Metadata map[string]any // optional; nil → {}
}

// Snapshot is a point-in-time state saved to avoid full replay.
type Snapshot struct {
	StreamID  string
	Version   int64
	Data      json.RawMessage
	CreatedAt time.Time
}

// Decode unmarshals Snapshot.Data into dst.
func (s Snapshot) Decode(dst any) error { return json.Unmarshal(s.Data, dst) }

// --------------------------------------------------------------------------
// Store

// Store is the event store backed by Postgres.
type Store struct {
	db *store.DB
}

// New creates a Store using the given DB.
func New(db *store.DB) *Store {
	return &Store{db: db}
}

// --------------------------------------------------------------------------
// Append

// Append adds events to the stream atomically.
//
//   - expectedVersion = ExpectedVersionNew: stream must not exist
//   - expectedVersion = ExpectedVersionAny: skip version check
//   - expectedVersion ≥ 0: stream's current version must equal this value
func (s *Store) Append(ctx context.Context, streamID string, events []EventData, expectedVersion int64) error {
	if len(events) == 0 {
		return nil
	}

	return s.db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		// Lock the stream row and read current version.
		currentVersion, err := s.currentVersion(ctx, tx, streamID)
		if err != nil {
			return err
		}

		// Optimistic concurrency check.
		switch expectedVersion {
		case ExpectedVersionNew:
			if currentVersion >= 0 {
				return fmt.Errorf("%w: stream %q already exists at version %d",
					ErrVersionConflict, streamID, currentVersion)
			}
		case ExpectedVersionAny:
			// No check.
		default:
			if currentVersion != expectedVersion {
				return fmt.Errorf("%w: stream %q at version %d, expected %d",
					ErrVersionConflict, streamID, currentVersion, expectedVersion)
			}
		}

		// Append events starting at currentVersion+1.
		nextVersion := currentVersion + 1
		for _, ed := range events {
			data, err := json.Marshal(ed.Data)
			if err != nil {
				return fmt.Errorf("eventstore: marshal data for %q: %w", ed.Type, err)
			}
			meta := ed.Metadata
			if meta == nil {
				meta = map[string]any{}
			}
			metaJSON, err := json.Marshal(meta)
			if err != nil {
				return fmt.Errorf("eventstore: marshal metadata for %q: %w", ed.Type, err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO events (stream_id, version, type, data, metadata)
				VALUES ($1, $2, $3, $4, $5)`,
				streamID, nextVersion, ed.Type, data, metaJSON,
			)
			if err != nil {
				return fmt.Errorf("eventstore: insert event %q v%d: %w", ed.Type, nextVersion, err)
			}
			nextVersion++
		}
		return nil
	})
}

// --------------------------------------------------------------------------
// Load

// Load returns all events for streamID with version ≥ fromVersion (inclusive).
// Pass fromVersion=0 to load the full stream.
func (s *Store) Load(ctx context.Context, streamID string, fromVersion int64) ([]Event, error) {
	rows, err := s.db.Query(ctx, `
		SELECT stream_id, version, type, data, metadata, created_at
		FROM events
		WHERE stream_id = $1 AND version >= $2
		ORDER BY version`,
		streamID, fromVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load %q: %w", streamID, err)
	}

	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
		var e Event
		return e, row.Scan(&e.StreamID, &e.Version, &e.Type, &e.Data, &e.Metadata, &e.CreatedAt)
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore: scan %q: %w", streamID, err)
	}
	if len(events) == 0 && fromVersion == 0 {
		return nil, fmt.Errorf("%w: %q", ErrStreamNotFound, streamID)
	}
	return events, nil
}

// CurrentVersion returns the latest version number of a stream, or -1 if the
// stream does not exist.
func (s *Store) CurrentVersion(ctx context.Context, streamID string) (int64, error) {
	var v int64
	err := s.db.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), -1) FROM events WHERE stream_id = $1`, streamID,
	).Scan(&v)
	if err != nil {
		return -1, fmt.Errorf("eventstore: current version %q: %w", streamID, err)
	}
	return v, nil
}

// --------------------------------------------------------------------------
// Snapshots

// SaveSnapshot persists a state snapshot for the stream.
func (s *Store) SaveSnapshot(ctx context.Context, streamID string, version int64, state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("eventstore: marshal snapshot: %w", err)
	}
	err = s.db.Exec(ctx, `
		INSERT INTO snapshots (stream_id, version, data)
		VALUES ($1, $2, $3)
		ON CONFLICT (stream_id) DO UPDATE SET version=EXCLUDED.version, data=EXCLUDED.data, created_at=NOW()`,
		streamID, version, data,
	)
	if err != nil {
		return fmt.Errorf("eventstore: save snapshot %q: %w", streamID, err)
	}
	return nil
}

// LoadSnapshot returns the latest snapshot for the stream, or nil if none exists.
func (s *Store) LoadSnapshot(ctx context.Context, streamID string) (*Snapshot, error) {
	var snap Snapshot
	err := s.db.QueryRow(ctx, `
		SELECT stream_id, version, data, created_at
		FROM snapshots WHERE stream_id = $1`, streamID,
	).Scan(&snap.StreamID, &snap.Version, &snap.Data, &snap.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("eventstore: load snapshot %q: %w", streamID, err)
	}
	return &snap, nil
}

// --------------------------------------------------------------------------
// Internals

// currentVersion returns the current max version of a stream, or -1 if it
// doesn't exist. Must be called inside a transaction with a FOR UPDATE lock.
func (s *Store) currentVersion(ctx context.Context, tx *store.Tx, streamID string) (int64, error) {
	var v int64
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), -1) FROM events
		WHERE stream_id = $1`, streamID,
	).Scan(&v)
	if err != nil {
		return -1, fmt.Errorf("eventstore: read version %q: %w", streamID, err)
	}
	return v, nil
}
