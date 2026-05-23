//go:build integration

package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/eventstore"
	"github.com/milad-ahmd/gkit-go/pkg/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
    stream_id   TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    type        TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id   TEXT        PRIMARY KEY,
    version     BIGINT      NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

func setupStore(t *testing.T) (*store.DB, *eventstore.Store) {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "gkit",
				"POSTGRES_PASSWORD": "secret",
				"POSTGRES_DB":       "gkit",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432/tcp")
	dsn := fmt.Sprintf("postgres://gkit:secret@%s:%s/gkit?sslmode=disable", host, port.Port())

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(db.Close)

	if err := db.Exec(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return db, eventstore.New(db)
}

func TestEventStore_Integration_AppendAndLoad(t *testing.T) {
	_, es := setupStore(t)
	ctx := context.Background()

	type OrderPlaced struct{ OrderID string }

	err := es.Append(ctx, "order-1", []eventstore.EventData{
		{Type: "OrderPlaced", Data: OrderPlaced{"order-1"}},
		{Type: "StockReserved", Data: map[string]any{"sku": "ABC", "qty": 2}},
	}, eventstore.ExpectedVersionNew)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	events, err := es.Load(ctx, "order-1", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Type != "OrderPlaced" {
		t.Fatalf("expected OrderPlaced, got %q", events[0].Type)
	}
	if events[0].Version != 0 {
		t.Fatalf("expected version 0, got %d", events[0].Version)
	}
	if events[1].Version != 1 {
		t.Fatalf("expected version 1, got %d", events[1].Version)
	}
}

func TestEventStore_Integration_VersionConflict(t *testing.T) {
	_, es := setupStore(t)
	ctx := context.Background()

	// Append to new stream.
	if err := es.Append(ctx, "order-2", []eventstore.EventData{
		{Type: "OrderPlaced", Data: map[string]any{}},
	}, eventstore.ExpectedVersionNew); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Trying to create it again should conflict.
	err := es.Append(ctx, "order-2", []eventstore.EventData{
		{Type: "OrderPlaced", Data: map[string]any{}},
	}, eventstore.ExpectedVersionNew)
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
}

func TestEventStore_Integration_StreamNotFound(t *testing.T) {
	_, es := setupStore(t)
	ctx := context.Background()

	_, err := es.Load(ctx, "nonexistent", 0)
	if !errors.Is(err, eventstore.ErrStreamNotFound) {
		t.Fatalf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestEventStore_Integration_SnapshotRoundTrip(t *testing.T) {
	_, es := setupStore(t)
	ctx := context.Background()

	type State struct{ Count int }

	if err := es.Append(ctx, "order-3", []eventstore.EventData{
		{Type: "OrderPlaced", Data: map[string]any{}},
	}, eventstore.ExpectedVersionNew); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := es.SaveSnapshot(ctx, "order-3", 0, State{Count: 1}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, err := es.LoadSnapshot(ctx, "order-3")
	if err != nil || snap == nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Version != 0 {
		t.Fatalf("expected version 0, got %d", snap.Version)
	}

	var s State
	if err := snap.Decode(&s); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if s.Count != 1 {
		t.Fatalf("expected Count=1, got %d", s.Count)
	}
}

func TestEventStore_Integration_CurrentVersion(t *testing.T) {
	_, es := setupStore(t)
	ctx := context.Background()

	if err := es.Append(ctx, "order-4", []eventstore.EventData{
		{Type: "E1", Data: map[string]any{}},
		{Type: "E2", Data: map[string]any{}},
		{Type: "E3", Data: map[string]any{}},
	}, eventstore.ExpectedVersionNew); err != nil {
		t.Fatalf("Append: %v", err)
	}

	v, err := es.CurrentVersion(ctx, "order-4")
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if v != 2 {
		t.Fatalf("expected version 2, got %d", v)
	}
}
