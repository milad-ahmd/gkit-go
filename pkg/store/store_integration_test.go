//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/store"
	"github.com/milad-ahmd/gkit-go/pkg/testutil"
)

func TestStore_Integration_Ping(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestStore_Integration_InsertAndQuery(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Exec(ctx, `CREATE TABLE gkit_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := db.Exec(ctx, `INSERT INTO gkit_test (name) VALUES ($1)`, "hello"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var name string
	if err := db.QueryRow(ctx, `SELECT name FROM gkit_test WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if name != "hello" {
		t.Fatalf("want %q, got %q", "hello", name)
	}
}

func TestStore_Integration_TransactionCommit(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Exec(ctx, `CREATE TABLE gkit_commit (id SERIAL PRIMARY KEY, val TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO gkit_commit (val) VALUES ($1)`, "committed")
		return err
	}); err != nil {
		t.Fatalf("with tx: %v", err)
	}

	var val string
	if err := db.QueryRow(ctx, `SELECT val FROM gkit_commit LIMIT 1`).Scan(&val); err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "committed" {
		t.Fatalf("want %q, got %q", "committed", val)
	}
}

func TestStore_Integration_TransactionRollback(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Exec(ctx, `CREATE TABLE gkit_rollback (id SERIAL PRIMARY KEY, val TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	intentional := errors.New("intentional rollback")
	err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO gkit_rollback (val) VALUES ($1)`, "should-not-exist"); err != nil {
			return err
		}
		return intentional
	})
	if !errors.Is(err, intentional) {
		t.Fatalf("expected intentional error, got %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM gkit_rollback`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}
