package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/store"
)

// TestMain skips the whole package unless TEST_POSTGRES_DSN is set.
// Run with:
//
//	TEST_POSTGRES_DSN="postgres://user:pass@localhost:5432/testdb?sslmode=disable" go test ./pkg/store/...
func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("TEST_POSTGRES_DSN")
	if d == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping integration test")
	}
	return d
}

func TestOpen_Ping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestExec_Query(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Create a temp table, insert, and select.
	if err := db.Exec(ctx,
		`CREATE TEMP TABLE store_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	for i := range 5 {
		if err := db.Exec(ctx,
			`INSERT INTO store_test (name) VALUES ($1)`, fmt.Sprintf("item-%d", i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	rows, err := db.Query(ctx, `SELECT id, name FROM store_test ORDER BY id`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 5 {
		t.Errorf("got %d rows, want 5", count)
	}
}

func TestWithTx_Commit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Exec(ctx,
		`CREATE TEMP TABLE tx_test (id SERIAL PRIMARY KEY, val INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO tx_test (val) VALUES ($1)`, 42)
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM tx_test`).Scan(&count); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestWithTx_Rollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Exec(ctx,
		`CREATE TEMP TABLE rollback_test (id SERIAL PRIMARY KEY, val INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO rollback_test (val) VALUES ($1)`, 99); err != nil {
			return err
		}
		return fmt.Errorf("intentional rollback")
	})
	if err == nil || err.Error() != "intentional rollback" {
		t.Fatalf("expected rollback error, got %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM rollback_test`).Scan(&count); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d after rollback, want 0", count)
	}
}
