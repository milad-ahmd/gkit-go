//go:build integration

package queue_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/queue"
	"github.com/milad-ahmd/gkit-go/pkg/store"
	"github.com/milad-ahmd/gkit-go/pkg/testutil"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 3,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs (run_at) WHERE status = 'pending';`

func setupQueue(t *testing.T) (*store.DB, *queue.Queue) {
	t.Helper()
	ctx := context.Background()

	db := testutil.StartPostgres(t)

	if err := db.Exec(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	q := queue.New(db, queue.WithPollInterval(50*time.Millisecond))
	return db, q
}

func TestQueue_Integration_EnqueueAndProcess(t *testing.T) {
	_, q := setupQueue(t)
	ctx := context.Background()

	type EmailJob struct{ To string }

	var processed atomic.Int32
	q.Register("send-email", func(ctx context.Context, p queue.Payload) error {
		var job EmailJob
		if err := p.Decode(&job); err != nil {
			return err
		}
		processed.Add(1)
		return nil
	})

	// Enqueue 3 jobs.
	for i := range 3 {
		if err := q.Enqueue(ctx, nil, "send-email", EmailJob{To: fmt.Sprintf("user%d@example.com", i)}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	q.Start(ctx, 2)
	defer q.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if processed.Load() < 3 {
		t.Fatalf("expected 3 processed jobs, got %d", processed.Load())
	}
}

func TestQueue_Integration_EnqueueInTransaction(t *testing.T) {
	db, q := setupQueue(t)
	ctx := context.Background()

	var processed atomic.Int32
	q.Register("tx-job", func(ctx context.Context, p queue.Payload) error {
		processed.Add(1)
		return nil
	})

	// Enqueue inside a transaction that commits.
	if err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		return q.Enqueue(ctx, tx, "tx-job", map[string]any{"committed": true})
	}); err != nil {
		t.Fatalf("WithTx enqueue: %v", err)
	}

	q.Start(ctx, 1)
	defer q.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if processed.Load() < 1 {
		t.Fatalf("expected job to be processed, got %d", processed.Load())
	}
}

func TestQueue_Integration_RolledBackTransactionDoesNotEnqueue(t *testing.T) {
	db, q := setupQueue(t)
	ctx := context.Background()

	var processed atomic.Int32
	q.Register("should-not-run", func(ctx context.Context, p queue.Payload) error {
		processed.Add(1)
		return nil
	})

	// Enqueue inside a transaction that rolls back.
	_ = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		_ = q.Enqueue(ctx, tx, "should-not-run", map[string]any{})
		return fmt.Errorf("rollback")
	})

	q.Start(ctx, 1)
	time.Sleep(300 * time.Millisecond)
	q.Stop()

	if processed.Load() != 0 {
		t.Fatalf("expected 0 processed after rollback, got %d", processed.Load())
	}
}
