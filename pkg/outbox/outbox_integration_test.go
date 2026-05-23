//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/outbox"
	"github.com/milad-ahmd/gkit-go/pkg/store"
	"github.com/milad-ahmd/gkit-go/pkg/testutil"
)

const schema = `
CREATE TABLE IF NOT EXISTS outbox_events (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);`

// mockPublisher records published events.
type mockPublisher struct {
	mu     sync.Mutex
	events []struct{ topic string; payload []byte }
}

func (m *mockPublisher) Publish(_ context.Context, topic string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, struct{ topic string; payload []byte }{topic, payload})
	return nil
}

func (m *mockPublisher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func TestOutbox_Integration_StoreAndRelay(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Exec(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	// Store two events in a transaction.
	if err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if err := outbox.Store(ctx, tx, "orders.placed", map[string]any{"id": "1"}); err != nil {
			return err
		}
		return outbox.Store(ctx, tx, "orders.placed", map[string]any{"id": "2"})
	}); err != nil {
		t.Fatalf("store events: %v", err)
	}

	pub := &mockPublisher{}
	relay := outbox.NewRelay(db, pub,
		outbox.WithRelayInterval(50*time.Millisecond),
		outbox.WithBatchSize(10),
	)
	relay.Start(ctx)
	defer relay.Stop()

	// Wait up to 2s for both events to be relayed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pub.count() >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if pub.count() < 2 {
		t.Fatalf("expected 2 relayed events, got %d", pub.count())
	}
}

func TestOutbox_Integration_RollbackDoesNotPublish(t *testing.T) {
	db := testutil.StartPostgres(t)
	ctx := context.Background()

	if err := db.Exec(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	// Store in a rolled-back transaction.
	_ = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		_ = outbox.Store(ctx, tx, "should.not.publish", map[string]any{"id": "x"})
		return fmt.Errorf("intentional rollback")
	})

	pub := &mockPublisher{}
	relay := outbox.NewRelay(db, pub, outbox.WithRelayInterval(50*time.Millisecond))
	relay.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	relay.Stop()

	if pub.count() != 0 {
		t.Fatalf("expected 0 relayed events after rollback, got %d", pub.count())
	}
}
