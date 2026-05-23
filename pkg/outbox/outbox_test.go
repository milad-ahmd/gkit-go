package outbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/outbox"
	"github.com/milad-ahmd/gkit-go/pkg/store"
)

func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("TEST_POSTGRES_DSN")
	if d == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping integration test")
	}
	return d
}

// inMemPublisher collects published events for assertions.
type inMemPublisher struct {
	mu     sync.Mutex
	events []publishedEvent
}

type publishedEvent struct {
	Topic   string
	Payload []byte
}

func (p *inMemPublisher) Publish(_ context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, publishedEvent{Topic: topic, Payload: payload})
	return nil
}

func (p *inMemPublisher) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func setupDB(t *testing.T) *store.DB {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, dsn(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(db.Close)

	// Create outbox table.
	err = db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS outbox_events (
			id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
			topic        TEXT        NOT NULL,
			payload      JSONB       NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			published_at TIMESTAMPTZ
		)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `DELETE FROM outbox_events`)
	})

	return db
}

func TestStore_InsertsEvent(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)

	type orderPlaced struct {
		ID string `json:"id"`
	}

	err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		return outbox.Store(ctx, tx, "orders.placed", orderPlaced{ID: "order-1"})
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&count); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestRelay_PublishesAndMarks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := setupDB(t)
	pub := &inMemPublisher{}

	// Insert 3 events.
	for i := range 3 {
		err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
			return outbox.Store(ctx, tx, "test.topic", map[string]any{"n": i})
		})
		if err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	relay := outbox.NewRelay(db, pub,
		outbox.WithRelayInterval(100*time.Millisecond),
		outbox.WithBatchSize(10),
	)
	relay.Start(ctx)
	defer relay.Stop()

	// Wait for relay to process.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pub.Len() == 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pub.Len() != 3 {
		t.Errorf("published %d events, want 3", pub.Len())
	}

	// All events should now be marked published.
	var pending int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending events after relay = %d, want 0", pending)
	}
}

func TestRelay_RollbackDoesNotStoreEvent(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)

	// Store inside a rolled-back transaction.
	_ = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if err := outbox.Store(ctx, tx, "ghost.topic", map[string]any{"x": 1}); err != nil {
			return err
		}
		return fmt.Errorf("intentional rollback")
	})

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events`).Scan(&count); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d after rollback, want 0", count)
	}
}

func TestRelay_PayloadIsValidJSON(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	pub := &inMemPublisher{}

	type payload struct {
		OrderID string  `json:"order_id"`
		Total   float64 `json:"total"`
	}

	err := db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		return outbox.Store(ctx, tx, "orders.placed", payload{OrderID: "x1", Total: 99.5})
	})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	relay := outbox.NewRelay(db, pub,
		outbox.WithRelayInterval(50*time.Millisecond),
	)
	relay.Start(ctx)
	defer relay.Stop()

	time.Sleep(300 * time.Millisecond)

	if pub.Len() == 0 {
		t.Fatal("no events published")
	}

	var got payload
	if err := json.Unmarshal(pub.events[0].Payload, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.OrderID != "x1" || got.Total != 99.5 {
		t.Errorf("got = %+v", got)
	}
}
