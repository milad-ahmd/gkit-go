// Package outbox implements the transactional outbox pattern for reliable
// event publishing.
//
// # Pattern
//
// In distributed systems, writing to a database and publishing a message to a
// broker are two separate operations. If the service crashes between them, the
// event is lost. The outbox pattern solves this by:
//
//  1. Writing the event to an outbox table in the SAME database transaction as
//     the business data.
//  2. A background relay process polls the outbox for unpublished events and
//     delivers them to the target broker/bus.
//
// # Schema
//
// Run the following migration before use:
//
//	CREATE TABLE IF NOT EXISTS outbox_events (
//	    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
//	    topic        TEXT        NOT NULL,
//	    payload      JSONB       NOT NULL,
//	    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    published_at TIMESTAMPTZ
//	);
//	CREATE INDEX IF NOT EXISTS outbox_events_unpublished
//	    ON outbox_events (created_at) WHERE published_at IS NULL;
//
// # Usage
//
//	// In your business logic (same tx as your domain write):
//	err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
//	    if err := repo.SaveOrder(ctx, tx, order); err != nil { return err }
//	    return outbox.Store(ctx, tx, "orders.placed", order)
//	})
//
//	// Background relay:
//	relay := outbox.NewRelay(db, publisher, outbox.WithRelayInterval(2*time.Second))
//	relay.Start(ctx)
//	defer relay.Stop()
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/miladhzz/gkit/pkg/store"
)

// Publisher delivers a raw event payload to a topic.
// Implement this interface to wire the relay to any message bus
// (pubsub.Bus, Kafka, NATS, SQS, etc.).
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// --------------------------------------------------------------------------
// Store — write side

// Store inserts an event into the outbox table within the given transaction.
// payload is JSON-encoded; it may be any JSON-serializable value.
func Store(ctx context.Context, tx *store.Tx, topic string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_events (topic, payload) VALUES ($1, $2)`,
		topic, raw,
	)
	if err != nil {
		return fmt.Errorf("outbox: insert event: %w", err)
	}
	return nil
}

// --------------------------------------------------------------------------
// Relay — read/publish side

// Relay polls the outbox table and delivers unpublished events.
type Relay struct {
	db        *store.DB
	publisher Publisher
	opts      relayOptions

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewRelay creates a Relay backed by the given DB and Publisher.
func NewRelay(db *store.DB, publisher Publisher, opts ...RelayOption) *Relay {
	o := defaultRelayOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Relay{
		db:        db,
		publisher: publisher,
		opts:      o,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start begins the background polling loop. It returns immediately;
// the relay runs until Stop is called or ctx is cancelled.
func (r *Relay) Start(ctx context.Context) {
	go r.run(ctx)
}

// Stop signals the relay to stop and waits for it to finish.
func (r *Relay) Stop() {
	r.once.Do(func() { close(r.stop) })
	<-r.done
}

func (r *Relay) run(ctx context.Context) {
	defer close(r.done)

	ticker := time.NewTicker(r.opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.relay(ctx); err != nil {
				r.opts.logger.Error("outbox: relay error", "error", err)
				if r.opts.onError != nil {
					r.opts.onError(err)
				}
			}
		}
	}
}

// relay fetches a batch of unpublished events, publishes each one, and marks
// them as published — all in a single transaction per batch.
func (r *Relay) relay(ctx context.Context) error {
	return r.db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, topic, payload
			FROM outbox_events
			WHERE published_at IS NULL
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED`,
			r.opts.batchSize,
		)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}

		type event struct {
			id      string
			topic   string
			payload []byte
		}
		events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (event, error) {
			var e event
			return e, row.Scan(&e.id, &e.topic, &e.payload)
		})
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if len(events) == 0 {
			return nil
		}

		for _, e := range events {
			if err := r.publisher.Publish(ctx, e.topic, e.payload); err != nil {
				return fmt.Errorf("publish topic %q id %s: %w", e.topic, e.id, err)
			}
		}

		// Bulk-mark as published.
		ids := make([]string, len(events))
		for i, e := range events {
			ids[i] = e.id
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET published_at = NOW()
			WHERE id = ANY($1::uuid[])`, ids,
		); err != nil {
			return fmt.Errorf("mark published: %w", err)
		}

		r.opts.logger.Debug("outbox: relayed events", "count", len(events))
		return nil
	})
}

// --------------------------------------------------------------------------
// Options

type relayOptions struct {
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
	onError   func(error)
}

func defaultRelayOptions() relayOptions {
	return relayOptions{
		interval:  5 * time.Second,
		batchSize: 100,
		logger:    slog.Default(),
	}
}

// RelayOption configures a Relay.
type RelayOption func(*relayOptions)

// WithRelayInterval sets how often the relay polls the outbox table.
func WithRelayInterval(d time.Duration) RelayOption {
	return func(o *relayOptions) { o.interval = d }
}

// WithBatchSize sets the maximum number of events processed per poll cycle.
func WithBatchSize(n int) RelayOption {
	return func(o *relayOptions) { o.batchSize = n }
}

// WithRelayLogger sets the logger for the relay.
func WithRelayLogger(l *slog.Logger) RelayOption {
	return func(o *relayOptions) { o.logger = l }
}

// WithOnError registers a callback invoked when a relay cycle fails.
func WithOnError(fn func(error)) RelayOption {
	return func(o *relayOptions) { o.onError = fn }
}
