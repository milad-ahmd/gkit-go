// Package queue provides a durable, Postgres-backed job queue.
//
// # Design
//
//   - Jobs are stored in a Postgres table with status: pending → running → done/failed.
//   - Workers use SELECT FOR UPDATE SKIP LOCKED for safe concurrent polling.
//   - Failed jobs are retried up to MaxAttempts with exponential backoff.
//   - Jobs beyond MaxAttempts are moved to a dead-letter state for inspection.
//   - Enqueue participates in the caller's transaction: if the tx rolls back,
//     the job is never created — atomic enqueue.
//
// # Schema (run via migration)
//
//	CREATE TABLE jobs (
//	    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
//	    type          TEXT        NOT NULL,
//	    payload       JSONB       NOT NULL,
//	    status        TEXT        NOT NULL DEFAULT 'pending',
//	    attempts      INT         NOT NULL DEFAULT 0,
//	    max_attempts  INT         NOT NULL DEFAULT 3,
//	    run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    last_error    TEXT,
//	    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
//	);
//	CREATE INDEX jobs_pending ON jobs (run_at) WHERE status = 'pending';
//
// # Usage
//
//	q := queue.New(db)
//
//	// Register handlers.
//	q.Register("send-email", func(ctx context.Context, p queue.Payload) error {
//	    var e EmailJob
//	    if err := p.Decode(&e); err != nil { return err }
//	    return mailer.Send(ctx, e)
//	})
//
//	// Enqueue (optionally in a transaction alongside domain writes).
//	err = db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
//	    if err := orderRepo.Save(ctx, tx, order); err != nil { return err }
//	    return q.Enqueue(ctx, tx, "send-email", EmailJob{To: order.Email}, queue.WithDelay(5*time.Second))
//	})
//
//	// Start workers.
//	q.Start(ctx, 4) // 4 concurrent workers
//	defer q.Stop()
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/milad-ahmd/gkit-go/pkg/store"
)

// Handler processes a job of a specific type.
type Handler func(ctx context.Context, payload Payload) error

// Payload wraps the raw JSON bytes stored in the job.
type Payload struct{ raw json.RawMessage }

// Decode unmarshals the payload into dst.
func (p Payload) Decode(dst any) error { return json.Unmarshal(p.raw, dst) }

// Raw returns the underlying JSON bytes.
func (p Payload) Raw() json.RawMessage { return p.raw }

// --------------------------------------------------------------------------
// Job

// Job represents a persisted unit of work.
type Job struct {
	ID          string
	Type        string
	Payload     Payload
	Status      string
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	LastError   string
	CreatedAt   time.Time
}

// --------------------------------------------------------------------------
// Enqueue options

type enqueueOpts struct {
	maxAttempts int
	delay       time.Duration
}

// EnqueueOption configures Enqueue behaviour.
type EnqueueOption func(*enqueueOpts)

// WithMaxAttempts sets the maximum number of execution attempts (default: 3).
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *enqueueOpts) { o.maxAttempts = n }
}

// WithDelay schedules the job to run after d.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *enqueueOpts) { o.delay = d }
}

// --------------------------------------------------------------------------
// Queue

// Queue manages job persistence and dispatching.
type Queue struct {
	db       *store.DB
	handlers map[string]Handler
	mu       sync.RWMutex
	log      *slog.Logger
	poll     time.Duration

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// New creates a Queue backed by the given DB.
func New(db *store.DB, opts ...Option) *Queue {
	q := &Queue{
		db:       db,
		handlers: make(map[string]Handler),
		log:      slog.Default(),
		poll:     2 * time.Second,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

// Option configures a Queue.
type Option func(*Queue)

// WithPollInterval sets how often workers poll for pending jobs (default: 2s).
func WithPollInterval(d time.Duration) Option {
	return func(q *Queue) { q.poll = d }
}

// WithQueueLogger sets the logger.
func WithQueueLogger(l *slog.Logger) Option {
	return func(q *Queue) { q.log = l }
}

// Register adds a handler for the given job type.
// Safe to call before Start. Not safe to call concurrently with itself.
func (q *Queue) Register(jobType string, h Handler) {
	q.mu.Lock()
	q.handlers[jobType] = h
	q.mu.Unlock()
}

// --------------------------------------------------------------------------
// Enqueue

// Enqueue inserts a job into the jobs table within the given transaction.
// If tx is nil, a fresh connection is used (no transactional guarantee).
func (q *Queue) Enqueue(ctx context.Context, tx *store.Tx, jobType string, payload any, opts ...EnqueueOption) error {
	o := &enqueueOpts{maxAttempts: 3}
	for _, opt := range opts {
		opt(o)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("queue: marshal payload: %w", err)
	}

	runAt := time.Now()
	if o.delay > 0 {
		runAt = runAt.Add(o.delay)
	}

	sql := `INSERT INTO jobs (type, payload, max_attempts, run_at)
	        VALUES ($1, $2, $3, $4)`

	if tx != nil {
		_, err = tx.Exec(ctx, sql, jobType, raw, o.maxAttempts, runAt)
	} else {
		err = q.db.Exec(ctx, sql, jobType, raw, o.maxAttempts, runAt)
	}
	if err != nil {
		return fmt.Errorf("queue: enqueue %q: %w", jobType, err)
	}
	return nil
}

// --------------------------------------------------------------------------
// Workers

// Start launches n concurrent workers. Each worker polls independently.
func (q *Queue) Start(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.workerLoop(ctx)
		}()
	}
	// Close done when all workers exit.
	go func() {
		wg.Wait()
		close(q.done)
	}()
}

// Stop signals all workers to stop and waits for them to finish.
func (q *Queue) Stop() {
	q.once.Do(func() { close(q.stop) })
	<-q.done
}

func (q *Queue) workerLoop(ctx context.Context) {
	ticker := time.NewTicker(q.poll)
	defer ticker.Stop()

	for {
		select {
		case <-q.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := q.processBatch(ctx); err != nil {
				q.log.Error("queue: process batch failed", "error", err)
			}
		}
	}
}

func (q *Queue) processBatch(ctx context.Context) error {
	return q.db.WithTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, type, payload, attempts, max_attempts
			FROM jobs
			WHERE status = 'pending' AND run_at <= NOW()
			ORDER BY run_at
			LIMIT 10
			FOR UPDATE SKIP LOCKED`)
		if err != nil {
			return err
		}

		jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) {
			var j Job
			var raw json.RawMessage
			err := row.Scan(&j.ID, &j.Type, &raw, &j.Attempts, &j.MaxAttempts)
			j.Payload = Payload{raw: raw}
			return j, err
		})
		if err != nil {
			return err
		}

		for _, job := range jobs {
			q.executeJob(ctx, tx, job)
		}
		return nil
	})
}

func (q *Queue) executeJob(ctx context.Context, tx *store.Tx, job Job) {
	q.mu.RLock()
	h, ok := q.handlers[job.Type]
	q.mu.RUnlock()

	if !ok {
		q.log.Warn("queue: no handler for job type", "type", job.Type, "id", job.ID)
		_, _ = tx.Exec(ctx, `UPDATE jobs SET status='failed', last_error=$1, updated_at=NOW() WHERE id=$2`,
			fmt.Sprintf("no handler for type %q", job.Type), job.ID)
		return
	}

	err := h(ctx, job.Payload)
	attempts := job.Attempts + 1

	if err == nil {
		_, _ = tx.Exec(ctx, `UPDATE jobs SET status='done', attempts=$1, updated_at=NOW() WHERE id=$2`, attempts, job.ID)
		q.log.Info("queue: job done", "type", job.Type, "id", job.ID)
		return
	}

	q.log.Error("queue: job failed", "type", job.Type, "id", job.ID, "attempts", attempts, "error", err)

	if attempts >= job.MaxAttempts {
		_, _ = tx.Exec(ctx, `UPDATE jobs SET status='dead', attempts=$1, last_error=$2, updated_at=NOW() WHERE id=$3`,
			attempts, err.Error(), job.ID)
		return
	}

	// Exponential backoff: 2^attempts * 10s (capped at 1h).
	backoff := time.Duration(1<<attempts) * 10 * time.Second
	if backoff > time.Hour {
		backoff = time.Hour
	}
	runAt := time.Now().Add(backoff)
	_, _ = tx.Exec(ctx, `UPDATE jobs SET status='pending', attempts=$1, last_error=$2, run_at=$3, updated_at=NOW() WHERE id=$4`,
		attempts, err.Error(), runAt, job.ID)
}
