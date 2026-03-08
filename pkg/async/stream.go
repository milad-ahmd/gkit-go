package async

import (
	"context"
	"sync"
	"time"
)

// --------------------------------------------------------------------------
// Stream[T]
//
// Stream is a lazy, push-based async data stream with backpressure.
//
// Internally it is a goroutine writing to a buffered channel. The buffer
// provides bounded backpressure: when full, the producer blocks until the
// consumer drains an item. This is the canonical Go producer-consumer pattern.
//
//	Goroutine (producer) ──► chan T (buffer) ──► Goroutine/caller (consumer)
//
// Operators (Map, Filter, Batch) each create a new goroutine and channel,
// forming a pipeline of goroutines connected by channels.

// Stream is a channel-based asynchronous sequence of values.
type Stream[T any] struct {
	ch    <-chan T
	errCh <-chan error
}

// newStream is the internal constructor used by operators.
func newStream[T any](ch <-chan T, errCh <-chan error) *Stream[T] {
	return &Stream[T]{ch: ch, errCh: errCh}
}

// Generate creates a Stream from a producer function.
// The producer calls send(item) to push values; returning false from send
// means the stream was cancelled and the producer should stop.
//
//	s := async.Generate(ctx, func(ctx context.Context, send func(int) bool) {
//	    for i := 0; i < 100; i++ {
//	        if !send(i) { return }
//	    }
//	})
func Generate[T any](ctx context.Context, fn func(ctx context.Context, send func(T) bool)) *Stream[T] {
	// Buffer of 16 gives the producer a head start without unbounded memory.
	ch := make(chan T, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		fn(ctx, func(v T) bool {
			select {
			case <-ctx.Done():
				return false
			case ch <- v:
				return true
			}
		})
	}()

	return newStream(ch, errCh)
}

// FromSlice creates a Stream that emits all values from slice s.
func FromSlice[T any](ctx context.Context, s []T) *Stream[T] {
	return Generate(ctx, func(ctx context.Context, send func(T) bool) {
		for _, v := range s {
			if !send(v) {
				return
			}
		}
	})
}

// --------------------------------------------------------------------------
// Terminal operators

// Collect drains the stream and returns all items as a slice.
func (s *Stream[T]) Collect(ctx context.Context) ([]T, error) {
	var out []T
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case v, ok := <-s.ch:
			if !ok {
				// Channel closed; check for error.
				select {
				case err := <-s.errCh:
					return out, err
				default:
					return out, nil
				}
			}
			out = append(out, v)
		}
	}
}

// ForEach calls fn for each item. Returns the first error from fn or the stream.
func (s *Stream[T]) ForEach(ctx context.Context, fn func(T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-s.ch:
			if !ok {
				select {
				case err := <-s.errCh:
					return err
				default:
					return nil
				}
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
}

// --------------------------------------------------------------------------
// Intermediate operators
//
// Each operator spawns a new goroutine and returns a new Stream.
// This forms a pipeline:
//
//	Generate ──ch──► Map ──ch──► Filter ──ch──► Batch ──ch──► Collect

// Map transforms each item with fn in a new goroutine.
func Map[In, Out any](s *Stream[In], fn func(In) Out) *Stream[Out] {
	ch := make(chan Out, cap(s.ch))
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)
		for v := range s.ch {
			ch <- fn(v)
		}
		// Propagate upstream error.
		if err := <-s.errCh; err != nil {
			errCh <- err
		}
	}()

	return newStream(ch, errCh)
}

// Filter passes only items for which keep returns true.
func Filter[T any](s *Stream[T], keep func(T) bool) *Stream[T] {
	ch := make(chan T, cap(s.ch))
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)
		for v := range s.ch {
			if keep(v) {
				ch <- v
			}
		}
		if err := <-s.errCh; err != nil {
			errCh <- err
		}
	}()

	return newStream(ch, errCh)
}

// Batch accumulates items into slices of at most size, or flushes after
// timeout since the last item was received.
//
//	Pattern: two-case select — new item extends the batch, timeout flushes it.
func Batch[T any](s *Stream[T], size int, timeout time.Duration) *Stream[[]T] {
	ch := make(chan []T, 4)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		buf := make([]T, 0, size)
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		flush := func() {
			if len(buf) > 0 {
				// Send a copy — buf will be reused.
				batch := make([]T, len(buf))
				copy(batch, buf)
				ch <- batch
				buf = buf[:0]
			}
			// Reset timer without leaking: drain first.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		}

		for {
			select {
			case v, ok := <-s.ch:
				if !ok {
					flush()
					if err := <-s.errCh; err != nil {
						errCh <- err
					}
					return
				}
				buf = append(buf, v)
				if len(buf) >= size {
					flush()
				}
			case <-timer.C:
				flush()
			}
		}
	}()

	return newStream(ch, errCh)
}

// Take returns a new Stream that emits at most n items.
func Take[T any](s *Stream[T], n int) *Stream[T] {
	ch := make(chan T, cap(s.ch))
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)
		count := 0
		for v := range s.ch {
			if count >= n {
				return
			}
			ch <- v
			count++
		}
	}()

	return newStream(ch, errCh)
}

// --------------------------------------------------------------------------
// FanOut — broadcast one channel to N independent receivers
//
// Pattern:
//
//	single in ──► goroutine dispatcher ──► [ch0, ch1, ch2, ...]
//
// Each output channel is buffered; slow receivers experience backpressure
// independently without blocking each other via a per-output select+default.
// If a receiver is full, the item is dropped for that receiver.

// FanOut broadcasts all items from in to n independent output channels.
// Each output channel receives a copy of every item.
func FanOut[T any](ctx context.Context, in <-chan T, n int) []<-chan T {
	outs := make([]chan T, n)
	for i := range outs {
		outs[i] = make(chan T, 16)
	}

	go func() {
		// Close all output channels when done.
		defer func() {
			for _, ch := range outs {
				close(ch)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				// Broadcast to all outputs.
				// Non-blocking send so one slow receiver doesn't block others.
				for _, out := range outs {
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Return as read-only channels.
	ros := make([]<-chan T, n)
	for i, ch := range outs {
		ros[i] = ch
	}
	return ros
}

// --------------------------------------------------------------------------
// FanIn (Merge) — combine N channels into one
//
// Pattern:
//
//	[ch0, ch1, ch2, ...] ──► goroutine per input ──► single out chan
//
// One goroutine per input; a WaitGroup closes the output after all inputs end.

// FanIn merges multiple input channels into one output channel.
// Terminates when all inputs are closed or ctx is cancelled.
func FanIn[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	out := make(chan T, 16)
	var wg sync.WaitGroup

	// One goroutine per input channel.
	// Each forwards items until its input closes or ctx is done.
	forward := func(in <-chan T) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	wg.Add(len(inputs))
	for _, in := range inputs {
		go forward(in)
	}

	// Close output once all inputs are done.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// --------------------------------------------------------------------------
// Tee — duplicate a stream into two independent receivers
//
// Pattern: one goroutine reads from in and sends to both a and b.
// Uses separate buffered channels so neither receiver blocks the other.

// Tee duplicates in into two independent channels.
// Items from in are delivered to both a and b.
func Tee[T any](ctx context.Context, in <-chan T) (<-chan T, <-chan T) {
	a := make(chan T, 16)
	b := make(chan T, 16)

	go func() {
		defer close(a)
		defer close(b)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				// Send to both; ctx cancellation prevents deadlock.
				select {
				case a <- v:
				case <-ctx.Done():
					return
				}
				select {
				case b <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return a, b
}

// --------------------------------------------------------------------------
// OrDone — unblock a channel receive when ctx is done
//
// Pattern: wrap any <-chan T so callers can range over it safely with
// context cancellation, without needing a select in every loop body.

// OrDone returns a channel that closes when either in closes or ctx is done.
//
//	for v := range async.OrDone(ctx, events) {
//	    process(v)
//	}
func OrDone[T any](ctx context.Context, in <-chan T) <-chan T {
	out := make(chan T, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
