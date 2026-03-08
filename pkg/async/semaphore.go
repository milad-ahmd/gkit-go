package async

import (
	"context"
	"time"
)

// --------------------------------------------------------------------------
// Semaphore
//
// A channel-based counting semaphore. The channel buffer acts as the permit
// pool: Acquire sends a token into the channel (blocking when full = 0 permits
// available); Release drains one token.
//
// This is a classic Go pattern — the channel IS the semaphore:
//
//	permits := make(chan struct{}, n)  // n permits
//	permits <- struct{}{}             // Acquire (block if full)
//	<-permits                         // Release

// Semaphore is a channel-based counting semaphore.
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a Semaphore with n total permits.
func NewSemaphore(n int) *Semaphore {
	return &Semaphore{ch: make(chan struct{}, n)}
}

// Acquire blocks until a permit is available or ctx is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}: // channel slot = permit acquired
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire attempts to acquire a permit without blocking.
// Returns true if successful.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release returns a permit to the pool. Must be called exactly once per
// successful Acquire. Panics if called more times than Acquire.
func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
		panic("async: semaphore released more times than acquired")
	}
}

// Available returns the number of currently available permits.
func (s *Semaphore) Available() int { return cap(s.ch) - len(s.ch) }

// Capacity returns the total number of permits.
func (s *Semaphore) Capacity() int { return cap(s.ch) }

// --------------------------------------------------------------------------
// Debounce
//
// Debounce groups rapid calls into one, fired after the quiet period expires.
//
// Pattern: timer reset via channel signal.
//
//	caller ──signal──► goroutine (resets timer) ──after quiet──► fn()
//
// The goroutine resets the timer every time it receives a signal.
// Only when no signal arrives for interval does it call fn.

// Debounce returns a function that delays calling fn until interval has elapsed
// since the last call. Multiple rapid calls result in a single fn invocation.
//
//	save := async.Debounce(500*time.Millisecond, func() { db.Save(state) })
//	// User typing — each keystroke resets the timer:
//	save(); save(); save()  // fn called once, 500ms after the last save()
func Debounce(interval time.Duration, fn func()) func() {
	// trigger carries the signal that "a call happened".
	trigger := make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(interval)
		timer.Stop() // don't fire until first call

		for {
			select {
			case <-trigger:
				// Reset the timer on every call.
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(interval)

			case <-timer.C:
				fn()
			}
		}
	}()

	return func() {
		// Non-blocking send: if trigger already has a pending signal, skip.
		// The goroutine will reset the timer regardless.
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
}

// --------------------------------------------------------------------------
// Throttle
//
// Throttle limits fn to at most one call per limit duration.
//
// Pattern: time.Ticker as a rate gate.
//
//	ticker (1/limit rate) ──► permits channel (capacity 1)
//	caller drains permit ──► calls fn if available

// Throttle returns a function that calls fn at most once per limit duration.
// Additional calls within the window are dropped.
//
//	send := async.Throttle(time.Second, func() { flushBuffer() })
//	// These three calls happen in the same second; only first goes through:
//	send(); send(); send()
func Throttle(limit time.Duration, fn func()) func() {
	permit := make(chan struct{}, 1)
	permit <- struct{}{} // first call goes through immediately

	go func() {
		ticker := time.NewTicker(limit)
		defer ticker.Stop()
		for range ticker.C {
			// Replenish the permit each tick.
			select {
			case permit <- struct{}{}:
			default: // already full; no call was made in this window
			}
		}
	}()

	return func() {
		select {
		case <-permit:
			fn()
		default:
			// Within the limit window, this call is dropped.
		}
	}
}

// --------------------------------------------------------------------------
// Barrier
//
// A Barrier is a rendezvous point: n goroutines call Wait and all block
// until the last one arrives, at which point all are released simultaneously.
//
// Pattern: WaitGroup + done channel broadcast.

// Barrier is a synchronisation point for n goroutines.
type Barrier struct {
	n    int
	wg   *countWg
	done chan struct{}
	once func()
}

// NewBarrier creates a Barrier for n goroutines.
func NewBarrier(n int) *Barrier {
	done := make(chan struct{})
	cw := newCountWg(n)

	b := &Barrier{n: n, wg: cw, done: done}
	b.once = func() {
		go func() {
			cw.wait()
			close(done)
		}()
	}
	b.once()
	return b
}

// Wait blocks until all n goroutines have called Wait.
// ctx cancellation causes an early return with ctx.Err().
func (b *Barrier) Wait(ctx context.Context) error {
	b.wg.arrive()
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// countWg is a simple countdown latch using a channel.
type countWg struct {
	ch chan struct{}
	n  int
}

func newCountWg(n int) *countWg {
	return &countWg{ch: make(chan struct{}, n), n: n}
}

func (c *countWg) arrive() { c.ch <- struct{}{} }

func (c *countWg) wait() {
	for range c.n {
		<-c.ch
	}
}
