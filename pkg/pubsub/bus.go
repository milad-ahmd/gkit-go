// Package pubsub provides a typed, in-process publish/subscribe event bus.
//
// Topics are plain strings. Subscribers receive events in their own goroutine,
// providing fan-out with isolation. Slow subscribers can opt into a buffered
// channel to avoid blocking publishers.
//
// Because Go generics don't support methods with new type parameters, the
// top-level Subscribe and Publish functions carry the type information:
//
//	unsub := pubsub.Subscribe[OrderPlaced](bus, "orders.placed", func(ctx context.Context, e pubsub.Event[OrderPlaced]) error {
//	    return processOrder(ctx, e.Payload)
//	})
//	defer unsub()
//
//	pubsub.Publish(bus, ctx, "orders.placed", OrderPlaced{ID: "123"})
package pubsub

import (
	"context"
	"log/slog"
	"sync"
)

// Event carries a typed payload along with contextual metadata.
type Event[T any] struct {
	Topic   string
	Payload T
}

// Handler processes a single event. A non-nil return value is logged but
// does not affect other subscribers.
type Handler[T any] func(ctx context.Context, event Event[T]) error

// rawHandler wraps a type-erased publish operation so the Bus can store
// heterogeneous subscriptions in a single map.
type rawHandler struct {
	bufSize int
	publish func(ctx context.Context, payload any)
	cancel  func()
}

// Bus is a topic-based event bus. The zero value is ready to use.
type Bus struct {
	mu     sync.RWMutex
	subs   map[string][]*rawHandler
	logger *slog.Logger
}

// NewBus creates a Bus with an optional structured logger for error reporting.
func NewBus(logger *slog.Logger) *Bus {
	if logger == nil {
		logger = slog.Default()
	}
	return &Bus{
		subs:   make(map[string][]*rawHandler),
		logger: logger,
	}
}

// Subscribe registers handler h on topic for events of type T.
// It returns an unsubscribe function that must be called to free resources.
//
// bufSize controls the depth of the subscriber's delivery channel; 0 means
// unbuffered (publisher blocks until the handler finishes).
func Subscribe[T any](b *Bus, topic string, h Handler[T], bufSize int) (unsubscribe func()) {
	ch := make(chan func(), max(bufSize, 0))

	ctx, cancel := context.WithCancel(context.Background())

	rh := &rawHandler{
		bufSize: bufSize,
		publish: func(pubCtx context.Context, payload any) {
			evt := Event[T]{Topic: topic, Payload: payload.(T)}
			fn := func() {
				if err := h(pubCtx, evt); err != nil {
					b.logger.ErrorContext(pubCtx, "pubsub handler error",
						slog.String("topic", topic),
						slog.Any("error", err),
					)
				}
			}
			select {
			case ch <- fn:
			default:
				// Drop if channel full (only when bufSize > 0 and full).
				b.logger.WarnContext(pubCtx, "pubsub: subscriber queue full, event dropped",
					slog.String("topic", topic),
				)
			}
		},
		cancel: cancel,
	}

	// Dispatch goroutine — one per subscription.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case fn, ok := <-ch:
				if !ok {
					return
				}
				fn()
			}
		}
	}()

	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], rh)
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		handlers := b.subs[topic]
		for i, s := range handlers {
			if s == rh {
				b.subs[topic] = append(handlers[:i], handlers[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		rh.cancel()
		close(ch)
	}
}

// Publish delivers payload to all current subscribers of topic.
// It is non-blocking for buffered subscribers; unbuffered subscribers may
// delay the caller until they finish processing.
func Publish[T any](b *Bus, ctx context.Context, topic string, payload T) {
	b.mu.RLock()
	handlers := make([]*rawHandler, len(b.subs[topic]))
	copy(handlers, b.subs[topic])
	b.mu.RUnlock()

	for _, h := range handlers {
		h.publish(ctx, payload)
	}
}

// Topics returns a sorted list of topics that have at least one subscriber.
func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]string, 0, len(b.subs))
	for t, handlers := range b.subs {
		if len(handlers) > 0 {
			topics = append(topics, t)
		}
	}
	return topics
}
