package pubsub_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/pubsub"
)

type OrderPlaced struct {
	ID     string
	Amount float64
}

type UserSignedUp struct {
	Email string
}

func TestBus_PublishSubscribe(t *testing.T) {
	bus := pubsub.NewBus(nil)
	var received atomic.Int32

	unsub := pubsub.Subscribe[OrderPlaced](bus, "orders.placed",
		func(_ context.Context, e pubsub.Event[OrderPlaced]) error {
			if e.Payload.ID == "order-1" {
				received.Add(1)
			}
			return nil
		}, 1)
	defer unsub()

	pubsub.Publish[OrderPlaced](bus, context.Background(), "orders.placed",
		OrderPlaced{ID: "order-1", Amount: 99.99})

	// Give the async handler goroutine time to run.
	time.Sleep(20 * time.Millisecond)

	if got := received.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1", got)
	}
}

func TestBus_MultipleSubscribers(t *testing.T) {
	bus := pubsub.NewBus(nil)
	var wg sync.WaitGroup

	const n = 5
	wg.Add(n)

	for range n {
		unsub := pubsub.Subscribe[string](bus, "ping",
			func(_ context.Context, _ pubsub.Event[string]) error {
				wg.Done()
				return nil
			}, 1)
		t.Cleanup(unsub)
	}

	pubsub.Publish[string](bus, context.Background(), "ping", "hello")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for all subscribers")
	}
}

func TestBus_Unsubscribe(t *testing.T) {
	bus := pubsub.NewBus(nil)
	var called atomic.Int32

	unsub := pubsub.Subscribe[int](bus, "nums",
		func(_ context.Context, _ pubsub.Event[int]) error {
			called.Add(1)
			return nil
		}, 1)

	pubsub.Publish[int](bus, context.Background(), "nums", 1)
	time.Sleep(20 * time.Millisecond)

	unsub() // should stop receiving

	pubsub.Publish[int](bus, context.Background(), "nums", 2)
	time.Sleep(20 * time.Millisecond)

	if got := called.Load(); got != 1 {
		t.Fatalf("handler called %d times after unsub, want 1", got)
	}
}

func TestBus_DifferentTopicsIsolated(t *testing.T) {
	bus := pubsub.NewBus(nil)
	var ordersReceived, usersReceived atomic.Int32

	u1 := pubsub.Subscribe[OrderPlaced](bus, "orders",
		func(_ context.Context, _ pubsub.Event[OrderPlaced]) error {
			ordersReceived.Add(1)
			return nil
		}, 1)
	defer u1()

	u2 := pubsub.Subscribe[UserSignedUp](bus, "users",
		func(_ context.Context, _ pubsub.Event[UserSignedUp]) error {
			usersReceived.Add(1)
			return nil
		}, 1)
	defer u2()

	pubsub.Publish[OrderPlaced](bus, context.Background(), "orders", OrderPlaced{ID: "x"})
	time.Sleep(20 * time.Millisecond)

	if ordersReceived.Load() != 1 {
		t.Error("orders subscriber not called")
	}
	if usersReceived.Load() != 0 {
		t.Error("users subscriber incorrectly called")
	}
}

func TestBus_Topics(t *testing.T) {
	bus := pubsub.NewBus(nil)

	u1 := pubsub.Subscribe[int](bus, "a", func(_ context.Context, _ pubsub.Event[int]) error { return nil }, 0)
	u2 := pubsub.Subscribe[int](bus, "b", func(_ context.Context, _ pubsub.Event[int]) error { return nil }, 0)
	defer u1()
	defer u2()

	topics := bus.Topics()
	if len(topics) != 2 {
		t.Fatalf("Topics: got %v, want 2 topics", topics)
	}
}
