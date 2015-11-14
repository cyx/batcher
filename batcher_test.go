package batcher

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	var _ Batcher = &batcher{}
}

type myConcreteType struct {
	Name string
}

func TestQueueTriggerBasics(t *testing.T) {
	// Obscenely long time. We want the batch by number
	// in this test.
	b := New(5, time.Second*1500)

	// Use channels to signal completion
	done := make(chan int)
	go b.Trigger(func(payload chan interface{}) {
		// We do a range'd loop to verify that the channel
		// has been closed and therefore won't block on us.
		t := 0 // total
		for range payload {
			t++
		}
		done <- t
	})

	// Make 5x+1 pushes to trigger a batch.
	// The extra push is because the payload only triggers when trying
	// to push to a full queue.
	for i := 0; i < 6; i++ {
		// Queue a hypothetical concrete type
		if err := b.Queue(myConcreteType{Name: fmt.Sprintf("John %d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case total := <-done:
		if total != 5 {
			t.Fatalf("Payload len expected to be 5, got %d", total)
		}

	case <-time.After(time.Second * 1):
		t.Fatal("Too long!")
	}
}

func TestQueueTriggerTimedFlush(t *testing.T) {
	// We want enough time to get a batch,
	// but fast enough that the test won't block long.
	b := New(5, time.Millisecond*10)

	// Use channels to signal completion
	done := make(chan chan interface{})
	go b.Trigger(func(payload chan interface{}) {
		done <- payload
	})

	// Queue a single hypothetical concrete type
	b.Queue(myConcreteType{Name: "John"})

	select {
	case payload := <-done:
		if len(payload) != 1 {
			t.Fatalf("Payload len expected to be 1, got %d", len(payload))
		}

	case <-time.After(time.Second * 1):
		t.Fatal("Too long!")
	}
}

func TestQueueTriggerCloseFlush(t *testing.T) {
	// We want the flush to be triggered by `Close()`.
	b := New(5, time.Second*1500)

	// Use channels to signal completion
	done := make(chan chan interface{})
	go b.Trigger(func(payload chan interface{}) {
		done <- payload
	})

	// Queue a single hypothetical concrete type
	b.Queue(myConcreteType{Name: "John"})
	b.Close()

	select {
	case payload := <-done:
		if len(payload) != 1 {
			t.Fatalf("Payload len expected to be 1, got %d", len(payload))
		}

	case <-time.After(time.Second * 1):
		t.Fatal("Too long!")
	}
}

func TestOverflowing(t *testing.T) {
	QueueSize = 5

	// We want the flush to be triggered by the buffer size.
	b := New(1, time.Second*1500)
	defer b.Close()

	// Use channels to signal completion
	done := make(chan bool, 100)
	result := make(chan interface{}, QueueSize)

	go b.Trigger(func(payload chan interface{}) {
		for e := range payload {
			select {
			case result <- e:
			default:
				done <- true
			}
		}
	})

	// Queue 1 more to trigger the overflow
	// Queue 1 additional more for result ch overflow
	for i := 0; i < QueueSize+2; i++ {
		// Queue a hypothetical concrete type
		if err := b.Queue(myConcreteType{Name: fmt.Sprintf("John %d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-done:
		if len(result) != QueueSize {
			t.Fatalf("Expected result len to be %d but got %d", QueueSize, len(result))
		}
	case <-time.After(time.Second):
		t.Fatal("Too slow")
	}
}

func BenchmarkQueueWithNoop(b *testing.B) {
	b.StartTimer()

	// Obscenely long timeout so it won't trigger at all.
	batching := New(1500, time.Second*1500)

	// Add up all the payload lengths that we get to verify
	// that input <=> output counts are the same.
	var ops uint64
	go batching.Trigger(func(payload chan interface{}) {
		atomic.AddUint64(&ops, uint64(len(payload)))
	})

	for i := 0; i < b.N; i++ {
		// Queue as fast as possible, erroring out if we get even
		// one single drop.
		if err := batching.Queue(myConcreteType{Name: fmt.Sprintf("John %d", i)}); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	batching.Close()

	// Basically we should be getting the same values here.
	fmt.Printf("Scheduled %d ops, Completed = %d\n", b.N, ops)
}
