package batcher

import (
	"fmt"
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
		b.Queue(myConcreteType{Name: fmt.Sprintf("John %d", i)})
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
