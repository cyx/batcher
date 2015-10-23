package batcher

import (
	"sync"
	"time"
)

// QueueSize is the size of the queue channel and should depend based
// on the load volume that you expect.
//
// If N=1500 and QueueSize=100, that means the max bandwidth you can
// handle is 150K pushes / second.
var QueueSize = 100

// Batcher gives you a generic concept of: Push, Trigger, Close.
type Batcher interface {
	Queue(elem interface{}) error
	Trigger(func(chan interface{}))
	Close()
}

// New gives you a initialized Batcher with batcher.QueueSize
// being the only pre-configured default. You can change that default
// by doing:
//
//         batcher.QueueSize = N
func New(count int, interval time.Duration) Batcher {
	return &batcher{
		count:    count,
		interval: interval,
		list:     make(chan interface{}, count),
		queue:    make(chan chan interface{}, QueueSize),
		closer:   make(chan struct{}),
	}
}

type batcher struct {
	sync.Mutex

	count    int
	interval time.Duration
	list     chan interface{}
	queue    chan chan interface{}
	closer   chan struct{}
}

func (b *batcher) Queue(elem interface{}) error {
	if len(b.list) >= b.count {
		b.queue <- b.unbatch()
	}
	b.list <- elem

	// No known errors to return at this time, but we
	// should reserve the right to have one for posterity.
	return nil
}

func (b *batcher) unbatch() chan interface{} {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// Copy the channel
	payload := b.list
	close(payload)

	// Re-initialize the list with an empty channel.
	b.list = make(chan interface{}, b.count)

	return payload
}

func (b *batcher) Trigger(fn func(chan interface{})) {
	for {
		select {
		case payload := <-b.queue:
			// Happy path.
			fn(payload)

		case <-time.After(b.interval):
			// Flush if we have anything, otherwise, we
			// start all over again with the timer reset.
			if len(b.list) >= 0 {
				fn(b.unbatch())
			}

		case <-b.closer:
			// Flush any other bits we might still have.
			fn(b.list)
		}
	}
}

func (b *batcher) Close() {
	close(b.closer)
}
