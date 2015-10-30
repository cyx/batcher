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

// Number of goroutines we should spawn that will actively do work
// in serial.
var Workers = 5

// Batcher gives you a generic concept of: Queue, Trigger, Close.
type Batcher interface {
	Queue(elem interface{}) error
	Trigger(func(chan interface{}))
	Close()
}

// New gives you a initialized Batcher with batcher.QueueSize
// and Workers being the pre configured defaults.
//
// You can change the defaults by doing:
//
//         batcher.QueueSize = N
//         batcher.Workers = M
func New(count int, interval time.Duration) Batcher {
	return &batcher{
		count:    count,
		interval: interval,
		list:     make(chan interface{}, count),
		closer:   make(chan struct{}),
		outbox:   make(chan chan interface{}, QueueSize),
		workers:  Workers,
	}
}

type batcher struct {
	wg sync.WaitGroup
	sync.Mutex

	count    int
	interval time.Duration
	list     chan interface{}
	closer   chan struct{}
	outbox   chan chan interface{}
	workers  int
}

func (b *batcher) Queue(elem interface{}) error {
	b.list <- elem

	// No known errors to return at this time, but we
	// should reserve the right to have one for posterity.
	return nil
}

func (b *batcher) spawn(fn func(chan interface{})) {
	for e := range b.outbox {
		fn(e)
	}
}

func (b *batcher) startWorkers(fn func(chan interface{})) {
	for i := b.workers; i > 0; i-- {
		// Signal start of worker
		b.wg.Add(1)
		go func() {
			// Signal completion of worker
			defer b.wg.Done()
			b.spawn(fn)
		}()
	}
}

func (b *batcher) Trigger(fn func(chan interface{})) {
	b.startWorkers(fn)

	buff := make(chan interface{}, b.count)
	for {
		select {
		case item := <-b.list:
			// Happy path.
			b.bufferMaybeBatch(&buff, item)

		case <-time.After(b.interval):
			// Flush if we have anything, otherwise, we
			// start all over again with the timer reset.
			if len(buff) > 0 {
				b.batch(&buff)
			}

		case <-b.closer:
			// Ensure range terminates
			close(b.list)

			// Flush any other bits we might still have.
			for item := range b.list {
				b.bufferMaybeBatch(&buff, item)
			}
			b.batch(&buff)
			close(b.outbox)
			return
		}
	}
}

func (b *batcher) Close() {
	close(b.closer)

	// Wait for all workers to finish.
	b.wg.Wait()
}

func (b *batcher) bufferMaybeBatch(buff *chan interface{}, item interface{}) {
	select {
	case *buff <- item:
	default:
		b.batch(buff)
		*buff <- item
	}
}

func (b *batcher) batch(buff *chan interface{}) {
	close(*buff)
	b.outbox <- *buff
	*buff = make(chan interface{}, b.count)
}
