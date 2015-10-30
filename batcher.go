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
// being the only pre-configured default. You can change that default
// by doing:
//
//         batcher.QueueSize = N
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
	sync.WaitGroup
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
		b.Add(1)
		go func() {
			// Signal completion of worker
			defer b.Done()
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
			select {
			case buff <- item:
			default:
				close(buff)
				b.outbox <- buff
				buff = make(chan interface{}, b.count)
				buff <- item
			}

		case <-time.After(b.interval):
			// Flush if we have anything, otherwise, we
			// start all over again with the timer reset.
			if len(buff) > 0 {
				close(buff)
				b.outbox <- buff
				buff = make(chan interface{}, b.count)
			}

		case <-b.closer:
			// Ensure range terminates
			close(b.list)

			// Flush any other bits we might still have.
			for elem := range b.list {
				select {
				case buff <- elem:
				default:
					close(buff)
					b.outbox <- buff
					buff = make(chan interface{}, b.count)
					buff <- elem
				}
			}
			close(buff)
			b.outbox <- buff
			close(b.outbox)
			return
		}
	}
}

func (b *batcher) Close() {
	close(b.closer)

	// Wait for all workers to finish.
	b.Wait()
}
