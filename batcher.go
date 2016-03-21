package batcher

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/apg/ln"
)

var Timeout = time.Millisecond * 10

// QueueSize is the size of the queue channel and should depend based
// on the load volume that you expect.
//
// If N=1500 and QueueSize=100, that means you can only batch up to
// 150,000 elements + the ListSize default of 1M (so 1.15M elements
// all in all).
var OutboxChannelSize = 128

// Number of goroutines we should spawn that will actively do work
// in serial.
var Workers = 64

// ListSize determines how much buffering you can get. If you push
// 1M elements fast enough before you can discard any of that in
// your workers, you'll start dropping messages.
// var ListSize = 1024 * 1024

// Batcher gives you a generic concept of: Queue, Trigger, Close.
type Batcher interface {
	Queue(elem interface{}) error
	Trigger(func(chan interface{}))
	Close() error
}

// New gives you a initialized Batcher with batcher.QueueSize
// and Workers being the pre configured defaults.
//
// You can change the defaults by doing:
//
//         batcher.QueueSize = N
//         batcher.Workers = M
func New(count int, interval time.Duration) Batcher {
	b := &batcher{
		count:     count,
		interval:  interval,
		list:      make(chan interface{}, count),
		closer:    make(chan struct{}),
		outbox:    make(chan chan interface{}, OutboxChannelSize),
		workers:   Workers,
		flushTick: time.Tick(interval),
		emitTick:  time.Tick(time.Second),
	}
	go b.flush()
	b.wg.Add(b.workers)
	return b
}

type batcher struct {
	wg        sync.WaitGroup
	count     int
	interval  time.Duration
	list      chan interface{}
	closer    chan struct{}
	outbox    chan chan interface{}
	workers   int
	flushTick <-chan time.Time
	emitTick  <-chan time.Time
}

var errDropped = errors.New("batcher error: dropped queued item")
var errClosed = errors.New("batcher error: already closed")

func (b *batcher) flush() {
	for {
		select {
		case <-b.emitTick:
			ln.Info(ln.F{
				"gauge#batcher.outbox.length": len(b.outbox),
				"gauge#batcher.list.length":   len(b.list),
			})

		case <-b.flushTick:
			buff := make(chan interface{}, b.count)
		LOOP:
			for {
				select {
				case e := <-b.list:
					buff <- e

				default:
					break LOOP
				}
			}
			select {
			case b.outbox <- buff:
			case <-b.closer:
			}
		case <-b.closer:
			return
		}
	}
}

func (b *batcher) Queue(elem interface{}) error {
	select {
	case <-b.closer:
		return errClosed
	case b.list <- elem:
	default:
		close(b.list)
		select {
		case b.outbox <- b.list:
			b.list = make(chan interface{}, b.count)
			b.list <- elem
		case <-b.closer:
		case <-time.After(Timeout):
			log.Printf("dropped")
			ln.Error(ln.F{"count#batcher.outbox.drop": 1})
		}
	}
	return nil
}

func (b *batcher) spawn(fn func(chan interface{})) {
	for e := range b.outbox {
		t0 := time.Now()
		fn(e)
		ln.Info(ln.F{"measure#batcher.worker.time": time.Since(t0)})
	}
}

func (b *batcher) startWorkers(fn func(chan interface{})) {
	for i := b.workers; i > 0; i-- {
		go func() {
			defer b.wg.Done()
			b.spawn(fn)
		}()
	}
}

func (b *batcher) Trigger(fn func(chan interface{})) {
	b.startWorkers(fn)
}

func (b *batcher) Close() error {
	close(b.list)
	if len(b.list) > 0 {
		b.outbox <- b.list
		b.list = nil
	}
	close(b.closer)
	close(b.outbox)

	// Wait for all workers to finish.
	b.wg.Wait()
	return nil
}

/*
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
*/
