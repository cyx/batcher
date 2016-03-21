package batcher

import (
	"errors"
	"sync"
	"time"
)

// Batcher gives you a generic concept of: Queue, Trigger, Close.
type Batcher interface {
	Queue(elem interface{}) error
	Trigger(func(chan interface{}))
	Close() error
}

func defaultConfig() *config {
	return &config{
		OutboxSize: 64,
		NumWorkers: 16,
		QueueSize:  1024 * 1024,
		Interval:   time.Second,
	}
}

type config struct {
	OutboxSize int
	NumWorkers int
	QueueSize  int
	Interval   time.Duration
}

type Option interface {
	Apply(*config)
}
type OptionFunc func(*config)

func (f OptionFunc) Apply(c *config) {
	f(c)
}

func OutboxSize(n int) Option {
	return OptionFunc(func(c *config) { c.OutboxSize = n })
}

func NumWorkers(n int) Option {
	return OptionFunc(func(c *config) { c.NumWorkers = n })
}

func QueueSize(n int) Option {
	return OptionFunc(func(c *config) { c.QueueSize = n })
}

func Interval(n time.Duration) Option {
	return OptionFunc(func(c *config) { c.Interval = n })
}

// New gives you a initialized Batcher with batcher.QueueSize
// and Workers being the pre configured defaults.
//
// You can change the defaults by doing:
//
//         batcher.QueueSize = N
//         batcher.Workers = M
func New(count int, options ...Option) Batcher {
	config := defaultConfig()
	for _, o := range options {
		o.Apply(config)
	}

	b := &batcher{
		count:    count,
		interval: config.Interval,
		list:     make(chan interface{}, config.QueueSize),
		closer:   make(chan struct{}),
		outbox:   make(chan chan interface{}, config.OutboxSize),
		workers:  config.NumWorkers,
	}
	b.wg.Add(b.workers)
	return b
}

type batcher struct {
	wg       sync.WaitGroup
	count    int
	interval time.Duration
	list     chan interface{}
	closer   chan struct{}
	outbox   chan chan interface{}
	workers  int
}

var errDropped = errors.New("batcher error: dropped queued item")
var errClosed = errors.New("batcher error: already closed")

func (b *batcher) Queue(elem interface{}) error {
	select {
	case <-b.closer:
		return errClosed
	case b.list <- elem:
		return nil
	default:
		return errDropped
	}
}

func (b *batcher) spawn(fn func(chan interface{})) {
	for e := range b.outbox {
		fn(e)
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
			// Nullify our b.list so pushes to it will block.
			// This is better than closing it because race conditions
			// might trigger panics.
			list := b.list
			b.list = nil

			// Flush any other bits we might still have.
		LOOP:
			for {
				select {
				case item := <-list:
					b.bufferMaybeBatch(&buff, item)
				default:
					break LOOP
				}
			}
			b.batch(&buff)

			// Make sure workers' ranges terminate
			close(b.outbox)
			return
		}
	}
}

func (b *batcher) Close() error {
	close(b.closer)

	// Wait for all workers to finish.
	b.wg.Wait()
	return nil
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
