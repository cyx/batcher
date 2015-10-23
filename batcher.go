package batcher

import "time"

type Batcher interface {
	Push(elem interface{}) error
	Next() <-chan Batch
}

func New(count int, interval time.Duration) Batcher {
	return &batcher{count, interval}
}

type batcher struct {
	count    int
	interval time.Duration
}

func (b *batcher) Push(elem interface{}) error {
	return nil
}

func (b *batcher) Next() <-chan Batch {
	return nil
}

type Batch []interface{}
