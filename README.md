# batcher

Allows you to schedule batches of objects in flight on a count / time basis.

## example

```go
// Do 1500 batches of things, or every 5 seconds
b := batcher.New(1500, time.Second * 5)
defer b.Close()

o := struct{}{}
// Queue any kind of interface in
b.Queue(o)

// Register a generic trigger function for when
// the queue either fills up, or the interval ticks.
go b.Trigger(func(payload chan interface{}) {
	for _, elem := range payload {
		// Do something with the payload.
	}
})
```
