# batcher

Allows you to schedule batches of objects in flight on a count / time basis.

## example

```go
// Do 1500 batches of things, or every 5 seconds
b := batcher.New(1500, time.Second * 5)
defer b.Close()

o := struct{}{}
// Push any kind of interface in
b.Push(o)

go func() {
	for _, batch := range b.Next() {
		for _, elem := range batch {
			// elem is an interface which you should
			// be able to cast to the concrete type you want.
		}
	}
}
```
