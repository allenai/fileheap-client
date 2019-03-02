package async

import (
	"sync"
)

// Limiter runs goroutines concurrently while throttling concurrent routines to a fixed threshold.
type Limiter struct {
	wg sync.WaitGroup
	c  chan struct{}
}

// NewLimiter creates a limiter. The limit must be positive.
func NewLimiter(limit int) *Limiter {
	if limit <= 0 {
		panic("limit: concurrency limits must be positive")
	}

	return &Limiter{c: make(chan struct{}, limit)}
}

// Go runs a routine asynchronously. If the limiter is at capacity, Go blocks
// until another routine returns.
//
// Note that if Go is called during Wait, the function may begin execution after
// the wait call. Therefore, all calls to Go should run before Wait.
func (l *Limiter) Go(fn func()) {
	l.wg.Add(1)
	l.c <- struct{}{}

	go func() {
		defer func() {
			<-l.c
			l.wg.Done()
		}()

		fn()
	}()
}

// Wait blocks until all outstanding routines complete.
func (l *Limiter) Wait() {
	l.wg.Wait()
}
