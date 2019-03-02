package async

import (
	"context"
	"sync"
)

// Error tracks errors asynchronously and records the first error encountered.
type Error struct {
	err  error
	lock sync.Mutex
}

// Report raises an error and calls the cancel function passed on initialization.
//
// Calling Report concurrently with Err produces undefined behavior.
func (e *Error) Report(err error) {
	if err != context.Canceled && err != nil {
		e.lock.Lock()
		if e.err == nil {
			e.err = err
		}
		e.lock.Unlock()
	}
}

// Err returns the first error reported, if any.
func (e *Error) Err() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.err
}
