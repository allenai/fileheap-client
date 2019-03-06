package cli

import (
	"fmt"
	"sync"
	"time"

	"github.com/beaker/fileheap/bytefmt"
)

// ProgressUpdate contains deltas for each tracked value.
type ProgressUpdate struct {
	FilesPending, FilesWritten int64
	BytesPending, BytesWritten int64
}

// ProgressTracker tracks the status of an operation.
type ProgressTracker interface {
	Update(*ProgressUpdate)
	Close() error
}

// NopTracker implements the ProgressTracker interface but does nothing.
type NopTracker struct{}

func (t *NopTracker) Update(u *ProgressUpdate) {}
func (t *NopTracker) Close() error {
	return nil
}

// NewDefaultTracker prints a message on each update and on close.
func NewDefaultTracker() ProgressTracker {
	return &progressTracker{start: time.Now()}
}

type progressTracker struct {
	lock  sync.Mutex
	p     ProgressUpdate
	start time.Time
}

func (t *progressTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.p.FilesPending += u.FilesPending
	t.p.FilesWritten += u.FilesWritten
	t.p.BytesPending += u.BytesPending
	t.p.BytesWritten += u.BytesWritten

	fmt.Printf(
		"Complete: %8d files, %-10s In Progress: %8d files, %-10s\n",
		t.p.FilesWritten,
		bytefmt.FormatBytes(t.p.BytesWritten),
		t.p.FilesPending,
		bytefmt.FormatBytes(t.p.BytesPending),
	)
}

func (t *progressTracker) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	elapsed := time.Since(t.start)
	fmt.Printf(
		"Completed in %s (%s)\n",
		elapsed.Truncate(time.Second/10),
		bytefmt.FormatRate(t.p.BytesWritten, elapsed),
	)
	return nil
}
