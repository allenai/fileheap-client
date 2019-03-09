package cli

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/beaker/fileheap/bytefmt"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"golang.org/x/crypto/ssh/terminal"
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

// NoTracker implements the ProgressTracker interface but does nothing.
var NoTracker = &nopTracker{}

// DefaultTracker prints a message on each update and on close.
func DefaultTracker() ProgressTracker {
	return &progressTracker{start: time.Now()}
}

// BoundedTracker shows the progress of an operation with a predefined size.
// Falls back to DefaultTracker if not in a terminal.
func BoundedTracker(ctx context.Context, totalFiles, totalBytes int64) ProgressTracker {
	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		return DefaultTracker()
	}

	p := &ProgressUpdate{}
	progress := mpb.NewWithContext(ctx, mpb.WithWidth(50))
	fileBar := progress.AddBar(totalFiles,
		mpb.PrependDecorators(
			decor.Name("Files: "),
			ratioDecorator),
		mpb.AppendDecorators(
			percentageDecorator,
			newDecorator(func(s *decor.Statistics) string {
				if p.FilesPending == 0 {
					return ""
				}
				return fmt.Sprintf(" %d in progress", p.FilesPending)
			}),
			decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))
	byteBar := progress.AddBar(totalBytes,
		mpb.PrependDecorators(
			decor.Name("Bytes: "),
			byteRatioDecorator),
		mpb.AppendDecorators(
			percentageDecorator,
			newDecorator(func(s *decor.Statistics) string {
				if p.BytesPending == 0 {
					return ""
				}
				return fmt.Sprintf(" %s in progress", bytefmt.FormatBytes(p.BytesPending))
			}),
			decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))

	return &boundedTracker{
		start:    time.Now(),
		p:        p,
		progress: progress,
		fileBar:  fileBar,
		byteBar:  byteBar,
	}
}

// UnboundedTracker shows the progress of an operation without a predefined size.
// Falls back to DefaultTracker if not in a terminal.
func UnboundedTracker(ctx context.Context) ProgressTracker {
	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		return DefaultTracker()
	}

	p := &ProgressUpdate{}
	progress := mpb.NewWithContext(ctx, mpb.WithWidth(0))
	fileBar := progress.AddBar(0, mpb.PrependDecorators(
		decor.Name("Files: "),
		countDecorator,
		newDecorator(func(s *decor.Statistics) string {
			if p.FilesPending == 0 {
				return ""
			}
			return fmt.Sprintf(" %d in progress", p.FilesPending)
		}),
		decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))
	byteBar := progress.AddBar(0, mpb.PrependDecorators(
		decor.Name("Bytes: "),
		byteCountDecorator,
		newDecorator(func(s *decor.Statistics) string {
			if p.BytesPending == 0 {
				return ""
			}
			return fmt.Sprintf(" %s in progress", bytefmt.FormatBytes(p.BytesPending))
		}),
		decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))

	return &unboundedTracker{
		start:    time.Now(),
		p:        p,
		progress: progress,
		fileBar:  fileBar,
		byteBar:  byteBar,
	}
}

func (p *ProgressUpdate) update(u *ProgressUpdate) {
	p.FilesPending += u.FilesPending
	p.FilesWritten += u.FilesWritten
	p.BytesPending += u.BytesPending
	p.BytesWritten += u.BytesWritten
}

type nopTracker struct{}

func (t *nopTracker) Update(u *ProgressUpdate) {}
func (t *nopTracker) Close() error {
	return nil
}

type progressTracker struct {
	lock  sync.Mutex
	p     ProgressUpdate
	start time.Time
}

func (t *progressTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.p.update(u)

	fmt.Printf(
		"Complete: %8d files, %-10s In Progress: %8d files, %-10s\n",
		t.p.FilesWritten,
		bytefmt.FormatBytes(t.p.BytesWritten),
		t.p.FilesPending,
		bytefmt.FormatBytes(t.p.BytesPending),
	)
}

func (t *progressTracker) Close() error {
	printCompletionMessage(&t.p, time.Since(t.start))
	return nil
}

type boundedTracker struct {
	lock             sync.Mutex
	start            time.Time
	p                *ProgressUpdate
	progress         *mpb.Progress
	fileBar, byteBar *mpb.Bar
}

func (t *boundedTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.p.update(u)

	t.fileBar.SetCurrent(t.p.FilesWritten)
	t.byteBar.SetCurrent(t.p.BytesWritten)
}

func (t *boundedTracker) Close() error {
	t.fileBar.SetTotal(t.fileBar.Current(), true)
	t.byteBar.SetTotal(t.byteBar.Current(), true)
	t.progress.Wait()
	printCompletionMessage(t.p, time.Since(t.start))
	return nil
}

type unboundedTracker struct {
	lock             sync.Mutex
	start            time.Time
	p                *ProgressUpdate
	progress         *mpb.Progress
	fileBar, byteBar *mpb.Bar
}

func (t *unboundedTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.p.update(u)

	t.fileBar.SetTotal(t.p.FilesWritten+t.p.FilesPending, false)
	t.fileBar.SetCurrent(t.p.FilesWritten)

	t.byteBar.SetTotal(t.p.BytesWritten+t.p.BytesPending, false)
	t.byteBar.SetCurrent(t.p.BytesWritten)
}

func (t *unboundedTracker) Close() error {
	t.fileBar.SetTotal(t.fileBar.Current(), true)
	t.byteBar.SetTotal(t.byteBar.Current(), true)
	t.progress.Wait()
	printCompletionMessage(t.p, time.Since(t.start))
	return nil
}

type decorator struct {
	decor.WC
	f func(s *decor.Statistics) string
}

func (d *decorator) Decor(s *decor.Statistics) string {
	return d.f(s)
}

func newDecorator(f func(s *decor.Statistics) string) *decorator {
	return &decorator{f: f}
}

var countDecorator = newDecorator(func(s *decor.Statistics) string {
	return fmt.Sprintf("%-10d", s.Current)
})

var ratioDecorator = newDecorator(func(s *decor.Statistics) string {
	return fmt.Sprintf("%-10d / %10d", s.Current, s.Total)
})

var byteCountDecorator = newDecorator(func(s *decor.Statistics) string {
	return fmt.Sprintf("%-10s", bytefmt.FormatBytes(s.Current))
})

var byteRatioDecorator = newDecorator(func(s *decor.Statistics) string {
	return fmt.Sprintf("%-10s / %10s", bytefmt.FormatBytes(s.Current), bytefmt.FormatBytes(s.Total))
})

var percentageDecorator = newDecorator(func(s *decor.Statistics) string {
	return fmt.Sprintf("%3d%%", int(math.Round(float64(100*s.Current))/float64(s.Total)))
})

func printCompletionMessage(p *ProgressUpdate, elapsed time.Duration) {
	fmt.Printf(
		"Completed in %s: %s, %d files/s\n",
		elapsed.Truncate(time.Second/10),
		bytefmt.FormatRate(p.BytesWritten, elapsed),
		int(math.Round(float64(p.FilesWritten)/elapsed.Seconds())),
	)
}
