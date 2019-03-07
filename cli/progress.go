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
func BoundedTracker(ctx context.Context, totalBytes int64) ProgressTracker {
	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		return DefaultTracker()
	}

	if totalBytes == 0 {
		return NoTracker
	}

	progress := mpb.NewWithContext(ctx, mpb.WithWidth(50))
	bar := progress.AddBar(totalBytes,
		mpb.PrependDecorators(newByteRatioDecorator(" %-10s / %10s")),
		mpb.AppendDecorators(
			newPercentageDecorator("%3d%% "),
			newRateDecorator("%s"),
			decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))
	return &boundedTracker{progress: progress, bar: bar}
}

// UnboundedTracker shows the progress of an operation without a predefined size.
// Falls back to DefaultTracker if not in a terminal.
func UnboundedTracker(ctx context.Context) ProgressTracker {
	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		return DefaultTracker()
	}

	var bytesTotal int64

	progress := mpb.NewWithContext(ctx,
		mpb.WithWidth(0),
		// Don't render when total is zero.
		mpb.ContainerOptOnCond(mpb.WithOutput(nil), func() bool { return bytesTotal == 0 }))
	bar := progress.AddBar(0, mpb.PrependDecorators(
		newByteCountDecorator(" %-10s"),
		newRateDecorator(" %s"),
		decor.OnComplete(decor.Spinner(nil, decor.WCSyncSpace), "✔")))
	return &unboundedTracker{progress: progress, bar: bar, bytesTotal: &bytesTotal}
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

type boundedTracker struct {
	lock     sync.Mutex
	progress *mpb.Progress
	bar      *mpb.Bar
}

func (t *boundedTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.bar.IncrBy(int(u.BytesWritten))
}

func (t *boundedTracker) Close() error {
	t.progress.Wait()
	return nil
}

type unboundedTracker struct {
	lock       sync.Mutex
	progress   *mpb.Progress
	bar        *mpb.Bar
	bytesTotal *int64
}

func (t *unboundedTracker) Update(u *ProgressUpdate) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if u.BytesPending > 0 {
		*t.bytesTotal = *t.bytesTotal + u.BytesPending
		t.bar.SetTotal(*t.bytesTotal, false)
	}
	t.bar.IncrBy(int(u.BytesWritten))
}

func (t *unboundedTracker) Close() error {
	if *t.bytesTotal == 0 {
		t.progress.Abort(t.bar, false)
	} else {
		t.progress.Wait()
	}
	return nil
}

type byteCountDecorator struct {
	decor.WC
	format string
}

func newByteCountDecorator(format string) *byteCountDecorator {
	return &byteCountDecorator{format: format}
}

func (d *byteCountDecorator) Decor(s *decor.Statistics) string {
	return fmt.Sprintf(d.format, bytefmt.FormatBytes(s.Current))
}

type byteRatioDecorator struct {
	decor.WC
	format string
}

func newByteRatioDecorator(format string) *byteRatioDecorator {
	return &byteRatioDecorator{format: format}
}

func (d *byteRatioDecorator) Decor(s *decor.Statistics) string {
	return fmt.Sprintf(
		d.format,
		bytefmt.FormatBytes(s.Current),
		bytefmt.FormatBytes(s.Total),
	)
}

type percentageDecorator struct {
	decor.WC
	format string
}

func newPercentageDecorator(format string) *percentageDecorator {
	return &percentageDecorator{format: format}
}

func (d *percentageDecorator) Decor(s *decor.Statistics) string {
	return fmt.Sprintf(d.format, int(math.Round(float64(100*s.Current))/float64(s.Total)))
}

type rateDecorator struct {
	decor.WC
	format string
	start  time.Time
}

func newRateDecorator(format string) *rateDecorator {
	return &rateDecorator{format: format, start: time.Now()}
}

func (d *rateDecorator) Decor(s *decor.Statistics) string {
	return fmt.Sprintf(d.format, bytefmt.FormatRate(s.Current, time.Since(d.start)))
}
