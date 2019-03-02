package cli

import (
	"context"
	"os"
	"path"
	"path/filepath"

	"github.com/beaker/fileheap/async"
	"github.com/beaker/fileheap/client"
	"github.com/pkg/errors"
)

// Upload the sourcePath to the targetPath in the targetPkg.
func (c *CLI) Upload(
	sourcePath string,
	targetPkg *client.DatasetRef,
	targetPath string,
	tracker ProgressTracker,
) error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	asyncErr := async.Error{}
	limiter := async.NewLimiter(c.concurrency)

	uploadBatch := func(batch *client.UploadBatch) {
		length := int64(batch.Length())
		size := batch.Size()

		tracker.Update(&ProgressUpdate{
			FilesPending: length,
			BytesPending: size,
		})

		if err := batch.Upload(ctx); err != nil {
			tracker.Update(&ProgressUpdate{
				FilesPending: -length,
				BytesPending: -size,
			})
			asyncErr.Report(err)
			cancel()
			return
		}

		tracker.Update(&ProgressUpdate{
			FilesWritten: length,
			FilesPending: -length,
			BytesWritten: size,
			BytesPending: -size,
		})
	}

	batch := targetPkg.NewUploadBatch()
	visitor := func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if err := asyncErr.Err(); err != nil {
			return err
		}

		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		if !batch.HasCapacity(info.Size()) {
			batchToUpload := batch
			limiter.Go(func() { uploadBatch(batchToUpload) })
			batch = targetPkg.NewUploadBatch()
		}

		relpath, err := filepath.Rel(sourcePath, filePath)
		if err != nil {
			return errors.WithStack(err)
		}

		file, err := os.Open(filePath)
		if err != nil {
			return errors.WithStack(err)
		}

		return batch.AddFile(path.Join(targetPath, relpath), file, info.Size())
	}
	if err := filepath.Walk(sourcePath, visitor); err != nil {
		return err
	}
	limiter.Go(func() { uploadBatch(batch) })
	limiter.Wait()
	if err := asyncErr.Err(); err != nil {
		return err
	}

	tracker.Close()
	return nil
}
