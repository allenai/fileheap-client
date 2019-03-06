package cli

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/beaker/fileheap/async"
	"github.com/beaker/fileheap/client"
	"github.com/pkg/errors"
)

// Download all files under the sourcePath in the sourcePkg to the targetPath.
func Download(
	ctx context.Context,
	sourcePkg *client.DatasetRef,
	sourcePath string,
	targetPath string,
	tracker ProgressTracker,
	concurrency int,
) error {
	if concurrency < 1 {
		return errors.New("concurrency must be positive")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	files := sourcePkg.Files(ctx, sourcePath)
	downloader := sourcePkg.DownloadBatch(ctx, files)
	asyncErr := async.Error{}
	limiter := async.NewLimiter(concurrency)
	for {
		if err := asyncErr.Err(); err != nil {
			return err
		}

		batch, err := downloader.Next()
		if err == client.ErrDone {
			break
		}
		if err != nil {
			return err
		}

		limiter.Go(func() {
			length := int64(batch.Length())
			size := batch.Size()

			tracker.Update(&ProgressUpdate{
				FilesPending: length,
				BytesPending: size,
			})

			reportError := func(err error) {
				tracker.Update(&ProgressUpdate{
					FilesPending: -length,
					BytesPending: -size,
				})
				asyncErr.Report(err)
				cancel()
			}

			for {
				info, reader, err := batch.Next()
				if err == client.ErrDone {
					break
				}
				if err != nil {
					reportError(errors.WithStack(err))
					return
				}
				defer reader.Close()

				filePath := path.Join(targetPath, info.Path)
				if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
					reportError(errors.WithStack(err))
					return
				}

				file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
				if err != nil {
					reportError(errors.WithStack(err))
					return
				}
				defer file.Close()

				if _, err := io.Copy(file, reader); err != nil {
					reportError(errors.WithStack(err))
					return
				}
			}

			tracker.Update(&ProgressUpdate{
				FilesWritten: length,
				FilesPending: -length,
				BytesWritten: size,
				BytesPending: -size,
			})
		})
	}
	limiter.Wait()
	if err := asyncErr.Err(); err != nil {
		return err
	}

	tracker.Close()
	return nil
}
