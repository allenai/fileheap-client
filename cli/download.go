package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/beaker/fileheap/api"
	"github.com/beaker/fileheap/async"
	"github.com/beaker/fileheap/client"
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

	// Create target directory explicitly for empty datasets.
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return err
	}

	files := &modifiedIterator{
		files:      sourcePkg.Files(ctx, sourcePath),
		targetPath: targetPath,
		tracker:    tracker,
	}
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

				// Wrap in a function to defer close until the end of each file
				// instead of the end of the batch.
				func() {
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
				}()
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

// modifiedFilter wraps a FileIterator and filters out files that already
// exist in the local filesystem and have the same content as the remote copy.
type modifiedIterator struct {
	files      client.Iterator
	targetPath string
	tracker    ProgressTracker
}

func (i *modifiedIterator) Next() (*api.FileInfo, error) {
	for {
		info, err := i.files.Next()
		if err != nil {
			return nil, err
		}

		filename := path.Join(i.targetPath, info.Path)
		finfo, err := os.Stat(filename)
		if os.IsNotExist(err) {
			return info, nil
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if finfo.Size() != info.Size {
			return info, nil
		}

		digest, err := getDigest(filename)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(digest, info.Digest) {
			return info, nil
		}

		// Local file is the same as remote. Mark as written.
		i.tracker.Update(&ProgressUpdate{
			FilesWritten: 1,
			BytesWritten: info.Size,
		})
	}
}

func getDigest(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, errors.WithStack(err)
	}
	return hash.Sum(nil), nil
}
