package client

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"

	"github.com/beaker/fileheap/api"
	"github.com/pkg/errors"
)

// UploadBatch contains files and their readers.
type UploadBatch struct {
	// Initial state.
	dataset *DatasetRef

	paths   []string
	readers []io.Reader
	sizes   []int64
	size    int64
}

// Length gets the number of files in a batch.
func (b *UploadBatch) Length() int {
	return len(b.paths)
}

// Size of the batch in bytes.
func (b *UploadBatch) Size() int64 {
	return b.size
}

// HasCapacity checks whether the batch has capacity for a file with the given size.
func (b *UploadBatch) HasCapacity(size int64) bool {
	if len(b.paths) == 0 {
		return true
	}

	return len(b.paths) < batchSizeLimit && b.size+size <= requestSizeLimit
}

// AddFile adds a file to the batch.
func (b *UploadBatch) AddFile(path string, reader io.Reader, size int64) error {
	if !b.HasCapacity(size) {
		return errors.New("batch does not have capacity for another file")
	}
	if b.dataset.id != b.dataset.id {
		return errors.New("file dataset does not match upload batch dataset")
	}

	b.paths = append(b.paths, path)
	b.readers = append(b.readers, reader)
	b.sizes = append(b.sizes, size)
	b.size += size
	return nil
}

// Upload the files in a batch. Closes all readers.
func (b *UploadBatch) Upload(ctx context.Context) error {
	defer func() {
		for _, reader := range b.readers {
			if closer, ok := reader.(io.Closer); ok {
				closer.Close()
			}
		}
	}()

	if len(b.paths) == 1 {
		return b.dataset.WriteFile(ctx, b.paths[0], b.readers[0], b.sizes[0])
	}

	buffer := new(bytes.Buffer)
	mw := multipart.NewWriter(buffer)
	for i, path := range b.paths {
		pw, err := mw.CreatePart(textproto.MIMEHeader{
			api.HeaderPath: {path},
		})
		if err != nil {
			return errors.WithStack(err)
		}
		io.Copy(pw, b.readers[i])
	}
	if err := mw.Close(); err != nil {
		return errors.WithStack(err)
	}

	url := path.Join("datasets", b.dataset.id, "batch/upload")
	req, err := b.dataset.client.newRetryableRequest(http.MethodPost, url, nil, buffer)
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())

	resp, err := newRetryableClient(nil).Do(req.WithContext(ctx))
	if err != nil {
		return errors.WithStack(err)
	}
	return errorFromResponse(resp)
}
