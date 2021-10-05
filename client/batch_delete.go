package client

import (
	"context"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"

	"github.com/pkg/errors"

	"github.com/beaker/fileheap/api"
)

// DeleteBatch contains a list of files to delete from a dataset.
type DeleteBatch struct {
	// Initial state.
	dataset *DatasetRef

	paths []string
}

// Length gets the number of files in a batch.
func (b *DeleteBatch) Length() int {
	return len(b.paths)
}

// HasCapacity checks whether the batch has capacity for another file.
func (b *DeleteBatch) HasCapacity() bool {
	return len(b.paths) < batchSizeLimit
}

// AddFile adds a file to the batch.
func (b *DeleteBatch) AddFile(path string) error {
	if !b.HasCapacity() {
		return errors.New("batch does not have capacity for another file")
	}

	b.paths = append(b.paths, path)
	return nil
}

// Delete all paths in the batch.
func (b *DeleteBatch) Delete(ctx context.Context) error {
	if len(b.paths) == 0 {
		return nil
	}
	if len(b.paths) == 1 {
		return b.dataset.DeleteFile(ctx, b.paths[0])
	}

	buffer := getBuffer()
	defer putBuffer(buffer)
	mw := multipart.NewWriter(buffer)
	for _, path := range b.paths {
		if _, err := mw.CreatePart(textproto.MIMEHeader{
			api.HeaderPath: {path},
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := mw.Close(); err != nil {
		return errors.WithStack(err)
	}

	url := path.Join("datasets", b.dataset.id, "batch/delete")
	req, err := b.dataset.client.newRequest(http.MethodPost, url, nil, buffer)
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())

	resp, err := b.dataset.client.do(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	return errorFromResponse(resp)
}
