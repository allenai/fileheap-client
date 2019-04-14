package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/beaker/fileheap/api"
)

// DatasetOpts allows clients to set options during creation of a new dataset.
type DatasetOpts struct{}

// NewDataset creates a new collection of files.
func (c *Client) NewDataset(ctx context.Context) (*DatasetRef, error) {
	resp, err := c.sendRequest(ctx, http.MethodPost, "/datasets", nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body api.Dataset
	if err := parseResponse(resp, &body); err != nil {
		return nil, err
	}

	return &DatasetRef{client: c, id: body.ID}, nil
}

// Dataset creates a reference to an existing dataset by ID.
func (c *Client) Dataset(id string) *DatasetRef {
	return &DatasetRef{client: c, id: id}
}

// DatasetRef is a reference to a dataset.
//
// Callers should not assume the ref is valid.
type DatasetRef struct {
	client *Client
	id     string
}

// Name returns the dataset's unique identifier.
func (d *DatasetRef) Name() string { return d.id }

// URL gets the URL of a dataset.
func (d *DatasetRef) URL() string {
	path := path.Join("/datasets", d.id)
	u := d.client.baseURL.ResolveReference(&url.URL{Path: path})
	return u.String()
}

// Info returns metadata about the dataset.
func (d *DatasetRef) Info(ctx context.Context) (*api.Dataset, error) {
	path := path.Join("/datasets", d.id)
	resp, err := d.client.sendRequest(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body api.Dataset
	if err := parseResponse(resp, &body); err != nil {
		return nil, err
	}
	return &body, nil
}

// Seal makes a dataset read-only. This operation is not reversible.
func (d *DatasetRef) Seal(ctx context.Context) error {
	path := path.Join("/datasets", d.id)
	body := &api.DatasetPatch{ReadOnly: true}

	resp, err := d.client.sendRequest(ctx, http.MethodPatch, path, nil, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return errorFromResponse(resp)
}

// Delete deletes a dataset and all of its files.
//
// This invalidates the DatasetRef and all associated file references.
func (d *DatasetRef) Delete(ctx context.Context) error {
	path := path.Join("/datasets", d.id)
	resp, err := d.client.sendRequest(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return errorFromResponse(resp)
}

// Files returns an iterator over all files in the dataset.
func (d *DatasetRef) Files(ctx context.Context, path string) *FileIterator {
	return &FileIterator{dataset: d, ctx: ctx, path: path}
}

// NewUploadBatch creates an UploadBatch.
func (d *DatasetRef) NewUploadBatch() *UploadBatch {
	return &UploadBatch{dataset: d}
}

// NewDeleteBatch creates a DeleteBatch.
func (d *DatasetRef) NewDeleteBatch() *DeleteBatch {
	return &DeleteBatch{dataset: d}
}

// DownloadBatch creates a BatchDownloader.
func (d *DatasetRef) DownloadBatch(ctx context.Context, files Iterator) *BatchDownloader {
	return &BatchDownloader{ctx: ctx, dataset: d, files: files}
}

// FileInfo returns metadata about a file in the dataset.
// Returns ErrFileNotFound if the file does not exist.
func (d *DatasetRef) FileInfo(ctx context.Context, filename string) (*api.FileInfo, error) {
	path := path.Join("/datasets", d.id, "files", filename)
	resp, err := d.client.sendRequest(ctx, http.MethodHead, path, nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFileNotFound
	}
	if err := errorFromResponse(resp); err != nil {
		return nil, err
	}

	info := &api.FileInfo{Path: filename, Size: resp.ContentLength}
	if d := resp.Header.Get(api.HeaderDigest); d != "" {
		info.Digest, err = api.DecodeDigest(d)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if t := resp.Header.Get("Last-Modified"); t != "" {
		info.Updated, err = time.Parse(api.HTTPTimeFormat, t)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return info, nil
}

// DeleteFile deletes a file in the dataset.
func (d *DatasetRef) DeleteFile(ctx context.Context, filename string) error {
	path := path.Join("/datasets", d.id, "files", filename)
	resp, err := d.client.sendRequest(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrFileNotFound
	}
	return errorFromResponse(resp)
}

// ReadFile reads the contents of a stored file.
//
// If the file doesn't exist, this returns ErrFileNotFound.
//
// The caller must call Close on the returned Reader when finished reading.
func (d *DatasetRef) ReadFile(ctx context.Context, filename string) (*Reader, error) {
	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRetryableRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	resp, err := newRetryableClient(nil).Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFileNotFound
	}
	if err := errorFromResponse(resp); err != nil {
		return nil, err
	}

	return &Reader{body: resp.Body, size: resp.ContentLength}, nil
}

// ReadFileRange reads at most length bytes from a file starting at the given offset.
// If length is negative, the file is read until the end.
//
// If the file doesn't exist, this returns ErrFileNotFound.
//
// The caller must call Close on the returned Reader when finished reading.
func (d *DatasetRef) ReadFileRange(
	ctx context.Context,
	filename string,
	offset, length int64,
) (*Reader, error) {
	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRetryableRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if length < 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	} else {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}

	resp, err := newRetryableClient(nil).Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFileNotFound
	}
	if err := errorFromResponse(resp); err != nil {
		return nil, err
	}

	return &Reader{body: resp.Body, size: resp.ContentLength}, nil
}

// WriteFile writes the source to the filename in this dataset.
//
// The file will be replaced if it exists or created if not. The file
// becomes available when Close returns successfully. The previous file is
// readable until the new file replaces it.
//
// It is the caller's responsibility to call Close when writing is complete.
func (d *DatasetRef) WriteFile(
	ctx context.Context,
	filename string,
	source io.Reader,
	size int64,
) error {
	var body io.Reader
	var digest []byte

	if size > requestSizeLimit {
		var err error
		digest, err = d.client.upload(ctx, source, size)
		if err != nil {
			return err
		}
	} else if size != 0 {
		body = source
	}

	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRetryableRequest(http.MethodPut, path, nil, body)
	if err != nil {
		return err
	}
	if digest != nil {
		req.Header.Set(api.HeaderDigest, api.EncodeDigest(digest))
	}
	if body != nil {
		req.ContentLength = size
	}

	client := newRetryableClient(&http.Client{
		Timeout: 5 * time.Minute,
	})
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return errors.WithStack(err)
	}
	return errorFromResponse(resp)
}
