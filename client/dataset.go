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

	"github.com/allenai/fileheap-client/api"
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

// FileIteratorOptions provides optional configuration to a file iterator.
type FileIteratorOptions struct {
	// Include presigned URLs to download each file. Note that this may result in slower response times.
	IncludeURLs bool

	// Maximum number of files to fetch in a single request.
	PageSize int

	// Prefix within the dataset. Only files that start with the prefix will be included.
	Prefix string
}

// Files returns an iterator over all files in the dataset.
func (d *DatasetRef) Files(ctx context.Context, opts *FileIteratorOptions) *FileIterator {
	i := &FileIterator{dataset: d, ctx: ctx}
	if opts != nil {
		i.opts = *opts
	}
	return i
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
func (d *DatasetRef) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	return d.ReadFileRange(ctx, filename, 0, -1)
}

// ReadFileRange reads at most length bytes from a file starting at the given offset.
// If length is negative, the file is read until the end. Length must not be zero.
//
// If the file doesn't exist, this returns ErrFileNotFound.
//
// The caller must call Close on the returned Reader when finished reading.
func (d *DatasetRef) ReadFileRange(
	ctx context.Context,
	filename string,
	offset, length int64,
) (io.ReadCloser, error) {
	r, err := d.readFileRange(ctx, filename, offset, length)
	if err != nil {
		return nil, err
	}

	// Create a pipe so that we can retry reading the file from where we left off
	// if there is an error. Errors are expected when reading the file takes longer
	// than the HTTP client's timeout.
	pr, pw := io.Pipe()
	go func() {
		defer r.Close()
		defer pw.Close()
		for {
			n, err := io.Copy(pw, r)
			if err == nil {
				return
			}
			if n == 0 {
				pw.CloseWithError(errors.WithStack(err))
				return
			}
			offset += n
			length -= n

			r.Close()
			r, err = d.readFileRange(ctx, filename, offset, length)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr, nil
}

func (d *DatasetRef) readFileRange(
	ctx context.Context,
	filename string,
	offset, length int64,
) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, errors.New("offset must not be negative")
	}
	if length == 0 {
		return nil, errors.New("length must not be zero")
	}

	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if offset != 0 && length < 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	} else if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}

	resp, err := d.client.do(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFileNotFound
	}
	if err := errorFromResponse(resp); err != nil {
		return nil, err
	}
	return resp.Body, nil
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
	// Only read size bytes from the source in case the source grows while writing.
	source = io.LimitReader(source, size)

	var body io.Reader
	var digest []byte

	if size > requestSizeLimit {
		var err error
		digest, err = d.client.upload(ctx, source, size)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				return errors.Errorf("%s truncated while uploading", filename)
			}
			return err
		}
	} else if size != 0 {
		buf := getBuffer()
		defer putBuffer(buf)
		if _, err := io.CopyN(buf, source, size); err != nil {
			if err == io.EOF {
				return errors.Errorf("%s truncated while uploading", filename)
			}
			return errors.WithStack(err)
		}
		body = buf
	}

	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRequest(http.MethodPut, path, nil, body)
	if err != nil {
		return err
	}
	if digest != nil {
		req.Header.Set(api.HeaderDigest, api.EncodeDigest(digest))
	}
	if body != nil {
		req.ContentLength = size
	}

	resp, err := d.client.do(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	return errorFromResponse(resp)
}

// AddFile to a dataset when the digest is already known.
func (d *DatasetRef) AddFile(
	ctx context.Context,
	filename string,
	digest []byte,
) error {
	path := path.Join("/datasets", d.id, "files", filename)
	req, err := d.client.newRequest(http.MethodPut, path, nil, nil)
	if err != nil {
		return err
	}
	req.Header.Set(api.HeaderDigest, api.EncodeDigest(digest))

	resp, err := d.client.do(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	return errorFromResponse(resp)
}
