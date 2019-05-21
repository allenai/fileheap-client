package client

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/beaker/fileheap/api"
	"github.com/pkg/errors"
)

// Iterator is an iterator over file information.
type Iterator interface {
	Next() (*api.FileInfo, error)
}

// FileIterator is an iterator over files within a dataset.
type FileIterator struct {
	dataset *DatasetRef
	path    string
	ctx     context.Context
	limit   int
	files   []api.FileInfo
	cursor  string

	// Whether the final request has been made.
	lastRequest bool
}

// Next gets the next file in the iterator. If iterator is expended it will
// return the sentinel error Done.
func (i *FileIterator) Next() (*api.FileInfo, error) {
	if len(i.files) != 0 {
		result := i.files[0]
		i.files = i.files[1:]
		return &result, nil
	}

	if i.lastRequest {
		return nil, ErrDone
	}

	path := path.Join("/datasets", i.dataset.id, "manifest")
	query := url.Values{"cursor": {i.cursor}, "path": {i.path}}
	if i.limit > 0 {
		query["limit"] = []string{strconv.Itoa(i.limit)}
	}
	resp, err := i.dataset.client.sendRequest(i.ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body api.ManifestPage
	if err := parseResponse(resp, &body); err != nil {
		return nil, err
	}

	i.files = body.Files
	i.cursor = body.Cursor
	if body.Cursor == "" {
		i.lastRequest = true
	}

	return i.Next()
}

// SetLimit sets the maximum number of files to list in a request.
func (i *FileIterator) SetLimit(limit int) error {
	if limit <= 0 {
		return errors.New("limit must be positive")
	}

	i.limit = limit
	return nil
}
