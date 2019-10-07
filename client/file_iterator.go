package client

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/beaker/fileheap/api"
)

// Iterator is an iterator over file information.
type Iterator interface {
	Next() (*api.FileInfo, error)
}

// FileIterator is an iterator over files within a dataset.
type FileIterator struct {
	ctx     context.Context
	dataset *DatasetRef

	// Optional configuration.
	opts FileIteratorOptions

	files  []api.FileInfo
	cursor string

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
	query := url.Values{"cursor": {i.cursor}, "path": {i.opts.Prefix}}
	if limit := i.opts.PageSize; limit > 0 {
		query["limit"] = []string{strconv.Itoa(limit)}
	}
	if i.opts.IncludeURLs {
		query["url"] = []string{"true"}
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
