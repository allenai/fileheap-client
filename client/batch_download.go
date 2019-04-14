package client

import (
	"bytes"
	"context"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/beaker/fileheap/api"
)

// BatchDownloader is an iterator over file batches.
type BatchDownloader struct {
	// Initial state.
	ctx     context.Context
	dataset *DatasetRef
	files   Iterator

	nextInfo *api.FileInfo
}

// Next gets the next batch of files.
// If the iterator is expended it will return the sentinel error Done.
func (d *BatchDownloader) Next() (*FileBatch, error) {
	var info *api.FileInfo
	if d.nextInfo != nil {
		info = d.nextInfo
		d.nextInfo = nil
	} else {
		var err error
		info, err = d.files.Next()
		if err != nil {
			return nil, err
		}
	}

	batch := []*api.FileInfo{info}
	batchSize := info.Size

	for {
		info, err := d.files.Next()
		if err == ErrDone {
			break
		}
		if err != nil {
			return nil, err
		}

		// Adding next file would make the batch too large; defer processing of next file.
		if len(batch) >= batchSizeLimit || batchSize+info.Size > requestSizeLimit {
			d.nextInfo = info
			break
		}

		batch = append(batch, info)
		batchSize += info.Size
	}

	return &FileBatch{
		ctx:     d.ctx,
		dataset: d.dataset,
		infos:   batch,
		size:    batchSize,
	}, nil
}

// FileBatch is a batch of files with readers.
type FileBatch struct {
	// Initial state.
	ctx     context.Context
	dataset *DatasetRef
	infos   []*api.FileInfo
	size    int64

	err  error
	read int // Number of files read.
	resp *http.Response
	mr   *multipart.Reader
}

// Length gets the number of files in a batch.
func (b *FileBatch) Length() int {
	return len(b.infos)
}

// Size of the batch in bytes.
func (b *FileBatch) Size() int64 {
	return b.size
}

// Next gets the next file and its reader in the iterator.
// If the iterator is expended it will return the sentinel error Done.
// The batch is closed if Next returns an error. Future calls will return the same error.
func (b *FileBatch) Next() (*api.FileInfo, *Reader, error) {
	if b.err != nil {
		return nil, nil, b.err
	}

	info, reader, err := b.next()
	if err != nil {
		b.err = err
		if b.resp != nil {
			b.resp.Body.Close()
		}
	}
	return info, reader, err
}

func (b *FileBatch) next() (*api.FileInfo, *Reader, error) {
	defer func() {
		b.read++
	}()

	if b.read >= len(b.infos) {
		return nil, nil, ErrDone
	}

	if len(b.infos) == 1 {
		reader, err := b.dataset.ReadFile(b.ctx, b.infos[0].Path)
		if err != nil {
			return nil, nil, err
		}
		return b.infos[0], reader, nil
	}

	if b.mr == nil {
		buf := new(bytes.Buffer)
		mw := multipart.NewWriter(buf)
		for _, info := range b.infos {
			if _, err := mw.CreatePart(textproto.MIMEHeader{
				api.HeaderDigest: {api.EncodeDigest(info.Digest)},
			}); err != nil {
				return nil, nil, errors.WithStack(err)
			}
		}
		if err := mw.Close(); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		url := path.Join("datasets", b.dataset.id, "batch/download")
		req, err := b.dataset.client.newRetryableRequest(http.MethodPost, url, nil, buf)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		req.Header.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())

		b.resp, err = newRetryableClient(&http.Client{
			Timeout: 5 * time.Minute,
		}).Do(req.WithContext(b.ctx))
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if err := errorFromResponse(b.resp); err != nil {
			return nil, nil, err
		}

		mediaType, params, err := mime.ParseMediaType(b.resp.Header.Get("Content-Type"))
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if mediaType != "multipart/mixed" {
			return nil, nil, errors.New("unexpected media type")
		}
		b.mr = multipart.NewReader(b.resp.Body, params["boundary"])
	}

	part, err := b.mr.NextPart()
	if err != nil {
		return nil, nil, errors.Errorf("batch error: %s", b.resp.Trailer.Get(api.HeaderBatchError))
	}

	info := b.infos[b.read]
	return info, &Reader{body: part, size: info.Size}, nil
}
