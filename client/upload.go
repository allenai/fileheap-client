package client

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/beaker/fileheap/api"
)

// upload writes the contents of a reader using the upload API.
// This is more expensive than putting the file directly, but is more resilient
// to networking errors and does not require the digest to be known beforehand.
// Note: upload does not support empty readers.
func (c *Client) upload(
	ctx context.Context,
	reader io.Reader,
	length int64,
) (digest []byte, err error) {
	resp, err := c.sendRequest(ctx, http.MethodPost, "/uploads", nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := errorFromResponse(resp); err != nil {
		return nil, err
	}
	uploadID := resp.Header.Get(api.HeaderUploadID)

	chunkSize := requestSizeLimit
	if length < int64(chunkSize) {
		// Avoid creating a massive buffer for small data.
		chunkSize = int(length)
	}
	buf := getBuffer()
	defer putBuffer(buf)

	var written int64
	for written < length {
		n, err := io.CopyN(buf, reader, int64(chunkSize))
		if err == io.EOF {
			if written+n != length {
				return nil, io.ErrUnexpectedEOF
			}
		} else if err != nil {
			return nil, errors.WithStack(err)
		}

		path := path.Join("/uploads", uploadID)
		req, err := c.newRetryableRequest(http.MethodPatch, path, nil, buf)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		req.ContentLength = n
		req.Header.Set("Upload-Length", strconv.FormatInt(length, 10))
		req.Header.Set("Upload-Offset", strconv.FormatInt(written, 10))

		client := newRetryableClient()
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if err := errorFromResponse(resp); err != nil {
			return nil, err
		}

		if str := resp.Header.Get(api.HeaderDigest); str != "" {
			parts := strings.SplitN(str, " ", 2)
			digest, err := base64.StdEncoding.DecodeString(parts[1])
			return digest, errors.WithStack(err)
		}

		written += n
		buf.Reset()
	}

	return nil, errors.New("service did not return digest")
}
