package client

import (
	"io"
)

// Reader reads a stored file.
type Reader struct {
	body io.ReadCloser
	size int64
}

// Close closes the reader. It must be called before disposing of the reader.
func (r *Reader) Close() error {
	return r.body.Close()
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

// Size returns the size of the reader's content in bytes.
func (r *Reader) Size() int64 {
	return r.size
}
