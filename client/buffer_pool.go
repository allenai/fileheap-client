package client

import (
	"bytes"
	"sync"
)

// Global pool of dynamically sized buffers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Get a buffer from the global pool. The buffer must be returned to the pool
// with putBuffer when it is no longer needed.
func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// Return a buffer to the global pool. The caller may not use the buffer
// once it has been returned to the pool.
func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}
