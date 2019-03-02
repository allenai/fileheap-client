package api

import (
	"time"
)

// Request size limits.
const (
	// Maximum number of files that can be included in a batch request.
	BatchSizeLimit = 500

	// Maximum size of a file that can be uploaded within a batch request or put directly.
	// Larger files must use the upload API.
	PutFileSizeLimit = 32 * 1024 * 1024
)

// Custom HTTP headers
const (
	// The Batch-Error header is sent as a trailer on batch responses.
	// If not empty, it indicates that there was an error processing
	// the batch request.
	HeaderBatchError = "Batch-Error"

	// The Path header indicates the path of a file in a batch operation.
	HeaderPath = "Path"

	// The Digest request and response header specifies cryptographic hashes for
	// a resource. The header must consist of the name of the digest algorithm
	// and the Base64-encoded checksum separated by a space.
	//
	// Example:
	// Digest: SHA256 qj7BbmrMgJ2LKBhmInYlar/S8bRBy1FXSTPz1L0RXRE=
	HeaderDigest = "Digest"

	// The Upload-Expires response header indicates the time after which an
	// unfinished upload expires.
	HeaderUploadExpires = "Upload-Expires"

	// The Upload-ID response header indicates the ID of a newly created upload.
	HeaderUploadID = "Upload-ID"

	// The Upload-Length request and response header indicates the size of an
	// entire upload in bytes. The value must be a non-negative integer.
	HeaderUploadLength = "Upload-Length"

	// The Upload-Offset request and response header indicates a byte offset
	// within a resource. The value must be a non-negative integer.
	HeaderUploadOffset = "Upload-Offset"
)

// Digest algorithms
const (
	SHA256 = "SHA256"
)

// HTTPTimeFormat is the standard HTTP format for timestamps.
const HTTPTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// Recognized scope classes. Example: "read:dataset[:datasetID]"
const (
	DatasetScope = "dataset"
	UploadScope  = "upload"
)

// Dataset is a collection of files.
type Dataset struct {
	ID      string    `json:"id"`
	Created time.Time `json:"created"`

	// Whether the dataset is locked for writes.
	ReadOnly bool `json:"readonly"`
}

// DatasetPatch allows modification of a dataset's mutable properties.
type DatasetPatch struct {
	// (optional) If true, lock the dataset for writes. Ignored if false.
	ReadOnly bool `json:"readonly,omitempty"`
}

// ManifestPage describes a list of files within a dataset.
type ManifestPage struct {
	// A list of files in the dataset, sorted by path. Results are limited to a
	// fix number of of files per request. If the res
	Files []FileInfo `json:"files"`

	// An optional cursor to retrieve further results.
	Cursor string `json:"cursor,omitempty"`
}

// FileInfo describes a single file within a dataset.
type FileInfo struct {
	// Path of the file relative to its dataset root.
	Path string `json:"path"`

	// Size of the file in bytes.
	Size int64 `json:"size"`

	// Cryptographic hash of the file's contents using the SHA256 algorithm.
	Digest []byte `json:"digest"`

	// Time at which the file was last updated.
	Updated time.Time `json:"updated"`
}
