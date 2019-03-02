package client

import "errors"

var (
	// ErrDone indicates an iterator is expended.
	ErrDone = errors.New("no more items in iterator")

	// ErrUploaded indicates a file upload is unnecessary as the service already
	// has the required data.
	ErrUploaded = errors.New("file is already uploaded")

	// ErrFileNotFound indicates that a file doesn't exist.
	ErrFileNotFound = errors.New("file not found")
)
