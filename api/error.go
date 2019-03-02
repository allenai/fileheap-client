package api

import "fmt"

// Error encodes an error as a JSON-serializable struct.
type Error struct {
	// HTTP status code, such as 404
	Code int `json:"code"`

	// The text of the error, which should follow the guidelines found at:
	// https://github.com/golang/go/wiki/CodeReviewComments#error-strings
	Message string `json:"message"`

	// (optional) Long-form detail, such as the error's call stack
	Detail string `json:"detail,omitempty"`
}

// Error implements the standard error interface.
func (e Error) Error() string {
	return e.Message
}

// Format implements the fmt.Formatter interface.
func (e Error) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s\n%s", e.Message, e.Detail)
			return
		}
		fallthrough
	case 's':
		fmt.Fprint(s, e.Message)
	case 'q':
		fmt.Fprintf(s, "%q", e.Message)
	}
}
