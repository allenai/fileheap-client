package api

import (
	"encoding/base64"
	"strings"

	"github.com/pkg/errors"
)

func EncodeDigest(digest []byte) string {
	return SHA256 + " " + base64.StdEncoding.EncodeToString(digest)
}

func DecodeDigest(digest string) ([]byte, error) {
	if digest == "" {
		return nil, nil
	}

	parts := strings.SplitN(digest, " ", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid digest: must include algorithm")
	}
	if parts[0] != SHA256 {
		return nil, errors.Errorf("invalid digest: %q is not a recognized algorithm", parts[0])
	}

	hash, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.Wrap(err, "invalid digest")
	}
	if len(hash) != 32 {
		return nil, errors.New("invalid digest: must be exactly 32 bytes")
	}

	return hash, nil
}
