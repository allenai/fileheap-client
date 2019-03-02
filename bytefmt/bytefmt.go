package bytefmt

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Binary byte sizes
const (
	_ = 1 << (iota * 10)
	KiB
	MiB
	GiB
)

func FormatBytes(n int64) string {
	return formatBytes(float64(n))
}

func FormatRate(n int64, d time.Duration) string {
	return formatBytes(float64(n)/d.Seconds()) + "/s"
}

func formatBytes(n float64) string {
	var suffix string
	switch {
	case n < KiB:
		suffix = "B"
	case n < MiB:
		n /= KiB
		suffix = "KiB"
	case n < GiB:
		n /= MiB
		suffix = "MiB"
	default:
		n /= GiB
		suffix = "GiB"
	}
	result := strconv.FormatFloat(n, 'f', 2, 64)
	result = strings.TrimRight(result, "0")
	result = strings.TrimRight(result, ".")
	return result + suffix
}

func ParseBytes(s string) (int64, error) {
	var n float64
	var unit string
	if n, err := fmt.Sscanf(s, "%f%s", &n, &unit); err != nil {
		// Allow no unit.
		if err != io.EOF || n != 1 {
			return 0, errors.WithStack(err)
		}
	}
	unit = strings.ToLower(unit)

	switch unit {
	case "", "b":
	case "kib", "kb", "k":
		n *= KiB
	case "mib", "mb", "m":
		n *= MiB
	case "gib", "gb", "g":
		n *= GiB
	default:
		return 0, errors.Errorf("invalid unit: %s", unit)
	}

	return int64(n), nil
}
