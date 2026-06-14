//go:build !linux

package media

import "os"

func punchHole(f *os.File, off, length int64) error {
	return nil
}
