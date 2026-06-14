//go:build linux

package media

import (
	"os"
	"syscall"
)

const (
	fallocPunchHole = 0x2
	fallocKeepSize  = 0x1
)

func punchHole(f *os.File, off, length int64) error {
	if length <= 0 {
		return nil
	}
	return syscall.Fallocate(int(f.Fd()), fallocPunchHole|fallocKeepSize, off, length)
}
