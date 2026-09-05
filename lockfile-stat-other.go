//go:build !windows

package bbolt

import "os"

// statLockFilePath preserves POSIX process locks by avoiding an extra open and
// close of the coordination file. Closing any descriptor can release its locks.
func statLockFilePath(path string) (os.FileInfo, error) {
	return os.Stat(path)
}
