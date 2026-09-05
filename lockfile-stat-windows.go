//go:build windows

package bbolt

import "os"

// statLockFilePath obtains identity from a shared handle while the writable
// mapping is live. Windows path stats defer the identity query to SameFile,
// whose zero-sharing open can fail against that mapping.
func statLockFilePath(path string) (os.FileInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.Stat()
}
