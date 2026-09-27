//go:build !linux && !darwin && !windows

package bbolt

// processStart returns zero: this platform reports no process start time, so
// stale reader detection relies on the pid alone.
func processStart(uint32) uint64 {
	return 0
}
