package bbolt

import "golang.org/x/sys/unix"

// processStart returns the start time of pid in microseconds since the Unix
// epoch, or zero when it cannot be read.
func processStart(pid uint32) uint64 {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", int(pid))
	if err != nil {
		return 0
	}
	start := info.Proc.P_starttime
	return uint64(start.Sec)*1_000_000 + uint64(start.Usec)
}
