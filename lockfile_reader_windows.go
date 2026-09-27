//go:build windows

package bbolt

import "golang.org/x/sys/windows"

// processAlive checks whether the given process ID is still alive on
// Windows by attempting to open the process handle with limited query
// permissions. If the handle can be opened, the process exists.
func processAlive(pid uint32) bool {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return false
	}
	_ = windows.CloseHandle(h)
	return true
}

// processStart returns the creation time of pid as a FILETIME, or zero when
// the process cannot be queried.
func processStart(pid uint32) uint64 {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return 0
	}
	defer windows.CloseHandle(h)

	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(h, &creation, &exit, &kernel, &user); err != nil {
		return 0
	}
	return uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
}
