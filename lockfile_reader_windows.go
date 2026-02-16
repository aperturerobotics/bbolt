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
