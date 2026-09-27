package bbolt

import (
	"bytes"
	"os"
	"strconv"
)

// processStart returns the start time of pid in clock ticks after boot, read
// from field 22 of /proc/<pid>/stat, or zero when it cannot be read.
func processStart(pid uint32) uint64 {
	stat, err := os.ReadFile("/proc/" + strconv.FormatUint(uint64(pid), 10) + "/stat")
	if err != nil {
		return 0
	}

	// The command name in field 2 may contain spaces, so count fields after
	// its closing parenthesis. Field 3 is the first after it.
	end := bytes.LastIndexByte(stat, ')')
	if end < 0 {
		return 0
	}
	fields := bytes.Fields(stat[end+1:])
	if len(fields) < 20 {
		return 0
	}
	start, err := strconv.ParseUint(string(fields[19]), 10, 64)
	if err != nil {
		return 0
	}
	return start
}
