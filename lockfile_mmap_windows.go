//go:build windows

package bbolt

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// mmapLockFile memory-maps the lock file with read-write shared access on Windows.
func mmapLockFile(f interface{ Fd() uintptr }, sz int) ([]byte, error) {
	sizehi := uint32(sz >> 32)
	sizelo := uint32(sz)

	h, errno := syscall.CreateFileMapping(syscall.Handle(f.(*os.File).Fd()), nil, syscall.PAGE_READWRITE, sizehi, sizelo, nil)
	if h == 0 {
		return nil, fmt.Errorf("CreateFileMapping lock file: %w", os.NewSyscallError("CreateFileMapping", errno))
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ|syscall.FILE_MAP_WRITE, 0, 0, 0)
	if addr == 0 {
		_ = syscall.CloseHandle(h)
		return nil, fmt.Errorf("MapViewOfFile lock file: %w", os.NewSyscallError("MapViewOfFile", errno))
	}

	if err := syscall.CloseHandle(h); err != nil {
		return nil, fmt.Errorf("CloseHandle lock file: %w", os.NewSyscallError("CloseHandle", err))
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), sz)
	return data, nil
}

// munmapLockFile unmaps the lock file on Windows.
func munmapLockFile(data []byte) error {
	addr := uintptr(unsafe.Pointer(&data[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
