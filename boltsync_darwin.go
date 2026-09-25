package bbolt

import "golang.org/x/sys/unix"

// barrierfsync orders written data before all later writes to the file
// without waiting for the drive to flush its cache. File systems without
// barrier support fall back to a full flush.
func barrierfsync(db *DB) error {
	if _, err := unix.FcntlInt(db.file.Fd(), unix.F_BARRIERFSYNC, 0); err == nil {
		return nil
	}
	return fdatasync(db)
}
