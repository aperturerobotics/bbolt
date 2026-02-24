//go:build !windows

package bbolt

import "golang.org/x/sys/unix"

// mlock locks memory of db file
func mlock(db *DB, fileSize int) error {
	sizeToLock := min(fileSize,
		// Can't lock more than mmaped slice
		db.datasz)
	if err := unix.Mlock(db.dataref[:sizeToLock]); err != nil {
		return err
	}
	return nil
}

// munlock unlocks memory of db file
func munlock(db *DB, fileSize int) error {
	if db.dataref == nil {
		return nil
	}

	sizeToUnlock := min(fileSize,
		// Can't unlock more than mmaped slice
		db.datasz)

	if err := unix.Munlock(db.dataref[:sizeToUnlock]); err != nil {
		return err
	}
	return nil
}
