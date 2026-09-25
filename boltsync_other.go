//go:build !darwin

package bbolt

// barrierfsync flushes written data; only darwin has a cheaper ordering sync.
func barrierfsync(db *DB) error {
	return fdatasync(db)
}
