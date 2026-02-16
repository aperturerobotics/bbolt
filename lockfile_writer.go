package bbolt

import "fmt"

// writerLockError wraps an error from a writer lock operation.
func writerLockError(op string, err error) error {
	return fmt.Errorf("bbolt: %s writer lock: %w", op, err)
}
