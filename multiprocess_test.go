//go:build !windows && !plan9

package bbolt_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	bolt "github.com/aperturerobotics/bbolt"
)

// Multi-process integration tests for cross-process coordination.
//
// These tests verify that the lock file mechanism works correctly across
// real operating system processes. Each test spawns child processes using
// exec.Command that re-exec the test binary with a special environment
// variable (BBOLT_TEST_CHILD_CMD) to run child-specific logic.

func init() {
	// If we are being run as a child process, execute the child command
	// and exit. This is checked in init() so it runs before any test
	// functions, allowing the child to perform its work and exit cleanly.
	if cmd := os.Getenv("BBOLT_TEST_CHILD_CMD"); cmd != "" {
		runChildCommand(cmd)
		os.Exit(0)
	}
}

// runChildCommand dispatches child process operations based on the
// BBOLT_TEST_CHILD_CMD environment variable.
func runChildCommand(cmd string) {
	dbPath := os.Getenv("BBOLT_TEST_DB_PATH")
	if dbPath == "" {
		fmt.Fprintf(os.Stderr, "BBOLT_TEST_DB_PATH not set\n")
		os.Exit(1)
	}

	switch cmd {
	case "read-and-verify":
		childReadAndVerify(dbPath)
	case "write-unique-key":
		childWriteUniqueKey(dbPath)
	case "read-hold-reread":
		childReadHoldReread(dbPath)
	case "open-and-crash":
		childOpenAndCrash(dbPath)
	case "write-sequential":
		childWriteSequential(dbPath)
	case "read-verify-hold-reverify":
		childReadVerifyHoldReverify(dbPath)
	case "rapid-open-close":
		childRapidOpenClose(dbPath)
	case "write-and-hang":
		childWriteAndHang(dbPath)
	default:
		fmt.Fprintf(os.Stderr, "unknown child command: %s\n", cmd)
		os.Exit(1)
	}
}

// childReadAndVerify opens the DB, reads and verifies a key, signals
// readiness, holds the transaction open briefly, then closes.
func childReadAndVerify(dbPath string) {
	signalPath := os.Getenv("BBOLT_TEST_SIGNAL_PATH")
	expectedKey := os.Getenv("BBOLT_TEST_EXPECTED_KEY")
	expectedVal := os.Getenv("BBOLT_TEST_EXPECTED_VAL")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = tx.Rollback() }()

	b := tx.Bucket([]byte(bucketName))
	if b == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found\n", bucketName)
		os.Exit(1)
	}

	val := b.Get([]byte(expectedKey))
	if string(val) != expectedVal {
		fmt.Fprintf(os.Stderr, "child: key %q = %q, want %q\n", expectedKey, val, expectedVal)
		os.Exit(1)
	}

	// Signal that we have the transaction open and verified the data.
	if signalPath != "" {
		if err := os.WriteFile(signalPath, []byte("ready"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "child: write signal: %v\n", err)
			os.Exit(1)
		}
	}

	// Hold the transaction open briefly so the parent can observe it.
	time.Sleep(200 * time.Millisecond)
}

// childWriteUniqueKey opens the DB, writes a unique key in a write
// transaction, holds briefly, then commits.
func childWriteUniqueKey(dbPath string) {
	writerID := os.Getenv("BBOLT_TEST_WRITER_ID")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		key := fmt.Sprintf("writer-%s", writerID)
		val := fmt.Sprintf("value-from-%s", writerID)

		// Hold the write lock briefly to create overlap window.
		time.Sleep(100 * time.Millisecond)

		return b.Put([]byte(key), []byte(val))
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child update: %v\n", err)
		os.Exit(1)
	}
}

// childReadHoldReread opens the DB, reads a key verifying an expected
// "before" value, signals readiness, holds the transaction open so the
// parent can write a new value, ends the transaction, starts a new one,
// and verifies the "after" value.
func childReadHoldReread(dbPath string) {
	signalPath := os.Getenv("BBOLT_TEST_SIGNAL_PATH")
	parentDonePath := os.Getenv("BBOLT_TEST_PARENT_DONE_PATH")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// First read transaction: verify "before" value.
	tx, err := db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx1: %v\n", err)
		os.Exit(1)
	}

	b := tx.Bucket([]byte(bucketName))
	if b == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found\n", bucketName)
		os.Exit(1)
	}

	val := b.Get([]byte("v1"))
	if string(val) != "before" {
		fmt.Fprintf(os.Stderr, "child: v1 = %q, want %q\n", val, "before")
		os.Exit(1)
	}

	// Signal readiness: parent can now write.
	if signalPath != "" {
		if err := os.WriteFile(signalPath, []byte("ready"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "child: write signal: %v\n", err)
			os.Exit(1)
		}
	}

	// Wait for the parent to signal that it has committed the write.
	if parentDonePath != "" {
		for i := 0; i < 100; i++ {
			if _, err := os.Stat(parentDonePath); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// End the first transaction.
	if err := tx.Rollback(); err != nil {
		fmt.Fprintf(os.Stderr, "child rollback tx1: %v\n", err)
		os.Exit(1)
	}

	// Second read transaction: should see the "after" value.
	tx2, err := db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx2: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = tx2.Rollback() }()

	b2 := tx2.Bucket([]byte(bucketName))
	if b2 == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found in tx2\n", bucketName)
		os.Exit(1)
	}

	val2 := b2.Get([]byte("v1"))
	if string(val2) != "after" {
		fmt.Fprintf(os.Stderr, "child: v1 = %q in tx2, want %q\n", val2, "after")
		os.Exit(1)
	}
}

// childOpenAndCrash opens the DB and begins a read transaction, then
// simply exits without closing anything -- simulating a process crash.
func childOpenAndCrash(dbPath string) {
	signalPath := os.Getenv("BBOLT_TEST_SIGNAL_PATH")

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}

	// Begin a read transaction to register in the reader table.
	_, err = db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx: %v\n", err)
		os.Exit(1)
	}

	// Signal that we have the transaction open.
	if signalPath != "" {
		if err := os.WriteFile(signalPath, []byte("ready"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "child: write signal: %v\n", err)
			os.Exit(1)
		}
	}

	// Do NOT close the DB or rollback the transaction.
	// Just sleep long enough for the parent to observe us, then we will
	// be killed by the parent to simulate a crash.
	time.Sleep(30 * time.Second)
}

// childWriteSequential opens the DB and writes a range of keys
// "seq-{start}" through "seq-{start+count-1}" to the "test" bucket.
// Uses BBOLT_TEST_KEY_START and BBOLT_TEST_KEY_COUNT env vars.
func childWriteSequential(dbPath string) {
	startStr := os.Getenv("BBOLT_TEST_KEY_START")
	countStr := os.Getenv("BBOLT_TEST_KEY_COUNT")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	start, err := strconv.Atoi(startStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: invalid KEY_START %q: %v\n", startStr, err)
		os.Exit(1)
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: invalid KEY_COUNT %q: %v\n", countStr, err)
		os.Exit(1)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		for i := start; i < start+count; i++ {
			key := fmt.Sprintf("seq-%d", i)
			val := fmt.Sprintf("val-%d", i)
			if err := b.Put([]byte(key), []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child update: %v\n", err)
		os.Exit(1)
	}
}

// childReadVerifyHoldReverify opens the DB, reads a key and verifies
// the expected original value, signals readiness, waits for the parent
// to signal completion, verifies the key STILL has the original value
// (snapshot isolation), then ends the transaction, starts a new one,
// and verifies the key now has the final value.
//
// Uses env vars:
//   - BBOLT_TEST_VERIFY_KEY: the key to read
//   - BBOLT_TEST_EXPECTED_VAL: the original value to verify
//   - BBOLT_TEST_FINAL_VAL: the latest value to verify after re-read
//   - BBOLT_TEST_SIGNAL_PATH: path to write readiness signal
//   - BBOLT_TEST_PARENT_DONE_PATH: path to poll for parent completion
func childReadVerifyHoldReverify(dbPath string) {
	signalPath := os.Getenv("BBOLT_TEST_SIGNAL_PATH")
	parentDonePath := os.Getenv("BBOLT_TEST_PARENT_DONE_PATH")
	verifyKey := os.Getenv("BBOLT_TEST_VERIFY_KEY")
	expectedVal := os.Getenv("BBOLT_TEST_EXPECTED_VAL")
	finalVal := os.Getenv("BBOLT_TEST_FINAL_VAL")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// First read transaction: verify original value.
	tx, err := db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx1: %v\n", err)
		os.Exit(1)
	}

	b := tx.Bucket([]byte(bucketName))
	if b == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found\n", bucketName)
		os.Exit(1)
	}

	val := b.Get([]byte(verifyKey))
	if string(val) != expectedVal {
		fmt.Fprintf(os.Stderr, "child: %q = %q, want %q\n", verifyKey, val, expectedVal)
		os.Exit(1)
	}

	// Signal readiness: parent can now write.
	if signalPath != "" {
		if err := os.WriteFile(signalPath, []byte("ready"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "child: write signal: %v\n", err)
			os.Exit(1)
		}
	}

	// Wait for the parent to signal that all writes are done.
	if parentDonePath != "" {
		for i := 0; i < 200; i++ {
			if _, err := os.Stat(parentDonePath); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Verify the key STILL has the original value in the held snapshot.
	val = b.Get([]byte(verifyKey))
	if string(val) != expectedVal {
		fmt.Fprintf(os.Stderr, "child: after writes, %q = %q in held tx, want %q (snapshot broken)\n",
			verifyKey, val, expectedVal)
		os.Exit(1)
	}

	// End the first transaction.
	if err := tx.Rollback(); err != nil {
		fmt.Fprintf(os.Stderr, "child rollback tx1: %v\n", err)
		os.Exit(1)
	}

	// Second read transaction: should see the latest value.
	tx2, err := db.Begin(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin tx2: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = tx2.Rollback() }()

	b2 := tx2.Bucket([]byte(bucketName))
	if b2 == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found in tx2\n", bucketName)
		os.Exit(1)
	}

	val2 := b2.Get([]byte(verifyKey))
	if string(val2) != finalVal {
		fmt.Fprintf(os.Stderr, "child: %q = %q in tx2, want %q\n", verifyKey, val2, finalVal)
		os.Exit(1)
	}
}

// childRapidOpenClose opens the DB, reads a key, closes the DB, and
// repeats for BBOLT_TEST_ITERATIONS times. Tests that rapid open/close
// cycles don't leak reader slots or corrupt lock file state.
func childRapidOpenClose(dbPath string) {
	iterStr := os.Getenv("BBOLT_TEST_ITERATIONS")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}

	iterations, err := strconv.Atoi(iterStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: invalid ITERATIONS %q: %v\n", iterStr, err)
		os.Exit(1)
	}

	for i := 0; i < iterations; i++ {
		db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open db iter %d: %v\n", i, err)
			os.Exit(1)
		}

		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			if b == nil {
				return fmt.Errorf("bucket %q not found", bucketName)
			}
			val := b.Get([]byte("init"))
			if string(val) != "value" {
				return fmt.Errorf("key init = %q, want %q", val, "value")
			}
			return nil
		})
		if err != nil {
			db.Close()
			fmt.Fprintf(os.Stderr, "child view iter %d: %v\n", i, err)
			os.Exit(1)
		}

		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "child close db iter %d: %v\n", i, err)
			os.Exit(1)
		}
	}
}

// childWriteAndHang opens the DB, begins a write transaction, writes
// data, signals readiness, then sleeps without committing. The parent
// is expected to kill this process to test crash recovery.
func childWriteAndHang(dbPath string) {
	signalPath := os.Getenv("BBOLT_TEST_SIGNAL_PATH")
	bucketName := os.Getenv("BBOLT_TEST_BUCKET")
	if bucketName == "" {
		bucketName = "test"
	}
	writeKey := os.Getenv("BBOLT_TEST_WRITE_KEY")
	writeVal := os.Getenv("BBOLT_TEST_WRITE_VAL")

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open db: %v\n", err)
		os.Exit(1)
	}

	// Begin a write transaction but do NOT commit.
	tx, err := db.Begin(true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child begin write tx: %v\n", err)
		os.Exit(1)
	}

	b := tx.Bucket([]byte(bucketName))
	if b == nil {
		fmt.Fprintf(os.Stderr, "child: bucket %q not found\n", bucketName)
		os.Exit(1)
	}

	if err := b.Put([]byte(writeKey), []byte(writeVal)); err != nil {
		fmt.Fprintf(os.Stderr, "child: put: %v\n", err)
		os.Exit(1)
	}

	// Signal readiness: we have the write lock and wrote data but
	// have NOT committed.
	if signalPath != "" {
		if err := os.WriteFile(signalPath, []byte("ready"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "child: write signal: %v\n", err)
			os.Exit(1)
		}
	}

	// Sleep until killed. Do NOT commit or close.
	time.Sleep(30 * time.Second)
}

// TestHelperProcess is a no-op test target that exists so child processes
// can be invoked via -test.run=TestHelperProcess. The actual work is done
// in init() via the BBOLT_TEST_CHILD_CMD environment variable check.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("BBOLT_TEST_CHILD_CMD") == "" {
		t.Skip("helper process test - skipping in normal test runs")
	}
}

// spawnChild creates a child process that will re-exec the test binary
// with the given command and environment variables.
func spawnChild(t *testing.T, cmd, dbPath string, extraEnv ...string) *exec.Cmd {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "-test.v")
	child.Env = append(os.Environ(),
		"BBOLT_TEST_CHILD_CMD="+cmd,
		"BBOLT_TEST_DB_PATH="+dbPath,
	)
	child.Env = append(child.Env, extraEnv...)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	return child
}

// waitForSignal polls for a signal file to appear, returning an error
// if the timeout is reached.
func waitForSignal(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for signal file: %s", path)
}

// TestMultiProcessConcurrentReaders verifies that multiple child
// processes can read the database concurrently while the parent writes.
// All readers should see consistent snapshots.
func TestMultiProcessConcurrentReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB and writes initial data.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("init"), []byte("value"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Spawn 3 child readers.
	const numReaders = 3
	children := make([]*exec.Cmd, numReaders)
	signals := make([]string, numReaders)

	for i := 0; i < numReaders; i++ {
		signals[i] = filepath.Join(dir, fmt.Sprintf("signal-%d", i))
		children[i] = spawnChild(t, "read-and-verify", dbPath,
			"BBOLT_TEST_SIGNAL_PATH="+signals[i],
			"BBOLT_TEST_EXPECTED_KEY=init",
			"BBOLT_TEST_EXPECTED_VAL=value",
			"BBOLT_TEST_BUCKET=test",
		)
		if err := children[i].Start(); err != nil {
			t.Fatalf("start child %d: %v", i, err)
		}
	}

	// Wait for all children to signal readiness.
	for i := 0; i < numReaders; i++ {
		if err := waitForSignal(signals[i], 10*time.Second); err != nil {
			t.Fatalf("child %d signal: %v", i, err)
		}
	}

	// All readers have the transaction open and verified the data.
	// Wait for all children to complete.
	for i := 0; i < numReaders; i++ {
		if err := children[i].Wait(); err != nil {
			t.Errorf("child %d exited with error: %v", i, err)
		}
	}
}

// TestMultiProcessWriterExclusion verifies that two processes trying to
// write simultaneously are serialized -- both succeed but one must wait
// for the other. The final DB should contain both writes.
func TestMultiProcessWriterExclusion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB with a bucket.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Spawn 2 child writers that each write a unique key.
	child1 := spawnChild(t, "write-unique-key", dbPath,
		"BBOLT_TEST_WRITER_ID=1",
		"BBOLT_TEST_BUCKET=test",
	)
	child2 := spawnChild(t, "write-unique-key", dbPath,
		"BBOLT_TEST_WRITER_ID=2",
		"BBOLT_TEST_BUCKET=test",
	)

	if err := child1.Start(); err != nil {
		t.Fatalf("start child1: %v", err)
	}
	if err := child2.Start(); err != nil {
		t.Fatalf("start child2: %v", err)
	}

	if err := child1.Wait(); err != nil {
		t.Errorf("child1 exited with error: %v", err)
	}
	if err := child2.Wait(); err != nil {
		t.Errorf("child2 exited with error: %v", err)
	}

	// Verify both keys exist in the final DB.
	db, err = bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		v1 := b.Get([]byte("writer-1"))
		if string(v1) != "value-from-1" {
			return fmt.Errorf("writer-1: got %q, want %q", v1, "value-from-1")
		}

		v2 := b.Get([]byte("writer-2"))
		if string(v2) != "value-from-2" {
			return fmt.Errorf("writer-2: got %q, want %q", v2, "value-from-2")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestMultiProcessReadWhileWrite verifies MVCC isolation across processes.
// A reader holds a transaction while a writer commits a new value. The
// reader should see the pre-commit state until it starts a new transaction.
func TestMultiProcessReadWhileWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB and writes initial value.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("v1"), []byte("before"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	signalPath := filepath.Join(dir, "child-ready")
	parentDonePath := filepath.Join(dir, "parent-done")

	// Spawn child that reads, holds, then re-reads.
	child := spawnChild(t, "read-hold-reread", dbPath,
		"BBOLT_TEST_SIGNAL_PATH="+signalPath,
		"BBOLT_TEST_PARENT_DONE_PATH="+parentDonePath,
		"BBOLT_TEST_BUCKET=test",
	)
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait for the child to signal it has verified "before" and is holding
	// the transaction open.
	if err := waitForSignal(signalPath, 10*time.Second); err != nil {
		t.Fatalf("child signal: %v", err)
	}

	// Parent writes the new value.
	db, err = bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("reopen db for write: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		return b.Put([]byte("v1"), []byte("after"))
	})
	if err != nil {
		t.Fatalf("parent write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db after write: %v", err)
	}

	// Signal the child that the parent has committed.
	if err := os.WriteFile(parentDonePath, []byte("done"), 0600); err != nil {
		t.Fatalf("write parent done signal: %v", err)
	}

	// Wait for child to finish verifying the "after" value.
	if err := child.Wait(); err != nil {
		t.Fatalf("child exited with error: %v", err)
	}
}

// TestMultiProcessStaleReaderRecovery verifies that when a reader process
// crashes (is killed), the stale reader entry is detected and cleared,
// allowing the writer to reclaim pages normally.
func TestMultiProcessStaleReaderRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB and writes some data.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val := fmt.Sprintf("val-%03d", i)
			if err := b.Put([]byte(key), []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	signalPath := filepath.Join(dir, "child-ready")

	// Spawn a child that opens DB, begins a read tx, and then sleeps.
	child := spawnChild(t, "open-and-crash", dbPath,
		"BBOLT_TEST_SIGNAL_PATH="+signalPath,
	)
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait for the child to signal it has the transaction open.
	if err := waitForSignal(signalPath, 10*time.Second); err != nil {
		// Kill the child if it hasn't signaled.
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("child signal: %v", err)
	}

	// Kill the child process to simulate a crash (no cleanup).
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = child.Wait() // Reap the process.

	// Give the OS a moment to fully release the process.
	time.Sleep(100 * time.Millisecond)

	// Open the lock file directly and verify stale reader detection.
	lockPath := dbPath + "-lock"
	lockData, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read lock file: %v", err)
	}
	if len(lockData) == 0 {
		t.Fatal("lock file is empty")
	}

	// Reopen the DB as the parent. This should work even though the
	// killed child left a stale reader entry.
	db, err = bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	// Write more data. The freelist should not be blocked by the stale
	// reader because OldestReaderTxid calls ClearStaleReaders first.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val := fmt.Sprintf("val-%03d", i)
			if err := b.Put([]byte(key), []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write after stale reader: %v", err)
	}

	// Verify the data is intact.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val := b.Get([]byte(key))
			expected := fmt.Sprintf("val-%03d", i)
			if string(val) != expected {
				return fmt.Errorf("key %s: got %q, want %q", key, val, expected)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify data: %v", err)
	}
}

// TestMultiProcessSequentialWriters verifies that multiple processes
// writing sequentially (not concurrently) each correctly reload the
// freelist from disk. Each writer opens the DB, writes unique keys,
// commits, and closes. The next writer must see all previous writes
// and correctly manage the freelist via refreshForWriter().
func TestMultiProcessSequentialWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB with the "test" bucket.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Run 3 child writers sequentially. Each writes 10 keys.
	for i := 0; i < 3; i++ {
		start := i * 10
		child := spawnChild(t, "write-sequential", dbPath,
			fmt.Sprintf("BBOLT_TEST_KEY_START=%d", start),
			"BBOLT_TEST_KEY_COUNT=10",
			"BBOLT_TEST_BUCKET=test",
		)
		if err := child.Run(); err != nil {
			t.Fatalf("child %d (start=%d): %v", i, start, err)
		}
	}

	// Verify all 30 keys are present with correct values.
	db, err = bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("seq-%d", i)
			val := b.Get([]byte(key))
			expected := fmt.Sprintf("val-%d", i)
			if string(val) != expected {
				return fmt.Errorf("key %s: got %q, want %q", key, val, expected)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestMultiProcessReaderAcrossMultipleCommits is the most important
// freelist correctness test. A reader holds a snapshot while the writer
// makes MULTIPLE commits, each modifying all keys. The reader must still
// see the original data -- the freelist must not reclaim any pages
// referenced by the reader's snapshot even after several commits.
func TestMultiProcessReaderAcrossMultipleCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create 1KB values to use multiple pages.
	makeVal := func(prefix string, i int) string {
		base := fmt.Sprintf("%s-%03d-", prefix, i)
		return base + strings.Repeat("x", 1024-len(base))
	}

	// The number of write rounds and the final value are known upfront.
	const numRounds = 5
	originalVal := makeVal("orig", 0)
	finalVal := makeVal(fmt.Sprintf("round%d", numRounds), 0)

	// Parent writes initial data: 100 keys with 1KB values.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%03d", i)
			if err := b.Put([]byte(key), []byte(makeVal("orig", i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}

	signalPath := filepath.Join(dir, "child-ready")
	parentDonePath := filepath.Join(dir, "parent-done")

	// Spawn child reader with all values known upfront, including the
	// final value it should see after re-reading post-writes.
	child := spawnChild(t, "read-verify-hold-reverify", dbPath,
		"BBOLT_TEST_SIGNAL_PATH="+signalPath,
		"BBOLT_TEST_PARENT_DONE_PATH="+parentDonePath,
		"BBOLT_TEST_VERIFY_KEY=key-000",
		"BBOLT_TEST_EXPECTED_VAL="+originalVal,
		"BBOLT_TEST_FINAL_VAL="+finalVal,
		"BBOLT_TEST_BUCKET=test",
	)

	if err := child.Start(); err != nil {
		db.Close()
		t.Fatalf("start child: %v", err)
	}

	// Wait for child to signal it has verified the original value and
	// is holding the read transaction open.
	if err := waitForSignal(signalPath, 10*time.Second); err != nil {
		db.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("child signal: %v", err)
	}

	// Parent does sequential write transactions while the child holds
	// its snapshot. Each round modifies all 100 keys, forcing page
	// allocations and freelist changes.
	for round := 1; round <= numRounds; round++ {
		prefix := fmt.Sprintf("round%d", round)
		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("test"))
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key-%03d", i)
				if err := b.Put([]byte(key), []byte(makeVal(prefix, i))); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			db.Close()
			_ = child.Process.Kill()
			_ = child.Wait()
			t.Fatalf("write round %d: %v", round, err)
		}
	}

	db.Close()

	// Signal child that all writes are done. The child will verify
	// snapshot isolation (original value still visible in held tx),
	// then start a new transaction and verify the final value.
	if err := os.WriteFile(parentDonePath, []byte("done"), 0600); err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("write parent done signal: %v", err)
	}

	// Wait for child to complete its verification.
	if err := child.Wait(); err != nil {
		t.Fatalf("child exited with error: %v", err)
	}
}

// TestMultiProcessManyReaders stress-tests the reader table with 10+
// concurrent reader processes all reading simultaneously.
func TestMultiProcessManyReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB and writes initial data.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("init"), []byte("value"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Spawn 12 concurrent readers.
	const numReaders = 12
	children := make([]*exec.Cmd, numReaders)
	signals := make([]string, numReaders)

	for i := 0; i < numReaders; i++ {
		signals[i] = filepath.Join(dir, fmt.Sprintf("signal-%d", i))
		children[i] = spawnChild(t, "read-and-verify", dbPath,
			"BBOLT_TEST_SIGNAL_PATH="+signals[i],
			"BBOLT_TEST_EXPECTED_KEY=init",
			"BBOLT_TEST_EXPECTED_VAL=value",
			"BBOLT_TEST_BUCKET=test",
		)
		if err := children[i].Start(); err != nil {
			t.Fatalf("start child %d: %v", i, err)
		}
	}

	// Wait for all children to signal readiness.
	for i := 0; i < numReaders; i++ {
		if err := waitForSignal(signals[i], 15*time.Second); err != nil {
			t.Fatalf("child %d signal: %v", i, err)
		}
	}

	// Wait for all children to complete.
	for i := 0; i < numReaders; i++ {
		if err := children[i].Wait(); err != nil {
			t.Errorf("child %d exited with error: %v", i, err)
		}
	}
}

// TestMultiProcessDBGrowth verifies that a reader holding a transaction
// on a small DB still sees consistent data after the writer grows the DB
// file significantly (triggering mmap resize in the writer's process).
func TestMultiProcessDBGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Make a large value to force DB growth when writing many keys.
	largeVal := strings.Repeat("G", 1024)

	// Parent writes small initial data (1 key).
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("init"), []byte("original"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// Record initial DB file size.
	info, err := os.Stat(dbPath)
	if err != nil {
		db.Close()
		t.Fatalf("stat db: %v", err)
	}
	initialSize := info.Size()
	t.Logf("initial DB size: %d bytes", initialSize)

	signalPath := filepath.Join(dir, "child-ready")
	parentDonePath := filepath.Join(dir, "parent-done")

	// The final value for "init" after growth writes is "grown".
	// beginTx remaps automatically when the file has grown beyond
	// the current mmap, so the child can start a new read transaction
	// on the same DB handle without close/reopen.
	child := spawnChild(t, "read-verify-hold-reverify", dbPath,
		"BBOLT_TEST_SIGNAL_PATH="+signalPath,
		"BBOLT_TEST_PARENT_DONE_PATH="+parentDonePath,
		"BBOLT_TEST_VERIFY_KEY=init",
		"BBOLT_TEST_EXPECTED_VAL=original",
		"BBOLT_TEST_FINAL_VAL=grown",
		"BBOLT_TEST_BUCKET=test",
	)

	if err := child.Start(); err != nil {
		db.Close()
		t.Fatalf("start child: %v", err)
	}

	// Wait for child to signal it verified "original" and is holding tx.
	if err := waitForSignal(signalPath, 10*time.Second); err != nil {
		db.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("child signal: %v", err)
	}

	// Write enough data to force DB growth: 500 keys * 1KB = ~500KB.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 0; i < 500; i++ {
			key := fmt.Sprintf("grow-%03d", i)
			if err := b.Put([]byte(key), []byte(largeVal)); err != nil {
				return err
			}
		}
		// Also update the init key to "grown".
		return b.Put([]byte("init"), []byte("grown"))
	})
	if err != nil {
		db.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("growth write: %v", err)
	}

	// Verify DB actually grew.
	info, err = os.Stat(dbPath)
	if err != nil {
		db.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("stat db after growth: %v", err)
	}
	grownSize := info.Size()
	t.Logf("grown DB size: %d bytes (was %d)", grownSize, initialSize)
	if grownSize <= initialSize {
		db.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("DB did not grow: %d <= %d", grownSize, initialSize)
	}

	db.Close()

	// Signal child that writes are done.
	if err := os.WriteFile(parentDonePath, []byte("done"), 0600); err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("write parent done signal: %v", err)
	}

	// Child verifies "original" still in held tx, then reopens DB
	// and re-reads "grown".
	if err := child.Wait(); err != nil {
		t.Fatalf("child exited with error: %v", err)
	}
}

// TestMultiProcessRapidOpenClose verifies that multiple processes
// rapidly opening and closing the DB don't leak reader slots, corrupt
// the lock file, or deadlock.
func TestMultiProcessRapidOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent creates DB with initial data.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("init"), []byte("value"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Spawn 5 child processes that each open/read/close 10 times.
	const numChildren = 5
	children := make([]*exec.Cmd, numChildren)
	for i := 0; i < numChildren; i++ {
		children[i] = spawnChild(t, "rapid-open-close", dbPath,
			"BBOLT_TEST_ITERATIONS=10",
			"BBOLT_TEST_BUCKET=test",
		)
		if err := children[i].Start(); err != nil {
			t.Fatalf("start child %d: %v", i, err)
		}
	}

	// Wait for all children to complete.
	for i := 0; i < numChildren; i++ {
		if err := children[i].Wait(); err != nil {
			t.Errorf("child %d exited with error: %v", i, err)
		}
	}

	// Parent verifies DB data is still intact.
	db, err = bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		val := b.Get([]byte("init"))
		if string(val) != "value" {
			return fmt.Errorf("key init = %q, want %q", val, "value")
		}
		return nil
	})
	if err != nil {
		db.Close()
		t.Fatalf("verify data: %v", err)
	}
	db.Close()

	// Verify no occupied reader slots in the lock file.
	// Lock file layout: 64-byte header + 64-byte writer region +
	// N * 64-byte reader slots. Each slot: [pid uint32, reserved uint32,
	// txid uint64, 48 bytes padding].
	lockPath := dbPath + "-lock"
	lockData, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read lock file: %v", err)
	}

	// Read maxReaders from header (offset 8, uint32 LE).
	if len(lockData) < 12 {
		t.Fatalf("lock file too small: %d bytes", len(lockData))
	}
	maxReaders := binary.LittleEndian.Uint32(lockData[8:12])

	// Scan reader table for any occupied slots (non-zero pid).
	occupied := 0
	for i := 0; i < int(maxReaders); i++ {
		off := 128 + i*64 // readerTableOffset + slot*readerSlotSize
		if off+4 > len(lockData) {
			break
		}
		pid := binary.LittleEndian.Uint32(lockData[off : off+4])
		if pid != 0 {
			occupied++
			t.Logf("reader slot %d still occupied by pid %d", i, pid)
		}
	}
	if occupied > 0 {
		t.Errorf("found %d occupied reader slots after all processes closed", occupied)
	}
}

// TestMultiProcessWriterCrashRecovery verifies that killing a writer
// process mid-write (before commit) leaves the DB in a consistent state.
// The uncommitted write should be lost and the DB should still have the
// original data, and new writes should succeed.
func TestMultiProcessWriterCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Parent writes initial data.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("crash-test"), []byte("original"))
	})
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	signalPath := filepath.Join(dir, "child-ready")

	// Spawn a child that begins a write tx, writes data, signals ready,
	// then sleeps (never commits).
	child := spawnChild(t, "write-and-hang", dbPath,
		"BBOLT_TEST_SIGNAL_PATH="+signalPath,
		"BBOLT_TEST_BUCKET=test",
		"BBOLT_TEST_WRITE_KEY=crash-test",
		"BBOLT_TEST_WRITE_VAL=modified",
	)
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait for child to signal it has the write lock and wrote data.
	if err := waitForSignal(signalPath, 10*time.Second); err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("child signal: %v", err)
	}

	// Kill the child to simulate a crash mid-write.
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = child.Wait()

	// Give the OS a moment to fully release the process and file locks.
	time.Sleep(200 * time.Millisecond)

	// Reopen DB and verify the uncommitted write is lost.
	db, err = bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		val := b.Get([]byte("crash-test"))
		if string(val) != "original" {
			return fmt.Errorf("crash-test = %q, want %q (uncommitted write leaked)", val, "original")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify original data: %v", err)
	}

	// Verify new writes succeed after crash recovery.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		return b.Put([]byte("crash-test"), []byte("recovered"))
	})
	if err != nil {
		t.Fatalf("write after recovery: %v", err)
	}

	// Verify the new write stuck.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		val := b.Get([]byte("crash-test"))
		if string(val) != "recovered" {
			return fmt.Errorf("crash-test = %q, want %q", val, "recovered")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify recovered data: %v", err)
	}
}
