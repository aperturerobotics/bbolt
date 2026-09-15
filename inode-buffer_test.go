package bbolt_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	bolt "github.com/aperturerobotics/bbolt"
	"github.com/aperturerobotics/bbolt/internal/btesting"
)

// TestInodeBufferTransactions keeps a reader alive while writers commit and
// roll back page mutations. Reusing mutable entry storage must preserve both
// the reader's snapshot and the last committed state after writer cleanup.
func TestInodeBufferTransactions(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{InitialMmapSize: 32 << 20})
	keys := make([][]byte, 2048)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
	}
	initial := bytes.Repeat([]byte("initial"), 32)
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Put(key, initial); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Rollback(); err != nil {
			t.Error(err)
		}
	}()
	abort := errors.New("abort writer")
	want := initial
	for round := range 8 {
		value := bytes.Repeat([]byte{byte(round)}, 256)
		err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("data"))
			for _, key := range keys {
				if err := bucket.Put(key, value); err != nil {
					return err
				}
			}
			if round%2 != 0 {
				return abort
			}
			return nil
		})
		if round%2 != 0 {
			if !errors.Is(err, abort) {
				t.Fatal(err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			want = value
		}
		if err := db.View(func(tx *bolt.Tx) error {
			for _, key := range keys {
				if !bytes.Equal(reader.Bucket([]byte("data")).Get(key), initial) {
					return fmt.Errorf("old snapshot changed at %q", key)
				}
				if !bytes.Equal(tx.Bucket([]byte("data")).Get(key), want) {
					return fmt.Errorf("committed value changed at %q", key)
				}
			}
			for err := range tx.Check() {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
