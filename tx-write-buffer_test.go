package bbolt

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestPageWriter preserves byte ranges and gaps while grouping adjacent pages,
// and reports a short file write instead of accepting a partial commit.
func TestPageWriter(t *testing.T) {
	data := bytes.Repeat([]byte{0xff}, 64)
	var writes int
	db := &DB{}
	db.ops.writeAt = func(p []byte, offset int64) (int, error) {
		writes++
		return copy(data[offset:], p), nil
	}
	tx := &Tx{db: db}
	w := pageWriter{tx: tx, buffer: make([]byte, 0, 8)}
	for _, item := range []struct {
		offset int64
		data   string
	}{{0, "abc"}, {3, "def"}, {10, "ghi"}, {13, "0123456789"}, {23, "end"}} {
		if err := w.write([]byte(item.data), item.offset); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if writes != 4 || tx.stats.GetWrite() != 4 {
		t.Fatalf("file writes=%d statistics=%d", writes, tx.stats.GetWrite())
	}
	if string(data[:6]) != "abcdef" || !bytes.Equal(data[6:10], bytes.Repeat([]byte{0xff}, 4)) || string(data[10:26]) != "ghi0123456789end" {
		t.Fatalf("written ranges=%q", data[:26])
	}
	db.ops.writeAt = func(p []byte, offset int64) (int, error) {
		return len(p) - 1, nil
	}
	if err := w.write([]byte("short"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error=%v", err)
	}
}
