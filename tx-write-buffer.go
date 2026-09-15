package bbolt

import (
	"io"
	"sync"
)

// pageWriteBuffer combines adjacent dirty pages into bounded file writes.
// Sync and metadata publication remain with the transaction's commit path.
type pageWriteBuffer [128 << 10]byte

var pageWriteBuffers = sync.Pool{New: func() any { return new(pageWriteBuffer) }}

// pageWriter borrows its buffer until all pending pages have been flushed.
type pageWriter struct {
	tx     *Tx
	buffer []byte
	offset int64
}

// write coalesces adjacent ranges. Large pages use the original direct path.
func (w *pageWriter) write(data []byte, offset int64) error {
	if len(w.buffer) != 0 && (offset != w.offset+int64(len(w.buffer)) || len(data) > cap(w.buffer)-len(w.buffer)) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if len(data) > cap(w.buffer) {
		return w.writeAt(data, offset)
	}
	if len(w.buffer) == 0 {
		w.offset = offset
	}
	w.buffer = append(w.buffer, data...)
	return nil
}

// flush writes the current contiguous run without changing durability policy.
func (w *pageWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	if err := w.writeAt(w.buffer, w.offset); err != nil {
		return err
	}
	w.buffer = w.buffer[:0]
	return nil
}

func (w *pageWriter) writeAt(data []byte, offset int64) error {
	n, err := w.tx.db.ops.writeAt(data, offset)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.tx.db.Logger().Errorf("writeAt failed, offset: %d: %v", offset, err)
		return err
	}
	w.tx.stats.IncWrite(1)
	return nil
}
