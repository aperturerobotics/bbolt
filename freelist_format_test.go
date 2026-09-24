package bbolt_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "github.com/aperturerobotics/bbolt"
	"github.com/aperturerobotics/bbolt/internal/btesting"
	"github.com/aperturerobotics/bbolt/internal/guts_cli"
)

// legacyFreelistPageFlag marks the page-id freelist pages older versions
// wrote.
const legacyFreelistPageFlag = 0x10

// Ensure that opening a file whose freelist page uses an older format rebuilds
// the freelist from the tree and writes it back in the span format.
func TestOpen_RebuildsLegacyFreelist(t *testing.T) {
	db := btesting.MustCreateDB(t)
	for _, name := range []string{"kept", "dropped"} {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := range 1000 {
				if err := b.Put(fmt.Appendf(nil, "%04d", i), make([]byte, 100)); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("dropped"))
	}))
	db.MustClose()

	meta, _, err := guts_cli.GetActiveMetaPage(db.Path())
	require.NoError(t, err)
	p, buf, err := guts_cli.ReadPage(db.Path(), uint64(meta.Freelist()))
	require.NoError(t, err)
	p.SetFlags(legacyFreelistPageFlag)
	require.NoError(t, guts_cli.WritePage(db.Path(), buf))

	db.MustReopen()
	db.MustCheck()
	require.NotZero(t, db.Stats().FreePageN)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("kept")).Put([]byte("new"), make([]byte, 100))
	}))
	db.MustCheck()
	db.MustClose()

	meta, _, err = guts_cli.GetActiveMetaPage(db.Path())
	require.NoError(t, err)
	p, _, err = guts_cli.ReadPage(db.Path(), uint64(meta.Freelist()))
	require.NoError(t, err)
	require.True(t, p.IsFreelistPage(), "freelist page type is %s", p.Typ())
	require.NotEmpty(t, p.FreelistPageSpans())
}
