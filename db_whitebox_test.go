package bbolt

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aperturerobotics/bbolt/errors"
	"github.com/aperturerobotics/bbolt/internal/common"
)

func TestOpenWithPreLoadFreelist(t *testing.T) {
	testCases := []struct {
		name                    string
		readonly                bool
		preLoadFreePage         bool
		expectedFreePagesLoaded bool
	}{
		{
			name:                    "write mode always load free pages",
			readonly:                false,
			preLoadFreePage:         false,
			expectedFreePagesLoaded: true,
		},
		{
			name:                    "readonly mode load free pages when flag set",
			readonly:                true,
			preLoadFreePage:         true,
			expectedFreePagesLoaded: true,
		},
		{
			name:                    "readonly mode doesn't load free pages when flag not set",
			readonly:                true,
			preLoadFreePage:         false,
			expectedFreePagesLoaded: false,
		},
	}

	fileName, err := prepareData(t)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(fileName, 0666, &Options{
				ReadOnly:        tc.readonly,
				PreLoadFreelist: tc.preLoadFreePage,
			})
			require.NoError(t, err)

			assert.Equal(t, tc.expectedFreePagesLoaded, db.freelist != nil)

			assert.NoError(t, db.Close())
		})
	}
}

func TestMethodPage(t *testing.T) {
	testCases := []struct {
		name            string
		readonly        bool
		preLoadFreePage bool
		expectedError   error
	}{
		{
			name:            "write mode",
			readonly:        false,
			preLoadFreePage: false,
			expectedError:   nil,
		},
		{
			name:            "readonly mode with preloading free pages",
			readonly:        true,
			preLoadFreePage: true,
			expectedError:   nil,
		},
		{
			name:            "readonly mode without preloading free pages",
			readonly:        true,
			preLoadFreePage: false,
			expectedError:   errors.ErrFreePagesNotLoaded,
		},
	}

	fileName, err := prepareData(t)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(fileName, 0666, &Options{
				ReadOnly:        tc.readonly,
				PreLoadFreelist: tc.preLoadFreePage,
			})
			require.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin(!tc.readonly)
			require.NoError(t, err)

			_, err = tx.Page(0)
			require.Equal(t, tc.expectedError, err)

			if tc.readonly {
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, tx.Commit())
			}

			require.NoError(t, db.Close())
		})
	}
}

func TestDBReaderSlotTracksOldestLocalReadTx(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0600, &Options{InitialMmapSize: 1 << 20})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	})
	require.NoError(t, err)

	rtx1, err := db.Begin(false)
	require.NoError(t, err)
	defer func() {
		if rtx1 != nil {
			require.NoError(t, rtx1.Rollback())
		}
	}()

	err = db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("test")).Put([]byte("k"), []byte("v1"))
	})
	require.NoError(t, err)

	rtx2, err := db.Begin(false)
	require.NoError(t, err)
	defer func() {
		if rtx2 != nil {
			require.NoError(t, rtx2.Rollback())
		}
	}()

	got := db.lockFile.OldestReaderTxid(^uint64(0))
	require.Equal(t, uint64(rtx1.ID()), got)

	require.NoError(t, rtx2.Rollback())
	rtx2 = nil

	got = db.lockFile.OldestReaderTxid(^uint64(0))
	require.Equal(t, uint64(rtx1.ID()), got)

	require.NoError(t, rtx1.Rollback())
	rtx1 = nil

	got = db.lockFile.OldestReaderTxid(123)
	require.Equal(t, uint64(123), got)
}

func TestDBReaderSlotTracksReadonlyTxWithoutFreelistPreload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0600, nil)
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		return err
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = Open(path, 0600, &Options{ReadOnly: true, PreLoadFreelist: false})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	require.Nil(t, db.freelist)

	rtx, err := db.Begin(false)
	require.NoError(t, err)
	defer func() {
		if rtx != nil {
			require.NoError(t, rtx.Rollback())
		}
	}()

	got := db.lockFile.OldestReaderTxid(^uint64(0))
	require.Equal(t, uint64(rtx.ID()), got)

	require.NoError(t, rtx.Rollback())
	rtx = nil

	got = db.lockFile.OldestReaderTxid(123)
	require.Equal(t, uint64(123), got)
}

func TestDBRestoreReadonlyTxidsToFreelist(t *testing.T) {
	for _, freelistType := range []FreelistType{FreelistArrayType, FreelistMapType} {
		t.Run(string(freelistType), func(t *testing.T) {
			db := &DB{
				activeReadTxIDs: map[common.Txid]uint64{3: 1},
				freelist:        newFreelist(freelistType),
			}

			db.restoreReadonlyTxidsToFreelist()
			db.freelist.Free(common.Txid(5), common.NewPage(10, common.LeafPageFlag, 0, 0))
			db.freelist.ReleasePendingPages()

			require.Zero(t, db.freelist.Allocate(common.Txid(6), 1))

			db.removeReadonlyTxid(common.Txid(3))
			db.freelist.ReleasePendingPages()

			require.Equal(t, common.Pgid(10), db.freelist.Allocate(common.Txid(6), 1))
		})
	}
}

func prepareData(t *testing.T) (string, error) {
	fileName := filepath.Join(t.TempDir(), "db")
	db, err := Open(fileName, 0666, nil)
	if err != nil {
		return "", err
	}
	if err := db.Close(); err != nil {
		return "", err
	}

	return fileName, nil
}
