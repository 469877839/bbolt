package bbolt

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

func TestFreelistCacheFile_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db.freelistcache")
	ids := common.Pgids{3, 7, 8, 9, 42, 1000}

	require.NoError(t, writeFreelistCacheFile(path, 4096, 123, 2048, ids))

	data, err := readFreelistCacheFile(path, 4096)
	require.NoError(t, err)
	require.Equal(t, common.Txid(123), data.txid)
	require.Equal(t, common.Pgid(2048), data.highWater)
	require.Equal(t, ids, data.ids)
}

func TestFreelistCacheFile_Empty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db.freelistcache")
	require.NoError(t, writeFreelistCacheFile(path, 4096, 5, 2, nil))

	data, err := readFreelistCacheFile(path, 4096)
	require.NoError(t, err)
	require.Equal(t, common.Txid(5), data.txid)
	require.Empty(t, data.ids)
}

func TestFreelistCacheFile_Invalid(t *testing.T) {
	validPath := filepath.Join(t.TempDir(), "valid")
	require.NoError(t, writeFreelistCacheFile(validPath, 4096, 123, 2048, common.Pgids{3, 7}))
	valid, err := os.ReadFile(validPath)
	require.NoError(t, err)

	testCases := []struct {
		name    string
		content []byte
	}{
		{"empty file", []byte{}},
		{"short header", valid[:20]},
		{"bad magic", mutate(valid, 0, 0xff)},
		{"bad version", mutate(valid, 4, 0xff)},
		{"wrong page size", mutate(valid, 8, 0xff)},
		{"truncated payload", valid[:len(valid)-4]},
		{"trailing garbage", append(append([]byte{}, valid...), 0x1)},
		{"corrupted payload", mutate(valid, len(valid)-1, 0xff)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			require.NoError(t, os.WriteFile(path, tc.content, 0600))
			_, err := readFreelistCacheFile(path, 4096)
			require.Error(t, err)
		})
	}

	// A missing file must surface as a plain os error.
	_, err = readFreelistCacheFile(filepath.Join(t.TempDir(), "nonexistent"), 4096)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func mutate(b []byte, offset int, delta byte) []byte {
	out := append([]byte{}, b...)
	out[offset] += delta
	return out
}

// TestFreelistCache_OutOfRangePageId verifies that a cache referencing pages
// beyond the high water mark is rejected, even if its checksum is valid.
func TestFreelistCache_OutOfRangePageId(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, 0600, &Options{NoFreelistSync: true, FreelistCache: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("b"))
		return err
	}))
	pageSize := db.pageSize
	require.NoError(t, db.Close())

	// Rewrite the cache with a page id beyond the high water mark, but with
	// a valid checksum and the correct txid.
	cachePath := freelistCachePath(path)
	data, err := readFreelistCacheFile(cachePath, pageSize)
	require.NoError(t, err)
	require.NoError(t, writeFreelistCacheFile(cachePath, pageSize, data.txid, data.highWater,
		common.Pgids{data.highWater + 10}))

	var buf bytes.Buffer
	db, err = Open(path, 0600, &Options{
		NoFreelistSync: true,
		FreelistCache:  true,
		Timeout:        5 * time.Second,
		Logger:         &DefaultLogger{Logger: log.New(&buf, "", 0)},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "out-of-range")
	require.Contains(t, buf.String(), "falling back to a full scan")
	require.NoError(t, db.Close())
}

// TestFreelistCache_AsyncWriteWhileOpen verifies that the throttled
// background writer keeps the cache reasonably fresh while the database is
// open, so that a crash (no clean Close) can still be recovered from the
// cache instead of a full scan.
func TestFreelistCache_AsyncWriteWhileOpen(t *testing.T) {
	oldInterval := freelistCacheWriteInterval
	freelistCacheWriteInterval = 0
	defer func() { freelistCacheWriteInterval = oldInterval }()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, 0600, &Options{NoFreelistSync: true, FreelistCache: true, Timeout: 5 * time.Second})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("b"))
			if err != nil {
				return err
			}
			return b.Put([]byte{byte(i)}, []byte("value"))
		}))
	}

	// The background writer snapshots the state at the beginning of a write
	// transaction, so it always lags one commit behind. Keep starting (and
	// rolling back) write transactions until the cache catches up with the
	// current meta.
	cachePath := freelistCachePath(path)
	var data *freelistCacheData
	require.Eventually(t, func() bool {
		tx, txErr := db.Begin(true)
		if txErr != nil {
			return false
		}
		_ = tx.Rollback()
		d, readErr := readFreelistCacheFile(cachePath, db.pageSize)
		if readErr != nil {
			return false
		}
		data = d
		return data.txid == db.meta().Txid()
	}, 10*time.Second, 10*time.Millisecond, "cache file should be written in the background while the db is open")

	// Simulate a crash: copy the db file and the (fresh) cache without
	// closing the database, then open the copy. The cache must be valid.
	crashPath := filepath.Join(dir, "crash.db")
	rawDB, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(crashPath, rawDB, 0600))
	rawCache, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(freelistCachePath(crashPath), rawCache, 0600))

	var buf bytes.Buffer
	crashDB, err := Open(crashPath, 0600, &Options{
		NoFreelistSync: true,
		FreelistCache:  true,
		Timeout:        5 * time.Second,
		Logger:         &DefaultLogger{Logger: log.New(&buf, "", 0)},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "loaded freelist from cache")
	require.NoError(t, crashDB.Close())

	require.NoError(t, db.Close())
}
