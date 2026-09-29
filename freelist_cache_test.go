package bbolt_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
)

// testLogger returns a logger writing into the given buffer, so tests can
// assert on the messages emitted by bbolt.
func testLogger(buf *bytes.Buffer) bolt.Logger {
	return &bolt.DefaultLogger{Logger: log.New(buf, "", 0)}
}

// populateCacheTestDB writes nKeys keys and then deletes half of them, so
// that the database ends up with a non-trivial set of free pages.
func populateCacheTestDB(t *testing.T, db *bolt.DB, nKeys int) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < nKeys; i++ {
			if err := b.Put([]byte(fmt.Sprintf("key-%06d", i)), bytes.Repeat([]byte("v"), 64)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < nKeys; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("key-%06d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
}

func cacheTestOptions() *bolt.Options {
	return &bolt.Options{
		NoFreelistSync: true,
		FreelistCache:  true,
		Timeout:        5 * time.Second,
	}
}

// TestFreelistCache_ReopenUsesCache verifies that a database closed cleanly
// leaves a cache behind, and that the next open rebuilds the freelist from
// the cache instead of scanning the whole file, yielding the same freelist
// as a full scan would.
func TestFreelistCache_ReopenUsesCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := bolt.Open(path, 0600, cacheTestOptions())
	require.NoError(t, err)
	populateCacheTestDB(t, db, 2000)
	require.NoError(t, db.Close())

	// A clean shutdown must have written the sidecar cache file.
	_, err = os.Stat(path + ".freelistcache")
	require.NoError(t, err, "expected freelist cache file to exist after clean close")

	// Reopen with the cache enabled: it must be used instead of a scan.
	var buf bytes.Buffer
	opts := cacheTestOptions()
	opts.Logger = testLogger(&buf)
	db, err = bolt.Open(path, 0600, opts)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "loaded freelist from cache")
	cachedFreeN := db.Stats().FreePageN
	require.Greater(t, cachedFreeN, 0, "expected some free pages after deleting half the keys")

	// The data must be intact.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		require.Equal(t, []byte(bytes.Repeat([]byte("v"), 64)), b.Get([]byte("key-000001")))
		return nil
	}))
	require.NoError(t, db.Close())

	// Reopen without the cache (full scan) and compare the freelist.
	db, err = bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.Equal(t, cachedFreeN, db.Stats().FreePageN, "cache-loaded freelist must match the scan result")
	require.NoError(t, db.Close())
}

// TestFreelistCache_StaleCacheAfterCrash simulates a crash: the database
// file has moved on (new commits) while the cache file still describes an
// older transaction. The stale cache must be discarded and bbolt must fall
// back to a full scan, without any data loss.
func TestFreelistCache_StaleCacheAfterCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := bolt.Open(path, 0600, cacheTestOptions())
	require.NoError(t, err)
	populateCacheTestDB(t, db, 1000)
	require.NoError(t, db.Close())

	// Reopen and commit more data. The cache file still describes the
	// previous generation of the database.
	db, err = bolt.Open(path, 0600, cacheTestOptions())
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		return b.Put([]byte("post-crash-key"), []byte("post-crash-value"))
	}))

	// Take a crash snapshot: the db file is consistent on disk after the
	// commit, but the cache file is stale (older txid). Copy both files
	// before closing, because Close would refresh the cache.
	crashPath := filepath.Join(dir, "crash.db")
	require.NoError(t, copyFile(path, crashPath))
	require.NoError(t, copyFile(path+".freelistcache", crashPath+".freelistcache"))
	require.NoError(t, db.Close())

	// Open the crash snapshot: the stale cache must be rejected.
	var buf bytes.Buffer
	opts := cacheTestOptions()
	opts.Logger = testLogger(&buf)
	db, err = bolt.Open(crashPath, 0600, opts)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "stale")
	require.Contains(t, buf.String(), "falling back to a full scan")

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		require.Equal(t, []byte("post-crash-value"), b.Get([]byte("post-crash-key")))
		require.NotNil(t, b.Get([]byte("key-000001")))
		return nil
	}))
	scannedFreeN := db.Stats().FreePageN
	require.Greater(t, scannedFreeN, 0)
	require.NoError(t, db.Close())

	// The close above wrote a fresh cache; reopening must use it.
	buf.Reset()
	db, err = bolt.Open(crashPath, 0600, opts)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "loaded freelist from cache")
	require.Equal(t, scannedFreeN, db.Stats().FreePageN)
	require.NoError(t, db.Close())
}

// TestFreelistCache_UnusableCache verifies that a missing, truncated or
// corrupted cache file never prevents the database from opening: bbolt logs
// the problem and falls back to a full scan.
func TestFreelistCache_UnusableCache(t *testing.T) {
	newDB := func(t *testing.T) (string, int) {
		path := filepath.Join(t.TempDir(), "test.db")
		db, err := bolt.Open(path, 0600, cacheTestOptions())
		require.NoError(t, err)
		populateCacheTestDB(t, db, 1000)
		// An extra (empty) write transaction releases the pages freed by
		// the deletes above from the pending list into the free list.
		require.NoError(t, db.Update(func(tx *bolt.Tx) error { return nil }))
		freeN := db.Stats().FreePageN
		require.NoError(t, db.Close())
		return path, freeN
	}

	readCache := func(t *testing.T, path string) []byte {
		data, err := os.ReadFile(path + ".freelistcache")
		require.NoError(t, err)
		return data
	}

	testCases := []struct {
		name        string
		corrupt     func(t *testing.T, path string)
		expectedLog string
	}{
		{
			name: "missing cache file",
			corrupt: func(t *testing.T, path string) {
				require.NoError(t, os.Remove(path+".freelistcache"))
			},
			expectedLog: "does not exist",
		},
		{
			name: "truncated header",
			corrupt: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path+".freelistcache", readCache(t, path)[:16], 0600))
			},
			expectedLog: "unusable",
		},
		{
			name: "truncated payload",
			corrupt: func(t *testing.T, path string) {
				data := readCache(t, path)
				require.NoError(t, os.WriteFile(path+".freelistcache", data[:len(data)-8], 0600))
			},
			expectedLog: "unusable",
		},
		{
			name: "garbage",
			corrupt: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path+".freelistcache", []byte("not a freelist cache at all"), 0600))
			},
			expectedLog: "unusable",
		},
		{
			name: "corrupted payload",
			corrupt: func(t *testing.T, path string) {
				data := readCache(t, path)
				data[len(data)-1] ^= 0xff // flip a bit in the last page id
				require.NoError(t, os.WriteFile(path+".freelistcache", data, 0600))
			},
			expectedLog: "unusable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path, expectedFreeN := newDB(t)
			require.Greater(t, expectedFreeN, 0)
			tc.corrupt(t, path)

			var buf bytes.Buffer
			opts := cacheTestOptions()
			opts.Logger = testLogger(&buf)
			db, err := bolt.Open(path, 0600, opts)
			require.NoError(t, err)
			require.Contains(t, buf.String(), tc.expectedLog)
			require.Contains(t, buf.String(), "falling back to a full scan")

			// The database must be fully functional and the freelist must
			// match the full scan result.
			require.Equal(t, expectedFreeN, db.Stats().FreePageN)
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				require.NotNil(t, tx.Bucket([]byte("data")))
				return nil
			}))
			require.NoError(t, db.Close())
		})
	}
}

// TestFreelistCache_DisabledByDefault verifies that no cache file is
// created unless the feature is explicitly enabled.
func TestFreelistCache_DisabledByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	populateCacheTestDB(t, db, 500)
	require.NoError(t, db.Close())

	_, err = os.Stat(path + ".freelistcache")
	require.True(t, os.IsNotExist(err), "no cache file expected when FreelistCache is not enabled")
}

// TestFreelistCache_ReadOnlyOpen verifies that a read-only process can
// consume the cache left behind by the last writer, and doesn't modify it.
func TestFreelistCache_ReadOnlyOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(path, 0600, cacheTestOptions())
	require.NoError(t, err)
	populateCacheTestDB(t, db, 1000)
	require.NoError(t, db.Close())

	cacheInfo, err := os.Stat(path + ".freelistcache")
	require.NoError(t, err)

	var buf bytes.Buffer
	db, err = bolt.Open(path, 0600, &bolt.Options{
		NoFreelistSync:  true,
		FreelistCache:   true,
		ReadOnly:        true,
		PreLoadFreelist: true,
		Logger:          testLogger(&buf),
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "loaded freelist from cache")
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte("data")))
		return nil
	}))
	require.NoError(t, db.Close())

	// A read-only open must not rewrite the cache.
	cacheInfoAfter, err := os.Stat(path + ".freelistcache")
	require.NoError(t, err)
	require.Equal(t, cacheInfo.ModTime(), cacheInfoAfter.ModTime())
}
