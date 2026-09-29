package bbolt_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
)

// cacheTestLogger captures all log output so tests can assert on the
// cache hit/miss decisions.
type cacheTestLogger struct {
	*bolt.DefaultLogger
	buf *bytes.Buffer
}

func newCacheTestLogger() *cacheTestLogger {
	buf := &bytes.Buffer{}
	lg := &bolt.DefaultLogger{Logger: log.New(buf, "", 0)}
	lg.EnableDebug()
	return &cacheTestLogger{DefaultLogger: lg, buf: buf}
}

func (l *cacheTestLogger) contains(substr string) bool {
	return strings.Contains(l.buf.String(), substr)
}

func freelistCacheOptions(l *cacheTestLogger) *bolt.Options {
	return &bolt.Options{
		NoFreelistSync: true,
		FreelistCache:  true,
		Logger:         l,
	}
}

// fillAndDelete writes a bunch of keys and deletes half of them so that
// the db has a non-trivial freelist, then returns the db path.
func fillAndDelete(t *testing.T, db *bolt.DB) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 2000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%06d", i)), make([]byte, 512)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < 2000; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("%06d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
}

func checkDB(t *testing.T, db *bolt.DB) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			return err
		}
		return nil
	}))
}

func freePageN(t *testing.T, path string, o *bolt.Options) int {
	t.Helper()
	db, err := bolt.Open(path, 0600, o)
	require.NoError(t, err)
	n := db.Stats().FreePageN
	require.NoError(t, db.Close())
	return n
}

func TestFreelistCache_DisabledByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true})
	require.NoError(t, err)
	fillAndDelete(t, db)
	require.NoError(t, db.Close())

	_, err = os.Stat(path + ".freelist.cache")
	require.True(t, os.IsNotExist(err), "no cache file should be written when the option is off")
}

func TestFreelistCache_NotWrittenOnCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)

	// The cache must not be written on the commit path, otherwise the
	// write amplification saved by NoFreelistSync would be added back.
	_, err = os.Stat(path + ".freelist.cache")
	require.True(t, os.IsNotExist(err), "cache file must not be written on commit")

	require.NoError(t, db.Close())
	_, err = os.Stat(path + ".freelist.cache")
	require.NoError(t, err, "cache file should be written on clean close")
}

func TestFreelistCache_HitAfterCleanClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")

	db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)
	require.NoError(t, db.Close())

	// Reopen: the freelist should be loaded from the cache.
	lg := newCacheTestLogger()
	db, err = bolt.Open(path, 0600, freelistCacheOptions(lg))
	require.NoError(t, err)
	require.True(t, lg.contains("loaded freelist from cache"), "expected a cache hit, got logs:\n%s", lg.buf.String())
	cachedFreePageN := db.Stats().FreePageN
	require.Greater(t, cachedFreePageN, 0)
	require.NoError(t, db.Close())

	// Reopen without the cache file: a full scan must produce the same
	// freelist, i.e. the cache agreed with the scan.
	require.NoError(t, os.Remove(path+".freelist.cache"))
	scannedFreePageN := freePageN(t, path, freelistCacheOptions(newCacheTestLogger()))
	require.Equal(t, scannedFreePageN, cachedFreePageN)

	// The db must stay fully functional and consistent afterwards.
	db, err = bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)
	checkDB(t, db)
	require.NoError(t, db.Close())
}

func TestFreelistCache_StaleCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cachePath := path + ".freelist.cache"

	db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)
	require.NoError(t, db.Close())

	// Save the cache written for the current state.
	staleCache, err := os.ReadFile(cachePath)
	require.NoError(t, err)

	// Simulate a crash: the db moves on with more commits, but the cache
	// on disk is the one from before those commits (e.g. the process died
	// before its clean-shutdown cache write, or an old binary - which
	// knows nothing about the cache - committed in the meantime).
	db, err = bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true})
	require.NoError(t, err)
	fillAndDelete(t, db)
	require.NoError(t, db.Close())
	require.NoError(t, os.WriteFile(cachePath, staleCache, 0600))

	// The stale cache must be detected and ignored.
	lg := newCacheTestLogger()
	db, err = bolt.Open(path, 0600, freelistCacheOptions(lg))
	require.NoError(t, err)
	require.True(t, lg.contains("is stale"), "expected a stale-cache warning, got logs:\n%s", lg.buf.String())
	require.NoError(t, db.Close())

	// The fallback scan must agree with a cache-free open.
	require.NoError(t, os.Remove(cachePath))
	expected := freePageN(t, path, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, os.Remove(cachePath))
	require.Equal(t, expected, freePageN(t, path, freelistCacheOptions(newCacheTestLogger())))
}

func TestFreelistCache_CorruptCache(t *testing.T) {
	testCases := []struct {
		name    string
		corrupt func(data []byte) []byte
	}{
		{"truncated", func(data []byte) []byte { return data[:len(data)/2] }},
		{"garbage", func(data []byte) []byte { return bytes.Repeat([]byte{0xAB}, len(data)) }},
		{"bitflip", func(data []byte) []byte { data[len(data)-1] ^= 0xFF; return data }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			cachePath := path + ".freelist.cache"

			db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
			require.NoError(t, err)
			fillAndDelete(t, db)
			require.NoError(t, db.Close())

			data, err := os.ReadFile(cachePath)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(cachePath, tc.corrupt(data), 0600))

			lg := newCacheTestLogger()
			db, err = bolt.Open(path, 0600, freelistCacheOptions(lg))
			require.NoError(t, err)
			require.True(t, lg.contains("is invalid"), "expected an invalid-cache warning, got logs:\n%s", lg.buf.String())
			fillAndDelete(t, db)
			checkDB(t, db)
			require.NoError(t, db.Close())
		})
	}
}

func TestFreelistCache_MissingCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)
	require.NoError(t, db.Close())

	require.NoError(t, os.Remove(path+".freelist.cache"))

	lg := newCacheTestLogger()
	db, err = bolt.Open(path, 0600, freelistCacheOptions(lg))
	require.NoError(t, err)
	require.True(t, lg.contains("does not exist"), "expected a missing-cache log, got logs:\n%s", lg.buf.String())
	require.NoError(t, db.Close())
}

func TestFreelistCache_ReadOnlyAttach(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")

	db, err := bolt.Open(path, 0600, freelistCacheOptions(newCacheTestLogger()))
	require.NoError(t, err)
	fillAndDelete(t, db)

	// The read-write process closes and leaves a cache behind; the next
	// process opens using that cache.
	require.NoError(t, db.Close())

	// Two read-only processes attach at the same time; both must open fine
	// using the cache left behind by the last writer.
	lg := newCacheTestLogger()
	roOptions := freelistCacheOptions(lg)
	roOptions.ReadOnly = true
	roOptions.PreLoadFreelist = true
	roDB, err := bolt.Open(path, 0600, roOptions)
	require.NoError(t, err)
	require.True(t, lg.contains("loaded freelist from cache"), "expected a cache hit, got logs:\n%s", lg.buf.String())

	lg2 := newCacheTestLogger()
	roOptions2 := freelistCacheOptions(lg2)
	roOptions2.ReadOnly = true
	roOptions2.PreLoadFreelist = true
	roDB2, err := bolt.Open(path, 0600, roOptions2)
	require.NoError(t, err)
	require.True(t, lg2.contains("loaded freelist from cache"), "expected a cache hit, got logs:\n%s", lg2.buf.String())

	require.NoError(t, roDB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).ForEach(func(k, v []byte) error { return nil })
	}))
	require.NoError(t, roDB.Close())
	require.NoError(t, roDB2.Close())

	// The cache left behind by the last close is still usable by the next
	// process.
	lg3 := newCacheTestLogger()
	db, err = bolt.Open(path, 0600, freelistCacheOptions(lg3))
	require.NoError(t, err)
	require.True(t, lg3.contains("loaded freelist from cache"), "expected a cache hit, got logs:\n%s", lg3.buf.String())
	checkDB(t, db)
	require.NoError(t, db.Close())
}
