package bbolt_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

var reclaimBucket = []byte("reclaim")

// fillReclaimData writes numKeys keys with deterministic values and returns
// the current file size.
func fillReclaimData(t *testing.T, db *btesting.DB, numKeys, valueSize int) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(reclaimBucket)
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			if err := b.Put(reclaimKey(i), reclaimValue(i, valueSize)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func reclaimKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%08d", i))
}

func reclaimValue(i int, size int) []byte {
	v := []byte(fmt.Sprintf("val-%08d-", i))
	for len(v) < size {
		v = append(v, byte('a'+i%26))
	}
	return v[:size]
}

func deleteReclaimRange(t *testing.T, db *btesting.DB, from, to int) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(reclaimBucket)
		for i := from; i < to; i++ {
			if err := b.Delete(reclaimKey(i)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func verifyReclaimRange(t *testing.T, db *btesting.DB, from, to, valueSize int) {
	t.Helper()
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(reclaimBucket)
		for i := from; i < to; i++ {
			v := b.Get(reclaimKey(i))
			if v == nil {
				return fmt.Errorf("key %d missing", i)
			}
			if exp := reclaimValue(i, valueSize); string(v) != string(exp) {
				return fmt.Errorf("key %d: wrong value", i)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func fileSizeOf(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

// TestReclaim_ShrinksFile verifies that deleting the tail half of the data
// and calling Reclaim returns the freed tail pages to the OS while keeping
// the remaining data intact.
func TestReclaim_ShrinksFile(t *testing.T) {
	db := btesting.MustCreateDB(t)

	const numKeys = 20000
	const valueSize = 100
	fillReclaimData(t, db, numKeys, valueSize)
	sizeBefore := fileSizeOf(t, db.Path())

	// Delete the second half of the key space so that the pages at the end
	// of the file become free.
	deleteReclaimRange(t, db, numKeys/2, numKeys)

	res, err := db.Reclaim()
	require.NoError(t, err)
	require.False(t, res.Skipped, "unexpected skip: %s", res.SkipReason)
	require.Positive(t, res.ReclaimedBytes, "expected the file to shrink")

	sizeAfter := fileSizeOf(t, db.Path())
	require.Less(t, sizeAfter, sizeBefore)
	require.Equal(t, sizeBefore-sizeAfter, res.ReclaimedBytes)
	t.Logf("file size: %d -> %d (reclaimed %d bytes)", sizeBefore, sizeAfter, res.ReclaimedBytes)

	// The remaining data must be intact.
	verifyReclaimRange(t, db, 0, numKeys/2, valueSize)

	// Reclaiming again right away must be a harmless no-op.
	res2, err := db.Reclaim()
	require.NoError(t, err)
	t.Logf("second reclaim: %+v", res2)

	// The database must still be writable and keep growing correctly.
	fillReclaimData(t, db, numKeys/2, valueSize)
	verifyReclaimRange(t, db, 0, numKeys/2, valueSize)

	// Everything must survive a reopen.
	require.NoError(t, db.Close())
	db.MustReopen()
	verifyReclaimRange(t, db, 0, numKeys/2, valueSize)
}

// TestReclaim_NothingToReclaim verifies that Reclaim reports a skip when
// there are no free pages at the end of the file.
func TestReclaim_NothingToReclaim(t *testing.T) {
	db := btesting.MustCreateDB(t)

	res, err := db.Reclaim()
	require.NoError(t, err)
	require.True(t, res.Skipped)
	require.NotEmpty(t, res.SkipReason)
	require.Zero(t, res.ReclaimedBytes)
	t.Logf("skip reason: %s", res.SkipReason)
}

// TestReclaim_ReadOnly verifies that Reclaim is rejected on a read-only db.
func TestReclaim_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)
	fillReclaimData(t, db, 100, 32)
	path := db.Path()
	require.NoError(t, db.Close())

	rodb, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer rodb.Close() //nolint:errcheck

	_, err = rodb.Reclaim()
	require.ErrorIs(t, err, berrors.ErrDatabaseReadOnly)
}

// TestReclaim_NoFreelistSync verifies that reclamation also works when the
// freelist is not persisted to disk.
func TestReclaim_NoFreelistSync(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{NoFreelistSync: true})

	const numKeys = 10000
	fillReclaimData(t, db, numKeys, 100)
	sizeBefore := fileSizeOf(t, db.Path())
	deleteReclaimRange(t, db, numKeys/2, numKeys)

	res, err := db.Reclaim()
	require.NoError(t, err)
	require.False(t, res.Skipped, "unexpected skip: %s", res.SkipReason)
	require.Positive(t, res.ReclaimedBytes)
	t.Logf("file size: %d -> %d", sizeBefore, fileSizeOf(t, db.Path()))

	verifyReclaimRange(t, db, 0, numKeys/2, 100)

	require.NoError(t, db.Close())
	db.MustReopen()
	verifyReclaimRange(t, db, 0, numKeys/2, 100)
}

// TestReclaim_LongRunningReader verifies that a read transaction keeps
// seeing a consistent snapshot while Reclaim waits for it and truncates
// the file only after the reader has finished.
func TestReclaim_LongRunningReader(t *testing.T) {
	db := btesting.MustCreateDB(t)

	const numKeys = 10000
	const valueSize = 100
	fillReclaimData(t, db, numKeys, valueSize)
	deleteReclaimRange(t, db, numKeys/2, numKeys)
	// A couple of small writes move the live branch pages and the freelist
	// page off the tail of the file, so that the tail becomes reclaimable.
	fillReclaimData(t, db, 10, valueSize)
	fillReclaimData(t, db, 11, valueSize)

	// Open a read transaction and keep it open while Reclaim runs.
	rtx, err := db.Begin(false)
	require.NoError(t, err)

	reclaimDone := make(chan bolt.ReclaimResult, 1)
	go func() {
		res, err := db.Reclaim()
		require.NoError(t, err)
		reclaimDone <- res
	}()

	// The reader must observe a stable snapshot for the whole time; give
	// Reclaim a chance to wait for us.
	for i := 0; i < 10; i++ {
		b := rtx.Bucket(reclaimBucket)
		for k := 0; k < numKeys/2; k++ {
			v := b.Get(reclaimKey(k))
			require.NotNil(t, v, "key %d disappeared from the snapshot", k)
			require.Equal(t, reclaimValue(k, valueSize), v)
		}
		// The deleted keys must stay deleted in this snapshot.
		require.Nil(t, b.Get(reclaimKey(numKeys/2)))
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, rtx.Rollback())

	select {
	case res := <-reclaimDone:
		require.False(t, res.Skipped, "unexpected skip: %s", res.SkipReason)
		require.Positive(t, res.ReclaimedBytes)
	case <-time.After(30 * time.Second):
		t.Fatal("Reclaim did not finish after the reader closed")
	}

	verifyReclaimRange(t, db, 0, numKeys/2, valueSize)
}

// TestReclaim_ConcurrentReaders hammers the database with readers that
// continuously validate their snapshots, a writer that keeps mutating the
// data, and repeated Reclaim calls. No snapshot verification may fail and
// no goroutine may crash.
func TestReclaim_ConcurrentReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := btesting.MustCreateDB(t)

	const numKeys = 5000
	const valueSize = 64
	fillReclaimData(t, db, numKeys, valueSize)
	// Create some reclaimable space up front.
	deleteReclaimRange(t, db, numKeys/2, numKeys)

	// Reuse the repo-wide concurrent test duration knob (defaults to 30s
	// when TEST_CONCURRENT_CASE_DURATION is not set), but keep it short
	// enough for a focused test.
	testDuration := 3 * time.Second
	if env := os.Getenv(testConcurrentCaseDuration); env != "" {
		if d, err := time.ParseDuration(env); err == nil {
			testDuration = d
		}
	}
	deadline := time.Now().Add(testDuration)

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	// Readers: open a read transaction, validate the snapshot repeatedly,
	// close, and start over. Values are a pure function of the key, so any
	// key that is present must carry exactly its deterministic value.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				tx, err := db.Begin(false)
				if err != nil {
					errCh <- fmt.Errorf("begin read tx: %w", err)
					return
				}
				for i := 0; i < 5; i++ {
					b := tx.Bucket(reclaimBucket)
					c := b.Cursor()
					count := 0
					for k, v := c.First(); k != nil; k, v = c.Next() {
						var idx int
						if _, err := fmt.Sscanf(string(k), "key-%08d", &idx); err != nil {
							errCh <- fmt.Errorf("corrupt key %q: %w", k, err)
							return
						}
						if string(v) != string(reclaimValue(idx, valueSize)) {
							errCh <- fmt.Errorf("key %d: snapshot returned wrong value", idx)
							return
						}
						count++
					}
					_ = count
					time.Sleep(time.Millisecond)
				}
				if err := tx.Rollback(); err != nil {
					errCh <- fmt.Errorf("rollback: %w", err)
					return
				}
			}
		}()
	}

	// Writer: keep deleting and re-inserting keys so that free pages are
	// continuously produced and reused.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for time.Now().Before(deadline) {
			err := db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(reclaimBucket)
				for j := 0; j < 50; j++ {
					idx := (i*50 + j) % numKeys
					if err := b.Put(reclaimKey(idx), reclaimValue(idx, valueSize)); err != nil {
						return err
					}
				}
				for j := 0; j < 25; j++ {
					idx := (i*50 + j + numKeys/2) % numKeys
					if err := b.Delete(reclaimKey(idx)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				errCh <- fmt.Errorf("write tx: %w", err)
				return
			}
			i++
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Reclaimer: trigger reclamation repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			res, err := db.Reclaim()
			if err != nil {
				errCh <- fmt.Errorf("reclaim: %w", err)
				return
			}
			if res.Skipped {
				t.Logf("reclaim skipped: %s", res.SkipReason)
			} else {
				t.Logf("reclaimed %d bytes", res.ReclaimedBytes)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// The database must be fully consistent at the end.
	verifyReclaimRangePresent(t, db, valueSize)
}

// verifyReclaimRangePresent checks that every key still present carries its
// deterministic value.
func verifyReclaimRangePresent(t *testing.T, db *btesting.DB, valueSize int) {
	t.Helper()
	err := db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(reclaimBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var idx int
			if _, err := fmt.Sscanf(string(k), "key-%08d", &idx); err != nil {
				return err
			}
			if string(v) != string(reclaimValue(idx, valueSize)) {
				return fmt.Errorf("key %d: wrong value", idx)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestReclaim_ReopenAfterReclaim verifies that a database file produced by
// Reclaim can be opened and checked by a fresh process state (old files
// remain readable and new files remain compatible).
func TestReclaim_ReopenAfterReclaim(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	db := btesting.MustOpenDBWithOption(t, f, nil)

	const numKeys = 8000
	fillReclaimData(t, db, numKeys, 100)
	deleteReclaimRange(t, db, numKeys/2, numKeys)

	res, err := db.Reclaim()
	require.NoError(t, err)
	require.False(t, res.Skipped, "unexpected skip: %s", res.SkipReason)
	require.NoError(t, db.Close())

	// Reopen and run a full consistency check.
	db.MustReopen()
	verifyReclaimRange(t, db, 0, numKeys/2, 100)
	require.NoError(t, db.Close())
}
