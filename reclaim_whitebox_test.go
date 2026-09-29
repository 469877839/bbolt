package bbolt

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	reclaimCrashHookEnv = "RECLAIM_CRASH_HOOK"
	reclaimCrashDBEnv   = "RECLAIM_CRASH_DB"
	reclaimCrashKeys    = 8000
)

// TestReclaim_CrashBeforeTruncate simulates a crash after the reduced high
// water mark has been committed and synced, but before the data file is
// truncated. The database must reopen cleanly; in the worst case the file
// simply stays larger than necessary.
func TestReclaim_CrashBeforeTruncate(t *testing.T) {
	reclaimCrashTest(t, "before")
}

// TestReclaim_CrashAfterTruncate simulates a crash after the data file has
// been truncated but before the truncation is synced to disk. The database
// must reopen cleanly and pass a full consistency check.
func TestReclaim_CrashAfterTruncate(t *testing.T) {
	reclaimCrashTest(t, "after")
}

func reclaimCrashTest(t *testing.T, hookPoint string) {
	path := filepath.Join(t.TempDir(), "db")

	cmd := exec.Command(os.Args[0], "-test.run=^TestReclaimCrashHelper$")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("%s=%s", reclaimCrashHookEnv, hookPoint),
		fmt.Sprintf("%s=%s", reclaimCrashDBEnv, path),
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "crashed child output:\n%s", out)

	// The database must open and be fully consistent after the crash.
	db, err := Open(path, 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.View(func(tx *Tx) error {
		for chkErr := range tx.Check() {
			return chkErr
		}
		b := tx.Bucket([]byte("reclaim"))
		if b == nil {
			return fmt.Errorf("bucket missing after crash")
		}
		for i := 0; i < reclaimCrashKeys/2; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			v := b.Get(k)
			if v == nil {
				return fmt.Errorf("key %d missing after crash", i)
			}
			if string(v) != string(reclaimCrashValue(i)) {
				return fmt.Errorf("key %d: wrong value after crash", i)
			}
		}
		return nil
	})
	require.NoError(t, err)

	// The database must still be fully usable: a follow-up reclamation and
	// further writes must work.
	_, err = db.Reclaim()
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("reclaim")).Put([]byte("post-crash-key"), []byte("post-crash-value"))
	}))
}

// TestReclaimCrashHelper is re-executed by the TestReclaim_Crash* tests in a
// subprocess. It populates a database, creates a reclaimable tail and then
// "crashes" (os.Exit) at the requested point inside Reclaim.
func TestReclaimCrashHelper(t *testing.T) {
	hookPoint := os.Getenv(reclaimCrashHookEnv)
	if hookPoint == "" {
		t.Skip("helper for the TestReclaim_Crash* tests")
	}
	path := os.Getenv(reclaimCrashDBEnv)

	opts := &Options{}
	if env := os.Getenv("TEST_FREELIST_TYPE"); env == string(FreelistMapType) {
		opts.FreelistType = FreelistMapType
	}
	db, err := Open(path, 0600, opts)
	require.NoError(t, err)

	// Populate and then delete the second half of the key space so that the
	// tail of the file becomes reclaimable.
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("reclaim"))
		if err != nil {
			return err
		}
		for i := 0; i < reclaimCrashKeys; i++ {
			if err := b.Put([]byte(fmt.Sprintf("key-%08d", i)), reclaimCrashValue(i)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("reclaim"))
		for i := reclaimCrashKeys / 2; i < reclaimCrashKeys; i++ {
			if err := b.Delete([]byte(fmt.Sprintf("key-%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	crash := func() { os.Exit(0) }
	switch hookPoint {
	case "before":
		reclaimTestHookBeforeTruncate = crash
	case "after":
		reclaimTestHookAfterTruncate = crash
	default:
		t.Fatalf("unknown hook point %q", hookPoint)
	}

	_, _ = db.Reclaim()
	// Reaching here means the hook did not fire; fail loudly.
	os.Exit(1)
}

func reclaimCrashValue(i int) []byte {
	v := []byte(fmt.Sprintf("val-%08d-", i))
	for len(v) < 100 {
		v = append(v, byte('a'+i%26))
	}
	return v[:100]
}
