package bbolt

import (
	"fmt"
	"runtime"
	"time"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// reclaimReaderPollInterval is how often Reclaim re-checks whether the read
// transactions that may still reference the tail of the data file have
// finished.
const reclaimReaderPollInterval = 10 * time.Millisecond

// Test-only hooks used to simulate a crash in the middle of a reclamation.
// They are nil in production builds.
var (
	// reclaimTestHookBeforeTruncate fires after the reduced high water mark
	// has been committed (and synced) but before the file is truncated.
	reclaimTestHookBeforeTruncate func()
	// reclaimTestHookAfterTruncate fires after the file has been truncated
	// but before the truncation is synced to disk.
	reclaimTestHookAfterTruncate func()
)

// ReclaimResult describes the outcome of a single Reclaim call.
type ReclaimResult struct {
	// ReclaimedBytes is the number of bytes truncated off the end of the
	// data file and returned to the operating system. It is 0 when the
	// file did not shrink (including when Skipped is true).
	ReclaimedBytes int64

	// Skipped reports whether the reclamation was skipped. When true,
	// SkipReason describes why nothing was reclaimed.
	Skipped bool

	// SkipReason is a human-readable explanation of why the reclamation
	// was skipped. It is empty when the reclamation was performed.
	SkipReason string
}

// Reclaim performs a one-shot online reclamation of the data file: pages at
// the tail of the file that are no longer referenced by any transaction are
// discarded and the file is truncated, returning the space to the operating
// system. The database stays open and fully usable the whole time; read and
// write transactions are never blocked for longer than a file truncation.
//
// The reclamation runs in two phases:
//
//  1. A regular write transaction commits a reduced high water mark
//     (meta.pgid) together with a freelist that no longer contains the
//     discarded tail pages. From this point on, no new read transaction can
//     reference the tail pages.
//  2. Once all read transactions that could still reference pages beyond
//     the high water mark have finished, the data file is truncated to the
//     high water mark and the truncation is synced to disk.
//
// The high water mark reduction is always durable before the file is
// truncated, so a crash at any point leaves the database in a consistent
// state; in the worst case the file simply stays larger than necessary.
//
// Reclaim waits (without holding any database lock) for old read
// transactions to finish before truncating. A read transaction that never
// finishes therefore blocks Reclaim indefinitely, just like it blocks page
// reclamation itself. Do not call Reclaim from a goroutine that holds an
// open transaction on the same database.
//
// Note: Reclaim is not supported on Windows, where the data file cannot be
// shrunk while it is memory-mapped; it is a no-op there.
func (db *DB) Reclaim() (ReclaimResult, error) {
	if db.readOnly {
		return ReclaimResult{}, berrors.ErrDatabaseReadOnly
	}
	if runtime.GOOS == "windows" {
		return ReclaimResult{
			Skipped:    true,
			SkipReason: "online truncation is not supported on windows",
		}, nil
	}

	// Phase 1: discard the free tail pages from the freelist and commit a
	// reduced high water mark (best effort; there may be no free run).
	if err := db.lowerHighWaterMark(); err != nil {
		return ReclaimResult{}, err
	}

	// Phase 2a: wait (lock-free) until no open read transaction can still
	// reference pages at or beyond the current high water mark.
	db.waitForReaders()

	// Phase 2b: truncate the data file under the writer lock.
	reclaimed, skipReason, err := db.truncateToHighWaterMark()
	if err != nil {
		return ReclaimResult{}, err
	}
	if skipReason != "" {
		return ReclaimResult{Skipped: true, SkipReason: skipReason}, nil
	}
	db.Logger().Infof("Reclaimed %d bytes from db file (%s)", reclaimed, db.path)
	return ReclaimResult{ReclaimedBytes: reclaimed}, nil
}

// lowerHighWaterMark discards the contiguous run of free pages at the tail
// of the data file from the freelist and commits a reduced high water mark
// via a regular write transaction. It is a no-op when there is no run of
// free pages at the tail.
func (db *DB) lowerHighWaterMark() error {
	found, relocate, err := db.tryLowerHighWaterMark()
	if err != nil || found {
		return err
	}
	if !relocate {
		return nil
	}
	// The persisted freelist page itself sits at the tail of the file
	// (e.g. right after a transaction that freed a lot of pages) and blocks
	// the reclaimable run. A no-op commit relocates it into the free list;
	// the freed old freelist page then becomes part of the tail run.
	if err := db.Update(func(tx *Tx) error { return nil }); err != nil {
		return fmt.Errorf("relocating the freelist page failed: %w", err)
	}
	_, _, err = db.tryLowerHighWaterMark()
	return err
}

// tryLowerHighWaterMark performs a single attempt of lowerHighWaterMark.
// It reports whether a reclaimable run was found and, if not, whether the
// persisted freelist page is the page blocking the tail (in which case a
// no-op commit can unblock it).
func (db *DB) tryLowerHighWaterMark() (found bool, relocate bool, err error) {
	tx, err := db.Begin(true)
	if err != nil {
		return false, false, err
	}

	hwm := tx.meta.Pgid()
	start := db.freelist.FreeTailRun(hwm)
	if start >= hwm {
		relocate = db.freelistPageAtTail(tx, hwm)
		_ = tx.Rollback()
		return false, relocate, nil
	}

	// Remove the tail pages from the freelist so that the freelist written
	// out by the commit no longer references them, then lower the high
	// water mark to the beginning of the run.
	db.freelist.DiscardFreePagesFrom(start)
	tx.meta.SetPgid(start)

	if err := tx.Commit(); err != nil {
		return false, false, fmt.Errorf("committing reduced high water mark failed: %w", err)
	}
	return true, false, nil
}

// freelistPageAtTail reports whether the persisted freelist page spans the
// last page below the high water mark, and therefore blocks the reclaimable
// tail run.
func (db *DB) freelistPageAtTail(tx *Tx, hwm common.Pgid) bool {
	fl := tx.meta.Freelist()
	if fl == common.PgidNoFreelist || fl >= hwm {
		return false
	}
	return fl+common.Pgid(db.page(fl).Overflow()) >= hwm-1
}

// waitForReaders blocks until every open read transaction has a snapshot
// high water mark less than or equal to the current one. It does not hold
// any database lock, so it never blocks other transactions.
func (db *DB) waitForReaders() {
	for {
		db.metalock.Lock()
		if !db.opened {
			db.metalock.Unlock()
			return
		}
		maxReader := common.Pgid(0)
		for pgid := range db.openReaderPgids {
			if pgid > maxReader {
				maxReader = pgid
			}
		}
		hwm := db.meta().Pgid()
		db.metalock.Unlock()

		// A reader is only a problem if its snapshot may reference pages
		// beyond the current high water mark.
		if maxReader <= hwm {
			return
		}
		time.Sleep(reclaimReaderPollInterval)
	}
}

// truncateToHighWaterMark shortens the data file to the current high water
// mark. It is serialized with write transactions via db.rwlock and only
// performs the truncation when it can prove that no transaction (open or
// future) and no freelist entry references the pages being cut off. It
// returns the number of bytes truncated away, or a skip reason.
func (db *DB) truncateToHighWaterMark() (int64, string, error) {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if !db.opened {
		return 0, "", berrors.ErrDatabaseNotOpen
	}

	db.metalock.Lock()
	hwm := db.meta().Pgid()
	maxReader := common.Pgid(0)
	for pgid := range db.openReaderPgids {
		if pgid > maxReader {
			maxReader = pgid
		}
	}
	db.metalock.Unlock()

	// A reader with a snapshot high water mark above the truncation point
	// may still touch the tail pages (e.g. via Tx.Page); leave the file as
	// is in that case. This can only happen when another Reclaim lowered
	// the high water mark concurrently.
	if maxReader > hwm {
		return 0, "a read transaction is still referencing pages beyond the high water mark", nil
	}

	fileSize, err := db.fileSize()
	if err != nil {
		return 0, "", err
	}
	targetSize := int64(hwm) * int64(db.pageSize)
	if targetSize >= int64(fileSize) {
		return 0, "no reclaimable space at the end of the database file", nil
	}

	// Safety net: no page beyond the high water mark may be tracked by the
	// freelist (neither free nor pending), otherwise truncating it away
	// would leave a dangling reference in the persisted freelist.
	for id := hwm; id < common.Pgid(int64(fileSize)/int64(db.pageSize)); id++ {
		if db.freelist.Freed(id) {
			return 0, "", fmt.Errorf("page %d beyond the high water mark (%d) is still tracked by the freelist", id, hwm)
		}
	}

	// Make sure the reduced high water mark (and the freelist written with
	// it) is durable before the file is shrunk, regardless of db.NoSync.
	// After a crash the file must never be smaller than what the persisted
	// meta page references.
	if err := fdatasync(db); err != nil {
		return 0, "", fmt.Errorf("syncing db file before truncation failed: %w", err)
	}

	if reclaimTestHookBeforeTruncate != nil {
		reclaimTestHookBeforeTruncate()
	}

	if err := db.file.Truncate(targetSize); err != nil {
		return 0, "", fmt.Errorf("truncating db file to %d bytes failed: %w", targetSize, err)
	}

	if reclaimTestHookAfterTruncate != nil {
		reclaimTestHookAfterTruncate()
	}

	// Sync again so the truncation itself is durable once Reclaim returns.
	// Losing it in a crash is harmless (the file simply stays larger), but
	// an explicit reclamation should survive a restart.
	if err := fdatasync(db); err != nil {
		return 0, "", fmt.Errorf("syncing db file after truncation failed: %w", err)
	}
	return int64(fileSize) - targetSize, "", nil
}
