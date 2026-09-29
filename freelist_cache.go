package bbolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt/internal/common"
)

// This file implements an optional, advisory on-disk cache of the freelist.
//
// When a database is opened with NoFreelistSync, the freelist is not persisted
// in the db file and has to be rebuilt by scanning every page on open. The
// cache stores a snapshot of the free page ids in a sidecar file next to the
// db so that a subsequent open can skip the full scan.
//
// The cache is only ever a hint: it is stamped with the meta transaction id
// and the high-water page id it was built from, and it is only trusted when
// both still match the meta pages of the db file. Any mismatch, corruption,
// truncation or absence of the cache file falls back to the full scan, so a
// stale or lost cache can never cause a data corruption. The cache is written
// outside of the commit path (on clean close and after a fallback scan), so
// enabling it does not add write amplification to commits.

const (
	// freelistCacheMagic identifies a freelist cache file.
	freelistCacheMagic uint32 = 0xB0B7FC01
	// freelistCacheVersion is the on-disk format version of the cache.
	freelistCacheVersion uint32 = 1
	// freelistCacheHeaderSize is the size of the serialized cache header:
	// magic(4) + version(4) + txid(8) + highWater(8) + count(8) + checksum(8).
	freelistCacheHeaderSize = 40
)

// defaultFreelistCachePath returns the default location of the freelist
// cache file for a given db file path.
func defaultFreelistCachePath(dbPath string) string {
	return dbPath + ".freelist.cache"
}

// readFreelistCache attempts to load the cached free page ids. It returns
// the ids and true only if the cache file exists, is well-formed and was
// written for exactly the current meta transaction id and high-water mark.
// In every other case it logs the reason and returns false, and the caller
// is expected to fall back to a full scan.
func (db *DB) readFreelistCache() ([]common.Pgid, bool) {
	if db.freelistCachePath == "" {
		return nil, false
	}
	lg := db.Logger()
	path := db.freelistCachePath

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			lg.Infof("freelist cache file (%s) does not exist, falling back to a full scan: %v", path, err)
		} else {
			lg.Warningf("failed to read freelist cache file (%s), falling back to a full scan: %v", path, err)
		}
		return nil, false
	}

	ids, txid, highWater, err := parseFreelistCache(data)
	if err != nil {
		lg.Warningf("freelist cache file (%s) is invalid, falling back to a full scan: %v", path, err)
		return nil, false
	}

	// The cache is only valid for the exact db state it was built from.
	// If another process (or an older binary) committed anything in the
	// meantime, the meta txid and/or the high-water mark moved on and the
	// cached ids may reference pages that have been re-allocated since.
	m := db.meta()
	if txid != m.Txid() || highWater != m.Pgid() {
		lg.Warningf("freelist cache file (%s) is stale (cache txid: %d, cache high-water: %d, db txid: %d, db high-water: %d), falling back to a full scan",
			path, txid, highWater, m.Txid(), m.Pgid())
		return nil, false
	}

	// Defensive validation: a cached id must never point at a meta page or
	// beyond the allocated page boundary.
	for _, id := range ids {
		if id < 2 || id >= highWater {
			lg.Warningf("freelist cache file (%s) contains out-of-range page id %d (high-water: %d), falling back to a full scan",
				path, id, highWater)
			return nil, false
		}
	}

	lg.Debugf("loaded freelist from cache file (%s): %d free pages at txid %d", path, len(ids), txid)
	return ids, true
}

// writeFreelistCache persists the given free page ids as the freelist cache,
// stamped with the given meta transaction id and high-water mark. The write
// is best-effort: any failure is logged but never propagated, because the
// cache is only an optimization and the db must remain fully usable without
// it. The file is written to a temporary file and atomically renamed so that
// a concurrent reader never observes a partially written cache.
func (db *DB) writeFreelistCache(ids []common.Pgid, txid common.Txid, highWater common.Pgid) {
	if db.freelistCachePath == "" {
		return
	}
	lg := db.Logger()
	path := db.freelistCachePath

	data := serializeFreelistCache(ids, txid, highWater)

	tmp, err := os.CreateTemp(filepath.Dir(path), ".freelist-cache-*")
	if err != nil {
		lg.Warningf("failed to create temporary freelist cache file for (%s): %v", path, err)
		return
	}
	tmpName := tmp.Name()
	defer func() {
		// No-op if the rename already succeeded.
		_ = os.Remove(tmpName)
	}()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		lg.Warningf("failed to write freelist cache file (%s): %v", path, err)
		return
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		lg.Warningf("failed to sync freelist cache file (%s): %v", path, err)
		return
	}
	if err = tmp.Close(); err != nil {
		lg.Warningf("failed to close freelist cache file (%s): %v", path, err)
		return
	}
	if err = os.Rename(tmpName, path); err != nil {
		lg.Warningf("failed to rename freelist cache file (%s): %v", path, err)
		return
	}
	lg.Debugf("wrote freelist cache file (%s): %d free pages at txid %d", path, len(ids), txid)
}

// serializeFreelistCache encodes the cache header and the page id list.
func serializeFreelistCache(ids []common.Pgid, txid common.Txid, highWater common.Pgid) []byte {
	data := make([]byte, freelistCacheHeaderSize+len(ids)*8)
	binary.LittleEndian.PutUint32(data[0:4], freelistCacheMagic)
	binary.LittleEndian.PutUint32(data[4:8], freelistCacheVersion)
	binary.LittleEndian.PutUint64(data[8:16], uint64(txid))
	binary.LittleEndian.PutUint64(data[16:24], uint64(highWater))
	binary.LittleEndian.PutUint64(data[24:32], uint64(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(data[freelistCacheHeaderSize+i*8:], uint64(id))
	}
	binary.LittleEndian.PutUint64(data[32:40], freelistCacheChecksum(data))
	return data
}

// parseFreelistCache decodes and validates a serialized cache. It returns
// the cached page ids along with the meta transaction id and high-water
// mark the cache was built from.
func parseFreelistCache(data []byte) (ids []common.Pgid, txid common.Txid, highWater common.Pgid, err error) {
	if len(data) < freelistCacheHeaderSize {
		return nil, 0, 0, fmt.Errorf("file too small: %d bytes", len(data))
	}
	if magic := binary.LittleEndian.Uint32(data[0:4]); magic != freelistCacheMagic {
		return nil, 0, 0, fmt.Errorf("invalid magic: %#x", magic)
	}
	if version := binary.LittleEndian.Uint32(data[4:8]); version != freelistCacheVersion {
		return nil, 0, 0, fmt.Errorf("unsupported version: %d", version)
	}
	txid = common.Txid(binary.LittleEndian.Uint64(data[8:16]))
	highWater = common.Pgid(binary.LittleEndian.Uint64(data[16:24]))
	count := binary.LittleEndian.Uint64(data[24:32])
	if binary.LittleEndian.Uint64(data[32:40]) != freelistCacheChecksum(data) {
		return nil, 0, 0, errors.New("checksum mismatch")
	}
	if uint64(len(data)) != uint64(freelistCacheHeaderSize)+count*8 {
		return nil, 0, 0, fmt.Errorf("truncated or trailing data: %d bytes for %d ids", len(data), count)
	}
	ids = make([]common.Pgid, count)
	for i := range ids {
		ids[i] = common.Pgid(binary.LittleEndian.Uint64(data[freelistCacheHeaderSize+i*8:]))
	}
	return ids, txid, highWater, nil
}

// freelistCacheChecksum computes the FNV-1a checksum over the header (with
// the checksum field itself zeroed out) and the page id list.
func freelistCacheChecksum(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data[0:32])
	_, _ = h.Write(data[freelistCacheHeaderSize:])
	return h.Sum64()
}
