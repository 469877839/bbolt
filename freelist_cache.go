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

// The freelist cache is a best-effort hint stored in a sidecar file next to
// the database file. It records the set of free (and pending) pages as of a
// specific committed transaction, so that a database opened with
// NoFreelistSync can skip the full file scan when the cache is still valid.
//
// The cache is never authoritative: before trusting it, bbolt verifies the
// recorded transaction id and high water mark against the current meta page.
// Any mismatch, corruption, truncation or concurrent torn read causes the
// cache to be discarded and bbolt falls back to scanning the whole file.
//
// File layout (all integers little endian):
//
//	0:  magic     uint32
//	4:  version   uint32
//	8:  pageSize  uint32
//	12: count     uint32  (number of page ids)
//	16: txid      uint64  (meta txid the snapshot was taken at)
//	24: highWater uint64  (meta pgid, i.e. the allocated page boundary)
//	32: checksum  uint64  (fnv64a over the page id payload)
//	40: page ids  [count]uint64
const (
	freelistCacheMagic      uint32 = 0xBFCA4E31
	freelistCacheVersion    uint32 = 1
	freelistCacheHeaderSize        = 40
)

// freelistCacheData is the decoded content of a freelist cache file.
type freelistCacheData struct {
	txid      common.Txid
	highWater common.Pgid
	ids       common.Pgids
}

// freelistCachePath returns the path of the sidecar cache file for a given
// database file.
func freelistCachePath(dbPath string) string {
	return dbPath + ".freelistcache"
}

// writeFreelistCacheFile serializes the given free page ids into a cache
// file. The file is written to a temporary file first and then atomically
// renamed into place, so a concurrent reader either observes the previous
// complete cache or the new one, but never a partially written file. A crash
// in the middle of the write leaves either the old cache or an unlinked
// temporary file behind, both of which are handled gracefully on read.
func writeFreelistCacheFile(path string, pageSize int, txid common.Txid, highWater common.Pgid, ids common.Pgids) (err error) {
	payload := make([]byte, len(ids)*8)
	for i, id := range ids {
		binary.LittleEndian.PutUint64(payload[i*8:], uint64(id))
	}

	h := fnv.New64a()
	_, _ = h.Write(payload)

	buf := make([]byte, freelistCacheHeaderSize, freelistCacheHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], freelistCacheMagic)
	binary.LittleEndian.PutUint32(buf[4:8], freelistCacheVersion)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(pageSize))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(ids)))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(txid))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(highWater))
	binary.LittleEndian.PutUint64(buf[32:40], h.Sum64())
	buf = append(buf, payload...)

	tmp, err := os.CreateTemp(filepath.Dir(path), ".freelistcache-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary cache file: %w", err)
	}
	tmpName := tmp.Name()
	// Best effort cleanup; a no-op once the rename below succeeded.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err = tmp.Write(buf); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to write cache file: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to sync cache file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("failed to close cache file: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		// On Windows renaming over an existing file fails; remove the old
		// cache first and retry. Losing the old cache is always safe.
		if rmErr := os.Remove(path); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return fmt.Errorf("failed to rename cache file: %w", err)
		}
		if err = os.Rename(tmpName, path); err != nil {
			return fmt.Errorf("failed to rename cache file: %w", err)
		}
	}
	return nil
}

// readFreelistCacheFile reads and fully validates a cache file. It returns
// an error if the file is missing, truncated, corrupted, written by an
// incompatible version or targeted at a different page size.
func readFreelistCacheFile(path string, pageSize int) (*freelistCacheData, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < freelistCacheHeaderSize {
		return nil, errors.New("cache file too small for header")
	}
	if magic := binary.LittleEndian.Uint32(raw[0:4]); magic != freelistCacheMagic {
		return nil, fmt.Errorf("invalid magic number: 0x%x", magic)
	}
	if version := binary.LittleEndian.Uint32(raw[4:8]); version != freelistCacheVersion {
		return nil, fmt.Errorf("unsupported cache version: %d", version)
	}
	if ps := int(binary.LittleEndian.Uint32(raw[8:12])); ps != pageSize {
		return nil, fmt.Errorf("page size mismatch: cache %d, db %d", ps, pageSize)
	}
	count := binary.LittleEndian.Uint32(raw[12:16])
	payload := raw[freelistCacheHeaderSize:]
	// A torn read (e.g. the cache was being rewritten by another process or
	// the writer crashed midway) shows up as a length or checksum mismatch.
	if uint64(len(payload)) != uint64(count)*8 {
		return nil, fmt.Errorf("cache file truncated or corrupted: want %d payload bytes, got %d", uint64(count)*8, len(payload))
	}
	h := fnv.New64a()
	_, _ = h.Write(payload)
	if sum := binary.LittleEndian.Uint64(raw[32:40]); sum != h.Sum64() {
		return nil, errors.New("checksum mismatch")
	}

	ids := make(common.Pgids, count)
	for i := range ids {
		ids[i] = common.Pgid(binary.LittleEndian.Uint64(payload[i*8:]))
	}
	return &freelistCacheData{
		txid:      common.Txid(binary.LittleEndian.Uint64(raw[16:24])),
		highWater: common.Pgid(binary.LittleEndian.Uint64(raw[24:32])),
		ids:       ids,
	}, nil
}
