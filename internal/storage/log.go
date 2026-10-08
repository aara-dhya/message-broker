package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const IndexEntrySize = 16 // 8 bytes (ID) + 8 bytes (Offset)

type IndexedLog struct {
	mu            sync.RWMutex // Upgraded to RWMutex for concurrent reads
	dataFile      *os.File
	indexFile     *os.File
	nextID        uint64
	currentOffset uint64
}

// NewIndexedLog opens or creates both the data log and its sidecar index.
func NewIndexedLog(basePath string) (*IndexedLog, error) {
	dataFile, err := os.OpenFile(basePath+".log", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	indexFile, err := os.OpenFile(basePath+".idx", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	dataInfo, err := dataFile.Stat()
	if err != nil {
		return nil, err
	}
	indexInfo, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}

	return &IndexedLog{
		dataFile:      dataFile,
		indexFile:     indexFile,
		currentOffset: uint64(dataInfo.Size()),
		nextID:        uint64(indexInfo.Size() / IndexEntrySize),
	}, nil
}

// Append writes the entry using stateless WriteAt to avoid Seek race conditions.
func (l *IndexedLog) Append(payload []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	id := l.nextID
	offset := l.currentOffset
	payloadLen := len(payload)

	// 1. Prepare 16-byte index entry
	var indexBuf [IndexEntrySize]byte
	binary.BigEndian.PutUint64(indexBuf[0:8], id)
	binary.BigEndian.PutUint64(indexBuf[8:16], offset)

	// Write index statelessly
	indexOffset := int64(id * IndexEntrySize)
	if _, err := l.indexFile.WriteAt(indexBuf[:], indexOffset); err != nil {
		return 0, fmt.Errorf("failed to write index entry: %w", err)
	}

	// 2. Prepare single buffer for data frame [Header + Payload]
	frameSize := 8 + payloadLen
	frameBuf := make([]byte, frameSize)
	binary.BigEndian.PutUint64(frameBuf[0:8], uint64(payloadLen))
	copy(frameBuf[8:], payload)

	// 3. Write data statelessly in a single syscall
	if _, err := l.dataFile.WriteAt(frameBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to write data frame: %w", err)
	}

	// 4. Update internal positions for the next append
	l.nextID++
	l.currentOffset += uint64(frameSize)

	return id, nil
}

// Read uses ReadAt to fetch data statelessly, allowing infinite concurrent consumers.
func (l *IndexedLog) Read(id uint64) ([]byte, error) {
	// --- Step 1: Concurrency Check ---
	l.mu.RLock()
	if id >= l.nextID {
		l.mu.RUnlock()
		return nil, fmt.Errorf("message id %d does not exist", id)
	}
	l.mu.RUnlock() // Drop the lock immediately! Disk I/O below is completely concurrent.

	// --- Step 2: Query the Index File ---
	indexOffset := int64(id * IndexEntrySize)
	var indexBuf [IndexEntrySize]byte
	if _, err := l.indexFile.ReadAt(indexBuf[:], indexOffset); err != nil {
		return nil, fmt.Errorf("failed to read index entry for ID %d: %w", id, err)
	}

	storedID := binary.BigEndian.Uint64(indexBuf[0:8])
	targetByteOffset := binary.BigEndian.Uint64(indexBuf[8:16])

	if storedID != id {
		return nil, fmt.Errorf("index corruption: expected ID %d, found %d", id, storedID)
	}

	// --- Step 3: Jump directly into the Data Log ---
	var headerBuf [8]byte
	if _, err := l.dataFile.ReadAt(headerBuf[:], int64(targetByteOffset)); err != nil {
		return nil, fmt.Errorf("failed to read payload size header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint64(headerBuf[:])
	payload := make([]byte, payloadLen)

	// Read payload by shifting the offset past the 8-byte header
	payloadOffset := int64(targetByteOffset) + 8
	if _, err := l.dataFile.ReadAt(payload, payloadOffset); err != nil {
		return nil, fmt.Errorf("failed to read payload bytes: %w", err)
	}

	return payload, nil
}

func (l *IndexedLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.indexFile.Close(); err != nil {
		return err
	}
	return l.dataFile.Close()
}
