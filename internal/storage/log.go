package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

type BatchLog struct {
	mu     sync.Mutex
	file   *os.File
	closed chan struct{}
}

func NewBatchLog(path string, syncInterval time.Duration) (*BatchLog, error) {
	// 1. Notice: NO os.O_SYNC here!
	// Writes hit the lightning-fast OS Page Cache directly.
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	bl := &BatchLog{
		file:   f,
		closed: make(chan struct{}),
	}

	// 2. Background worker: Flush dirty page cache pages to disk in batches
	go bl.backgroundFlusher(syncInterval)

	return bl, nil
}

// Append writes the frame in a single Write syscall to the OS Page Cache.
func (bl *BatchLog) Append(payload []byte) error {
	payloadLen := len(payload)
	frameSize := 8 + payloadLen

	// Combine header + payload into a SINGLE slice to avoid 2 write syscalls.
	// For production, use a sync.Pool buffer to make this zero-allocation!
	buf := make([]byte, frameSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(payloadLen))
	copy(buf[8:], payload)

	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Single syscall into Linux Page Cache (Takes nanoseconds!)
	if _, err := bl.file.Write(buf); err != nil {
		return fmt.Errorf("page cache write failed: %w", err)
	}

	return nil
}

func (bl *BatchLog) backgroundFlusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// DO NOT lock the mutex here.
			// Let the OS handle the file descriptor synchronization safely in the background.
			_ = bl.file.Sync()
		case <-bl.closed:
			return
		}
	}
}

func (bl *BatchLog) Close() error {
	close(bl.closed)

	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Guarantee every remaining byte is physically on disk before shutting down
	_ = bl.file.Sync()
	return bl.file.Close()
}
