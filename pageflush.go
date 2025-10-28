package witchbolt

import (
	"fmt"
	"time"
)

// PageFlushObserver is notified when a set of dirty pages has been flushed to disk.
type PageFlushObserver interface {
	OnPageFlush(info PageFlushInfo) error
}

// PageFlushObserverRegistration contains an observer and optional lifecycle callbacks.
type PageFlushObserverRegistration struct {
	Observer PageFlushObserver
	Start    func(*DB) (PageFlushObserver, error)
	Close    func() error
}

// PageFlushInfo captures metadata about a completed page flush.
type PageFlushInfo struct {
	TxID          uint64
	ParentTxID    uint64
	DBPath        string
	PageSize      int
	PageCount     int
	HighWaterMark uint64
	Timestamp     time.Time
	Frames        []PageFrame
}

// PageFrame contains a single page payload for observers.
type PageFrame struct {
	ID       uint64
	Overflow uint32
	Data     []byte
}

func (f PageFrame) validate(pageSize int) error {
	expected := int((f.Overflow + 1) * uint32(pageSize))
	if len(f.Data) != expected {
		return fmt.Errorf("page frame length mismatch: have %d, want %d", len(f.Data), expected)
	}
	return nil
}
