package stream

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

// Replica provides storage for Stream artefacts.
type Replica interface {
	// Name returns a stable identifier for logs and metrics.
	Name() string
	// PutSnapshot persists a full snapshot blob for the given generation.
	PutSnapshot(ctx context.Context, generation string, snapshot *Snapshot) error

	// PutSegment persists an incremental segment blob.
	PutSegment(ctx context.Context, generation string, segment *Segment) error

	// Prune applies retention rules and deletes expired data.
	Prune(ctx context.Context, generation string, retention RetentionConfig) error

	// FetchSnapshot retrieves the referenced snapshot blob.
	FetchSnapshot(ctx context.Context, generation string, desc *SnapshotDescriptor) (*Snapshot, error)

	// FetchSegment retrieves the referenced segment blob.
	FetchSegment(ctx context.Context, generation string, desc SegmentDescriptor) (*Segment, error)

	// LatestState returns the newest generation snapshot metadata for restores.
	LatestState(ctx context.Context) (*RestoreState, error)

	// Close releases any held resources.
	Close(ctx context.Context) error
}

// RestoreState describes the current head artefact for a replica.
type RestoreState struct {
	Generation   string
	Snapshot     *SnapshotDescriptor
	Segments     []SegmentDescriptor
	LastUploaded time.Time
}

// SnapshotDescriptor references a stored snapshot object.
type SnapshotDescriptor struct {
	Name      string
	Timestamp time.Time
	Size      int64
}

// SegmentDescriptor references a single segment artefact.
type SegmentDescriptor struct {
	Name      string
	FirstTxID uint64
	LastTxID  uint64
	Timestamp time.Time
	Size      int64
}

func snapshotObjectName(generation string, created time.Time, txid uint64) string {
	return path.Join(generation, "snapshots", fmt.Sprintf("%s-%016x.snapshot.cbor", created.Format(time.RFC3339Nano), txid))
}

func segmentObjectName(generation string, txid uint64) string {
	return path.Join(generation, "segments", fmt.Sprintf("%016x.segment.cbor", txid))
}

func parseSnapshotObject(name string) (time.Time, uint64, error) {
	parts := strings.Split(name, "-")
	if len(parts) < 2 {
		return time.Time{}, 0, fmt.Errorf("invalid snapshot object name")
	}
	ts, err := time.Parse(time.RFC3339Nano, strings.Join(parts[:len(parts)-1], "-"))
	if err != nil {
		return time.Time{}, 0, err
	}
	last := parts[len(parts)-1]
	last = strings.TrimSuffix(last, ".snapshot.cbor")
	val, err := strconv.ParseUint(last, 16, 64)
	if err != nil {
		return time.Time{}, 0, err
	}
	return ts, val, nil
}

func parseSegmentObject(name string) (uint64, error) {
	name = strings.TrimSuffix(name, ".segment.cbor")
	return strconv.ParseUint(name, 16, 64)
}
