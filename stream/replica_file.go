package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const stateFileName = "_state.json"

// FileReplica persists artefacts to the local filesystem.
type FileReplica struct {
	name     string
	basePath string
	mu       sync.Mutex
}

// FileReplicaConfig defines the local filesystem replica behaviour.
type FileReplicaConfig struct {
	Path string `json:"path"`
}

func (cfg *FileReplicaConfig) buildReplica(_ context.Context) (Replica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("file replica config is nil")
	}
	return NewFileReplica(cfg)
}

// NewFileReplica constructs a FileReplica backed by a directory tree.
func NewFileReplica(cfg *FileReplicaConfig) (*FileReplica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("file replica config is nil")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("file replica path is empty")
	}
	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, fmt.Errorf("create replica path: %w", err)
	}
	replicaName := cfg.Path
	return &FileReplica{
		name:     replicaName,
		basePath: cfg.Path,
	}, nil
}

// Name implements Replica.
func (r *FileReplica) Name() string {
	return r.name
}

// PutSnapshot writes the snapshot payload and updates replica state.
func (r *FileReplica) PutSnapshot(ctx context.Context, generation string, snapshot *Snapshot) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	dir := filepath.Join(r.basePath, generation, "snapshots")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	filename := fmt.Sprintf("%s-%016x.snapshot.cbor", snapshot.Header.CreatedAt.Format(time.RFC3339Nano), snapshot.Header.TxID)
	if err := writeSnapshotFile(filepath.Join(dir, filename), snapshot); err != nil {
		return err
	}
	desc := SnapshotDescriptor{
		Name:      filepath.ToSlash(filepath.Join(generation, "snapshots", filename)),
		Timestamp: snapshot.Header.CreatedAt,
		Size:      int64(len(snapshot.Data)),
	}
	return r.updateState(&RestoreState{
		Generation:   generation,
		Snapshot:     &desc,
		Segments:     nil,
		LastUploaded: time.Now().UTC(),
	})
}

// PutSegment writes the segment payload and adds it to replica state.
func (r *FileReplica) PutSegment(ctx context.Context, generation string, segment *Segment) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	dir := filepath.Join(r.basePath, generation, "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create segment dir: %w", err)
	}
	filename := fmt.Sprintf("%016x.segment.cbor", segment.Header.TxID)
	if err := writeSegmentFile(filepath.Join(dir, filename), segment); err != nil {
		return err
	}
	desc := SegmentDescriptor{
		Name:      filepath.ToSlash(filepath.Join(generation, "segments", filename)),
		FirstTxID: segment.Header.ParentTxID + 1,
		LastTxID:  segment.Header.TxID,
		Timestamp: time.Now().UTC(),
		Size:      int64(len(segment.Data)),
	}
	return r.appendSegment(generation, desc)
}

// Prune removes expired artefacts according to retention policy.
func (r *FileReplica) Prune(ctx context.Context, generation string, retention RetentionConfig) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	entries, err := os.ReadDir(r.basePath)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-retention.SnapshotRetention)
	for _, entry := range entries {
		if entry.IsDir() {
			if err := pruneGeneration(filepath.Join(r.basePath, entry.Name()), cutoff); err != nil {
				return err
			}
		}
	}
	return nil
}

// FetchSnapshot retrieves the referenced snapshot payload from disk.
func (r *FileReplica) FetchSnapshot(ctx context.Context, generation string, desc *SnapshotDescriptor) (*Snapshot, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	path := filepath.Join(r.basePath, filepath.FromSlash(desc.Name))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeSnapshotFile(data)
}

// FetchSegment retrieves the referenced segment payload from disk.
func (r *FileReplica) FetchSegment(ctx context.Context, generation string, desc SegmentDescriptor) (*Segment, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	path := filepath.Join(r.basePath, filepath.FromSlash(desc.Name))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeSegmentFile(data)
}

// LatestState returns the most recent restore metadata.
func (r *FileReplica) LatestState(ctx context.Context) (*RestoreState, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return r.readState()
}

// Close releases resources. No-op for file replica.
func (r *FileReplica) Close(context.Context) error { return nil }

func (r *FileReplica) statePath() string {
	return filepath.Join(r.basePath, stateFileName)
}

func (r *FileReplica) readState() (*RestoreState, error) {
	data, err := os.ReadFile(r.statePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &RestoreState{}, nil
		}
		return nil, err
	}
	var state RestoreState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *FileReplica) updateState(state *RestoreState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeState(state)
}

func (r *FileReplica) appendSegment(generation string, desc SegmentDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readState()
	if err != nil {
		return err
	}
	if state.Generation != generation {
		state = &RestoreState{Generation: generation}
	}
	state.Segments = append(state.Segments, desc)
	state.LastUploaded = time.Now().UTC()
	return r.writeState(state)
}

func (r *FileReplica) writeState(state *RestoreState) error {
	path := r.statePath()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeSnapshotFile(path string, snapshot *Snapshot) error {
	data, err := marshalSnapshot(snapshot)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeSegmentFile(path string, segment *Segment) error {
	data, err := marshalSegment(segment)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func pruneGeneration(dir string, cutoff time.Time) error {
	snapDir := filepath.Join(dir, "snapshots")
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	type snapInfo struct {
		path    string
		created time.Time
		txid    uint64
	}
	var snaps []snapInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snapshot.cbor") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(snapDir, entry.Name()))
		if err != nil {
			continue
		}
		snap, err := decodeSnapshotFile(data)
		if err != nil {
			continue
		}
		snaps = append(snaps, snapInfo{
			path:    filepath.Join(snapDir, entry.Name()),
			created: snap.Header.CreatedAt,
			txid:    snap.Header.TxID,
		})
	}
	if len(snaps) == 0 {
		return nil
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].created.After(snaps[j].created) })

	var keep []snapInfo
	for _, snap := range snaps {
		if snap.created.After(cutoff) || len(keep) == 0 {
			keep = append(keep, snap)
		} else {
			_ = os.Remove(snap.path)
		}
	}

	oldest := keep[len(keep)-1]
	segDir := filepath.Join(dir, "segments")
	segEntries, err := os.ReadDir(segDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range segEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".segment.cbor") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(segDir, entry.Name()))
		if err != nil {
			continue
		}
		segment, err := decodeSegmentFile(data)
		if err != nil {
			continue
		}
		if segment.Header.TxID <= oldest.txid {
			_ = os.Remove(filepath.Join(segDir, entry.Name()))
		}
	}
	return nil
}
