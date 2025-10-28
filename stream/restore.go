package stream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (c *Controller) ensureRestored(ctx context.Context) error {
	target := c.db.Path()
	if c.config.Restore.TargetPath != "" {
		target = c.config.Restore.TargetPath
	}

	info, err := os.Stat(target)
	if err == nil && info.Size() > 0 {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	snapshot, segments, err := c.localRestoreState()
	if err != nil {
		return err
	}

	if snapshot == nil {
		snapshot, segments, err = replicaRestoreState(ctx, c.replicas)
		if err != nil {
			return err
		}
	}

	if snapshot == nil {
		return fmt.Errorf("stream: no snapshots available for restore")
	}

	tempDir := c.config.Restore.TempDir
	if tempDir == "" {
		tempDir = filepath.Dir(target)
	}

	if err := restoreToTarget(snapshot, segments, target, tempDir); err != nil {
		return fmt.Errorf("restore to target: %w", err)
	}
	return nil
}

func (c *Controller) localRestoreState() (*Snapshot, []*Segment, error) {
	entries, err := os.ReadDir(c.shadowDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	var bestSnapshot *Snapshot
	var bestSegments []*Segment
	var bestCreated time.Time

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		genDir := filepath.Join(c.shadowDir, entry.Name())
		snapshots, err := os.ReadDir(filepath.Join(genDir, "snapshots"))
		if err != nil {
			continue
		}
		for _, snapEntry := range snapshots {
			if snapEntry.IsDir() || !strings.HasSuffix(snapEntry.Name(), ".snapshot.cbor") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(genDir, "snapshots", snapEntry.Name()))
			if err != nil {
				continue
			}
			snapshot, err := decodeSnapshotFile(data)
			if err != nil {
				continue
			}
			if bestSnapshot != nil && !snapshot.Header.CreatedAt.After(bestCreated) {
				continue
			}
			segments, err := loadSegmentsFromDir(filepath.Join(genDir, "segments"), snapshot.Header.TxID)
			if err != nil {
				return nil, nil, err
			}
			bestSnapshot = snapshot
			bestSegments = segments
			bestCreated = snapshot.Header.CreatedAt
		}
	}

	return bestSnapshot, bestSegments, nil
}

func replicaRestoreState(ctx context.Context, replicas []Replica) (*Snapshot, []*Segment, error) {
	for _, replica := range replicas {
		state, err := replica.LatestState(ctx)
		if err != nil || state == nil || state.Snapshot == nil {
			continue
		}
		snapshot, err := replica.FetchSnapshot(ctx, state.Generation, state.Snapshot)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch snapshot from %s: %w", replica.Name(), err)
		}
		var segments []*Segment
		for _, desc := range state.Segments {
			segment, err := replica.FetchSegment(ctx, state.Generation, desc)
			if err != nil {
				return nil, nil, fmt.Errorf("fetch segment from %s: %w", replica.Name(), err)
			}
			segments = append(segments, segment)
		}
		return snapshot, segments, nil
	}
	return nil, nil, nil
}

func restoreToTarget(snapshot *Snapshot, segments []*Segment, targetPath, tempDir string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}

	rawSnapshot, err := decompressBuffer(snapshot.Header.Compression, snapshot.Data)
	if err != nil {
		return fmt.Errorf("decompress snapshot: %w", err)
	}

	tmp, err := os.CreateTemp(tempDir, "stream-restore-*.db")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(rawSnapshot); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := applySegments(tmpName, snapshot.Header.PageSize, segments); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := os.Chmod(tmpName, 0o600); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func applySegments(path string, pageSize int, segments []*Segment) error {
	if len(segments) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Header.TxID < segments[j].Header.TxID
	})

	for _, segment := range segments {
		if err := populateSegmentPages(segment); err != nil {
			return err
		}
		for _, frame := range segment.Pages {
			offset := int64(frame.ID) * int64(pageSize)
			if _, err := f.WriteAt(frame.Data, offset); err != nil {
				return fmt.Errorf("write segment frame: %w", err)
			}
		}
	}

	return f.Sync()
}

func loadSegmentsFromDir(dir string, afterTxID uint64) ([]*Segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var segments []*Segment
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".segment.cbor") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		segment, err := decodeSegmentFile(data)
		if err != nil {
			return nil, err
		}
		if segment.Header.TxID <= afterTxID {
			continue
		}
		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Header.TxID < segments[j].Header.TxID
	})
	return segments, nil
}

func decodeSnapshotFile(data []byte) (*Snapshot, error) {
	var payload struct {
		Header SnapshotHeader `cbor:"header"`
		Data   []byte         `cbor:"data"`
	}
	if err := cborDecMode.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode snapshot file: %w", err)
	}
	return &Snapshot{
		Header: payload.Header,
		Data:   payload.Data,
	}, nil
}

func decodeSegmentFile(data []byte) (*Segment, error) {
	var payload struct {
		Header SegmentHeader `cbor:"header"`
		Data   []byte        `cbor:"data"`
	}
	if err := cborDecMode.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode segment file: %w", err)
	}
	segment := &Segment{
		Header: payload.Header,
		Data:   payload.Data,
	}
	if err := populateSegmentPages(segment); err != nil {
		return nil, err
	}
	return segment, nil
}

func populateSegmentPages(segment *Segment) error {
	if len(segment.Pages) > 0 {
		return nil
	}
	raw, err := decompressBuffer(segment.Header.Compression, segment.Data)
	if err != nil {
		return fmt.Errorf("decompress segment: %w", err)
	}
	var payload struct {
		Header SegmentHeader `cbor:"header"`
		Pages  []PageFrame   `cbor:"pages"`
	}
	if err := cborDecMode.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decode segment payload: %w", err)
	}
	segment.Pages = payload.Pages
	return nil
}

// RestoreStandalone builds replicas from configuration and restores the database to disk.
func RestoreStandalone(ctx context.Context, cfg Config) error {
	replicas, err := BuildReplicas(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeReplicas(ctx, replicas)

	snapshot, segments, err := replicaRestoreState(ctx, replicas)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("stream: no replica snapshots available")
	}

	target := cfg.Restore.TargetPath
	if target == "" {
		return fmt.Errorf("stream: restore target path is required")
	}
	tempDir := cfg.Restore.TempDir
	if tempDir == "" {
		tempDir = filepath.Dir(target)
	}
	return restoreToTarget(snapshot, segments, target, tempDir)
}

func closeReplicas(ctx context.Context, replicas []Replica) {
	for _, replica := range replicas {
		_ = replica.Close(ctx)
	}
}
