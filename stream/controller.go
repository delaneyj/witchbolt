package stream

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/crc64"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/delaneyj/witchbolt"
)

// Controller implements a witchbolt.PageFlushObserver and orchestrates replication.
type Controller struct {
	db     *witchbolt.DB
	config Config

	replicas    []Replica
	shadowDir   string
	compression compressionSettings

	mu              sync.RWMutex
	currentGen      string
	lastTxID        uint64
	lastSnapshot    time.Time
	lastReplication time.Time
	replicaLag      map[string]time.Time

	retentionCh chan struct{}
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

var crcTable = crc64.MakeTable(crc64.ISO)

// NewController creates a stream controller for the provided database.
func NewController(db *witchbolt.DB, cfg Config, replicas []Replica) (*Controller, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if cfg.ShadowDir == "" {
		cfg.ShadowDir = filepath.Join(filepath.Dir(db.Path()), "stream")
	}
	if err := os.MkdirAll(cfg.ShadowDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shadow dir: %w", err)
	}
	compression := cfg.Compression.normalized()
	cfg.Compression.Codec = compression.Codec
	cfg.Compression.Level = compression.Level
	cfg.Compression.Window = compression.Window

	ctrl := &Controller{
		db:          db,
		config:      cfg,
		replicas:    replicas,
		shadowDir:   cfg.ShadowDir,
		compression: compression,
		replicaLag:  make(map[string]time.Time),
		retentionCh: make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
	}
	return ctrl, nil
}

// Enable constructs and starts a controller based on the provided configuration.
func Enable(ctx context.Context, db *witchbolt.DB, cfg Config) (*Controller, error) {
	replicas, err := BuildReplicas(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ctrl, err := NewController(db, cfg, replicas)
	if err != nil {
		return nil, err
	}
	db.RegisterPageFlushObserver(ctrl)
	if err := ctrl.Start(ctx); err != nil {
		db.UnregisterPageFlushObserver(ctrl)
		return nil, err
	}
	return ctrl, nil
}

// Start attaches the controller to the DB and launches background tasks.
func (c *Controller) Start(ctx context.Context) error {
	if c.config.SnapshotInterval <= 0 {
		c.config.SnapshotInterval = 6 * time.Hour
	}
	if c.config.Retention.CheckInterval <= 0 {
		c.config.Retention.CheckInterval = time.Hour
	}
	if c.config.Retention.SnapshotRetention <= 0 {
		c.config.Retention.SnapshotRetention = 24 * time.Hour
	}
	if c.config.Restore.Enabled {
		if err := c.ensureRestored(ctx); err != nil {
			return fmt.Errorf("auto-restore: %w", err)
		}
	}
	c.wg.Add(1)
	go c.retentionLoop()
	return nil
}

// Stop detaches the controller and waits for background tasks to finish.
func (c *Controller) Stop(ctx context.Context) error {
	close(c.closeCh)
	c.wg.Wait()
	c.db.UnregisterPageFlushObserver(c)
	var errs []error
	for _, replica := range c.replicas {
		if err := replica.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close replicas: %v", errs)
	}
	return nil
}

// OnPageFlush implements witchbolt.PageFlushObserver.
func (c *Controller) OnPageFlush(info witchbolt.PageFlushInfo) error {
	segment, err := c.buildSegment(info)
	if err != nil {
		return err
	}
	return c.persistSegment(info, segment)
}

func (c *Controller) buildSegment(info witchbolt.PageFlushInfo) (*Segment, error) {
	frames := make([]PageFrame, len(info.Frames))
	for i, frame := range info.Frames {
		frames[i] = PageFrame{
			ID:       frame.ID,
			Overflow: frame.Overflow,
			Data:     append([]byte(nil), frame.Data...),
		}
	}

	createdAt := info.Timestamp
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	header := SegmentHeader{
		Magic:             segmentMagic,
		Version:           segmentVersion,
		TxID:              info.TxID,
		ParentTxID:        info.ParentTxID,
		PageCount:         len(frames),
		PageSize:          info.PageSize,
		Compression:       c.compression.Codec,
		CompressionLevel:  c.compression.Level,
		CompressionWindow: c.compression.Window,
		CreatedAt:         createdAt,
		HighWaterMark:     info.HighWaterMark,
	}

	segment := &Segment{
		Header: header,
		Pages:  frames,
	}
	payload := buildSegmentPayload(segment)
	raw, err := encodeSegmentCBORPayload(&payload)
	if err != nil {
		return nil, fmt.Errorf("marshal segment payload: %w", err)
	}
	compressed, err := compressBuffer(c.compression, raw)
	if err != nil {
		return nil, fmt.Errorf("compress segment payload: %w", err)
	}
	segment.Data = compressed
	segment.Header.Checksum = crc64.Checksum(compressed, crcTable)
	return segment, nil
}

func (c *Controller) persistSegment(info witchbolt.PageFlushInfo, segment *Segment) error {
	logger := c.db.Logger()

	c.mu.Lock()
	generation := c.currentGen
	if generation == "" || (c.lastTxID != 0 && info.ParentTxID != c.lastTxID) {
		generation = newGenerationID()
		logger.Infof("stream: starting generation %s (tx=%d)", generation, info.TxID)
		c.currentGen = generation
		c.lastTxID = 0
	}
	c.lastTxID = info.TxID
	c.lastReplication = time.Now()
	c.mu.Unlock()

	if err := c.writeSegmentToShadow(generation, segment); err != nil {
		return err
	}

	ctx := context.Background()
	var errs []error
	for _, replica := range c.replicas {
		if err := replica.PutSegment(ctx, generation, segment); err != nil {
			errs = append(errs, fmt.Errorf("%s put segment: %w", replica.Name(), err))
		} else {
			c.mu.Lock()
			c.replicaLag[replica.Name()] = time.Now()
			c.mu.Unlock()
		}
	}

	if err := c.maybeSnapshot(ctx, generation); err != nil {
		errs = append(errs, err)
	}

	c.triggerRetention()

	if len(errs) > 0 {
		return aggregateErrors("persist segment", errs)
	}
	return nil
}

func (c *Controller) writeSegmentToShadow(generation string, segment *Segment) error {
	dir := filepath.Join(c.shadowDir, generation, "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create segment dir: %w", err)
	}
	filename := fmt.Sprintf("%016x.segment.cbor", segment.Header.TxID)
	path := filepath.Join(dir, filename)
	encoded, err := marshalSegment(segment)
	if err != nil {
		return fmt.Errorf("marshal segment file: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write segment file: %w", err)
	}
	return nil
}

func (c *Controller) maybeSnapshot(ctx context.Context, generation string) error {
	interval := c.config.SnapshotInterval
	if interval <= 0 {
		return nil
	}

	c.mu.RLock()
	last := c.lastSnapshot
	c.mu.RUnlock()

	if !last.IsZero() && time.Since(last) < interval {
		return nil
	}

	snapshot, err := c.createSnapshot(ctx, generation)
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	if snapshot == nil {
		return nil
	}

	c.mu.Lock()
	c.lastSnapshot = snapshot.Header.CreatedAt
	c.mu.Unlock()

	return nil
}

func (c *Controller) createSnapshot(ctx context.Context, generation string) (*Snapshot, error) {
	var snap *Snapshot
	err := c.db.View(func(tx *witchbolt.Tx) error {
		var buf bytes.Buffer
		if _, err := tx.WriteTo(&buf); err != nil {
			return fmt.Errorf("tx.WriteTo: %w", err)
		}
		pageSize := tx.DB().Info().PageSize
		if pageSize <= 0 {
			return fmt.Errorf("invalid page size: %d", pageSize)
		}
		raw := buf.Bytes()
		compressed, err := compressBuffer(c.compression, raw)
		if err != nil {
			return fmt.Errorf("compress snapshot: %w", err)
		}
		pageCount := uint64(len(raw)) / uint64(pageSize)
		txNum := uint64(0)
		if id := tx.ID(); id >= 0 {
			txNum = uint64(id)
		}
		snap = &Snapshot{
			Header: SnapshotHeader{
				Magic:             segmentMagic,
				Version:           segmentVersion,
				TxID:              txNum,
				PageCount:         pageCount,
				PageSize:          pageSize,
				Compression:       c.compression.Codec,
				CompressionLevel:  c.compression.Level,
				CompressionWindow: c.compression.Window,
				CreatedAt:         time.Now().UTC(),
			},
			Data: compressed,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, nil
	}

	if err := c.writeSnapshotToShadow(generation, snap); err != nil {
		return nil, err
	}

	var errs []error
	for _, replica := range c.replicas {
		if err := replica.PutSnapshot(ctx, generation, snap); err != nil {
			errs = append(errs, fmt.Errorf("%s put snapshot: %w", replica.Name(), err))
		} else {
			c.mu.Lock()
			c.replicaLag[replica.Name()] = time.Now()
			c.mu.Unlock()
		}
	}

	if len(errs) > 0 {
		return nil, aggregateErrors("replicate snapshot", errs)
	}

	c.triggerRetention()
	return snap, nil
}

func (c *Controller) writeSnapshotToShadow(generation string, snapshot *Snapshot) error {
	dir := filepath.Join(c.shadowDir, generation, "snapshots")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	filename := fmt.Sprintf("%s-%016x.snapshot.cbor", snapshot.Header.CreatedAt.Format(time.RFC3339Nano), snapshot.Header.TxID)
	path := filepath.Join(dir, filename)
	encoded, err := marshalSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot file: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write snapshot file: %w", err)
	}
	return nil
}

func (c *Controller) retentionLoop() {
	defer c.wg.Done()
	interval := c.config.Retention.CheckInterval
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.enforceRetention(context.Background())
		case <-c.retentionCh:
			c.enforceRetention(context.Background())
		}
	}
}

func (c *Controller) enforceRetention(ctx context.Context) {
	if len(c.replicas) == 0 {
		return
	}
	c.mu.RLock()
	retention := c.config.Retention
	generation := c.currentGen
	c.mu.RUnlock()
	for _, replica := range c.replicas {
		if err := replica.Prune(ctx, generation, retention); err != nil {
			c.db.Logger().Warningf("stream: prune %s failed: %v", replica.Name(), err)
		}
	}
}

func (c *Controller) triggerRetention() {
	select {
	case c.retentionCh <- struct{}{}:
	default:
	}
}

// DataLossWindow reports the current worst-case replication lag.
func (c *Controller) DataLossWindow() time.Duration {
	now := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.replicaLag) == 0 {
		if c.lastReplication.IsZero() {
			return 0
		}
		return now.Sub(c.lastReplication)
	}
	var maxLag time.Duration
	for _, ts := range c.replicaLag {
		if ts.IsZero() {
			continue
		}
		lag := now.Sub(ts)
		if lag > maxLag {
			maxLag = lag
		}
	}
	return maxLag
}

func aggregateErrors(prefix string, errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	parts := make([]string, len(errs))
	for i, err := range errs {
		parts[i] = err.Error()
	}
	return fmt.Errorf("%s: %s", prefix, strings.Join(parts, "; "))
}

func newGenerationID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
