package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var errS3ObjectNotFound = errors.New("s3 object not found")

// S3CompatibleConfig configures a generic S3-compatible backend.
type S3CompatibleConfig struct {
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	AccessKey      string `json:"accessKey"`
	SecretKey      string `json:"secretKey"`
	SessionToken   string `json:"sessionToken"`
	Insecure       bool   `json:"insecure"`
	ForcePathStyle bool   `json:"forcePathStyle"`
}

func (cfg *S3CompatibleConfig) buildReplica(ctx context.Context) (Replica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("s3 replica config is nil")
	}
	return NewS3CompatibleReplica(ctx, cfg)
}

// S3CompatibleReplica stores artefacts in any S3-compatible object storage.
type S3CompatibleReplica struct {
	name   string
	client *minio.Client
	cfg    S3CompatibleConfig
	mu     sync.Mutex
}

// NewS3CompatibleReplica constructs an S3-compatible replica backed by MinIO client.
func NewS3CompatibleReplica(ctx context.Context, cfg *S3CompatibleConfig) (*S3CompatibleReplica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("s3 replica config is nil")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
		if cfg.Region != "" {
			endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.Region)
		}
	}
	var creds *credentials.Credentials
	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.SessionToken != "" {
		creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        creds,
		Secure:       !cfg.Insecure,
		Region:       cfg.Region,
		BucketLookup: bucketLookupStyle(cfg.ForcePathStyle),
	})
	if err != nil {
		return nil, err
	}
	replicaName := fmt.Sprintf("s3://%s", cfg.Bucket)
	if cfg.Prefix != "" {
		replicaName = fmt.Sprintf("s3://%s/%s", cfg.Bucket, cfg.Prefix)
	}
	return &S3CompatibleReplica{name: replicaName, client: client, cfg: *cfg}, nil
}

func bucketLookupStyle(forcePath bool) minio.BucketLookupType {
	if forcePath {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}

// Name implements Replica.
func (r *S3CompatibleReplica) Name() string { return r.name }

// Close satisfies the Replica interface. MinIO client does not hold open resources.
func (r *S3CompatibleReplica) Close(context.Context) error { return nil }

// PutSnapshot uploads the snapshot artefact and updates replica state.
func (r *S3CompatibleReplica) PutSnapshot(ctx context.Context, generation string, snapshot *Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	objectName := prefixedKey(r.cfg.Prefix, snapshotObjectName(generation, snapshot.Header.CreatedAt, snapshot.Header.TxID))
	if err := r.putObject(ctx, objectName, snapshot.Data); err != nil {
		return err
	}
	desc := SnapshotDescriptor{Name: objectName, Timestamp: snapshot.Header.CreatedAt, Size: int64(len(snapshot.Data))}
	return r.updateState(ctx, generation, &desc, nil)
}

// PutSegment uploads the segment artefact and appends metadata to replica state.
func (r *S3CompatibleReplica) PutSegment(ctx context.Context, generation string, segment *Segment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	objectName := prefixedKey(r.cfg.Prefix, segmentObjectName(generation, segment.Header.TxID))
	if err := r.putObject(ctx, objectName, segment.Data); err != nil {
		return err
	}
	desc := SegmentDescriptor{
		Name:      objectName,
		FirstTxID: segment.Header.ParentTxID + 1,
		LastTxID:  segment.Header.TxID,
		Timestamp: segment.Header.CreatedAt,
		Size:      int64(len(segment.Data)),
	}
	return r.updateState(ctx, generation, nil, &desc)
}

// Prune applies the retention policy to snapshots and segments.
func (r *S3CompatibleReplica) Prune(ctx context.Context, generation string, retention RetentionConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if retention.SnapshotRetention <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-retention.SnapshotRetention)
	snapshotsPrefix := prefixedKey(r.cfg.Prefix, path.Join(generation, "snapshots"))
	var keepTxID uint64
	if err := r.walkObjects(ctx, snapshotsPrefix, func(obj minio.ObjectInfo) error {
		created, txid, err := parseSnapshotObject(path.Base(obj.Key))
		if err != nil {
			return nil
		}
		if created.After(cutoff) || keepTxID == 0 {
			if txid > keepTxID {
				keepTxID = txid
			}
			return nil
		}
		return r.removeObject(ctx, obj.Key)
	}); err != nil {
		return err
	}
	if keepTxID == 0 {
		return nil
	}
	segmentsPrefix := prefixedKey(r.cfg.Prefix, path.Join(generation, "segments"))
	return r.walkObjects(ctx, segmentsPrefix, func(obj minio.ObjectInfo) error {
		txid, err := parseSegmentObject(path.Base(obj.Key))
		if err != nil {
			return nil
		}
		if txid <= keepTxID {
			return r.removeObject(ctx, obj.Key)
		}
		return nil
	})
}

// FetchSnapshot downloads and decodes a snapshot artefact.
func (r *S3CompatibleReplica) FetchSnapshot(ctx context.Context, generation string, desc *SnapshotDescriptor) (*Snapshot, error) {
	data, err := r.getObject(ctx, desc.Name)
	if err != nil {
		return nil, err
	}
	return decodeSnapshotFile(data)
}

// FetchSegment downloads and decodes a segment artefact.
func (r *S3CompatibleReplica) FetchSegment(ctx context.Context, generation string, desc SegmentDescriptor) (*Segment, error) {
	data, err := r.getObject(ctx, desc.Name)
	if err != nil {
		return nil, err
	}
	return decodeSegmentFile(data)
}

// LatestState retrieves the replica state manifest.
func (r *S3CompatibleReplica) LatestState(ctx context.Context) (*RestoreState, error) {
	data, err := r.getObject(ctx, r.stateKey())
	if err != nil {
		if errors.Is(err, errS3ObjectNotFound) {
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

func (r *S3CompatibleReplica) updateState(ctx context.Context, generation string, snapshot *SnapshotDescriptor, segment *SegmentDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.LatestState(ctx)
	if err != nil {
		return err
	}
	if state.Generation != generation {
		state = &RestoreState{Generation: generation}
	}
	if snapshot != nil {
		state.Snapshot = snapshot
		state.Segments = nil
	}
	if segment != nil {
		state.Segments = append(state.Segments, *segment)
	}
	state.LastUploaded = time.Now().UTC()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.putObject(ctx, r.stateKey(), data)
}

func (r *S3CompatibleReplica) putObject(ctx context.Context, key string, body []byte) error {
	reader := bytes.NewReader(body)
	_, err := r.client.PutObject(ctx, r.cfg.Bucket, key, reader, int64(len(body)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (r *S3CompatibleReplica) getObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := r.client.GetObject(ctx, r.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		if isS3NotFound(err) {
			return nil, errS3ObjectNotFound
		}
		return nil, err
	}
	defer obj.Close()
	if _, statErr := obj.Stat(); statErr != nil {
		if isS3NotFound(statErr) {
			return nil, errS3ObjectNotFound
		}
		return nil, statErr
	}
	data, readErr := io.ReadAll(obj)
	if readErr != nil {
		if isS3NotFound(readErr) {
			return nil, errS3ObjectNotFound
		}
		return nil, readErr
	}
	return data, nil
}

func (r *S3CompatibleReplica) removeObject(ctx context.Context, key string) error {
	return r.client.RemoveObject(ctx, r.cfg.Bucket, key, minio.RemoveObjectOptions{})
}

func (r *S3CompatibleReplica) walkObjects(ctx context.Context, prefix string, fn func(minio.ObjectInfo) error) error {
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for object := range r.client.ListObjects(ctx, r.cfg.Bucket, opts) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if object.Err != nil {
			if isS3NotFound(object.Err) {
				return nil
			}
			return object.Err
		}
		if err := fn(object); err != nil {
			return err
		}
	}
	return nil
}

func (r *S3CompatibleReplica) stateKey() string {
	return prefixedKey(r.cfg.Prefix, stateFileName)
}

func prefixedKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return path.Join(prefix, key)
}

func isS3NotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == 404 {
		return true
	}
	switch resp.Code {
	case "NoSuchKey", "NoSuchBucket", "NotFound", "NoSuchUpload":
		return true
	}
	return false
}
