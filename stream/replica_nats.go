package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// NATSReplicaConfig configures the NATS JetStream replica backend.
type NATSReplicaConfig struct {
	URL     string   `json:"url"`
	Bucket  string   `json:"bucket"`
	Prefix  string   `json:"prefix"`
	Creds   string   `json:"creds"`
	NKey    string   `json:"nkey"`
	RootCAs []string `json:"rootCAs"`
}

func (cfg *NATSReplicaConfig) buildReplica(ctx context.Context) (Replica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nats replica config is nil")
	}
	return NewNATSReplica(ctx, cfg)
}

// NATSReplica persists artefacts via NATS JetStream object storage.
type NATSReplica struct {
	name    string
	cfg     NATSReplicaConfig
	connMu  sync.Mutex
	stateMu sync.Mutex
	nc      *nats.Conn
	js      jetstream.JetStream
	store   jetstream.ObjectStore
}

// NewNATSReplica constructs a JetStream-backed replica using the provided configuration.
func NewNATSReplica(_ context.Context, cfg *NATSReplicaConfig) (*NATSReplica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nats replica config is nil")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("nats bucket is required")
	}
	clean := *cfg
	clean.Prefix = strings.Trim(clean.Prefix, "/")
	return &NATSReplica{name: formatNATSReplicaName(clean), cfg: clean}, nil
}

// Name implements Replica.
func (r *NATSReplica) Name() string { return r.name }

// Close terminates the JetStream connection.
func (r *NATSReplica) Close(context.Context) error {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.nc != nil {
		r.nc.Close()
		r.nc = nil
		r.js = nil
		r.store = nil
	}
	return nil
}

// PutSnapshot uploads snapshot artefact data into JetStream.
func (r *NATSReplica) PutSnapshot(ctx context.Context, generation string, snapshot *Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := r.connect(ctx)
	if err != nil {
		return err
	}
	objectName := prefixedKey(r.cfg.Prefix, snapshotObjectName(generation, snapshot.Header.CreatedAt, snapshot.Header.TxID))
	if _, err := store.PutBytes(ctx, objectName, snapshot.Data); err != nil {
		return err
	}
	desc := &SnapshotDescriptor{
		Name:      objectName,
		Timestamp: snapshot.Header.CreatedAt,
		Size:      int64(len(snapshot.Data)),
	}
	return r.updateState(ctx, store, generation, desc, nil)
}

// PutSegment uploads a segment artefact to JetStream.
func (r *NATSReplica) PutSegment(ctx context.Context, generation string, segment *Segment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := r.connect(ctx)
	if err != nil {
		return err
	}
	objectName := prefixedKey(r.cfg.Prefix, segmentObjectName(generation, segment.Header.TxID))
	if _, err := store.PutBytes(ctx, objectName, segment.Data); err != nil {
		return err
	}
	desc := &SegmentDescriptor{
		Name:      objectName,
		FirstTxID: segment.Header.ParentTxID + 1,
		LastTxID:  segment.Header.TxID,
		Timestamp: segment.Header.CreatedAt,
		Size:      int64(len(segment.Data)),
	}
	return r.updateState(ctx, store, generation, nil, desc)
}

// Prune removes stale artefacts according to retention rules.
func (r *NATSReplica) Prune(ctx context.Context, generation string, retention RetentionConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if retention.SnapshotRetention <= 0 {
		return nil
	}
	store, err := r.connect(ctx)
	if err != nil {
		return err
	}
	return pruneNATSGeneration(ctx, store, r.cfg.Prefix, generation, retention.SnapshotRetention)
}

// FetchSnapshot downloads and decodes the referenced snapshot object.
func (r *NATSReplica) FetchSnapshot(ctx context.Context, generation string, desc *SnapshotDescriptor) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	data, err := store.GetBytes(ctx, desc.Name)
	if err != nil {
		return nil, err
	}
	return decodeSnapshotFile(data)
}

// FetchSegment downloads and decodes the referenced segment object.
func (r *NATSReplica) FetchSegment(ctx context.Context, generation string, desc SegmentDescriptor) (*Segment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	data, err := store.GetBytes(ctx, desc.Name)
	if err != nil {
		return nil, err
	}
	return decodeSegmentFile(data)
}

// LatestState retrieves the replica manifest from JetStream.
func (r *NATSReplica) LatestState(ctx context.Context) (*RestoreState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	state, err := r.loadState(ctx, store)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (r *NATSReplica) updateState(ctx context.Context, store jetstream.ObjectStore, generation string, snapshot *SnapshotDescriptor, segment *SegmentDescriptor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	state, err := r.loadState(ctx, store)
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
	stateKey := prefixedKey(r.cfg.Prefix, stateFileName)
	if err := deleteObjectIfExists(ctx, store, stateKey); err != nil {
		return err
	}
	_, err = store.PutBytes(ctx, stateKey, data)
	return err
}

func (r *NATSReplica) loadState(ctx context.Context, store jetstream.ObjectStore) (*RestoreState, error) {
	stateKey := prefixedKey(r.cfg.Prefix, stateFileName)
	data, err := store.GetBytes(ctx, stateKey)
	if err != nil {
		if isNATSNotFound(err) {
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

func (r *NATSReplica) connect(ctx context.Context) (jetstream.ObjectStore, error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.store != nil {
		return r.store, nil
	}
	opts := []nats.Option{
		nats.Name("witchbolt-stream"),
	}
	if r.cfg.Creds != "" {
		if r.cfg.NKey != "" {
			opts = append(opts, nats.UserCredentials(r.cfg.Creds, r.cfg.NKey))
		} else {
			opts = append(opts, nats.UserCredentials(r.cfg.Creds))
		}
	} else if r.cfg.NKey != "" {
		nkeyOpt, err := natsNKeyOption(r.cfg.NKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, nkeyOpt)
	}
	if len(r.cfg.RootCAs) > 0 {
		opts = append(opts, nats.RootCAs(r.cfg.RootCAs...))
	}
	url := r.cfg.URL
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	store, err := js.ObjectStore(ctx, r.cfg.Bucket)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream object store %q: %w", r.cfg.Bucket, err)
	}
	r.nc = nc
	r.js = js
	r.store = store
	return store, nil
}

func formatNATSReplicaName(cfg NATSReplicaConfig) string {
	uri := cfg.URL
	if uri == "" {
		uri = nats.DefaultURL
	}
	if parsed, err := url.Parse(uri); err == nil {
		parsed.User = nil
		uri = parsed.String()
	}
	uri = strings.TrimRight(uri, "/")
	if cfg.Bucket != "" {
		uri = uri + "/" + cfg.Bucket
		if cfg.Prefix != "" {
			uri = uri + "/" + strings.Trim(cfg.Prefix, "/")
		}
	}
	return uri
}

func natsNKeyOption(seedPath string) (nats.Option, error) {
	data, err := os.ReadFile(seedPath)
	if err != nil {
		return nil, fmt.Errorf("read nkey seed: %w", err)
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, fmt.Errorf("nkey seed is empty")
	}
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		kp, err := nkeys.ParseDecoratedNKey(data)
		if err != nil {
			return nil, fmt.Errorf("parse decorated nkey: %w", err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("derive nkey public key: %w", err)
		}
		return nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
			sig, err := kp.Sign(nonce)
			if err != nil {
				return nil, err
			}
			return sig, nil
		}), nil
	}
	kp, err := nkeys.FromSeed([]byte(trimmed))
	if err != nil {
		return nil, fmt.Errorf("load nkey seed: %w", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("derive nkey public key: %w", err)
	}
	kp.Wipe()
	seed := trimmed
	return nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed([]byte(seed))
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		sig, err := kp.Sign(nonce)
		if err != nil {
			return nil, err
		}
		return sig, nil
	}), nil
}

func pruneNATSGeneration(ctx context.Context, store jetstream.ObjectStore, prefix, generation string, retention time.Duration) error {
	snapPrefix := prefixedKey(prefix, path.Join(generation, "snapshots"))
	segPrefix := prefixedKey(prefix, path.Join(generation, "segments"))
	infos, err := store.List(ctx)
	if err != nil {
		return err
	}
	type snapInfo struct {
		name    string
		created time.Time
		txid    uint64
	}
	var snaps []snapInfo
	prefixWithSlash := func(p string) string {
		if p == "" {
			return ""
		}
		return p + "/"
	}
	snapPrefixSlash := prefixWithSlash(snapPrefix)
	segPrefixSlash := prefixWithSlash(segPrefix)
	for _, info := range infos {
		if info.Deleted {
			continue
		}
		if snapPrefixSlash != "" && strings.HasPrefix(info.Name, snapPrefixSlash) {
			base := path.Base(info.Name)
			created, txid, err := parseSnapshotObject(base)
			if err != nil {
				continue
			}
			snaps = append(snaps, snapInfo{name: info.Name, created: created, txid: txid})
		}
	}
	if len(snaps) == 0 {
		return nil
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].created.After(snaps[j].created) })
	cutoff := time.Now().Add(-retention)
	var keepTxID uint64
	for idx, snap := range snaps {
		if snap.created.After(cutoff) || idx == 0 {
			if snap.txid > keepTxID {
				keepTxID = snap.txid
			}
			continue
		}
		_ = store.Delete(ctx, snap.name)
	}
	if keepTxID == 0 {
		keepTxID = snaps[0].txid
	}
	for _, info := range infos {
		if info.Deleted {
			continue
		}
		if segPrefixSlash != "" && strings.HasPrefix(info.Name, segPrefixSlash) {
			base := path.Base(info.Name)
			txid, err := parseSegmentObject(base)
			if err != nil {
				continue
			}
			if txid <= keepTxID {
				_ = store.Delete(ctx, info.Name)
			}
		}
	}
	return nil
}

func deleteObjectIfExists(ctx context.Context, store jetstream.ObjectStore, name string) error {
	if err := store.Delete(ctx, name); err != nil && !isNATSNotFound(err) {
		return err
	}
	return nil
}

func isNATSNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrObjectNotFound) || errors.Is(err, nats.ErrKeyNotFound)
}
