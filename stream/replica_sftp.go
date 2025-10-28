package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTPReplicaConfig configures the SFTP replica backend.
type SFTPReplicaConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	KeyPath  string `json:"keyPath"`
	Path     string `json:"path"`
}

func (cfg *SFTPReplicaConfig) buildReplica(ctx context.Context) (Replica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sftp replica config is nil")
	}
	return NewSFTPReplica(ctx, cfg)
}

// SFTPReplica persists artefacts over SFTP.
type SFTPReplica struct {
	name      string
	cfg       SFTPReplicaConfig
	connMu    sync.Mutex
	stateMu   sync.Mutex
	sshClient *ssh.Client
	client    *sftp.Client
}

// NewSFTPReplica constructs an SFTP replica backed by the provided configuration.
func NewSFTPReplica(_ context.Context, cfg *SFTPReplicaConfig) (*SFTPReplica, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sftp replica config is nil")
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("sftp host is required")
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("sftp user is required")
	}
	if cfg.Password == "" && cfg.KeyPath == "" {
		return nil, fmt.Errorf("sftp password or keyPath is required")
	}
	clean := *cfg
	clean.Path = path.Clean(clean.Path)
	if clean.Path == "." {
		clean.Path = ""
	}
	r := &SFTPReplica{name: formatSFTPReplicaName(clean), cfg: clean}
	return r, nil
}

// Name implements Replica.
func (r *SFTPReplica) Name() string { return r.name }

// Close releases any open SFTP/SSH connections.
func (r *SFTPReplica) Close(context.Context) error {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.client != nil {
		_ = r.client.Close()
		r.client = nil
	}
	if r.sshClient != nil {
		_ = r.sshClient.Close()
		r.sshClient = nil
	}
	return nil
}

// PutSnapshot uploads the snapshot artefact to the remote target and updates replica state.
func (r *SFTPReplica) PutSnapshot(ctx context.Context, generation string, snapshot *Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	client, err := r.connect()
	if err != nil {
		return err
	}
	remoteDir := r.remotePath(path.Join(generation, "snapshots"))
	if err := ensureRemoteDir(client, remoteDir); err != nil {
		return err
	}
	filename := fmt.Sprintf("%s-%016x.snapshot.cbor", snapshot.Header.CreatedAt.Format(time.RFC3339Nano), snapshot.Header.TxID)
	remotePath := path.Join(remoteDir, filename)
	if err := writeRemoteFile(client, remotePath, snapshot.Data); err != nil {
		return err
	}
	desc := &SnapshotDescriptor{
		Name:      path.Join(generation, "snapshots", filename),
		Timestamp: snapshot.Header.CreatedAt,
		Size:      int64(len(snapshot.Data)),
	}
	return r.updateState(ctx, client, generation, desc, nil)
}

// PutSegment uploads a segment artefact and records metadata in replica state.
func (r *SFTPReplica) PutSegment(ctx context.Context, generation string, segment *Segment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	client, err := r.connect()
	if err != nil {
		return err
	}
	remoteDir := r.remotePath(path.Join(generation, "segments"))
	if err := ensureRemoteDir(client, remoteDir); err != nil {
		return err
	}
	filename := fmt.Sprintf("%016x.segment.cbor", segment.Header.TxID)
	remotePath := path.Join(remoteDir, filename)
	if err := writeRemoteFile(client, remotePath, segment.Data); err != nil {
		return err
	}
	desc := &SegmentDescriptor{
		Name:      path.Join(generation, "segments", filename),
		FirstTxID: segment.Header.ParentTxID + 1,
		LastTxID:  segment.Header.TxID,
		Timestamp: segment.Header.CreatedAt,
		Size:      int64(len(segment.Data)),
	}
	return r.updateState(ctx, client, generation, nil, desc)
}

// Prune removes expired artefacts as dictated by the retention policy.
func (r *SFTPReplica) Prune(ctx context.Context, generation string, retention RetentionConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if retention.SnapshotRetention <= 0 {
		return nil
	}
	client, err := r.connect()
	if err != nil {
		return err
	}
	baseDir := r.remotePath(generation)
	return pruneSFTPGeneration(client, baseDir, retention.SnapshotRetention)
}

// FetchSnapshot downloads and decodes the referenced snapshot blob.
func (r *SFTPReplica) FetchSnapshot(ctx context.Context, generation string, desc *SnapshotDescriptor) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := r.connect()
	if err != nil {
		return nil, err
	}
	data, err := readRemoteFile(client, r.remotePath(desc.Name))
	if err != nil {
		return nil, err
	}
	return decodeSnapshotFile(data)
}

// FetchSegment downloads and decodes the referenced segment blob.
func (r *SFTPReplica) FetchSegment(ctx context.Context, generation string, desc SegmentDescriptor) (*Segment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := r.connect()
	if err != nil {
		return nil, err
	}
	data, err := readRemoteFile(client, r.remotePath(desc.Name))
	if err != nil {
		return nil, err
	}
	return decodeSegmentFile(data)
}

// LatestState retrieves the replica metadata manifest.
func (r *SFTPReplica) LatestState(ctx context.Context) (*RestoreState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := r.connect()
	if err != nil {
		return nil, err
	}
	state, err := r.loadState(client)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (r *SFTPReplica) updateState(ctx context.Context, client *sftp.Client, generation string, snapshot *SnapshotDescriptor, segment *SegmentDescriptor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	state, err := r.loadState(client)
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
	return writeRemoteFile(client, r.remotePath(stateFileName), data)
}

func (r *SFTPReplica) loadState(client *sftp.Client) (*RestoreState, error) {
	data, err := readRemoteFile(client, r.remotePath(stateFileName))
	if err != nil {
		if isSFTPNotExist(err) {
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

func (r *SFTPReplica) connect() (*sftp.Client, error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.client != nil {
		return r.client, nil
	}
	if r.cfg.Password == "" && r.cfg.KeyPath == "" {
		return nil, fmt.Errorf("sftp password or keyPath is required")
	}
	auth := []ssh.AuthMethod{}
	if r.cfg.Password != "" {
		auth = append(auth, ssh.Password(r.cfg.Password))
	}
	if r.cfg.KeyPath != "" {
		pem, err := os.ReadFile(r.cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("sftp: read key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(pem)
		if err != nil {
			return nil, fmt.Errorf("sftp: parse key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	config := &ssh.ClientConfig{
		User:            r.cfg.User,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}
	port := r.cfg.Port
	if port == 0 {
		port = 22
	}
	addr := net.JoinHostPort(r.cfg.Host, strconv.Itoa(port))
	sshClient, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	client, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, err
	}
	r.sshClient = sshClient
	r.client = client
	return client, nil
}

func (r *SFTPReplica) remotePath(rel string) string {
	rel = path.Clean(rel)
	if rel == "." {
		rel = ""
	}
	base := r.cfg.Path
	if base == "" || base == "." {
		if rel == "" {
			return ""
		}
		return rel
	}
	if rel == "" {
		return base
	}
	return path.Join(base, rel)
}

func formatSFTPReplicaName(cfg SFTPReplicaConfig) string {
	port := cfg.Port
	if port == 0 {
		port = 22
	}
	host := cfg.Host
	if port != 22 {
		host = net.JoinHostPort(host, strconv.Itoa(port))
	}
	if cfg.User != "" {
		host = cfg.User + "@" + host
	}
	pathPart := cfg.Path
	if pathPart != "" && !strings.HasPrefix(pathPart, "/") {
		pathPart = "/" + pathPart
	}
	return "sftp://" + host + pathPart
}

func writeRemoteFile(client *sftp.Client, filename string, data []byte) error {
	if err := ensureRemoteDir(client, path.Dir(filename)); err != nil {
		return err
	}
	f, err := client.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return nil
}

func readRemoteFile(client *sftp.Client, filename string) ([]byte, error) {
	f, err := client.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func ensureRemoteDir(client *sftp.Client, dir string) error {
	if dir == "" || dir == "." || dir == "/" {
		return nil
	}
	return client.MkdirAll(dir)
}

func pruneSFTPGeneration(client *sftp.Client, base string, retention time.Duration) error {
	snapDir := path.Join(base, "snapshots")
	entries, err := client.ReadDir(snapDir)
	if err != nil {
		if isSFTPNotExist(err) {
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
		created, txid, err := parseSnapshotObject(entry.Name())
		if err != nil {
			continue
		}
		snaps = append(snaps, snapInfo{
			path:    path.Join(snapDir, entry.Name()),
			created: created,
			txid:    txid,
		})
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
		_ = client.Remove(snap.path)
	}
	if keepTxID == 0 {
		keepTxID = snaps[0].txid
	}
	segDir := path.Join(base, "segments")
	segEntries, err := client.ReadDir(segDir)
	if err != nil {
		if isSFTPNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range segEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".segment.cbor") {
			continue
		}
		txid, err := parseSegmentObject(entry.Name())
		if err != nil {
			continue
		}
		if txid <= keepTxID {
			_ = client.Remove(path.Join(segDir, entry.Name()))
		}
	}
	return nil
}

func isSFTPNotExist(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, fs.ErrNotExist) {
		return true
	}
	var statusErr *sftp.StatusError
	if errors.As(err, &statusErr) {
		if statusErr.Code == 2 {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such file") || strings.Contains(msg, "does not exist")
}
