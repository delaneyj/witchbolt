package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

// CompressionType enumerates the available wire compression codecs.
type CompressionType string

const (
	// CompressionNone disables compression for segments and snapshots.
	CompressionNone CompressionType = "none"
	// CompressionZSTD compresses payloads with Zstandard.
	CompressionZSTD CompressionType = "zstd"
)

// CompressionConfig defines codec-agnostic tuning parameters.
type CompressionConfig struct {
	Codec  CompressionType `json:"codec"`
	Level  int             `json:"level,omitempty"`
	Window int             `json:"window,omitempty"`
}

func bytesTrimSpace(b []byte) []byte {
	return bytes.TrimSpace(b)
}

func (c *CompressionConfig) UnmarshalJSON(data []byte) error {
	type alias CompressionConfig
	data = bytesTrimSpace(data)
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		var codec string
		if err := json.Unmarshal(data, &codec); err != nil {
			return err
		}
		c.Codec = CompressionType(codec)
		return nil
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*c = CompressionConfig(aux)
	return nil
}

// Config drives the stream controller behaviour.
type Config struct {
	// ShadowDir stores local segments and snapshots before upload.
	ShadowDir string `json:"shadowDir"`

	// SnapshotInterval controls how frequently full snapshots are taken.
	SnapshotInterval time.Duration `json:"snapshotInterval"`

	// Retention governs automatic pruning of old artefacts.
	Retention RetentionConfig `json:"retention"`

	// Compression configures the codec and tuning options for artefacts.
	Compression CompressionConfig `json:"compression"`

	// Replicas defines zero or more remote destinations.
	Replicas []ReplicaConfig `json:"replicas"`

	// Restore enables automatic restore on startup if the database file
	// does not exist or fails validation.
	Restore RestoreConfig `json:"restore"`

	// DataLossWindowThreshold controls the alerting threshold for acceptable
	// replication lag duration. Zero disables warnings.
	DataLossWindowThreshold time.Duration `json:"dataLossWindowThreshold"`
}

// RetentionConfig describes snapshot & segment pruning rules.
type RetentionConfig struct {
	// SnapshotInterval optionally overrides Config.SnapshotInterval for
	// retention enforcement. Zero inherits the controller interval.
	SnapshotInterval time.Duration `json:"snapshotInterval"`

	// SnapshotRetention is the minimum duration to keep snapshots.
	SnapshotRetention time.Duration `json:"snapshotRetention"`

	// CheckInterval configures how often the pruning loop runs.
	CheckInterval time.Duration `json:"checkInterval"`
}

// RestoreConfig instructs the controller how and when to restore.
type RestoreConfig struct {
	// Enabled toggles automatic restores.
	Enabled bool `json:"enabled"`

	// TargetPath allows overriding the default database path.
	TargetPath string `json:"targetPath"`

	// TempDir controls where intermediate restore files live.
	TempDir string `json:"tempDir"`
}

// ReplicaConfig describes a backend-specific replica configuration.
type ReplicaConfig interface {
	buildReplica(ctx context.Context) (Replica, error)
}

func (c CompressionConfig) normalized() compressionSettings {
	if c.Codec == "" {
		c.Codec = CompressionZSTD
		if c.Level == 0 {
			c.Level = 6
		}
	}
	settings := compressionSettings(c)
	return normalizeCompressionSettings(settings)
}
