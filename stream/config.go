package stream

import (
	"bytes"
	"encoding/json"
	"time"

	yaml "gopkg.in/yaml.v3"
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
	Codec  CompressionType `json:"codec" yaml:"codec"`
	Level  int             `json:"level,omitempty" yaml:"level,omitempty"`
	Window int             `json:"window,omitempty" yaml:"window,omitempty"`
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

func (c *CompressionConfig) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		var codec string
		if err := value.Decode(&codec); err != nil {
			return err
		}
		c.Codec = CompressionType(codec)
		return nil
	default:
		type alias CompressionConfig
		var aux alias
		if err := value.Decode(&aux); err != nil {
			return err
		}
		*c = CompressionConfig(aux)
		return nil
	}
}

// Config drives the stream controller behaviour.
type Config struct {
	// ShadowDir stores local segments and snapshots before upload.
	ShadowDir string `json:"shadowDir" yaml:"shadow_dir"`

	// SnapshotInterval controls how frequently full snapshots are taken.
	SnapshotInterval time.Duration `json:"snapshotInterval" yaml:"snapshot_interval"`

	// Retention governs automatic pruning of old artefacts.
	Retention RetentionConfig `json:"retention" yaml:"retention"`

	// Compression configures the codec and tuning options for artefacts.
	Compression CompressionConfig `json:"compression" yaml:"compression"`

	// Replicas defines zero or more remote destinations.
	Replicas []ReplicaConfig `json:"replicas" yaml:"replicas"`

	// Restore enables automatic restore on startup if the database file
	// does not exist or fails validation.
	Restore RestoreConfig `json:"restore" yaml:"restore"`

	// DataLossWindowThreshold controls the alerting threshold for acceptable
	// replication lag duration. Zero disables warnings.
	DataLossWindowThreshold time.Duration `json:"dataLossWindowThreshold" yaml:"data_loss_window_threshold"`
}

// RetentionConfig describes snapshot & segment pruning rules.
type RetentionConfig struct {
	// SnapshotInterval optionally overrides Config.SnapshotInterval for
	// retention enforcement. Zero inherits the controller interval.
	SnapshotInterval time.Duration `json:"snapshotInterval" yaml:"snapshot_interval"`

	// SnapshotRetention is the minimum duration to keep snapshots.
	SnapshotRetention time.Duration `json:"snapshotRetention" yaml:"snapshot_retention"`

	// CheckInterval configures how often the pruning loop runs.
	CheckInterval time.Duration `json:"checkInterval" yaml:"check_interval"`
}

// RestoreConfig instructs the controller how and when to restore.
type RestoreConfig struct {
	// Enabled toggles automatic restores.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// TargetPath allows overriding the default database path.
	TargetPath string `json:"targetPath" yaml:"target_path"`

	// TempDir controls where intermediate restore files live.
	TempDir string `json:"tempDir" yaml:"temp_dir"`
}

// ReplicaConfig describes an individual replica target.
type ReplicaConfig struct {
	// Type selects the backend implementation (currently "file" or "s3").
	Type string `json:"type" yaml:"type"`

	// Name is a human-readable identifier for metrics/logging.
	Name string `json:"name" yaml:"name"`

	File *FileReplicaConfig  `json:"file,omitempty" yaml:"file,omitempty"`
	S3   *S3CompatibleConfig `json:"s3,omitempty" yaml:"s3,omitempty"`
	SFTP *SFTPReplicaConfig  `json:"sftp,omitempty" yaml:"sftp,omitempty"`
	NATS *NATSReplicaConfig  `json:"nats,omitempty" yaml:"nats,omitempty"`
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
