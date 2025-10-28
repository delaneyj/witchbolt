# Stream Architecture

Stream extends `github.com/delaneyj/witchbolt` with Litestream-style, page-level
replication. The controller hooks into the database commit path, captures the
pages written for each transaction, and persists them to a local shadow log
before forwarding them to one or more remote replicas.

## Replication model

- **Page frames:** Each commit emits a list of page frames that have been
  dirtied. Stream mirrors these frames into a segment file that can be
  replayed to reconstruct the database state.
- **Generations:** A generation is a contiguous snapshot plus all subsequent
  segments. Generations rotate automatically if the controller detects a gap or
  an out-of-order transaction.
- **Snapshots:** Full database snapshots are taken at configurable intervals to
  bound recovery time. Snapshots are versioned by generation and timestamp.
- **Retention:** Background retention jobs delete expired snapshots and any
  segments that are older than the oldest retained snapshot.
- **Data loss window:** The controller tracks the timestamp of the latest
  successful replication to each replica and reports the maximum lag.

## Storage replicas

Stream ships with pluggable replica backends:

- `file`: write segments and snapshots to a local directory tree.
- `s3`: stream artefacts to any S3-compatible API via the MinIO client (AWS, GCP, Azure, MinIO, etc.).
- `sftp`: push artefacts over SSH/SFTP to a remote host.
- `nats`: store artefacts in a pre-provisioned NATS JetStream object store bucket.

These implementations are direct ports of Litestream's storage clients adapted to
Stream's segment/snapshot format. Each backend exposes the same interface so new
destinations can be added without modifying the core controller.

## Compression

Segments and snapshots are compressed with Zstandard by default. The
`compression` block in the controller config accepts `codec: "none"` to disable
compression or `codec: "zstd"` (default) with an optional quality `level`
(mapped to the closest Zstandard encoder level). These options apply globally
to ensure deterministic restores.

```go
Compression: stream.CompressionConfig{
	Codec: stream.CompressionZSTD,
	Level: 6,
}
```

## Usage

Register Stream via the `PageFlushObservers` option when opening a database:

```go
db, err := witchbolt.Open(path, 0600, &witchbolt.Options{
	PageFlushObservers: []witchbolt.PageFlushObserverRegistration{
		stream.Observer(context.Background(), stream.Config{
			ShadowDir:        "/var/lib/myapp/stream",
			SnapshotInterval: 5 * time.Minute,
			Compression:      stream.CompressionConfig{Codec: stream.CompressionZSTD, Level: 6},
		Replicas: []stream.ReplicaConfig{
			&stream.FileReplicaConfig{Path: "/backups"},
			&stream.S3CompatibleConfig{
				Bucket:   "example-bucket",
				Prefix:   "stream",
				Endpoint: "s3.us-east-1.amazonaws.com",
				Region:   "us-east-1",
			},
			&stream.SFTPReplicaConfig{
				Host:    "backup.example.com",
				User:    "replicator",
				KeyPath: "/etc/witchbolt/sftp_key",
				Path:    "backups/db",
			},
			&stream.NATSReplicaConfig{
				URL:    "nats://nats.example.com:4222",
				Bucket: "litestream-backups",
				Prefix: "cluster-a/db",
				Creds:  "/etc/witchbolt/nats.creds",
			},
		},
		}),
	},
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

## Restore flow

1. Discover the newest generation and snapshot.
2. Download and decompress the snapshot into a scratch location.
3. Fetch and apply all newer segments.
4. Atomically move the restored database into place.

The controller exposes a helper that will optionally run this flow automatically
before opening the database, ensuring nodes can bootstrap themselves.

## Provenance

The Stream module and its replica targets are derived from Ben Johnson's
[Litestream](https://github.com/benbjohnson/litestream) project and carry the
same Apache 2.0 licensing obligations. Please refer to the top-level README and
LICENSE for attribution details.
