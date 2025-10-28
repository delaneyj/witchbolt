# bbolt overview
- Purpose: Go key/value store maintained under go.etcd.io/bbolt, compatibility fork of Bolt with reliability/stability improvements.
- Primary usage: embedded transactional database and CLI utility (`cmd/witchbolt`).
- Structure highlights: library code at repo root (`db.go`, `bucket.go`, etc.); CLI entrypoint under `cmd/witchbolt`; supporting tooling/tests in `internal/` (freelist, robustness, surgeon, etc.); scripts/Makefile for dev tasks.
- Key dependencies: standard Go stdlib; golangci-lint config; optional gofail tooling for failpoints.