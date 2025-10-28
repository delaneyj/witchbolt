# Style & conventions
- Language: Go (>= version in `.go-version`).
- Formatting: enforce `gofmt -s`, `goimports`, and `gci` (std/default/go.etcd.io sections). `make fmt` verifies formatting.
- Linting: `golangci-lint` with errcheck, govet, ineffassign, staticcheck (custom exclusions) and unused enabled.
- Testing ethos: run go tests under both freelist configurations using env vars `TEST_FREELIST_TYPE` and `BBOLT_VERIFY`.
- Prefer standard Go error handling; repo uses failpoints tooling (`go.etcd.io/gofail`) in specialized tests.