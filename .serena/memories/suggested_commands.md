# Suggested commands
- `make fmt` – verify gofmt/goimports/gci formatting (use `./scripts/fix.sh` to auto-fix).
- `make lint` – run golangci-lint across modules.
- `make test` – execute go test matrix for both freelist implementations (respects `CPU`, `TIMEOUT`, etc.).
- `make coverage` – produce separate coverage profiles for freelist modes.
- `make build` – compile CLI binary to `bin/bbolt`.
- `make gofail-enable` / `make gofail-disable` – toggle failpoints before running robustness suites.
- `make test-robustness` – run failpoint/robustness tests (needs sudo).
- `go run go.etcd.io/bbolt/cmd/witchbolt@latest` – run CLI without installing.
- `go install go.etcd.io/bbolt/cmd/witchbolt@latest` – install CLI binary.