# Task completion checklist
- Ensure Go code is formatted (`make fmt` or gofmt/goimports/gci).
- Run relevant `go test` targets; default `make test` covers both freelist configurations.
- If CLI functionality affected, rebuild (`make build`) or run targeted command to verify.
- Update documentation or changelog entries if behavior changes.