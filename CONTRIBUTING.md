# Contributing to JaisCloud

Thank you for your interest in contributing — we appreciate improvements of all sizes.

Quick links
- Quick start and setup: [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)
- Code of Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

Getting started
1. Fork the repository and create a branch on your fork (e.g. `feat/my-change`).
2. Implement your change and add tests where appropriate.
3. Run the unit tests and linters locally:

```bash
# unit tests
go test ./internal/... -v -count=1

# run integration tests (requires local server running)
go test ./tests/integration/ -v -count=1 -timeout 120s

# format and lint
gofmt -w .
goimports -w .
golangci-lint run ./...
```

Submitting a PR
- Open a pull request from your fork to `main` with a clear description of the change.
- Include one or more tests or explain why tests are not applicable.
- Ensure CI passes (unit + integration tests run on PRs).
- Use small, focused PRs where possible and include the issue number if present.

Coding guidelines
- Follow existing project style and keep changes minimal and focused.
- Keep functions short and ensure error cases are handled.

Commit messages
- Use conventional, descriptive commit messages. Example: `provider: add visibility timeout handling`.

Maintainer reviews
- PRs will be reviewed by maintainers; please address review comments promptly.

Contribution license
By contributing you agree that your contributions will be licensed under the project's Apache-2.0 license.

---

## Developer reference

### Adding a new AWS service

1. Add one `ServiceDescriptor` entry to `internal/aws/adapter/services.go`.
   Detection, SigV4 allow-list, Action routing, and provider mapping all update automatically.
2. Create `internal/aws/adapter/services/<service>_codec.go` implementing `adapter.Codec`.
3. Create `internal/aws/provider/<service>/` with the business logic.

No switch statements anywhere — the single `awsServices` slice drives everything.

### Adding a new resource store (Snapshotter)

Every store that holds persistent state must implement `snapshottypes.Snapshotter` and be registered with the admin handler so it participates in export/import/snapshot:

```go
type Snapshotter interface {
    Snapshot(ctx context.Context, w io.Writer) error
    Restore(ctx context.Context, r io.Reader) error
    Reset()
    IsEmpty(ctx context.Context) (bool, error)
    Name() string
}
```

Checklist:
- [ ] Implement all five `Snapshotter` methods.
- [ ] Register via `adminHandler.RegisterSnapshotter(store)` in `cmd/jaiscloud-aws/main.go`.
- [ ] Register via `adminHandler.RegisterResetter(store)` for reset support.
- [ ] `Restore` is idempotent: called on an empty store after `Reset()`.
- [ ] `IsEmpty` returns `true` when the store holds zero items.
- [ ] Unit-test the round-trip: `Snapshot → Restore → compare`.

### Schema version bump rules

Both version constants live in `internal/persistence/version/version.go`.

#### Snapshot schema version (`CodeSnapshotVersion`)

Bump only when the `version.Envelope` struct or on-disk snapshot format changes in a backward-incompatible way. Increment by 1 and update `CheckSnapshotVersion` if a migration path is needed. Current value: `3`.

#### DB schema version (`CodeDBSchemaVersion`)

Bump every time a SQL migration file is added to `internal/store/migrations/`. The value must equal the total number of migration files.

Steps:
1. Create `internal/store/migrations/<NNN>_<description>.sql` (zero-padded 3-digit sequence).
2. Increment `CodeDBSchemaVersion` in `internal/persistence/version/version.go`.
3. Run `go build ./...` and `go test -race ./internal/store/...`.

Migration file conventions:
- Files are applied in lexicographic order; never leave gaps (use `SELECT 1;` placeholder files to fill gaps).
- Each migration must be idempotent (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`).
- Never modify or delete an existing migration file.

Current value: `15` (files `001` through `015`).

### Import cycle rules

`internal/*` (shared) must **never** import `internal/aws/`, `internal/azure/`, or `internal/gcp/`. When shared infrastructure needs an interface implemented by providers, define the interface in a small neutral package (e.g. `internal/snapshottypes/types.go`). Both shared and provider packages then import the neutral package.

### Snapshot/restore conventions

- `Export` returns a **gzip tarball** (`Content-Type: application/x-tar`).
  - First entry: `envelope.json` (`version.Envelope`).
  - Subsequent entries: `blobs/<prefix>/<key>` (S3 blob bodies).
- `Import` auto-detects gzip (magic `0x1f 0x8b`) vs. legacy JSON.
  - `?dry_run=true` validates without mutating state.
  - Refused when any store is non-empty.
  - On failure all stores roll back via `Reset()`.
- Named snapshots stored under `<data-dir>/snapshots/<name>/`:
  - `snapshot.tar.gz` — full tarball.
  - `metadata.json` — `SnapshotMetadata`.

### Periodic state persistence

When `--data-dir` is set, `persistence/snapshot.SnapshotLoop` saves `state.json` every `JAISCLOUD_SNAPSHOT_INTERVAL` (default 5m). On startup `state.json` is loaded automatically unless `--fresh-start` is passed. `SaveNow(ctx)` triggers an immediate save (e.g. on graceful shutdown).
