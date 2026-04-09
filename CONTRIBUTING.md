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
