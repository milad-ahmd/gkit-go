# Contributing to gkit

Thank you for considering a contribution to **gkit**! Every improvement — bug fix, new package, or doc update — is valued.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Commit Messages](#commit-messages)
- [Pull Request Process](#pull-request-process)
- [Package Design Guidelines](#package-design-guidelines)

---

## Code of Conduct

Be kind and respectful. We follow the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

---

## Getting Started

1. **Fork** the repo and clone your fork:
   ```bash
   git clone https://github.com/<your-handle>/gkit-go.git
   cd gkit-go
   ```
2. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/milad-ahmd/gkit-go.git
   ```
3. Create a feature branch:
   ```bash
   git checkout -b feat/my-feature
   ```

---

## Development Setup

**Prerequisites:**

| Tool | Version |
|---|---|
| Go | ≥ 1.22 |
| Docker | ≥ 24 (for integration tests) |
| golangci-lint | v1.57+ |

```bash
go mod download
```

---

## Running Tests

```bash
# Unit tests (no external services needed)
go test ./...

# Unit tests with race detector
go test -race ./...

# Integration tests (requires Docker)
go test -tags integration -race ./...

# Specific package
go test -v ./pkg/retry/...

# Benchmarks
go test -bench=. -benchmem ./pkg/cache/...

# Lint
golangci-lint run
```

---

## Commit Messages

We use **Conventional Commits**:

```
feat(pkg): add new capability
fix(retry): correct backoff overflow
test(cache): add TTL edge-case test
docs(readme): update usage example
chore(ci): bump golangci-lint version
```

Rules:
- Use the package name as the scope: `feat(ratelimit):`, `fix(pool):`
- Imperative mood: "add", "fix", "remove" — not "added", "fixes"
- Keep the subject line ≤ 72 characters

---

## Pull Request Process

1. Ensure `go test -race ./...` passes locally
2. Ensure `golangci-lint run` passes (explain any suppressions)
3. Add or update tests for any changed behaviour
4. Update the relevant package doc comment if the API changes
5. Fill out the PR template fully
6. Request review from `@milad-ahmd`

PRs that add a new package should include:
- The package itself with a top-level doc comment
- Unit tests with ≥ 80% coverage
- An entry in `README.md`

---

## Package Design Guidelines

- **Zero magic** — no global state, no `init()` side-effects
- **Context-first** — every blocking call takes `context.Context` as its first argument
- **Options pattern** — use functional options (`type Option func(*options)`) for configuration
- **Error wrapping** — use `fmt.Errorf("pkg: %w", err)` consistently
- **No heavy frameworks** — stdlib + the existing `go.mod` deps are the budget
- **Generics** — use type parameters when they eliminate meaningful boilerplate
