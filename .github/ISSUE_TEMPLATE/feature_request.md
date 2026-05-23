---
name: Feature Request
about: Propose a new package or enhancement to an existing one
title: "feat(<package>): <short description>"
labels: ["enhancement"]
assignees: milad-ahmd
---

## Summary

<!-- One-paragraph description of what you want -->

## Motivation

<!-- Why is this needed? What problem does it solve? -->

## Proposed API

```go
// Sketch the public API surface you have in mind

package mypackage

// Config holds ...
type Config struct { ... }

// New creates ...
func New(cfg Config) (*Client, error) { ... }
```

## Alternatives Considered

<!-- Have you looked at other libraries or approaches? -->

## Acceptance Criteria

- [ ] Unit tests with ≥ 80% coverage
- [ ] Integration test if external service is involved
- [ ] Entry in README.md
- [ ] CHANGELOG.md updated
