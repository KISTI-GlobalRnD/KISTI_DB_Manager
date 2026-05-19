# ADR 0001: MkDocs is the documentation source of truth

## Status

Accepted.

## Context

The repository needs both public technical documentation and practical operator runbooks.
It is tempting to split English technical docs into MkDocs and Korean operator docs into GitHub Wiki.

That split would create two maintenance surfaces:

- MkDocs pages that are versioned with code and checked by `mkdocs build --strict`
- Wiki pages that are easier to edit but not checked with the repository

For this project, command sequences, OpenAlex defaults, artifact contracts, and benchmark-derived recommendations change with code.
Documentation drift is therefore more expensive than the convenience of a separate Wiki.

## Decision

MkDocs under `docs/` is the source of truth for maintained documentation.

README stays a short portal.
GitHub Wiki, if used, is limited to scratch notes or temporary collaboration.
Korean operator runbooks live under `docs/ko/` so they are versioned and validated with the rest of the docs.

## Consequences

- Documentation changes should pass `mkdocs build --strict`.
- Durable operator procedures belong in `docs/operator-guides/` or `docs/ko/`.
- Maintainer-facing rationale belongs in `docs/architecture/`, `docs/design/`, `docs/performance/`, or `docs/decisions/`.
- Generated run artifacts stay outside `docs/` unless explicitly selected as public examples.
- README should not accumulate CLI details, benchmark notes, or internal module maps.
