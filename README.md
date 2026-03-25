# wee-events

`wee-events` is a small Rust event-sourcing toolkit extracted from a larger
application so it can live as a reusable, open-source workspace.

It focuses on a compact core API, derive macros for ergonomic domain modeling,
and a libSQL-backed SQLite store for local and production-friendly persistence.

## Getting Started

**Prerequisites:** [mise](https://mise.jdx.dev/) is recommended for tool setup.

1. Install the project tools with `mise install`.
2. Use `just` to run the common workspace tasks.

```sh
mise exec -- just           # Show available commands
mise exec -- just quality   # fmt-check, cargo check, clippy, and tests
mise exec -- just fmt       # Format the workspace
```

If you prefer to run Cargo directly, use the same commands through `mise exec --`.

## Key Features

- **Small event-sourcing core**: aggregate, event, command, renderer, and store primitives.
- **Typed identifiers**: dedicated types for aggregate IDs, event IDs, revisions, and names.
- **Derive macros**: `Command` and `DomainEvent` derives generate consistent names from enums.
- **In-memory store**: useful for tests and lightweight workflows.
- **SQLite store**: append-only event persistence with separate event/document stores and projection helpers.
- **Workspace test support**: shared store conformance tests for event store implementations.

## Workspace Layout

This repository contains:

- `crates/wee-events`: core event-sourcing types, traits, renderer support, and an in-memory store
- `crates/wee-events-macros`: derive macros for `Command` and `DomainEvent`
- `crates/wee-events-sqlite`: a libSQL-backed SQLite event store plus document and projection helpers

```text
crates/
  wee-events/
  wee-events-macros/
  wee-events-sqlite/
```

## Development

The workspace includes a root `justfile` for the standard quality checks:

```sh
mise exec -- just quality
```

That recipe runs:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## Status

The workspace builds and passes its quality checks as a standalone Rust project.
Before making the repository public, fill in package metadata such as
`repository`, `homepage`, and `documentation` with the final public URLs.
