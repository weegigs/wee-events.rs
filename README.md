# wee-events

`wee-events` is a Rust event-sourcing toolkit with a compact core API, derive
macros for domain modeling, and a libSQL-backed SQLite store for local and
production-friendly persistence.

It is part of the `wee-events` family alongside the original
[`wee-events`](https://github.com/weegigs/wee-events) project and
[`wee-events-go`](https://github.com/weegigs/wee-events-go).

## Features

- **Compact event-sourcing core**: aggregate, event, command, renderer, and store primitives.
- **Typed identifiers**: dedicated types for aggregate IDs, event IDs, revisions, and names.
- **Derive macros**: `Command` and `DomainEvent` derives generate consistent names from enums.
- **In-memory store**: useful for tests and lightweight workflows.
- **SQLite store**: append-only event persistence with document storage and projection helpers.
- **Store conformance tests**: shared test support for event store implementations.

## Crates

- `crates/wee-events`: core types, traits, renderer support, and an in-memory store
- `crates/wee-events-macros`: derive macros for `Command` and `DomainEvent`
- `crates/wee-events-sqlite`: a libSQL-backed SQLite event store plus document and projection helpers

```text
crates/
  wee-events/
  wee-events-macros/
  wee-events-sqlite/
```

## Development

[mise](https://mise.jdx.dev/) is recommended for tool setup.

```sh
mise install
mise exec -- just         # Show available commands
mise exec -- just check   # fmt-check, cargo check, clippy, and tests
mise exec -- just fmt     # Format the workspace
```

If you prefer to run Cargo directly, use the same commands through `mise exec --`.

The `check` recipe runs:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
