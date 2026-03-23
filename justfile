default:
    @just --list

check: fmt fmt-check cargo-check clippy test

fmt:
    mise exec -- cargo fmt --all

fmt-check:
    mise exec -- cargo fmt --all -- --check

cargo-check:
    mise exec -- cargo check --workspace --all-targets --all-features

clippy:
    mise exec -- cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    mise exec -- cargo test --workspace --all-features
