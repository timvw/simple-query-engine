#!/bin/bash
cargo fmt
cargo clippy --all-targets --workspace -- -D warnings