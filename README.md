# simple-query-engine
Playground to learn about rust and query engines


## Development
First clone the test data repository:

```bash
git submodule update --init --recursive
```

When this does not work, manually run the following:

```bash
git submodule add -f https://github.com/apache/arrow-testing.git testing
git submodule add -f https://github.com/apache/parquet-testing.git parquet-testing
```

Use typical rust toolchain:

```bash
cargo build
cargo test

cargo fmt
cargo clippy --all-targets --workspace -- -D warnings
```
