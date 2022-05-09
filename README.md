# simple-query-engine
Playground to learn about rust and query engines

Based on "How query engines work" by [Andy Grove](https://github.com/andygrove).

## Design

Pipeline:
```pre
Logical Plan -> 
  Query optimizer::optimise -> Logical Plan
    Query Planner::create_physical_plan -> Physical plan
      Physical Plan::execute -> Result
```

## Resources

* https://leanpub.com/how-query-engines-work
* https://github.com/andygrove/how-query-engines-work
* https://tokio.rs/
* https://docs.rs/async-stream/latest/async_stream/


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

cargo doc
```
