use std::env;
use std::error::Error;
use std::path::PathBuf;

/// Returns the arrow test data directory, which is by default stored
/// in a git submodule rooted at `testing/data`.
///
/// The default can be overridden by the optional environment
/// variable `ARROW_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion::test_util::arrow_test_data();
/// let csvdata = format!("{}/csv/aggregate_test_100.csv", testdata);
/// assert!(std::path::PathBuf::from(csvdata).exists());
/// ```
pub fn arrow_test_data() -> String {
    match get_data_dir("ARROW_TEST_DATA", "../../testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {}", err),
    }
}

/// Returns the parquet test data directory, which is by default
/// stored in a git submodule rooted at
/// `parquest-testing/data`.
///
/// The default can be overridden by the optional environment variable
/// `PARQUET_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion::test_util::parquet_test_data();
/// let filename = format!("{}/binary.parquet", testdata);
/// assert!(std::path::PathBuf::from(filename).exists());
/// ```
pub fn parquet_test_data() -> String {
    match get_data_dir("PARQUET_TEST_DATA", "../../parquet-testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get parquet data dir: {}", err),
    }
}

/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
fn get_data_dir(udf_env: &str, submodule_data: &str) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                    .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
            udf_env,
            pb.display(),
        ).into())
    }
}