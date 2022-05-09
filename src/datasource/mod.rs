use std::fmt;
use std::fmt::Formatter;
use crate::datasource::parquet::Parquet;
use crate::{RecordBatchStream, Result};
use arrow2::datatypes::Schema;

pub trait DataSourceCapabilities {
    /// Returns the schema of the underlying data (or should it be the schema of the projection?)
    fn schema(&self) -> Schema;

    /// Returns a stream of RecordBatches
    ///
    /// # Arguments
    ///
    /// * `maybe_field_names` - When provided restricts the returned fields to the ones provided, otherwise all available fields
    ///
    /// # Examples
    /// let ds: DataSource = todo!();
    fn scan(&self, maybe_field_names: Option<Vec<String>>) -> RecordBatchStream;
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Parquet(Parquet),
}

impl DataSource {
    pub fn parquet(file_path: String) -> Result<DataSource> {
        let parquet = Parquet::new(file_path)?;
        Ok(DataSource::Parquet(parquet))
    }
}

impl DataSourceCapabilities for DataSource {
    fn schema(&self) -> Schema {
        match self {
            DataSource::Parquet(x) => x.schema(),
        }
    }

    fn scan(&self, maybe_field_names: Option<Vec<String>>) -> RecordBatchStream {
        match self {
            DataSource::Parquet(x) => x.scan(maybe_field_names),
        }
    }
}

impl fmt::Display for DataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataSource::Parquet(parquet) => write!(f, "Parquet file: {}", parquet.file_path),
        }
    }
}

pub mod parquet;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use crate::util::test::parquet_test_data;
    use regex::Regex;

    #[test]
    fn test_format_parquet() -> Result<()>{
        let test_file = format!("{}/alltypes_plain.parquet", parquet_test_data());
        let datasource = DataSource::parquet(test_file)?;

        let expected_re = Regex::new(r"Parquet file: (.*?)/alltypes_plain.parquet").unwrap();

        //println!("{}", format!("{}", datasource));
        assert!(expected_re.is_match(&format!("{}", datasource)));

        Ok(())
    }
}
