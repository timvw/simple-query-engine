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

pub mod parquet;
