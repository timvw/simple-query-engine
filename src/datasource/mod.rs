use crate::RecordBatchStream;
use arrow2::datatypes::Schema;
use crate::datasource::parquet::Parquet;

pub trait DataSourceCapabilities {
    /// Returns the schema of the underlying data (or should it be the schema of the projection?)
    fn schema(&self) -> Schema;

    /// Returns a stream of RecordBatches
    ///
    /// # Arguments
    ///
    /// * `maybe_projection` - When provided restricts the returned fields to the ones provided, otherwise all available fields
    ///
    /// # Examples
    /// let ds: DataSource = todo!();
    fn scan(&self, maybe_projection: Option<Vec<String>>) -> RecordBatchStream;
}

pub enum DataSource {
    Parquet(Parquet),
}

impl DataSourceCapabilities for DataSource {

    fn schema(&self) -> Schema {
        match self {
            DataSource::Parquet(x) => x.schema(),
        }
    }

    fn scan(&self, maybe_projection: Option<Vec<String>>) -> RecordBatchStream {
        match self {
            DataSource::Parquet(x) => x.scan(maybe_projection),
        }
    }
}

pub mod parquet;
