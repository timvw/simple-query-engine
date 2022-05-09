use crate::RecordBatchStream;
use arrow2::datatypes::Schema;

pub trait DataSource {
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

pub mod parquet;
