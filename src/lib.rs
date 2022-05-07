use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;

pub mod error;
pub mod datasource;
pub mod logical;
pub mod physical;

pub type RecordBatch = Chunk<Arc<dyn Array>>;