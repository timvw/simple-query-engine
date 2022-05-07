use std::pin::Pin;
use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use futures::Stream;
use error::Result;

pub mod error;
pub mod datasource;
pub mod logical;
pub mod physical;
pub mod planner;
pub mod optimiser;

pub type RecordBatch = Chunk<Arc<dyn Array>>;
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + Sync + 'static>>;
