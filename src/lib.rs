use std::pin::Pin;
use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use futures::Stream;
use error::Result;
use futures::StreamExt;

pub mod error;
pub mod datasource;
pub mod logical;
pub mod physical;
pub mod planner;
pub mod optimiser;

pub type RecordBatch = Chunk<Arc<dyn Array>>;
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + Sync + 'static>>;

pub fn schema_projected(schema: Schema, projection: Vec<String>) -> Schema {
    // TODO: should validate that all columns are actually present...
    let retained: Vec<Field> = schema.fields.into_iter().filter(|f| projection.contains(&f.name)).collect();
    Schema::from(retained)
}

pub async fn pretty_print(mut rbs: RecordBatchStream, schema: Schema) {
    let names = schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>();
    let mut all_record_batches = Vec::new();
    for rb in rbs.next().await {
        if rb.is_ok() {
            all_record_batches.push(rb.unwrap());
        }
    }
    println!("results: ");
    println!("{}", arrow2::io::print::write(&all_record_batches[..], &names));
}
