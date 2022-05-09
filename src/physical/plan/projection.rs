use crate::physical::expression::{PhysicalExpression, PhysicalExpressionCapabilities};
use crate::physical::plan::{PhyiscalPlan, PhysicalPlanCapabilities};
use crate::RecordBatchStream;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::StreamExt;

#[derive(Debug, Clone)]
pub struct Projection {
    pub input: PhyiscalPlan,
    pub schema: Schema,
    pub expressions: Vec<PhysicalExpression>,
}

#[async_trait]
impl PhysicalPlanCapabilities for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    async fn execute(&self) -> RecordBatchStream {
        let mut rbs = self.input.execute().await;
        let expressions = self.expressions.clone();
        let output = stream! {
            for rrb in rbs.next().await {
                let rb = rrb.unwrap();
                let mut columns = Vec::new();
                for e in expressions.clone() {
                    let column = e.evaluate(rb.clone());
                    columns.push(column)
                }
                let result_chunk = Chunk::new(columns);
                yield Ok(result_chunk);
            }
        };
        Box::pin(output) as RecordBatchStream
    }
}
