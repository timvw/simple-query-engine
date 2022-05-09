use crate::physical::expression::column::Column;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

trait PhysicalExpressionCapabilities {
    fn evaluate(&self, input: RecordBatch) -> &Arc<dyn Array>;
}

pub enum PhysicalExpression {
    Column(Column),
}

impl PhysicalExpressionCapabilities for PhysicalExpression {
    fn evaluate(&self, input: RecordBatch) -> &Arc<dyn Array> {
        match self {
            PhysicalExpression::Column(column) => column.evaluate(input),
        }
    }
}

pub mod column;
