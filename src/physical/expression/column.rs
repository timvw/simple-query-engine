use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct Column {
    pub index: usize,
}

impl PhysicalExpressionCapabilities for Column {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        let columns = input.columns();
        let column = columns.get(self.index).unwrap();
        column.clone()
    }
}
