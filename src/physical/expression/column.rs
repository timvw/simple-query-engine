use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

pub struct Column {
    pub idx: usize,
}

impl PhysicalExpressionCapabilities for Column {
    fn evaluate(&self, _input: RecordBatch) -> &Arc<dyn Array> {
        todo!()
    }
}
