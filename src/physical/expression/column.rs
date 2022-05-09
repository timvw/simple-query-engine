use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

pub struct Column {
    pub index: usize,
}

impl PhysicalExpressionCapabilities for Column {
    fn evaluate(&self, _input: RecordBatch) -> &Arc<dyn Array> {
        todo!()
    }
}
