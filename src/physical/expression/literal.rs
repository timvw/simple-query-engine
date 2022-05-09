use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Literal {
    pub name: String,
    pub value: String,
}

impl PhysicalExpressionCapabilities for Literal {
    fn evaluate(&self, _input: RecordBatch) -> &Arc<dyn Array> {
        todo!()
    }
}
