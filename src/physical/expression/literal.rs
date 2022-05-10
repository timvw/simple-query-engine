use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::{Array, Utf8Array};
use std::sync::Arc;
use crate::datatypes::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct Literal {
    pub name: String,
    pub value: ScalarValue,
}

impl PhysicalExpressionCapabilities for Literal {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        let array = match self.value.clone() {
            ScalarValue::Utf8(ov) => Utf8Array::<i32>::from_iter(vec![ov; input.len()]),
        };
        Arc::new(array)
    }
}
