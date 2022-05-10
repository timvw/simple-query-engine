use crate::datatypes::scalar::ScalarValue;
use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::{Array, PrimitiveArray, Utf8Array};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Literal {
    pub name: String,
    pub value: ScalarValue,
}

impl PhysicalExpressionCapabilities for Literal {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        match self.value.clone() {
            ScalarValue::Int8(ov) => Arc::new(PrimitiveArray::<i8>::from(vec![ov; input.len()])),
            ScalarValue::Int16(ov) => Arc::new(PrimitiveArray::<i16>::from(vec![ov; input.len()])),
            ScalarValue::Int32(ov) => Arc::new(PrimitiveArray::<i32>::from(vec![ov; input.len()])),
            ScalarValue::Int64(ov) => Arc::new(PrimitiveArray::<i64>::from(vec![ov; input.len()])),
            ScalarValue::Utf8(ov) => Arc::new(Utf8Array::<i32>::from_iter(vec![ov; input.len()])),
        }
    }
}
