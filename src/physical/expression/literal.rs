use crate::physical::expression::PhysicalExpressionCapabilities;
use crate::RecordBatch;
use arrow2::array::{Array, Utf8Array};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Utf8Literal {
    pub name: String,
    pub value: String,
}

impl PhysicalExpressionCapabilities for Utf8Literal {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        let mut v: Vec<Option<String>> = Vec::new();
        for _ in 0..input.len() {
            v.push(Some(self.value.clone()));
        }
        let array = Utf8Array::<i32>::from_iter(v);
        Arc::new(array)
    }
}
