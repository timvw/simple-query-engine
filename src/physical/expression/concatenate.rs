use crate::physical::expression::{PhysicalExpression, PhysicalExpressionCapabilities};
use crate::RecordBatch;
use arrow2::array::Array;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Concatenate {
    pub expressions: Vec<PhysicalExpression>,
}

impl PhysicalExpressionCapabilities for Concatenate {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        let mut columns = Vec::new();
        for e in self.expressions.clone() {
            let column = e.evaluate(input.clone());
            columns.push(column);
        }
        let arrays = columns.iter().map(|c| c.deref()).collect::<Vec<_>>();
        let result_array = arrow2::compute::concatenate::concatenate(&arrays).unwrap();
        Arc::from(result_array)
    }
}
